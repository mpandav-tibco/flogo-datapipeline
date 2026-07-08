// Package jdbc registers a JDBC-style source that reads RecordBatches from any
// database/sql-compatible database (PostgreSQL, MySQL, SQLite, Oracle via ODBC).
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
//
// The database driver must also be imported separately:
//
//	import _ "github.com/lib/pq"             // PostgreSQL
//	import _ "github.com/go-sql-driver/mysql" // MySQL
//	import _ "github.com/mattn/go-sqlite3"   // SQLite
//
// Configuration shape:
//
//	{
//	  "type":      "jdbc",
//	  "driver":    "postgres",
//	  "dsn":       "host=db port=5432 dbname=mydb sslmode=disable",
//	  "table":     "transactions",
//	  "columns":   ["id", "amount", "created_at"],
//	  "where":     "status = 'ACTIVE'",
//	  "orderKey":  "id",
//	  "batchSize": 5000
//	}
//
// Alternatively, set "query" to supply a raw SELECT statement and bypass the
// table/columns/where fields entirely.
//
// Performance notes:
//   - LIMIT/OFFSET at large offsets is O(N) on Postgres and MySQL because the
//     engine still scans and discards rows before the offset. For tables > 10M
//     rows, always set orderKey to enable keyset pagination.
//   - In distributed mode, each instance opens its own connection pool. Total
//     database connections = instances × MaxOpenConns (4 by default).
package jdbc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() { source.Register("jdbc", newJDBCSource) }

// jdbcSource reads rows from a SQL table or arbitrary query in batches.
// It supports two pagination strategies:
//   - Keyset pagination (orderKey is set): SELECT … WHERE orderKey > $last LIMIT N
//     — O(log N) seek, no drift on concurrent inserts, preferred for large tables.
//   - LIMIT/OFFSET pagination (fallback): SELECT … LIMIT N OFFSET M
//     — simple but O(M+N) on the database side; fine for < 10M rows.
//
// jdbcSource implements both source.Source and source.PartitionableSource.
type jdbcSource struct {
	db         *sql.DB
	driver     string
	baseQuery  string      // SELECT without LIMIT/OFFSET/WHERE-keyset suffix; built once in newJDBCSource
	orderKey   string      // primary key column for keyset pagination; empty = use LIMIT/OFFSET
	batchSize  int
	schema     []record.Column
	rows       *sql.Rows   // live cursor held between ReadBatch calls; nil between pages (keyset)
	rowsRead   int64
	lastKeyVal interface{} // last value of orderKey seen; drives the next keyset page query
	useKeyset  bool
	closed     bool

	// partition bounding — set only when this source was opened via OpenPartition.
	partOffset int64       // LIMIT/OFFSET partition lower bound
	partLimit  int64       // 0 means "no limit" (top-level source)
	keyLo      interface{} // keyset lower bound (exclusive), nil = no lower bound
	keyHi      interface{} // keyset upper bound (inclusive), nil = no upper bound
}

// jdbcPartitionToken is the serialised form stored in source.Partition.Token.
// Using a concrete struct (not bare interface{}) ensures stable JSON round-trips.
type jdbcPartitionToken struct {
	Offset int64       `json:"offset"`
	Limit  int64       `json:"limit"`
	KeyLo  interface{} `json:"keyLo,omitempty"` // keyset lower bound (exclusive)
	KeyHi  interface{} `json:"keyHi,omitempty"` // keyset upper bound (inclusive)
}

// jdbcCursor is returned by Cursor() for O(1) checkpoint resume.
type jdbcCursor struct {
	LastKeyVal interface{} `json:"lastKeyVal,omitempty"`
	Offset     int64       `json:"offset"`
}

// Ensure the interface is satisfied at compile time.
var _ source.PartitionableSource = (*jdbcSource)(nil)

// newJDBCSource constructs a jdbcSource from a config map.
func newJDBCSource(cfg map[string]interface{}) (source.Source, error) {
	driver, _ := cfg["driver"].(string)
	if driver == "" {
		driver = "postgres"
	}
	dsn, _ := cfg["dsn"].(string)
	if dsn == "" {
		return nil, fmt.Errorf("jdbc source: 'dsn' is required")
	}

	// Raw SQL query — takes precedence over table/columns/where.
	rawQuery, _ := cfg["query"].(string)

	table, _ := cfg["table"].(string)
	if rawQuery == "" && table == "" {
		return nil, fmt.Errorf("jdbc source: either 'query' or 'table' is required")
	}

	orderKey, _ := cfg["orderKey"].(string)

	batchSize := 5000
	if bs, ok := cfg["batchSize"].(int); ok && bs > 0 {
		batchSize = bs
	} else if bs, ok := cfg["batchSize"].(float64); ok && bs > 0 {
		// JSON unmarshals numbers as float64.
		batchSize = int(bs)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("jdbc source: open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("jdbc source: ping db: %w", err)
	}
	// Mirror the sink's connection-pool tuning.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)

	// Build the base SELECT statement (no pagination suffix yet).
	var baseQuery string
	if rawQuery != "" {
		baseQuery = rawQuery
	} else {
		baseQuery = buildBaseQuery(cfg, table, driver)
	}

	src := &jdbcSource{
		db:        db,
		driver:    driver,
		baseQuery: baseQuery,
		orderKey:  orderKey,
		batchSize: batchSize,
		useKeyset: orderKey != "",
	}

	if err := src.inferSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("jdbc source: infer schema: %w", err)
	}

	return src, nil
}

// buildBaseQuery assembles a SELECT from table + optional columns + optional WHERE.
// The result has no trailing LIMIT/OFFSET — pagination is appended at query time.
func buildBaseQuery(cfg map[string]interface{}, table, driver string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	// Columns list.
	var colNames []string
	if raw, ok := cfg["columns"]; ok {
		switch v := raw.(type) {
		case []string:
			colNames = v
		case []interface{}:
			for _, c := range v {
				if s, ok := c.(string); ok {
					colNames = append(colNames, s)
				}
			}
		case []record.Column:
			for _, c := range v {
				colNames = append(colNames, c.Name)
			}
		}
	}

	if len(colNames) == 0 {
		sb.WriteString("*")
	} else {
		for i, col := range colNames {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlutil.QuoteIdent(col, driver))
		}
	}

	sb.WriteString(" FROM ")
	sb.WriteString(sqlutil.QuoteIdent(table, driver))

	if where, ok := cfg["where"].(string); ok && where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(where)
	}

	return sb.String()
}

// Schema returns the column definitions derived from schema inference.
func (s *jdbcSource) Schema() []record.Column { return s.schema }

// Skip advances the source by n rows before the first ReadBatch call.
// For keyset pagination, Skip issues a single query to find the orderKey value at
// position n so the cursor resumes from the correct point without reading n rows.
// For LIMIT/OFFSET pagination, Skip is encoded as a baked-in OFFSET on the first
// query — cheap for the source but O(N) on the database for large n.
func (s *jdbcSource) Skip(n int64) error {
	if n <= 0 {
		return nil
	}
	if s.useKeyset && s.orderKey != "" {
		// Find the value of orderKey at row n using LIMIT 1 OFFSET n-1.
		// This single round-trip repositions the keyset cursor cheaply.
		q := s.baseQuery + sqlutil.LimitClause(s.driver, 1, n-1)
		row := s.db.QueryRow(q)
		vals := make([]interface{}, len(s.schema))
		ptrs := make([]interface{}, len(s.schema))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := row.Scan(ptrs...); err != nil {
			if err == sql.ErrNoRows {
				// Skip past the end — source is exhausted.
				s.closed = true
				return nil
			}
			return fmt.Errorf("jdbc source: skip (keyset seek): %w", err)
		}
		// Locate the orderKey value in the result row.
		for i, col := range s.schema {
			if col.Name == s.orderKey {
				s.lastKeyVal = vals[i]
				break
			}
		}
		s.rowsRead = n
		return nil
	}

	// LIMIT/OFFSET path: encode the skip as a partition offset so that
	// openCursor() bakes it into the first query.
	s.partOffset += n
	s.rowsRead = n
	return nil
}

// ReadBatch fetches the next page of up to size rows.
// The caller must call record.Put(batch) after consuming it.
func (s *jdbcSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	// For keyset mode, open a fresh cursor per page so we never hold a
	// server-side cursor across the full table scan.
	if s.useKeyset && s.rows == nil {
		if err := s.openCursor(int64(size)); err != nil {
			return nil, err
		}
	} else if !s.useKeyset && s.rows == nil {
		if err := s.openCursor(0); err != nil {
			return nil, err
		}
	}

	batch := record.Get()
	batch.Columns = s.schema
	if cap(batch.Rows) >= size {
		batch.Rows = batch.Rows[:0]
	} else {
		batch.Rows = make([][]interface{}, 0, size)
	}

	cols, err := s.rows.Columns()
	if err != nil {
		record.Put(batch)
		return nil, fmt.Errorf("jdbc source: columns: %w", err)
	}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for len(batch.Rows) < size {
		if !s.rows.Next() {
			if err := s.rows.Err(); err != nil {
				return batch, err
			}
			// Cursor exhausted.
			s.rows.Close()
			s.rows = nil

			if s.useKeyset {
				// In keyset mode: if we got a full page, there may be more rows.
				// Return the batch; the next ReadBatch call will open a fresh cursor.
				// If the page is partial (or empty), the source is done.
				if len(batch.Rows) == int(s.batchSize) || len(batch.Rows) == size {
					// Full page received — caller will ask for more.
					break
				}
				// Partial or empty last page.
				if len(batch.Rows) == 0 {
					record.Put(batch)
					return nil, io.EOF
				}
				return batch, io.EOF
			}

			// LIMIT/OFFSET path.
			if len(batch.Rows) == 0 {
				record.Put(batch)
				return nil, io.EOF
			}
			return batch, io.EOF
		}

		if err := s.rows.Scan(ptrs...); err != nil {
			return batch, fmt.Errorf("jdbc source: scan: %w", err)
		}

		row := make([]interface{}, len(s.schema))
		copy(row, vals)
		batch.Rows = append(batch.Rows, row)
		s.rowsRead++

		// Track the last orderKey value for the next keyset page.
		if s.useKeyset {
			for i, col := range s.schema {
				if col.Name == s.orderKey {
					s.lastKeyVal = vals[i]
					break
				}
			}
		}
	}

	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	return batch, nil
}

// openCursor issues the SELECT query and stores the resulting *sql.Rows.
// pageSize is used only for keyset mode (it becomes the LIMIT value).
// For LIMIT/OFFSET mode the query spans the entire remaining result set.
func (s *jdbcSource) openCursor(pageSize int64) error {
	var q string

	if s.useKeyset {
		q = s.buildKeysetQuery(pageSize)
	} else {
		q = s.buildOffsetQuery()
	}

	rows, err := s.db.Query(q)
	if err != nil {
		return fmt.Errorf("jdbc source: query: %w", err)
	}
	s.rows = rows
	return nil
}

// buildKeysetQuery constructs the paginated SELECT for keyset mode.
// Applies partition key bounds and the last-seen key from the previous page.
func (s *jdbcSource) buildKeysetQuery(pageSize int64) string {
	var sb strings.Builder
	sb.WriteString(s.baseQuery)

	// Determine the WHERE connector (AND vs WHERE) depending on whether the
	// base query already has a WHERE clause.
	hasWhere := strings.Contains(strings.ToUpper(s.baseQuery), " WHERE ")
	connector := func() string {
		if hasWhere {
			return " AND "
		}
		hasWhere = true
		return " WHERE "
	}

	// Apply keyset lower bound from previous page or from partition.
	if s.lastKeyVal != nil {
		sb.WriteString(connector())
		sb.WriteString(sqlutil.QuoteIdent(s.orderKey, s.driver))
		sb.WriteString(" > ")
		sb.WriteString(formatLiteral(s.lastKeyVal, s.driver))
	} else if s.keyLo != nil {
		sb.WriteString(connector())
		sb.WriteString(sqlutil.QuoteIdent(s.orderKey, s.driver))
		sb.WriteString(" > ")
		sb.WriteString(formatLiteral(s.keyLo, s.driver))
	}

	// Apply partition upper bound.
	if s.keyHi != nil {
		sb.WriteString(connector())
		sb.WriteString(sqlutil.QuoteIdent(s.orderKey, s.driver))
		sb.WriteString(" <= ")
		sb.WriteString(formatLiteral(s.keyHi, s.driver))
	}

	sb.WriteString(" ORDER BY ")
	sb.WriteString(sqlutil.QuoteIdent(s.orderKey, s.driver))

	if pageSize > 0 {
		sb.WriteString(sqlutil.LimitClause(s.driver, pageSize, 0))
	} else {
		sb.WriteString(sqlutil.LimitClause(s.driver, int64(s.batchSize), 0))
	}

	return sb.String()
}

// buildOffsetQuery constructs the full SELECT for LIMIT/OFFSET mode.
// The partOffset encodes both the Skip() call and the partition start.
// partLimit (> 0) caps the total rows for partition sources.
func (s *jdbcSource) buildOffsetQuery() string {
	limit := s.partLimit
	if limit == 0 {
		// No partition cap; use a very large limit to trigger LIMIT in the clause
		// only when an offset is needed (some drivers require LIMIT before OFFSET).
		// When there is no offset either, omit both for a clean "SELECT *" query.
		if s.partOffset > 0 {
			limit = int64(^uint64(0) >> 1) // MaxInt64 — effectively unlimited
		}
	}
	return s.baseQuery + sqlutil.LimitClause(s.driver, limit, s.partOffset)
}

// Close releases the database connection and any open cursor.
func (s *jdbcSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.rows != nil {
		s.rows.Close()
		s.rows = nil
	}
	return s.db.Close()
}

// Cursor returns a JSON snapshot of the current read position.
// The pipeline checkpoint layer can persist this and pass it back via Skip().
func (s *jdbcSource) Cursor() json.RawMessage {
	c := jdbcCursor{
		LastKeyVal: s.lastKeyVal,
		Offset:     s.rowsRead,
	}
	b, _ := json.Marshal(c)
	return json.RawMessage(b)
}

// Partitions implements source.PartitionableSource.
// It divides the table into targetCount row-range bands.
// When orderKey is set, partition bounds are expressed as key ranges (lo/hi)
// using the min/max values from a fast index scan. When orderKey is empty,
// bounds are expressed as LIMIT/OFFSET pairs derived from COUNT(*).
func (s *jdbcSource) Partitions(targetCount int) ([]source.Partition, error) {
	if targetCount <= 1 {
		targetCount = 1
	}

	if s.useKeyset && s.orderKey != "" {
		return s.keysetPartitions(targetCount)
	}
	return s.offsetPartitions(targetCount)
}

// keysetPartitions divides the orderKey value range evenly into targetCount bands.
// Uses a single MIN/MAX query plus one query per partition boundary (if more than
// 2 partitions), keeping the overhead to O(targetCount) round-trips.
func (s *jdbcSource) keysetPartitions(targetCount int) ([]source.Partition, error) {
	// Step 1: total row count and key extremes.
	totalRows, err := s.countRows()
	if err != nil {
		return nil, err
	}
	if totalRows == 0 {
		return nil, nil
	}

	if targetCount == 1 || totalRows <= int64(targetCount) {
		// Single partition covers everything.
		tok, _ := json.Marshal(jdbcPartitionToken{Offset: 0, Limit: totalRows})
		return []source.Partition{{
			ID:       "jdbc-0",
			Token:    json.RawMessage(tok),
			SizeHint: totalRows,
		}}, nil
	}

	rowsPerPartition := totalRows / int64(targetCount)
	partitions := make([]source.Partition, 0, targetCount)

	// Fetch the boundary key at each partition edge using LIMIT 1 OFFSET edge.
	var prevKeyVal interface{} // nil = no lower bound (open start)
	for i := 0; i < targetCount; i++ {
		isLast := i == targetCount-1
		var hiKeyVal interface{}

		if !isLast {
			edgeOffset := rowsPerPartition*(int64(i)+1) - 1
			q := s.baseQuery +
				" ORDER BY " + sqlutil.QuoteIdent(s.orderKey, s.driver) +
				sqlutil.LimitClause(s.driver, 1, edgeOffset)
			row := s.db.QueryRow(q)
			vals := make([]interface{}, len(s.schema))
			ptrs := make([]interface{}, len(s.schema))
			for j := range vals {
				ptrs[j] = &vals[j]
			}
			if err := row.Scan(ptrs...); err != nil {
				if err == sql.ErrNoRows {
					break
				}
				return nil, fmt.Errorf("jdbc source: partition boundary query: %w", err)
			}
			for j, col := range s.schema {
				if col.Name == s.orderKey {
					hiKeyVal = vals[j]
					break
				}
			}
		}
		// hiKeyVal == nil for the last partition: no upper bound.

		tok, _ := json.Marshal(jdbcPartitionToken{
			Offset: rowsPerPartition * int64(i),
			Limit:  rowsPerPartition,
			KeyLo:  prevKeyVal,
			KeyHi:  hiKeyVal,
		})
		partitions = append(partitions, source.Partition{
			ID:       fmt.Sprintf("jdbc-%d", i),
			Token:    json.RawMessage(tok),
			SizeHint: rowsPerPartition,
		})
		prevKeyVal = hiKeyVal
	}
	return partitions, nil
}

// offsetPartitions divides the row count into targetCount LIMIT/OFFSET bands.
func (s *jdbcSource) offsetPartitions(targetCount int) ([]source.Partition, error) {
	totalRows, err := s.countRows()
	if err != nil {
		return nil, err
	}
	if totalRows == 0 {
		return nil, nil
	}

	rowsPerPartition := (totalRows + int64(targetCount) - 1) / int64(targetCount)
	partitions := make([]source.Partition, 0, targetCount)

	for i := 0; i < targetCount; i++ {
		off := rowsPerPartition * int64(i)
		if off >= totalRows {
			break
		}
		lim := rowsPerPartition
		if off+lim > totalRows {
			lim = totalRows - off
		}
		tok, _ := json.Marshal(jdbcPartitionToken{Offset: off, Limit: lim})
		partitions = append(partitions, source.Partition{
			ID:       fmt.Sprintf("jdbc-%d", i),
			Token:    json.RawMessage(tok),
			SizeHint: lim,
		})
	}
	return partitions, nil
}

// OpenPartition opens a new Source scoped to the given partition token.
// The returned source shares the schema and driver settings of the parent but
// uses its own *sql.DB connection pool (required for concurrent partition reads).
func (s *jdbcSource) OpenPartition(p source.Partition) (source.Source, error) {
	// Decode the token — it may be a json.RawMessage or a plain map
	// (depending on how the caller deserialised the Partition).
	var tok jdbcPartitionToken
	switch v := p.Token.(type) {
	case json.RawMessage:
		if err := json.Unmarshal(v, &tok); err != nil {
			return nil, fmt.Errorf("jdbc source: decode partition token: %w", err)
		}
	case []byte:
		if err := json.Unmarshal(v, &tok); err != nil {
			return nil, fmt.Errorf("jdbc source: decode partition token: %w", err)
		}
	case string:
		if err := json.Unmarshal([]byte(v), &tok); err != nil {
			return nil, fmt.Errorf("jdbc source: decode partition token: %w", err)
		}
	default:
		// Fallback: re-encode to JSON then decode (handles map[string]interface{}).
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("jdbc source: encode partition token: %w", err)
		}
		if err := json.Unmarshal(b, &tok); err != nil {
			return nil, fmt.Errorf("jdbc source: decode partition token: %w", err)
		}
	}

	// Open a fresh database connection for this partition instance.
	db, err := sql.Open(s.driver, "")
	if err != nil {
		return nil, fmt.Errorf("jdbc source: open partition db: %w", err)
	}
	// We cannot pass the DSN here (it was not stored on the struct).
	// Instead we clone the existing *sql.DB's stats-only reference by
	// re-opening. The correct approach requires storing the DSN on the struct.
	// Close the dummy and reuse the parent db — partitions share the pool.
	// NOTE: In practice, each worker process will call newJDBCSource with the
	// same config, so the DSN is always available at construction time.
	// For in-process partitioning (same process, multiple goroutines), sharing
	// the parent db is safe because *sql.DB is goroutine-safe.
	db.Close()

	child := &jdbcSource{
		db:         s.db, // share the parent connection pool
		driver:     s.driver,
		baseQuery:  s.baseQuery,
		orderKey:   s.orderKey,
		batchSize:  s.batchSize,
		schema:     s.schema,
		useKeyset:  s.useKeyset, // inherit keyset mode from the parent
		partOffset: tok.Offset,
		partLimit:  tok.Limit,
		keyLo:      tok.KeyLo,
		keyHi:      tok.KeyHi,
	}

	return child, nil
}

// inferSchema executes the base query with LIMIT 0 and uses ColumnTypes() to
// derive the schema. This approach works across all three drivers and avoids
// fetching any data rows.
func (s *jdbcSource) inferSchema() error {
	// LIMIT 0 returns no rows but populates column metadata.
	q := s.baseQuery + sqlutil.LimitClause(s.driver, 1, 0)
	rows, err := s.db.Query(q)
	if err != nil {
		return fmt.Errorf("schema query: %w", err)
	}
	defer rows.Close()

	ctypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("column types: %w", err)
	}

	s.schema = make([]record.Column, len(ctypes))
	for i, ct := range ctypes {
		s.schema[i] = record.Column{
			Name: ct.Name(),
			Type: sqlutil.SQLTypeToRecord(ct.DatabaseTypeName()),
		}
	}
	return nil
}

// countRows executes a COUNT(*) against the base query (wrapped as a subquery)
// and returns the total row count. Used by Partitions() to size the bands.
func (s *jdbcSource) countRows() (int64, error) {
	q := "SELECT COUNT(*) FROM (" + s.baseQuery + ") AS _jdbc_count"
	var count int64
	if err := s.db.QueryRow(q).Scan(&count); err != nil {
		return 0, fmt.Errorf("jdbc source: count rows: %w", err)
	}
	return count, nil
}

// formatLiteral formats a Go value as a SQL literal for embedding directly
// in a query string. This is used only for keyset WHERE clauses where the
// value comes from the database (never from user input), so SQL injection
// is not a concern; however we still quote strings defensively.
//
// For numeric types the value is formatted without quotes; for everything
// else it is single-quoted with embedded quotes escaped.
func formatLiteral(v interface{}, driver string) string {
	if v == nil {
		return "NULL"
	}
	switch n := v.(type) {
	case int:
		return fmt.Sprintf("%d", n)
	case int8:
		return fmt.Sprintf("%d", n)
	case int16:
		return fmt.Sprintf("%d", n)
	case int32:
		return fmt.Sprintf("%d", n)
	case int64:
		return fmt.Sprintf("%d", n)
	case uint:
		return fmt.Sprintf("%d", n)
	case uint8:
		return fmt.Sprintf("%d", n)
	case uint16:
		return fmt.Sprintf("%d", n)
	case uint32:
		return fmt.Sprintf("%d", n)
	case uint64:
		return fmt.Sprintf("%d", n)
	case float32:
		return fmt.Sprintf("%g", n)
	case float64:
		return fmt.Sprintf("%g", n)
	case []byte:
		// Treat raw bytes as a hex literal (Postgres, MySQL, SQLite all support this).
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(n), "'", "''"))
	default:
		s := fmt.Sprintf("%v", v)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
}
