// Package jdbc registers a JDBC-style source that reads RecordBatches from any
// database/sql-compatible database (PostgreSQL, MySQL, SQL Server, and Oracle).
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/jdbc"
//
// The database driver must also be imported separately:
//
//	import _ "github.com/lib/pq"               // PostgreSQL
//	import _ "github.com/go-sql-driver/mysql"   // MySQL
//	import _ "github.com/microsoft/go-mssqldb"  // SQL Server
//	import _ "github.com/sijms/go-ora/v2"       // Oracle
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
//   - SQL Server and Oracle page via the SQL:2008 OFFSET/FETCH clause, which is
//     only valid alongside a stable ORDER BY. In table mode those drivers
//     therefore require orderKey to be set; supply a raw 'query' (carrying your
//     own ORDER BY) to bypass that requirement.
//   - In distributed mode, each instance opens its own connection pool. Total
//     database connections = instances × MaxOpenConns (4 by default).
package jdbc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	corelog "github.com/project-flogo/core/support/log"
)

func init() { source.Register("jdbc", newJDBCSource) }

var jlog = corelog.ChildLogger(corelog.RootLogger(), "jdbc-source")

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
	baseQuery  string // SELECT without LIMIT/OFFSET/WHERE-keyset suffix; built once in newJDBCSource
	orderKey   string // primary key column for keyset pagination; empty = use LIMIT/OFFSET
	batchSize  int
	schema     []record.Column
	rows       *sql.Rows // live cursor held between ReadBatch calls; nil between pages (keyset)
	rowsRead   int64
	lastKeyVal interface{} // last value of orderKey seen; drives the next keyset page query
	useKeyset  bool
	closed     bool

	// partition bounding — set only when this source was opened via OpenPartition.
	partOffset int64       // LIMIT/OFFSET partition lower bound
	partLimit  int64       // 0 means "no limit" (top-level source)
	keyLo      interface{} // keyset lower bound (exclusive), nil = no lower bound
	keyHi      interface{} // keyset upper bound (inclusive), nil = no upper bound
	sharedDB   bool        // true when db is borrowed from parent; Close() must not call db.Close()
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
var _ source.Reconciler = (*jdbcSource)(nil)

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

	// SQL Server and Oracle paginate with the SQL:2008 OFFSET/FETCH clause, which
	// is only valid alongside a stable ORDER BY. In table mode we therefore require
	// an orderKey (it drives keyset pagination and supplies that ORDER BY). Raw
	// 'query' mode is exempt — the caller owns ordering there.
	if orderKey == "" && rawQuery == "" {
		switch driver {
		case "sqlserver", "mssql", "oracle":
			return nil, fmt.Errorf("jdbc source: driver %q requires 'orderKey' for reliable pagination (OFFSET/FETCH needs a stable ORDER BY); set orderKey to a unique/primary-key column, or supply a raw 'query' with your own ORDER BY", driver)
		}
	}

	batchSize := 5000
	if bsRaw, ok := cfg["batchSize"]; ok {
		switch bs := bsRaw.(type) {
		case int:
			if bs > 0 {
				batchSize = bs
			} else {
				jlog.Warnf("jdbc source: 'batchSize' %v is invalid (must be a positive number); using default %d", bsRaw, batchSize)
			}
		case float64:
			// JSON unmarshals numbers as float64.
			if bs > 0 {
				batchSize = int(bs)
			} else {
				jlog.Warnf("jdbc source: 'batchSize' %v is invalid (must be a positive number); using default %d", bsRaw, batchSize)
			}
		default:
			jlog.Warnf("jdbc source: 'batchSize' %v has an unsupported type %T; using default %d", bsRaw, bsRaw, batchSize)
		}
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
		// HWM cannot be safely appended to an arbitrary raw query (it may already
		// have GROUP BY / ORDER BY / subqueries). Incremental filtering is only
		// applied in table mode; surface a hint so operators aren't surprised.
		if col, _ := cfg["hwmColumn"].(string); col != "" {
			if v, _ := cfg["hwmValue"].(string); v != "" {
				fmt.Printf("jdbc source: incremental HWM is not applied to a raw 'query' — include the watermark predicate in your query WHERE clause, or use 'table' mode for automatic HWM filtering\n")
			}
		}
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

	// Optional user WHERE clause and/or an incremental high-water-mark predicate.
	// The HWM predicate (col > lastValue) is injected by the engine via the
	// hwmColumn/hwmValue config keys for incremental.mode=hwm pipelines so the
	// database only returns rows newer than the last persisted watermark.
	conds := make([]string, 0, 2)
	if where, ok := cfg["where"].(string); ok && where != "" {
		conds = append(conds, where)
	}
	if pred := hwmPredicate(cfg, driver); pred != "" {
		conds = append(conds, pred)
	}
	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}

	return sb.String()
}

// hwmPredicate builds the incremental high-water-mark WHERE predicate
// ("col" > <literal>) from the hwmColumn / hwmValue / hwmValueType config keys
// injected by the engine for incremental.mode=hwm pipelines. Returns "" when no
// watermark is configured or no prior value exists (first full-table load).
//
// The value originates from the watermark store (which the pipeline itself
// wrote from earlier DB rows), not from end-user input; it is nonetheless
// formatted defensively — numerics are validated, everything else is single-
// quoted with embedded quotes escaped — so the resulting SQL is always well
// formed and injection-safe.
func hwmPredicate(cfg map[string]interface{}, driver string) string {
	col, _ := cfg["hwmColumn"].(string)
	val, _ := cfg["hwmValue"].(string)
	if col == "" || val == "" {
		return ""
	}
	valueType, _ := cfg["hwmValueType"].(string)
	var lit string
	switch valueType {
	case "int":
		if _, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err != nil {
			// Not a clean integer — fall back to a quoted literal so the query
			// stays valid rather than embedding an unquoted bad token.
			lit = sqlutil.QuoteLiteral(val, driver)
		} else {
			lit = strings.TrimSpace(val)
		}
	case "decimal":
		if _, ok := record.ParseDecimal(strings.TrimSpace(val)); !ok {
			// Not a clean decimal — quote defensively so the SQL stays valid.
			lit = sqlutil.QuoteLiteral(val, driver)
		} else {
			// Emit as an unquoted numeric literal so NUMERIC/DECIMAL columns compare
			// numerically (not lexically) against the watermark.
			lit = strings.TrimSpace(val)
		}
	default: // "datetime", "string", or unset — quote as a string literal.
		lit = sqlutil.QuoteLiteral(val, driver)
	}
	return sqlutil.QuoteIdent(col, driver) + " > " + lit
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
		// Find the value of orderKey at row n using ORDER BY orderKey + LIMIT 1
		// OFFSET n-1. This single round-trip repositions the keyset cursor cheaply.
		// The ORDER BY makes "row n" deterministic and is mandatory for the
		// OFFSET/FETCH form used by SQL Server and Oracle.
		q := s.baseQuery +
			" ORDER BY " + sqlutil.QuoteIdent(s.orderKey, s.driver) +
			sqlutil.LimitClause(s.driver, 1, n-1)
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

// closeCursor closes and forgets the open result set, if any.
func (s *jdbcSource) closeCursor() {
	if s.rows != nil {
		_ = s.rows.Close()
		s.rows = nil
	}
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

	// Rows are carved from slabs (len==cap each, so they never alias) rather
	// than allocated one by one. Slabs grow in chunks so a small poll result
	// doesn't pay for a full batch-size allocation.
	width := len(s.schema)
	var slab []interface{}
	orderIdx := -1
	if s.useKeyset {
		for i, col := range s.schema {
			if col.Name == s.orderKey {
				orderIdx = i
				break
			}
		}
	}

	for len(batch.Rows) < size {
		if !s.rows.Next() {
			if err := s.rows.Err(); err != nil {
				s.closeCursor()
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
			// Release the server-side cursor (and its pooled connection) now
			// rather than holding it until Close.
			s.closeCursor()
			return batch, fmt.Errorf("jdbc source: scan: %w", err)
		}

		if len(slab) < width {
			slab = make([]interface{}, width*min(size-len(batch.Rows), 256))
		}
		row := slab[:width:width]
		slab = slab[width:]
		for i := range s.schema {
			if i < len(vals) {
				row[i] = coerceScanned(vals[i], s.schema[i])
			}
		}
		batch.Rows = append(batch.Rows, row)
		s.rowsRead++

		// Track the last orderKey value for the next keyset page.
		if orderIdx >= 0 {
			s.lastKeyVal = row[orderIdx]
		}
	}

	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}

	// Keyset mode opens one cursor per page (LIMIT size). When the page fills to
	// exactly `size` rows the loop above exits without the cursor reaching
	// exhaustion, leaving s.rows open on a now-drained LIMIT query. Close it here
	// so the next ReadBatch opens a fresh cursor seeded with the updated
	// lastKeyVal (WHERE orderKey > lastKeyVal). Without this the next call would
	// read the drained cursor, get zero rows, and stop after a single page.
	if s.useKeyset && s.rows != nil {
		s.rows.Close()
		s.rows = nil
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
// Partition children share the parent's *sql.DB pool; only the parent closes it.
func (s *jdbcSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.rows != nil {
		s.rows.Close()
		s.rows = nil
	}
	if s.sharedDB {
		return nil
	}
	return s.db.Close()
}

// ReconciliationCount implements source.Reconciler: runs query (typically
// "SELECT COUNT(*) FROM ...") against this source's own connection —
// independent of the ReadBatch/keyset-pagination path — and returns the
// single integer result. Used by a data contract's optional post-run
// reconciliation check, not by normal reads.
func (s *jdbcSource) ReconciliationCount(query string) (int64, error) {
	var n int64
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		return 0, fmt.Errorf("jdbc source: reconciliation query: %w", err)
	}
	return n, nil
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
		sharedDB:   true, // do not close the shared pool when this partition is closed
	}

	return child, nil
}

// inferSchema executes the base query with LIMIT 0 and uses ColumnTypes() to
// derive the schema. This approach works across all three drivers and avoids
// fetching any data rows.
func (s *jdbcSource) inferSchema() error {
	// Fetch column metadata without reading data rows. SQL Server's OFFSET/FETCH
	// limiter requires an ORDER BY (and Oracle's is non-deterministic without one),
	// so we can't reuse LimitClause on an unordered probe. Wrapping the base query
	// in an always-false filter returns zero rows while still exposing full column
	// metadata, and needs no ORDER BY. A bare (AS-less) subquery alias is valid on
	// every supported driver.
	var q string
	switch s.driver {
	case "sqlserver", "mssql", "oracle":
		q = "SELECT * FROM (" + s.baseQuery + ") dp_schema WHERE 1 = 0"
	default: // postgres, mysql — LIMIT 1 is cheapest and preserves existing behaviour
		q = s.baseQuery + sqlutil.LimitClause(s.driver, 1, 0)
	}
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
	case time.Time:
		// ISO-8601 timestamp literal for Postgres/MySQL/SQL Server; Oracle timestamp
		// keyset keys may need an NLS-compatible format (numeric keys are unaffected).
		return "'" + n.Format("2006-01-02 15:04:05.999999-07:00") + "'"
	case []byte:
		// Treat raw bytes as a hex literal (Postgres, MySQL, SQL Server all support this).
		return sqlutil.QuoteLiteral(string(n), driver)
	default:
		s := fmt.Sprintf("%v", v)
		return sqlutil.QuoteLiteral(s, driver)
	}
}

// coerceScanned normalises a value produced by database/sql's Scan-into-any into
// a typed Go value that matches the declared column type. Drivers (notably
// lib/pq) return NUMERIC/DECIMAL columns as []byte and some return numbers as
// strings; without this, arithmetic transforms (e.g. multiply) and typed sinks
// (Parquet) see raw bytes instead of numbers. Values already scanned as the
// right Go type (int64, float64, bool, time.Time) are passed through unchanged.
func coerceScanned(v interface{}, col record.Column) interface{} {
	if v == nil {
		return nil
	}
	// Reduce []byte to string so the type-specific parsing below can run.
	switch b := v.(type) {
	case []byte:
		v = string(b)
	}

	switch col.Type {
	case record.TypeInt:
		switch n := v.(type) {
		case int64:
			return n
		case int32:
			return int64(n)
		case float64:
			return int64(n)
		case string:
			if iv, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
				return iv
			}
			// Some drivers hand back "12.0" for integer-typed expressions.
			if fv, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
				return int64(fv)
			}
		}
	case record.TypeDecimal:
		// Preserve exactness: drivers hand NUMERIC back as a string/[]byte (already
		// reduced to string above) or a number — convert to an exact Decimal rather
		// than truncating through float64.
		if d, ok := record.ToDecimal(v); ok {
			return d
		}
	case record.TypeFloat:
		switch n := v.(type) {
		case float64:
			return n
		case float32:
			return float64(n)
		case int64:
			return float64(n)
		case string:
			if fv, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
				return fv
			}
		}
	case record.TypeBool:
		switch n := v.(type) {
		case bool:
			return n
		case string:
			switch strings.ToLower(strings.TrimSpace(n)) {
			case "true", "t", "1", "yes", "y":
				return true
			case "false", "f", "0", "no", "n":
				return false
			}
		case int64:
			return n != 0
		}
	case record.TypeDate:
		switch n := v.(type) {
		case time.Time:
			return n
		case string:
			for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999-07:00", "2006-01-02 15:04:05", "2006-01-02"} {
				if t, err := time.Parse(layout, strings.TrimSpace(n)); err == nil {
					return t
				}
			}
		}
	case record.TypeString:
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return v
}
