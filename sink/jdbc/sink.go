// Package jdbc registers a JDBC-style sink that writes RecordBatches to any
// database/sql-compatible database (PostgreSQL, MySQL, SQL Server, and Oracle).
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
//
// The database driver must also be imported:
//
//	import _ "github.com/lib/pq"                  // PostgreSQL
//	import _ "github.com/go-sql-driver/mysql"     // MySQL
//	import _ "github.com/microsoft/go-mssqldb"    // SQL Server
//	import _ "github.com/sijms/go-ora/v2"         // Oracle
//
// # Delete-capable mode (opColumn)
//
// Set "opColumn" to a metadata column present in every batch (but not itself a
// target-table column, e.g. a Debezium-style "__op" field written by the kafka
// source's cdc-envelope deserializer) whose value classifies each row. Rows
// whose opColumn value matches (case-insensitively) one of "deleteValues"
// (default ["d", "delete"]) are DELETEd by "conflictKey" instead of written;
// every other row is inserted/upserted as usual. Requires a non-empty
// conflictKey. This turns a periodic full/incremental refresh into a true
// mirror of the source table — rows removed upstream are removed here too —
// without requiring a full CDC pipeline.
package jdbc

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

var jlog = corelog.ChildLogger(corelog.RootLogger(), "jdbc")

func init() {
	sink.Register("jdbc", newJDBCSink)
}

// conflictStrategy controls INSERT behaviour on primary-key conflicts.
type conflictStrategy string

const (
	conflictInsert conflictStrategy = "insert" // plain INSERT — fails on conflict
	conflictUpsert conflictStrategy = "upsert" // INSERT ... ON CONFLICT DO UPDATE (Postgres) or INSERT ... ON DUPLICATE KEY UPDATE (MySQL)
	conflictIgnore conflictStrategy = "ignore" // INSERT ... ON CONFLICT DO NOTHING / INSERT IGNORE
)

// jdbcSink writes batches to a SQL database table using batched INSERTs.
type jdbcSink struct {
	db           *sql.DB
	table        string
	columns      []string // ordered column names for the current INSERT statement
	batchSize    int
	conflictKey  []string // columns that form the upsert conflict key
	onConflict   conflictStrategy
	driver       string
	buf          [][]interface{} // sink-owned row copies, not pool-managed batch pointers
	rowsBuffered int
	schema       []record.Column // sink's current known schema; grows with autoMigrate

	// autoMigrate, when true, causes the sink to issue ALTER TABLE ... ADD COLUMN
	// statements for any new columns it encounters in subsequent WriteBatch calls.
	// Without autoMigrate, extra columns arriving after the first batch are logged
	// as warnings and silently dropped so the INSERT remains valid.
	// autoMigrate is intended for use together with schemadrift policy=additive.
	autoMigrate bool

	// opColumn, when set, turns on delete-capable mode: it names a metadata
	// column (present in every incoming batch but NOT a target-table column,
	// e.g. a Debezium-style "__op" field) whose value classifies each row as an
	// upsert or a delete. Rows whose opColumn value matches (case-insensitively)
	// one of deleteValues are DELETEd by conflictKey instead of written; every
	// other row is inserted/upserted as usual. Requires a non-empty conflictKey
	// (the delete needs a key to delete BY). See writeBatchWithOp / flushMixed.
	opColumn     string
	deleteValues []string
	deleteBuf    [][]interface{} // buffered conflictKey value tuples pending DELETE

	// deleteBufKeys indexes deleteBuf by conflictKey string (see
	// jdbcRowKeyString) so a later upsert row for the same key within the same
	// flush can find and remove its now-stale pending delete in O(1) instead of
	// rescanning deleteBuf. Without this, a delete followed by a re-create for
	// the same key before the next flush (a legitimate CDC sequence — a row
	// deleted then recreated) would still be DELETEd, because flushMixed always
	// runs all deletes after all inserts regardless of the true arrival order.
	deleteBufKeys map[string]int // key string -> index into deleteBuf

	// rowsDeleted is the running total of rows actually removed by delete-
	// capable mode's DELETE statements (sql.Result.RowsAffected(), summed
	// across every committed flush) — exposed via SinkStats for lineage/audit.
	// Only incremented after a flush's transaction actually COMMITS, so a
	// rolled-back flush (see the transaction-atomicity test) never inflates it.
	rowsDeleted int64
}

var (
	_ sink.StatsReporter = (*jdbcSink)(nil)
	_ sink.Reconciler    = (*jdbcSink)(nil)
)

func newJDBCSink(cfg map[string]interface{}) (sink.Sink, error) {
	driver, _ := cfg["driver"].(string)
	if driver == "" {
		driver = "postgres"
	}
	dsn, _ := cfg["dsn"].(string)
	if dsn == "" {
		return nil, fmt.Errorf("jdbc sink: 'dsn' (data source name) is required")
	}
	table, _ := cfg["table"].(string)
	if table == "" {
		return nil, fmt.Errorf("jdbc sink: 'table' is required")
	}

	batchSize := 1000
	if bsRaw, ok := cfg["batchSize"]; ok {
		if bs, ok := bsRaw.(int); ok && bs > 0 {
			batchSize = bs
		} else {
			jlog.Warnf("jdbc sink: 'batchSize' %v is invalid (must be a positive int); using default %d", bsRaw, batchSize)
		}
	}

	onConflict := conflictInsert
	if oc, ok := cfg["onConflict"].(string); ok {
		onConflict = conflictStrategy(oc)
	}

	var conflictKey []string
	if ck, ok := cfg["conflictKey"].([]interface{}); ok {
		for _, k := range ck {
			if s, ok := k.(string); ok {
				conflictKey = append(conflictKey, s)
			}
		}
	}

	autoMigrate := false
	if am, ok := cfg["autoMigrate"].(bool); ok {
		autoMigrate = am
	}

	// Validate config that doesn't need a connection BEFORE opening one, so a
	// misconfigured opColumn/conflictKey fails fast without a network round trip.
	opColumn, _ := cfg["opColumn"].(string)
	var deleteValues []string
	if dv, ok := cfg["deleteValues"].([]interface{}); ok {
		for _, v := range dv {
			if s, ok := v.(string); ok {
				deleteValues = append(deleteValues, s)
			}
		}
	}
	if opColumn != "" {
		if len(conflictKey) == 0 {
			return nil, fmt.Errorf("jdbc sink: 'opColumn' requires a non-empty 'conflictKey' (deletes need a key to delete by)")
		}
		if len(deleteValues) == 0 {
			deleteValues = []string{"d", "delete"}
		}
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("jdbc sink: open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("jdbc sink: ping db: %w", err)
	}
	// Tune connection pool for batch writes.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)

	return &jdbcSink{
		db:           db,
		table:        table,
		batchSize:    batchSize,
		conflictKey:  conflictKey,
		onConflict:   onConflict,
		driver:       driver,
		autoMigrate:  autoMigrate,
		opColumn:     opColumn,
		deleteValues: deleteValues,
	}, nil
}

// WriteBatch buffers rows and flushes when the buffer exceeds batchSize.
// Rows are copied out of the batch immediately so the caller can safely call
// record.Put(batch) as soon as WriteBatch returns without corrupting the
// sink's buffer (fixes use-after-free / sync.Pool misuse bugs).
//
// Schema evolution: if the batch's schema has grown relative to the sink's
// known schema (possible when the upstream schemadrift transform runs with
// policy=additive), reconcileSchema is called to either issue ALTER TABLE
// statements (autoMigrate:true) or drop the extra columns (autoMigrate:false).
func (s *jdbcSink) WriteBatch(batch *record.RecordBatch) error {
	if s.opColumn != "" {
		return s.writeBatchWithOp(batch)
	}
	if len(s.schema) == 0 {
		// First batch: copy schema and column names before the caller can call
		// record.Put, which nils batch.Columns. A direct assignment would leave
		// s.schema pointing at a slice that Put will zero out.
		s.schema = make([]record.Column, len(batch.Columns))
		copy(s.schema, batch.Columns)
		s.columns = make([]string, len(batch.Columns))
		for i, col := range batch.Columns {
			s.columns[i] = col.Name
		}
		// autoMigrate=true also creates the target table when it does not yet
		// exist, using the first batch's schema. Without this, a fresh table
		// (the common case for the Studio "auto-migrate table" toggle) would fail
		// the INSERT with "relation does not exist".
		if s.autoMigrate {
			if err := s.ensureTable(); err != nil {
				return err
			}
		}
	} else if len(batch.Columns) != len(s.schema) || !schemasEqual(s.schema, batch.Columns) {
		// Subsequent batch: schema may have drifted (additive policy upstream).
		// reconcileSchema either migrates the database table or trims the batch.
		if err := s.reconcileSchema(batch); err != nil {
			return err
		}
	}

	// Copy rows into sink-owned storage. Do NOT retain the batch pointer:
	// the caller calls record.Put(batch) immediately after this returns,
	// which sets batch.Rows = batch.Rows[:0], making any stored pointer
	// yield an empty slice in flush() and silently dropping all rows.
	//
	// When reconcileSchema trimmed the batch to s.columns width, rows may be
	// wider than s.columns. We copy only the first len(s.columns) values so
	// buildInsert always receives rows whose width matches s.columns.
	ncols := len(s.columns)
	for _, row := range batch.Rows {
		rowCopy := make([]interface{}, ncols)
		for i := 0; i < ncols && i < len(row); i++ {
			rowCopy[i] = row[i]
		}
		s.buf = append(s.buf, rowCopy)
	}
	s.rowsBuffered += batch.Len()

	if s.rowsBuffered >= s.batchSize {
		return s.flush()
	}
	return nil
}

// writeBatchWithOp is WriteBatch's delete-capable path (opColumn configured).
// It mirrors WriteBatch's schema-capture/reconcile logic exactly, but against
// a "logical" column list with opColumn removed (opColumn is metadata, not a
// target-table column) and splits rows by opColumn's value into the upsert
// buffer (s.buf, written by the existing insert/upsert machinery unchanged)
// and the delete buffer (s.deleteBuf, a conflictKey value tuple per deleted
// row, flushed by flushMixed via buildDelete).
//
// Row values are gathered by COLUMN NAME (batch.ColIndex), not position: unlike
// the plain path's positional prefix-copy, opColumn's position within
// batch.Columns is arbitrary, so a name-indexed gather is required to place
// values into s.columns/s.conflictKey order correctly.
func (s *jdbcSink) writeBatchWithOp(batch *record.RecordBatch) error {
	logicalCols := stripColumn(batch.Columns, s.opColumn)

	if len(s.schema) == 0 {
		s.schema = make([]record.Column, len(logicalCols))
		copy(s.schema, logicalCols)
		s.columns = make([]string, len(logicalCols))
		for i, col := range logicalCols {
			s.columns[i] = col.Name
		}
		if s.autoMigrate {
			if err := s.ensureTable(); err != nil {
				return err
			}
		}
	} else if len(logicalCols) != len(s.schema) || !schemasEqual(s.schema, logicalCols) {
		if err := s.reconcileSchema(&record.RecordBatch{Columns: logicalCols}); err != nil {
			return err
		}
	}

	opIdx := batch.ColIndex(s.opColumn)
	if opIdx < 0 {
		return fmt.Errorf("jdbc sink: opColumn %q not present in batch", s.opColumn)
	}
	colIdx := make([]int, len(s.columns))
	for i, name := range s.columns {
		idx := batch.ColIndex(name)
		if idx < 0 {
			return fmt.Errorf("jdbc sink: column %q (from the sink's known schema) missing in batch", name)
		}
		colIdx[i] = idx
	}
	keyIdx := make([]int, len(s.conflictKey))
	for i, name := range s.conflictKey {
		idx := batch.ColIndex(name)
		if idx < 0 {
			return fmt.Errorf("jdbc sink: conflictKey column %q missing in batch", name)
		}
		keyIdx[i] = idx
	}
	// Position of each conflictKey column within colIdx/s.columns, so an
	// upsert row's key can be extracted for the stale-pending-delete check
	// below without a second name lookup.
	keyPosInCols := make([]int, len(s.conflictKey))
	for i, name := range s.conflictKey {
		for j, col := range s.columns {
			if col == name {
				keyPosInCols[i] = j
				break
			}
		}
	}

	upserted := 0
	for _, row := range batch.Rows {
		if isDeleteOp(row[opIdx], s.deleteValues) {
			key := make([]interface{}, len(keyIdx))
			for i, idx := range keyIdx {
				key[i] = row[idx]
			}
			s.appendPendingDelete(key)
			continue
		}
		rowCopy := make([]interface{}, len(colIdx))
		for i, idx := range colIdx {
			rowCopy[i] = row[idx]
		}
		// A later create/update for the same key supersedes any pending
		// delete for it buffered earlier in this same flush (e.g. a row
		// deleted then re-created before the next checkpoint) — remove it so
		// flushMixed's fixed insert-then-delete statement order doesn't wipe
		// the re-create back out.
		if len(s.deleteBufKeys) > 0 {
			key := make([]interface{}, len(keyPosInCols))
			for i, pos := range keyPosInCols {
				key[i] = rowCopy[pos]
			}
			s.removeStalePendingDelete(key)
		}
		s.buf = append(s.buf, rowCopy)
		upserted++
	}
	s.rowsBuffered += upserted

	if s.rowsBuffered+len(s.deleteBuf) >= s.batchSize {
		return s.flush()
	}
	return nil
}

// appendPendingDelete buffers a conflictKey tuple for DELETE and indexes it in
// deleteBufKeys so a later upsert row for the same key (see
// removeStalePendingDelete) can find and remove it in O(1).
func (s *jdbcSink) appendPendingDelete(key []interface{}) {
	if s.deleteBufKeys == nil {
		s.deleteBufKeys = make(map[string]int)
	}
	k := deleteKeyString(key)
	// A duplicate delete for the same key within one flush is harmless (the
	// DELETE statement is idempotent), so just keep the existing index.
	if _, exists := s.deleteBufKeys[k]; !exists {
		s.deleteBufKeys[k] = len(s.deleteBuf)
		s.deleteBuf = append(s.deleteBuf, key)
	}
}

// removeStalePendingDelete removes key's pending delete (if any) from
// deleteBuf via a swap-with-last-element removal (delete order is
// inconsequential for buildDelete's OR-of-ANDs statement), keeping
// deleteBufKeys consistent with the swap.
func (s *jdbcSink) removeStalePendingDelete(key []interface{}) {
	k := deleteKeyString(key)
	idx, ok := s.deleteBufKeys[k]
	if !ok {
		return
	}
	last := len(s.deleteBuf) - 1
	if idx != last {
		s.deleteBuf[idx] = s.deleteBuf[last]
		s.deleteBufKeys[deleteKeyString(s.deleteBuf[idx])] = idx
	}
	s.deleteBuf = s.deleteBuf[:last]
	delete(s.deleteBufKeys, k)
}

// deleteKeyString renders an already-extracted conflictKey value tuple (in
// conflictKey column order) as a single comparable string, using the same
// NUL-separated form as jdbcRowKeyString so no combination of stringified
// values can collide across a key-column boundary.
func deleteKeyString(key []interface{}) string {
	var sb strings.Builder
	for _, v := range key {
		fmt.Fprintf(&sb, "%v\x00", v)
	}
	return sb.String()
}

// stripColumn returns cols with the named column removed (order preserved).
func stripColumn(cols []record.Column, name string) []record.Column {
	out := make([]record.Column, 0, len(cols))
	for _, c := range cols {
		if c.Name != name {
			out = append(out, c)
		}
	}
	return out
}

// isDeleteOp reports whether v (opColumn's value for a row) matches one of
// deleteValues, case-insensitively. Non-string values (including nil, e.g. a
// row whose opColumn was never set) are never treated as deletes.
func isDeleteOp(v interface{}, deleteValues []string) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	for _, dv := range deleteValues {
		if strings.EqualFold(s, dv) {
			return true
		}
	}
	return false
}

// reconcileSchema brings the sink's known schema into alignment with the
// incoming batch's schema. It is called only when a schema difference is
// detected, so the common (no-drift) path pays zero overhead.
//
// When autoMigrate is true:
//   - New columns in the batch that are absent from s.schema trigger an
//     ALTER TABLE ... ADD COLUMN statement for each new column.
//   - On success, s.schema and s.columns are extended to include the new
//     columns, and subsequent flushes use the wider INSERT statement.
//   - Dropped columns (present in s.schema, absent in batch) are left in
//     s.schema; the INSERT will send NULL for those positions because
//     schema.Project (upstream) already filled them with nil.
//
// When autoMigrate is false:
//   - New columns in the batch are logged as warnings and the batch is
//     treated as if those columns do not exist — the INSERT uses only the
//     original s.columns. Rows in the batch that are wider than s.schema
//     are silently truncated in WriteBatch's copy loop.
//   - This is safe: the upstream schemadrift=project transform guarantees
//     that batches never carry extra columns unless policy=additive is used.
func (s *jdbcSink) reconcileSchema(batch *record.RecordBatch) error {
	// Build a name set of the sink's current known columns for O(1) lookup.
	known := make(map[string]struct{}, len(s.schema))
	for _, col := range s.schema {
		known[col.Name] = struct{}{}
	}

	// Identify genuinely new columns in the incoming batch.
	var newCols []record.Column
	for _, col := range batch.Columns {
		if _, ok := known[col.Name]; !ok {
			newCols = append(newCols, col)
		}
	}

	if len(newCols) == 0 {
		// Only dropped columns or retyped columns — no structural change needed.
		// The INSERT continues to use the existing s.columns; nil values fill
		// dropped positions (set by schema.Project upstream).
		return nil
	}

	if !s.autoMigrate {
		// Log and ignore new columns; the copy loop in WriteBatch will trim rows.
		for _, col := range newCols {
			jlog.Warnf("jdbc sink table=%q: new column %q (type=%s) received but autoMigrate=false — column will be dropped from INSERT",
				s.table, col.Name, col.Type)
		}
		return nil
	}

	// autoMigrate=true: flush pending rows first so they are written with the
	// old schema before we alter the table. This avoids INSERT column-count
	// mismatches if any buffered rows predate the schema change.
	if err := s.flush(); err != nil {
		return fmt.Errorf("jdbc sink: pre-migration flush: %w", err)
	}

	// Issue an ALTER TABLE ADD for each new column. "ADD COLUMN" is the
	// PostgreSQL/MySQL form; SQL Server and Oracle both use bare "ADD" — on
	// SQL Server "COLUMN" is not valid there ("Incorrect syntax near the
	// keyword 'COLUMN'"), and on Oracle "COLUMN" is a reserved word that
	// cannot appear in that position (ORA-03050: invalid identifier).
	for _, col := range newCols {
		sqlType := recordTypeToSQL(col.Type, s.driver)
		var stmt string
		switch s.driver {
		case "sqlserver", "mssql", "oracle":
			stmt = fmt.Sprintf("ALTER TABLE %s ADD %s %s",
				sqlutil.QuoteIdent(s.table, s.driver),
				sqlutil.QuoteIdent(col.Name, s.driver),
				sqlType,
			)
		default: // postgres, mysql
			stmt = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				sqlutil.QuoteIdent(s.table, s.driver),
				sqlutil.QuoteIdent(col.Name, s.driver),
				sqlType,
			)
		}
		jlog.Infof("jdbc sink table=%q: autoMigrate — %s", s.table, stmt)
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("jdbc sink: alter table add column %q: %w", col.Name, err)
		}
		// Extend the sink's known schema and column list.
		s.schema = append(s.schema, col)
		s.columns = append(s.columns, col.Name)
	}

	return nil
}

// ensureTable issues a CREATE TABLE IF NOT EXISTS for s.table using the sink's
// current known schema. It is called on the first batch when autoMigrate is
// true so that a not-yet-existing target table is created automatically (the
// Studio "auto-migrate table" toggle). Existing tables are left untouched;
// subsequent additive columns are handled by reconcileSchema's ALTER TABLE path.
//
// When conflictKey is set, the CREATE TABLE also declares it as the PRIMARY
// KEY. This is required for Postgres's "ON CONFLICT (col) DO ..." — Postgres
// rejects that clause with "no unique or exclusion constraint matching the ON
// CONFLICT specification" unless the target columns already have one — and is
// harmless (indeed correct) for the other three dialects too, whose upsert
// forms (ON DUPLICATE KEY UPDATE / MERGE) don't strictly require it. Without
// this, autoMigrate:true + onConflict:upsert/ignore against a table that does
// not yet exist would always fail on Postgres on the very first upsert.
func (s *jdbcSink) ensureTable() error {
	if len(s.schema) == 0 {
		return nil
	}
	cols := make([]string, len(s.schema))
	for i, col := range s.schema {
		cols[i] = fmt.Sprintf("%s %s",
			sqlutil.QuoteIdent(col.Name, s.driver),
			recordTypeToSQL(col.Type, s.driver),
		)
	}
	if len(s.conflictKey) > 0 {
		quoted := make([]string, len(s.conflictKey))
		for i, k := range s.conflictKey {
			quoted[i] = sqlutil.QuoteIdent(k, s.driver)
		}
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoted, ", ")))
	}
	createStmt := fmt.Sprintf("CREATE TABLE %s (%s)",
		sqlutil.QuoteIdent(s.table, s.driver),
		strings.Join(cols, ", "),
	)
	// "CREATE TABLE IF NOT EXISTS" is a PostgreSQL/MySQL extension — it is NOT
	// valid T-SQL (SQL Server rejects it with "Incorrect syntax near 'IF'")
	// and is only supported on very recent Oracle releases (23c+), so relying
	// on it would silently break autoMigrate on older-but-still-common Oracle
	// versions. Each dialect gets its own guarded/portable form instead.
	var stmt string
	switch s.driver {
	case "sqlserver", "mssql":
		stmt = fmt.Sprintf("IF OBJECT_ID(N'%s', N'U') IS NULL EXEC(N'%s')",
			strings.ReplaceAll(s.table, "'", "''"),
			strings.ReplaceAll(createStmt, "'", "''"),
		)
	case "oracle":
		stmt = fmt.Sprintf(
			"BEGIN EXECUTE IMMEDIATE '%s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;",
			strings.ReplaceAll(createStmt, "'", "''"),
		)
	default: // postgres, mysql
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
			sqlutil.QuoteIdent(s.table, s.driver),
			strings.Join(cols, ", "),
		)
	}
	jlog.Infof("jdbc sink table=%q: autoMigrate — %s", s.table, stmt)
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("jdbc sink: create table %q: %w", s.table, err)
	}
	return nil
}

// schemasEqual reports whether two column lists have the same names and types
// in the same order. It is used as a fast equality check before calling
// reconcileSchema (which builds maps and is more expensive).
func schemasEqual(a, b []record.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}

// recordTypeToSQL maps a record.ColumnType to a SQL column type string
// appropriate for the target driver. Used by the autoMigrate ALTER TABLE path.
// Defaults to TEXT/VARCHAR(65535) for unknown types, which is safe for all
// supported drivers.
func recordTypeToSQL(ct record.ColumnType, driver string) string {
	switch ct {
	case record.TypeInt:
		switch driver {
		case "oracle":
			return "NUMBER(19)" // Oracle has no BIGINT
		default: // postgres, mysql, sqlserver
			return "BIGINT"
		}
	case record.TypeFloat:
		switch driver {
		case "mysql", "sqlserver", "mssql":
			return "FLOAT"
		case "oracle":
			return "BINARY_DOUBLE"
		default:
			return "DOUBLE PRECISION"
		}
	case record.TypeDecimal:
		// MySQL DECIMAL/NUMERIC without an explicit precision default to (10,0) —
		// zero fractional digits — which silently truncates. Give it a precision and
		// scale that cover money and most NUMERIC data. Postgres NUMERIC is
		// arbitrary-precision, so leave it unbounded there.
		switch driver {
		case "mysql", "sqlserver", "mssql":
			return "DECIMAL(38,10)"
		case "oracle":
			return "NUMBER(38,10)"
		default:
			return "NUMERIC"
		}
	case record.TypeBool:
		switch driver {
		case "mysql":
			return "TINYINT(1)"
		case "sqlserver", "mssql":
			return "BIT"
		case "oracle":
			return "NUMBER(1)" // Oracle has no native boolean in SQL DDL
		default:
			return "BOOLEAN"
		}
	case record.TypeDate:
		switch driver {
		case "mysql":
			return "DATETIME"
		case "sqlserver", "mssql":
			return "DATETIME2"
		case "oracle":
			return "TIMESTAMP"
		default:
			return "TIMESTAMP WITH TIME ZONE"
		}
	default: // TypeString, TypeAny, unknown
		switch driver {
		case "mysql":
			return "VARCHAR(65535)"
		case "sqlserver", "mssql":
			return "NVARCHAR(MAX)"
		case "oracle":
			return "CLOB"
		default:
			return "TEXT"
		}
	}
}

// Flush writes all buffered rows to the database.
func (s *jdbcSink) Flush() error {
	return s.flush()
}

// flush dispatches to the plain insert-only path (the historical, unchanged
// behavior when opColumn is not configured) or the delete-capable path.
func (s *jdbcSink) flush() error {
	if s.opColumn != "" {
		return s.flushMixed()
	}
	return s.flushInsertsOnly()
}

// flushMixed flushes both the upsert buffer and the delete buffer. When both
// are non-empty they run in one transaction, so a batch containing both
// upserts and deletes commits or rolls back as a single unit of work.
func (s *jdbcSink) flushMixed() error {
	if len(s.buf) == 0 && len(s.deleteBuf) == 0 {
		return nil
	}

	var flushDeleted int64
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("jdbc sink: begin tx: %w", err)
	}

	if len(s.buf) > 0 {
		allRows := s.dedupUpsertRows(s.buf)
		s.buf = s.buf[:0]
		s.rowsBuffered = 0
		ncols := len(s.columns)
		maxRows := maxRowsPerInsert(s.driver, ncols)
		for start := 0; start < len(allRows); start += maxRows {
			end := start + maxRows
			if end > len(allRows) {
				end = len(allRows)
			}
			query, args := s.buildInsert(allRows[start:end])
			if _, err := tx.Exec(query, args...); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("jdbc sink: exec insert chunk: %w", err)
			}
		}
	}

	if len(s.deleteBuf) > 0 {
		allKeys := s.deleteBuf
		s.deleteBuf = s.deleteBuf[:0]
		s.deleteBufKeys = nil
		maxRows := maxRowsPerInsert(s.driver, len(s.conflictKey))
		for start := 0; start < len(allKeys); start += maxRows {
			end := start + maxRows
			if end > len(allKeys) {
				end = len(allKeys)
			}
			query, args := s.buildDelete(allKeys[start:end])
			result, err := tx.Exec(query, args...)
			if err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("jdbc sink: exec delete chunk: %w", err)
			}
			if n, aerr := result.RowsAffected(); aerr == nil {
				flushDeleted += n
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("jdbc sink: commit tx: %w", err)
	}
	s.rowsDeleted += flushDeleted
	return nil
}

// buildDelete builds a DELETE FROM ... WHERE (k1=? AND k2=?) OR (k1=? AND ...)
// statement for a chunk of conflictKey value tuples. The OR-of-ANDs form (vs.
// a row-value "(k1,k2) IN (...)" constructor) is deliberate: it is the one
// syntax all four supported dialects accept identically — SQL Server has no
// row-value IN constructor at all, unlike Postgres/MySQL/Oracle.
func (s *jdbcSink) buildDelete(keys [][]interface{}) (string, []interface{}) {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(sqlutil.QuoteIdent(s.table, s.driver))
	sb.WriteString(" WHERE ")
	args := make([]interface{}, 0, len(keys)*len(s.conflictKey))
	p := 1
	for i, key := range keys {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		sb.WriteString("(")
		for j, col := range s.conflictKey {
			if j > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(sqlutil.QuoteIdent(col, s.driver))
			sb.WriteString(" = ")
			sb.WriteString(sqlutil.Placeholder(p, s.driver))
			p++
			args = append(args, key[j])
		}
		sb.WriteString(")")
	}
	return sb.String(), args
}

// dedupUpsertRows keeps only the LAST occurrence of each conflictKey value
// tuple in rows, when onConflict is "upsert". This is not a data-loss
// shortcut — it is required for correctness: Postgres rejects "ON CONFLICT DO
// UPDATE" statements that would affect the same conflict target twice
// ("ON CONFLICT DO UPDATE command cannot affect row a second time"), which a
// naive multi-row upsert hits the moment two changes to the SAME key are
// buffered in the same flush — the routine shape of a real change-data-capture
// stream (e.g. a row created then updated again before the next checkpoint).
// Keeping only the most recent write for each key within the flush yields the
// exact same final database state a row-by-row apply would produce, since an
// earlier buffered row for that key is already superseded before this
// statement runs. "ignore"/"insert" modes are left untouched: Postgres's
// restriction is specific to DO UPDATE (DO NOTHING has no such restriction,
// and plain INSERT should legitimately surface a duplicate-key error).
func (s *jdbcSink) dedupUpsertRows(rows [][]interface{}) [][]interface{} {
	if s.onConflict != conflictUpsert || len(s.conflictKey) == 0 || len(rows) < 2 {
		return rows
	}

	keyIdx := make([]int, 0, len(s.conflictKey))
	for _, name := range s.conflictKey {
		for i, col := range s.columns {
			if col == name {
				keyIdx = append(keyIdx, i)
				break
			}
		}
	}
	if len(keyIdx) != len(s.conflictKey) {
		return rows // conflictKey not found in s.columns — leave as-is; let the DB surface any real error
	}

	keys := make([]string, len(rows))
	lastIdx := make(map[string]int, len(rows))
	for i, row := range rows {
		keys[i] = jdbcRowKeyString(row, keyIdx)
		lastIdx[keys[i]] = i
	}
	if len(lastIdx) == len(rows) {
		return rows // no duplicate keys in this flush — the common case, zero extra allocation
	}

	out := make([][]interface{}, 0, len(lastIdx))
	for i, row := range rows {
		if lastIdx[keys[i]] == i {
			out = append(out, row)
		}
	}
	return out
}

// jdbcRowKeyString renders the conflictKey column values of one row as a
// single comparable string, using a NUL separator so no combination of
// stringified values can collide across different key-column boundaries.
func jdbcRowKeyString(row []interface{}, keyIdx []int) string {
	var sb strings.Builder
	for _, idx := range keyIdx {
		fmt.Fprintf(&sb, "%v\x00", row[idx])
	}
	return sb.String()
}

func (s *jdbcSink) flushInsertsOnly() error {
	if len(s.buf) == 0 {
		return nil
	}

	// s.buf already contains sink-owned [][]interface{} rows; use directly.
	// dedupUpsertRows keeps only the last occurrence of each conflictKey value
	// within this flush — see its doc comment for why that is required for
	// correctness, not just an optimisation.
	allRows := s.dedupUpsertRows(s.buf)
	s.buf = s.buf[:0]
	s.rowsBuffered = 0

	ncols := len(s.columns)
	if len(allRows) == 0 || ncols == 0 {
		return nil
	}

	// Chunk rows so a single statement never exceeds the driver's bind-parameter
	// limit (rows × columns). SQL Server caps at 2100 parameters, so a wide table
	// at the default batch size would otherwise fail. When one chunk suffices we
	// execute it directly; multiple chunks run in a single transaction so a
	// partial failure rolls back atomically.
	maxRows := maxRowsPerInsert(s.driver, ncols)
	if len(allRows) <= maxRows {
		query, args := s.buildInsert(allRows)
		if _, err := s.db.Exec(query, args...); err != nil {
			return fmt.Errorf("jdbc sink: exec insert: %w", err)
		}
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("jdbc sink: begin tx: %w", err)
	}
	for start := 0; start < len(allRows); start += maxRows {
		end := start + maxRows
		if end > len(allRows) {
			end = len(allRows)
		}
		query, args := s.buildInsert(allRows[start:end])
		if _, err := tx.Exec(query, args...); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("jdbc sink: exec insert chunk: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("jdbc sink: commit tx: %w", err)
	}
	return nil
}

// maxRowsPerInsert returns the largest number of rows that fit in one statement
// without exceeding the driver's bind-parameter limit (rows × columns), leaving
// a small headroom for reserved binds.
func maxRowsPerInsert(driver string, ncols int) int {
	if ncols <= 0 {
		return 1
	}
	var maxParams int
	switch driver {
	case "sqlserver", "mssql":
		maxParams = 2100
	case "oracle":
		maxParams = 30000 // Oracle's limit is 65535; stay well under
	default: // postgres, mysql
		maxParams = 65535
	}
	n := (maxParams - 8) / ncols
	if n < 1 {
		n = 1
	}
	return n
}

// buildInsert dispatches to the dialect-appropriate statement builder. Postgres,
// MySQL and SQL Server use a multi-row INSERT (with ON CONFLICT / ON DUPLICATE /
// MERGE conflict handling); Oracle uses INSERT ALL — or MERGE for upsert/ignore —
// because it has no multi-row VALUES constructor.
func (s *jdbcSink) buildInsert(rows [][]interface{}) (string, []interface{}) {
	mergeable := (s.onConflict == conflictUpsert || s.onConflict == conflictIgnore) && len(s.conflictKey) > 0
	switch s.driver {
	case "oracle":
		if mergeable {
			return s.buildMerge(rows)
		}
		return s.buildInsertAllOracle(rows)
	case "sqlserver", "mssql":
		if mergeable {
			return s.buildMerge(rows)
		}
		return s.buildMultiRowInsert(rows)
	default: // postgres, mysql
		return s.buildMultiRowInsert(rows)
	}
}

// buildMultiRowInsert constructs a multi-row INSERT ... VALUES statement.
// For Postgres upsert: INSERT ... ON CONFLICT (key) DO UPDATE SET col=EXCLUDED.col
// For MySQL upsert:    INSERT ... ON DUPLICATE KEY UPDATE col=VALUES(col)
// For MySQL ignore:    INSERT IGNORE INTO ...
// For Postgres ignore: INSERT ... ON CONFLICT DO NOTHING
// SQL Server plain INSERT also uses this path; its upsert/ignore use buildMerge.
func (s *jdbcSink) buildMultiRowInsert(rows [][]interface{}) (string, []interface{}) {
	cols := s.columns
	ncols := len(cols)
	nrows := len(rows)

	var sb strings.Builder
	switch {
	case s.onConflict == conflictIgnore && s.driver == "mysql":
		sb.WriteString("INSERT IGNORE INTO ")
	default:
		// MySQL upsert uses plain INSERT INTO with ON DUPLICATE KEY UPDATE suffix
		// (not REPLACE INTO, which performs DELETE+INSERT and resets AUTO_INCREMENT,
		// fires DELETE triggers, and can violate FK constraints mid-statement).
		sb.WriteString("INSERT INTO ")
	}
	sb.WriteString(sqlutil.QuoteIdent(s.table, s.driver))
	sb.WriteString(" (")
	for i, col := range cols {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(sqlutil.QuoteIdent(col, s.driver))
	}
	sb.WriteString(") VALUES ")

	args := make([]interface{}, 0, nrows*ncols)
	for r := 0; r < nrows; r++ {
		if r > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		for c := 0; c < ncols; c++ {
			if c > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlutil.Placeholder(r*ncols+c+1, s.driver))
			var val interface{}
			if c < len(rows[r]) {
				val = rows[r][c]
			}
			args = append(args, val)
		}
		sb.WriteByte(')')
	}

	// Postgres UPSERT suffix.
	if s.onConflict == conflictUpsert && s.driver == "postgres" && len(s.conflictKey) > 0 {
		sb.WriteString(" ON CONFLICT (")
		for i, k := range s.conflictKey {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlutil.QuoteIdent(k, s.driver))
		}
		sb.WriteString(") DO UPDATE SET ")
		first := true
		for _, col := range cols {
			if !contains(s.conflictKey, col) {
				if !first {
					sb.WriteByte(',')
				}
				q := sqlutil.QuoteIdent(col, s.driver)
				sb.WriteString(q)
				sb.WriteString("=EXCLUDED.")
				sb.WriteString(q)
				first = false
			}
		}
	} else if s.onConflict == conflictIgnore && s.driver == "postgres" {
		sb.WriteString(" ON CONFLICT DO NOTHING")
	} else if s.onConflict == conflictUpsert && s.driver == "mysql" {
		// MySQL in-place upsert: update non-key columns using VALUES() reference.
		// VALUES(col) returns the value that was proposed for col in this INSERT,
		// which is correct for all MySQL versions. (MySQL 8.0.20+ also accepts the
		// row-alias syntax but VALUES() remains broadly compatible.)
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")
		first := true
		for _, col := range cols {
			if !contains(s.conflictKey, col) {
				if !first {
					sb.WriteByte(',')
				}
				q := sqlutil.QuoteIdent(col, s.driver)
				sb.WriteString(q)
				sb.WriteString("=VALUES(")
				sb.WriteString(q)
				sb.WriteByte(')')
				first = false
			}
		}
	}

	return sb.String(), args
}

// quotedColList returns the sink's columns as a comma-separated, driver-quoted
// identifier list, e.g. "id","amount" (postgres) or [id],[amount] (sql server).
func (s *jdbcSink) quotedColList() string {
	var sb strings.Builder
	for i, col := range s.columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(sqlutil.QuoteIdent(col, s.driver))
	}
	return sb.String()
}

// buildInsertAllOracle builds an Oracle multi-row INSERT. Oracle has no
// multi-row VALUES constructor, so each row is a separate INTO clause and the
// statement is terminated by a mandatory trailing SELECT from dual:
//
//	INSERT ALL INTO t (a,b) VALUES (:1,:2) INTO t (a,b) VALUES (:3,:4) SELECT 1 FROM dual
func (s *jdbcSink) buildInsertAllOracle(rows [][]interface{}) (string, []interface{}) {
	ncols := len(s.columns)
	table := sqlutil.QuoteIdent(s.table, s.driver)
	colList := s.quotedColList()

	var sb strings.Builder
	sb.WriteString("INSERT ALL")
	args := make([]interface{}, 0, len(rows)*ncols)
	p := 1
	for _, row := range rows {
		sb.WriteString(" INTO ")
		sb.WriteString(table)
		sb.WriteString(" (")
		sb.WriteString(colList)
		sb.WriteString(") VALUES (")
		for c := 0; c < ncols; c++ {
			if c > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(sqlutil.Placeholder(p, s.driver))
			p++
			var v interface{}
			if c < len(row) {
				v = row[c]
			}
			args = append(args, v)
		}
		sb.WriteByte(')')
	}
	sb.WriteString(" SELECT 1 FROM dual")
	return sb.String(), args
}

// buildMerge builds a MERGE upsert/ignore statement for SQL Server and Oracle,
// which have no ON CONFLICT / ON DUPLICATE KEY. The row source differs by dialect
// — SQL Server uses a VALUES table constructor, Oracle a UNION ALL of SELECTs
// from dual — but the ON / WHEN MATCHED / WHEN NOT MATCHED structure is shared.
// conflictIgnore (or a key-only table) omits the WHEN MATCHED UPDATE clause.
func (s *jdbcSink) buildMerge(rows [][]interface{}) (string, []interface{}) {
	oracle := s.driver == "oracle"
	cols := s.columns
	ncols := len(cols)
	table := sqlutil.QuoteIdent(s.table, s.driver)
	colList := s.quotedColList()

	// Non-key columns are the ones an upsert updates (a key column cannot be
	// updated inside MERGE — it is referenced by the ON clause).
	var nonKey []string
	for _, col := range cols {
		if !contains(s.conflictKey, col) {
			nonKey = append(nonKey, col)
		}
	}
	doUpdate := s.onConflict == conflictUpsert && len(nonKey) > 0

	args := make([]interface{}, 0, len(rows)*ncols)
	var sb strings.Builder
	sb.WriteString("MERGE INTO ")
	sb.WriteString(table)
	sb.WriteString(" tgt USING (")

	p := 1
	if oracle {
		// (SELECT :1 "A",:2 "B" FROM dual UNION ALL SELECT :3,:4 FROM dual)
		for r, row := range rows {
			if r > 0 {
				sb.WriteString(" UNION ALL ")
			}
			sb.WriteString("SELECT ")
			for c := 0; c < ncols; c++ {
				if c > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(sqlutil.Placeholder(p, s.driver))
				p++
				if r == 0 { // name source columns via the first SELECT of the UNION
					sb.WriteByte(' ')
					sb.WriteString(sqlutil.QuoteIdent(cols[c], s.driver))
				}
				var v interface{}
				if c < len(row) {
					v = row[c]
				}
				args = append(args, v)
			}
			sb.WriteString(" FROM dual")
		}
		sb.WriteString(") src")
	} else {
		// VALUES (@p1,@p2),(@p3,@p4)
		sb.WriteString("VALUES ")
		for r, row := range rows {
			if r > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')
			for c := 0; c < ncols; c++ {
				if c > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(sqlutil.Placeholder(p, s.driver))
				p++
				var v interface{}
				if c < len(row) {
					v = row[c]
				}
				args = append(args, v)
			}
			sb.WriteByte(')')
		}
		sb.WriteString(") AS src (")
		sb.WriteString(colList)
		sb.WriteByte(')')
	}

	sb.WriteString(" ON (")
	for i, k := range s.conflictKey {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		q := sqlutil.QuoteIdent(k, s.driver)
		sb.WriteString("tgt.")
		sb.WriteString(q)
		sb.WriteString("=src.")
		sb.WriteString(q)
	}
	sb.WriteByte(')')

	if doUpdate {
		sb.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		for i, col := range nonKey {
			if i > 0 {
				sb.WriteByte(',')
			}
			q := sqlutil.QuoteIdent(col, s.driver)
			sb.WriteString("tgt.")
			sb.WriteString(q)
			sb.WriteString("=src.")
			sb.WriteString(q)
		}
	}

	sb.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	sb.WriteString(colList)
	sb.WriteString(") VALUES (")
	for c, col := range cols {
		if c > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("src.")
		sb.WriteString(sqlutil.QuoteIdent(col, s.driver))
	}
	sb.WriteByte(')')
	if !oracle {
		sb.WriteByte(';') // SQL Server requires MERGE to be terminated with a semicolon
	}
	return sb.String(), args
}

// Close flushes any remaining buffered rows and closes the database connection.
// Both flush and db.Close errors are surfaced via errors.Join so neither is
// silently discarded (fixes bug 2: flush error was previously dropped with _).
func (s *jdbcSink) Close() error {
	flushErr := s.flush()
	dbErr := s.db.Close()
	return errors.Join(flushErr, dbErr)
}

// SinkStats implements sink.StatsReporter. RowsDeleted is only meaningful in
// delete-capable mode (opColumn set); RowsWritten/RowsFailed are left at -1
// ("not tracked" — the pipeline already tracks an aggregate RowsWritten count
// independently of this sink).
func (s *jdbcSink) SinkStats() sink.Stats {
	rd := int64(-1)
	if s.opColumn != "" {
		rd = s.rowsDeleted
	}
	return sink.Stats{RowsWritten: -1, RowsFailed: -1, RowsDeleted: rd}
}

// ReconciliationCount implements sink.Reconciler: runs query (typically
// "SELECT COUNT(*) FROM ...") against this sink's own connection and returns
// the single integer result. Used by a data contract's optional post-run
// reconciliation check. Must be called BEFORE Close() — Close() closes the
// underlying connection this method reuses.
func (s *jdbcSink) ReconciliationCount(query string) (int64, error) {
	var n int64
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		return 0, fmt.Errorf("jdbc sink: reconciliation query: %w", err)
	}
	return n, nil
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
