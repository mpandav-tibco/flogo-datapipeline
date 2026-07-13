// Package jdbc registers a JDBC-style sink that writes RecordBatches to any
// database/sql-compatible database (PostgreSQL, MySQL, SQLite, Oracle via ODBC).
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/jdbc"
//
// The database driver must also be imported:
//
//	import _ "github.com/lib/pq"            // PostgreSQL
//	import _ "github.com/go-sql-driver/mysql" // MySQL
//	import _ "github.com/mattn/go-sqlite3"  // SQLite
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
}

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

	batchSize := 1000
	if bs, ok := cfg["batchSize"].(int); ok && bs > 0 {
		batchSize = bs
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

	return &jdbcSink{
		db:          db,
		table:       table,
		batchSize:   batchSize,
		conflictKey: conflictKey,
		onConflict:  onConflict,
		driver:      driver,
		autoMigrate: autoMigrate,
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

	// Issue ALTER TABLE ... ADD COLUMN for each new column.
	for _, col := range newCols {
		sqlType := recordTypeToSQL(col.Type, s.driver)
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			sqlutil.QuoteIdent(s.table, s.driver),
			sqlutil.QuoteIdent(col.Name, s.driver),
			sqlType,
		)
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
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		sqlutil.QuoteIdent(s.table, s.driver),
		strings.Join(cols, ", "),
	)
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
		return "BIGINT"
	case record.TypeFloat:
		return "DOUBLE PRECISION"
	case record.TypeDecimal:
		return "NUMERIC"
	case record.TypeBool:
		switch driver {
		case "mysql":
			return "TINYINT(1)"
		default:
			return "BOOLEAN"
		}
	case record.TypeDate:
		switch driver {
		case "mysql":
			return "DATETIME"
		default:
			return "TIMESTAMP WITH TIME ZONE"
		}
	default: // TypeString, TypeAny, unknown
		switch driver {
		case "mysql":
			return "VARCHAR(65535)"
		default:
			return "TEXT"
		}
	}
}

// Flush writes all buffered rows to the database.
func (s *jdbcSink) Flush() error {
	return s.flush()
}

func (s *jdbcSink) flush() error {
	if len(s.buf) == 0 {
		return nil
	}

	// s.buf already contains sink-owned [][]interface{} rows; use directly.
	allRows := s.buf
	s.buf = s.buf[:0]
	s.rowsBuffered = 0

	if len(allRows) == 0 {
		return nil
	}

	// Build and execute the bulk INSERT.
	query, args := s.buildInsert(allRows)
	if _, err := s.db.Exec(query, args...); err != nil {
		return fmt.Errorf("jdbc sink: exec insert: %w", err)
	}
	return nil
}

// buildInsert constructs a multi-row INSERT statement.
// For Postgres upsert: INSERT ... ON CONFLICT (key) DO UPDATE SET col=EXCLUDED.col
// For MySQL upsert:    INSERT ... ON DUPLICATE KEY UPDATE col=VALUES(col)
// For MySQL ignore:    INSERT IGNORE INTO ...
// For Postgres ignore: INSERT ... ON CONFLICT DO NOTHING
func (s *jdbcSink) buildInsert(rows [][]interface{}) (string, []interface{}) {
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

// Close flushes any remaining buffered rows and closes the database connection.
// Both flush and db.Close errors are surfaced via errors.Join so neither is
// silently discarded (fixes bug 2: flush error was previously dropped with _).
func (s *jdbcSink) Close() error {
	flushErr := s.flush()
	dbErr := s.db.Close()
	return errors.Join(flushErr, dbErr)
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
