package jdbc

// Adversarial, multi-dialect live tests: the same class of bug the Postgres
// tests found (autoMigrate creating a table with no usable conflict target;
// "same key twice in one flush" breaking a multi-row upsert statement) is
// exercised here against SQL Server and Oracle too, since both use a
// completely different upsert mechanism (MERGE, not ON CONFLICT) that could
// plausibly have its own version of the restriction — or none at all. Skips
// per-dialect when its DSN env var is unset.

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

type dialectFixture struct {
	driver string
	dsn    string
	// countQuery returns "SELECT COUNT(*) FROM <table>" for the dialect (all
	// four use the same syntax, kept as a field for clarity/extension).
}

func liveDialects(t *testing.T) []dialectFixture {
	t.Helper()
	var out []dialectFixture
	if dsn := os.Getenv("DP_TEST_PG_DSN"); dsn != "" {
		out = append(out, dialectFixture{driver: "postgres", dsn: dsn})
	}
	if dsn := os.Getenv("DP_TEST_MSSQL_DSN"); dsn != "" {
		out = append(out, dialectFixture{driver: "sqlserver", dsn: dsn})
	}
	if dsn := os.Getenv("DP_TEST_ORACLE_DSN"); dsn != "" {
		out = append(out, dialectFixture{driver: "oracle", dsn: dsn})
	}
	if len(out) == 0 {
		t.Skip("set DP_TEST_PG_DSN / DP_TEST_MSSQL_DSN / DP_TEST_ORACLE_DSN to run the multi-dialect adversarial jdbc tests")
	}
	return out
}

func sqlDriverName(dialect string) string {
	switch dialect {
	case "sqlserver":
		return "sqlserver"
	case "oracle":
		return "oracle"
	default:
		return "postgres"
	}
}

// TestJDBCSink_Live_AutoMigrateUpsertFreshTable_AllDialects regression-tests
// the ensureTable primary-key fix across every available live dialect: a
// fresh table, autoMigrate:true, onConflict:upsert must succeed on the very
// first write, not just on Postgres.
func TestJDBCSink_Live_AutoMigrateUpsertFreshTable_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_am_upsert_%d", time.Now().UnixNano())
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + sqlutil.QuoteIdent(table, d.driver)) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "alice"}, {int64(2), "bob"},
			}}); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (autoMigrate+upsert on a fresh table must succeed): %v", err)
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + sqlutil.QuoteIdent(table, d.driver)).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 2 {
				t.Errorf("row count = %d, want 2", count)
			}
		})
	}
}

// TestJDBCSink_Live_SameKeyTwiceInOneFlush_AllDialects regression-tests the
// dedup-before-upsert fix across every available live dialect: two changes to
// the SAME conflict key, buffered in the same flush with nothing forcing an
// intermediate checkpoint (the routine shape of a real CDC stream), must not
// break the sink — the final state must reflect the LAST write.
func TestJDBCSink_Live_SameKeyTwiceInOneFlush_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_dup_key_%d", time.Now().UnixNano())
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + sqlutil.QuoteIdent(table, d.driver)) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"batchSize": 100, // large enough that nothing auto-flushes between WriteBatch calls
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "status", Type: record.TypeString}}

			// Two WriteBatch calls, no Flush() in between — both land in the
			// SAME buffered flush at Close(), exactly like a batch-mode CDC
			// consumer draining several changes to the same row in one poll.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "open"},
			}}); err != nil {
				t.Fatalf("WriteBatch 1: %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "closed"}, // same id=1 again, before any flush
			}}); err != nil {
				t.Fatalf("WriteBatch 2: %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (same key twice in one flush must not break the upsert): %v", err)
			}

			qt := sqlutil.QuoteIdent(table, d.driver)
			qStatus := sqlutil.QuoteIdent("status", d.driver)
			qID := sqlutil.QuoteIdent("id", d.driver)
			var status string
			if err := db.QueryRow("SELECT " + qStatus + " FROM " + qt + " WHERE " + qID + " = 1").Scan(&status); err != nil {
				t.Fatalf("select: %v", err)
			}
			if status != "closed" {
				t.Errorf("status = %q, want %q (the LAST write must win)", status, "closed")
			}
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 1 {
				t.Errorf("row count = %d, want 1 (not 2 — must not have inserted twice)", count)
			}
		})
	}
}

// TestJDBCSink_Live_DeleteCapable_AllDialects proves the opColumn mixed
// insert+update+delete transaction works on every available live dialect, not
// just Postgres — SQL Server and Oracle build the insert side very
// differently (MERGE / INSERT ALL) from the DELETE side (the shared
// OR-of-ANDs form), so a dialect-specific interaction bug is plausible.
func TestJDBCSink_Live_DeleteCapable_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_del_cap_%d", time.Now().UnixNano())
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + sqlutil.QuoteIdent(table, d.driver)) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "status", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "open", "c"}, {int64(2), "open", "c"}, {int64(3), "open", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (seed): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "closed", "u"}, {int64(2), "", "d"}, {int64(4), "open", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (mixed): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (mixed insert+delete transaction): %v", err)
			}

			qID := sqlutil.QuoteIdent("id", d.driver)
			qStatus := sqlutil.QuoteIdent("status", d.driver)
			rows, err := db.Query("SELECT " + qID + ", " + qStatus + " FROM " + sqlutil.QuoteIdent(table, d.driver) + " ORDER BY " + qID)
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			defer rows.Close()
			type gotRow struct {
				id     int64
				status string
			}
			var got []gotRow
			for rows.Next() {
				var r gotRow
				if err := rows.Scan(&r.id, &r.status); err != nil {
					t.Fatalf("scan: %v", err)
				}
				got = append(got, r)
			}
			want := []gotRow{{1, "closed"}, {3, "open"}, {4, "open"}}
			if len(got) != len(want) {
				t.Fatalf("rows = %+v, want %+v (id=2 must have been deleted)", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// TestJDBCSink_Live_DeleteByNonexistentKey_AllDialects proves deleting a key
// that does not exist in the table is a harmless no-op, not an error — the
// natural case when a CDC delete event arrives for a row the target never
// received (e.g. it was inserted and deleted upstream between snapshot and
// stream cutover).
func TestJDBCSink_Live_DeleteByNonexistentKey_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_del_missing_%d", time.Now().UnixNano())
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + sqlutil.QuoteIdent(table, d.driver)) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "status", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			// A delete for a key that was never inserted — must not error.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(999), "", "d"},
			}}); err != nil {
				t.Fatalf("WriteBatch (delete of nonexistent key): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (delete-only batch, no matching row): %v", err)
			}
		})
	}
}
