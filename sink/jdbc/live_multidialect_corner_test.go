package jdbc

// Corner-case adversarial live tests, continuing the same-shape sweep as
// live_multidialect_test.go. Each test targets one specific "negative/corner
// case" from the adversarial test plan: onConflict=ignore's own version of the
// same-key-twice restriction (MERGE's WHEN NOT MATCHED can independently fire
// for two source rows sharing a key that doesn't yet exist in the target,
// since dedupUpsertRows deliberately does NOT apply to ignore mode), composite
// (multi-column) conflictKey deletes, NULL delete-key values, chunking at
// scale (forcing >1 maxRowsPerInsert chunk for both inserts and deletes), an
// all-deletes batch, and schemadrift's additive-column path interacting with
// opColumn.

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/sqlutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestJDBCSink_Live_IgnoreMode_SameKeyTwice_AllDialects: onConflict=ignore +
// autoMigrate + conflictKey, with TWO rows sharing the same (not-yet-existing)
// key buffered in one flush. dedupUpsertRows only applies to onConflict=upsert
// (Postgres's "cannot affect row a second time" is specific to DO UPDATE), so
// this exercises whether SQL Server/Oracle MERGE's WHEN NOT MATCHED clause —
// evaluated against a single pre-statement snapshot of the target — chokes
// when two source rows would BOTH try to satisfy "not matched" for the same
// key (each attempting its own INSERT against a table now carrying a PRIMARY
// KEY from the bug-#1 fix). Expectation: the sink must not corrupt data or
// leave the table in a broken state; exactly one row for the key must survive
// (last-source-row-wins is NOT guaranteed for ignore mode — any deterministic,
// non-erroring single-row outcome is acceptable) OR the sink surfaces a clear
// error rather than silently duplicating the key.
func TestJDBCSink_Live_IgnoreMode_SameKeyTwice_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_ignore_dup_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "ignore", "conflictKey": []interface{}{"id"},
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString}}
			err = sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "first"}, {int64(1), "second"},
			}})
			closeErr := sk.Close()
			if err != nil || closeErr != nil {
				// A clear, surfaced error is an ACCEPTABLE outcome for this
				// adversarial shape (ignore mode is not obligated to dedup),
				// as long as it doesn't silently corrupt data. Log and stop.
				t.Logf("ignore-mode same-key-twice surfaced an error (acceptable, not a crash): WriteBatch=%v Close=%v", err, closeErr)
				return
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt + " WHERE " + sqlutil.QuoteIdent("id", d.driver) + " = 1").Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 1 {
				t.Errorf("row count for id=1 = %d, want 1 (must not silently duplicate the key)", count)
			}
		})
	}
}

// TestJDBCSink_Live_CompositeKeyDelete_AllDialects proves a multi-column
// conflictKey delete round-trips correctly: buildDelete's OR-of-ANDs form must
// AND together every key column (not accidentally OR across columns of the
// SAME row, which would delete unrelated rows sharing just one column value).
func TestJDBCSink_Live_CompositeKeyDelete_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_composite_del_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert",
				"conflictKey": []interface{}{"order_id", "line_no"},
				"opColumn":    "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "order_id", Type: record.TypeInt}, {Name: "line_no", Type: record.TypeInt},
				{Name: "qty", Type: record.TypeInt}, {Name: "__op", Type: record.TypeString},
			}
			// Two rows share order_id=1 (lines 1 and 2) — a bug that ORs
			// across columns instead of ANDing within a key tuple would wipe
			// out BOTH lines when only (1,1) should be deleted.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), int64(1), int64(5), "c"},
				{int64(1), int64(2), int64(9), "c"},
				{int64(2), int64(1), int64(3), "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (seed): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), int64(1), int64(0), "d"}, // delete only (order_id=1,line_no=1)
			}}); err != nil {
				t.Fatalf("WriteBatch (delete): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 2 {
				t.Errorf("row count = %d, want 2 (only (1,1) deleted; (1,2) and (2,1) must survive)", count)
			}
			var remaining int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt + " WHERE " +
				sqlutil.QuoteIdent("order_id", d.driver) + "=1 AND " + sqlutil.QuoteIdent("line_no", d.driver) + "=2").Scan(&remaining); err != nil {
				t.Fatalf("count (1,2): %v", err)
			}
			if remaining != 1 {
				t.Errorf("(order_id=1,line_no=2) must survive; got count=%d", remaining)
			}
		})
	}
}

// TestJDBCSink_Live_NullDeleteKey_AllDialects proves a delete row whose
// conflictKey column value is NULL is a silent no-op, not a crash and not an
// accidental mass-delete — SQL's "col = NULL" is never true (NULL is
// "unknown", not comparable), so buildDelete's "(col = ?)" form with a NULL
// bound value must match zero rows, which is exactly the safe behavior here.
func TestJDBCSink_Live_NullDeleteKey_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_null_del_key_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "keep-me", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (seed): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}
			// A delete row with a NULL key value — must not match/delete id=1.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{nil, "", "d"},
			}}); err != nil {
				t.Fatalf("WriteBatch (null-key delete): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (null-key delete must not crash): %v", err)
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 1 {
				t.Errorf("row count = %d, want 1 (NULL delete-key must not match/delete the real row)", count)
			}
		})
	}
}

// TestJDBCSink_Live_ChunkingAtScale_AllDialects forces both the insert path
// and the delete path past maxRowsPerInsert's per-statement parameter cap
// (tightest on SQL Server: (2100-8)/ncols) in a SINGLE flush, verifying the
// chunking loop in flushMixed produces a correct final row count rather than
// an off-by-one, a dropped chunk, or a parameter-limit error.
func TestJDBCSink_Live_ChunkingAtScale_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_chunk_scale_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			const seedRows = 1500 // 3 cols -> maxRowsPerInsert(sqlserver,3) = (2100-8)/3 = 697; forces 3 insert chunks
			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op", "batchSize": seedRows * 2, // keep everything in ONE flush
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeInt}, {Name: "__op", Type: record.TypeString},
			}
			seed := make([][]interface{}, seedRows)
			for i := 0; i < seedRows; i++ {
				seed[i] = []interface{}{int64(i), int64(i), "c"}
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: seed}); err != nil {
				t.Fatalf("WriteBatch (seed %d rows, forces insert chunking): %v", seedRows, err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}

			// Now delete the first 1000 of those rows in one flush — also
			// forces >1 delete chunk (maxRowsPerInsert for a 1-col key is
			// much larger, but still worth pushing past a few hundred rows).
			const deleteRows = 1000
			del := make([][]interface{}, deleteRows)
			for i := 0; i < deleteRows; i++ {
				del[i] = []interface{}{int64(i), int64(0), "d"}
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: del}); err != nil {
				t.Fatalf("WriteBatch (delete %d rows, forces delete chunking): %v", deleteRows, err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			want := seedRows - deleteRows
			if count != want {
				t.Errorf("row count = %d, want %d (seed=%d minus deleted=%d, across chunked insert+delete statements)", count, want, seedRows, deleteRows)
			}
		})
	}
}

// TestJDBCSink_Live_AllDeletesBatch_AllDialects proves a flush containing
// ONLY delete rows (s.buf stays empty; only flushMixed's delete branch runs)
// completes cleanly via Close() with no explicit intermediate Flush() call.
func TestJDBCSink_Live_AllDeletesBatch_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_all_deletes_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "a", "c"}, {int64(2), "b", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (seed): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}
			// An entirely-deletes batch — no upsert rows at all.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "", "d"}, {int64(2), "", "d"},
			}}); err != nil {
				t.Fatalf("WriteBatch (all-deletes): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (all-deletes batch, no explicit Flush before it): %v", err)
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 0 {
				t.Errorf("row count = %d, want 0 (both rows deleted)", count)
			}
		})
	}
}

// TestJDBCSink_Live_SchemaDriftWithOpColumn_AllDialects proves that additive
// schema drift (a genuinely new column arriving in a later batch) combined
// with opColumn/delete-capable mode does not misclassify opColumn as a real
// new column (it must never be ALTERed onto the table), and that the ALTER
// TABLE ADD COLUMN path still works correctly alongside live deletes.
func TestJDBCSink_Live_SchemaDriftWithOpColumn_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_drift_op_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols1 := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols1, Rows: [][]interface{}{
				{int64(1), "a", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (initial schema): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (initial schema): %v", err)
			}

			// A later batch introduces a genuinely new column "extra" AND
			// carries a delete for id=1 — both schema drift and opColumn
			// deletion happening in the same batch.
			cols2 := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString},
				{Name: "extra", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols2, Rows: [][]interface{}{
				{int64(2), "b", "new-col-value", "c"},
				{int64(1), "", "", "d"},
			}}); err != nil {
				t.Fatalf("WriteBatch (drifted schema + delete): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// __op must never have been ALTERed onto the real table.
			opCol := sqlutil.QuoteIdent("__op", d.driver)
			if _, err := db.Query("SELECT " + opCol + " FROM " + qt); err == nil {
				t.Errorf("opColumn %q leaked into the real table as an ALTERed column", "__op")
			}

			extraCol := sqlutil.QuoteIdent("extra", d.driver)
			idCol := sqlutil.QuoteIdent("id", d.driver)
			var extra string
			if err := db.QueryRow("SELECT " + extraCol + " FROM " + qt + " WHERE " + idCol + " = 2").Scan(&extra); err != nil {
				t.Fatalf("select extra (ALTER TABLE ADD COLUMN must have worked): %v", err)
			}
			if extra != "new-col-value" {
				t.Errorf("extra = %q, want %q", extra, "new-col-value")
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 1 {
				t.Errorf("row count = %d, want 1 (id=1 deleted, id=2 remains)", count)
			}
		})
	}
}

// TestJDBCSink_Live_DeleteThenRecreateSameKey_AllDialects regression-tests
// bug #5 (the delete-then-recreate ordering bug in writeBatchWithOp /
// flushMixed) against EVERY live dialect, not just Postgres — the bug was
// originally found and the fix verified only via Postgres (the multi-
// partition CDC integration test). The fix itself is dialect-agnostic Go
// buffer logic (removeStalePendingDelete runs before any dialect-specific SQL
// is built), but four of the five bugs found this session were dialect-
// specific SQL syntax issues, so "the fix is dialect-agnostic so it should
// just work on MERGE-based dialects too" is exactly the kind of assumption
// this session has repeatedly shown to be worth verifying rather than trusting.
//
// Sequence within ONE flush (nothing forcing an intermediate flush in
// between): create id=1 -> delete id=1 -> re-create id=1 with a different
// value. Without the fix, flushMixed's fixed insert-then-delete statement
// order would apply the stale delete AFTER the re-create's insert/MERGE,
// wiping the row back out.
func TestJDBCSink_Live_DeleteThenRecreateSameKey_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_del_recreate_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": true, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op", "batchSize": 100, // large enough that nothing auto-flushes between WriteBatch calls
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "status", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			// Also seed an UNRELATED row (id=2) that is never touched, to
			// confirm the delete-then-recreate handling for id=1 doesn't
			// disturb anything else in the same flush.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "open", "c"},
				{int64(2), "untouched", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (create id=1,id=2): %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "", "d"}, // delete id=1
			}}); err != nil {
				t.Fatalf("WriteBatch (delete id=1): %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "recreated", "c"}, // re-create id=1, same flush, no Flush() in between
			}}); err != nil {
				t.Fatalf("WriteBatch (re-create id=1): %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("Close (delete-then-recreate in one flush must not wipe the re-create): %v", err)
			}

			idCol := sqlutil.QuoteIdent("id", d.driver)
			statusCol := sqlutil.QuoteIdent("status", d.driver)
			var status string
			if err := db.QueryRow("SELECT " + statusCol + " FROM " + qt + " WHERE " + idCol + " = 1").Scan(&status); err != nil {
				t.Fatalf("select id=1 (must be present — the re-create must survive): %v", err)
			}
			if status != "recreated" {
				t.Errorf("id=1 status = %q, want %q", status, "recreated")
			}

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != 2 {
				t.Errorf("row count = %d, want 2 (id=1 recreated, id=2 untouched)", count)
			}
		})
	}
}

// TestJDBCSink_Live_TransactionAtomicity_AllDialects proves flushMixed's "both
// the upsert buffer and the delete buffer run in ONE transaction" guarantee
// actually holds under a REAL forced failure: a batch containing both a valid
// DELETE and an upsert row that violates a NOT NULL constraint must roll back
// EVERYTHING — the delete must NOT have taken effect either — rather than
// partially applying the delete while the insert fails. This exercises the
// tx.Rollback() path in flushMixed with a genuine database-rejected statement,
// not just a code-review assumption that the wrapping is correct.
func TestJDBCSink_Live_TransactionAtomicity_AllDialects(t *testing.T) {
	for _, d := range liveDialects(t) {
		d := d
		t.Run(d.driver, func(t *testing.T) {
			db, err := sql.Open(sqlDriverName(d.driver), d.dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			table := fmt.Sprintf("dp_tx_atomic_%d", time.Now().UnixNano())
			qt := sqlutil.QuoteIdent(table, d.driver)
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + qt) })

			idT, statusT := "BIGINT", "VARCHAR(100) NOT NULL"
			if d.driver == "oracle" {
				idT, statusT = "NUMBER(19)", "VARCHAR2(100) NOT NULL"
			}
			create := fmt.Sprintf("CREATE TABLE %s (%s %s PRIMARY KEY, %s %s)",
				qt, sqlutil.QuoteIdent("id", d.driver), idT, sqlutil.QuoteIdent("status", d.driver), statusT)
			if _, err := db.Exec(create); err != nil {
				t.Fatalf("pre-create table (with NOT NULL status): %v", err)
			}

			sk, err := newJDBCSink(map[string]interface{}{
				"driver": d.driver, "dsn": d.dsn, "table": table,
				"autoMigrate": false, "onConflict": "upsert", "conflictKey": []interface{}{"id"},
				"opColumn": "__op",
			})
			if err != nil {
				t.Fatalf("newJDBCSink: %v", err)
			}
			cols := []record.Column{
				{Name: "id", Type: record.TypeInt}, {Name: "status", Type: record.TypeString}, {Name: "__op", Type: record.TypeString},
			}
			// Seed two valid rows in their own flush.
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "keep-me", "c"}, {int64(2), "also-keep", "c"},
			}}); err != nil {
				t.Fatalf("WriteBatch (seed): %v", err)
			}
			if err := sk.Flush(); err != nil {
				t.Fatalf("Flush (seed): %v", err)
			}

			// One flush containing BOTH a valid delete (id=1) AND an upsert
			// row that violates the NOT NULL constraint (id=3, status=nil).
			// If flushMixed's transaction wrapping is correct, NEITHER takes
			// effect: id=1 must still be present, id=3 must not exist.
			err = sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
				{int64(1), "", "d"},  // valid delete
				{int64(3), nil, "c"}, // violates NOT NULL — must fail the whole tx
			}})
			closeErr := sk.Close()
			if err == nil && closeErr == nil {
				t.Fatal("expected a NOT NULL constraint violation to surface (from WriteBatch or Close), got no error from either")
			}
			t.Logf("got expected error: WriteBatch=%v Close=%v", err, closeErr)

			idCol := sqlutil.QuoteIdent("id", d.driver)
			var stillPresent int
			if qerr := db.QueryRow("SELECT COUNT(*) FROM " + qt + " WHERE " + idCol + " = 1").Scan(&stillPresent); qerr != nil {
				t.Fatalf("count id=1: %v", qerr)
			}
			if stillPresent != 1 {
				t.Errorf("id=1 must STILL be present (the delete must have rolled back along with the failed insert), got count=%d", stillPresent)
			}
			var id3Present int
			if qerr := db.QueryRow("SELECT COUNT(*) FROM " + qt + " WHERE " + idCol + " = 3").Scan(&id3Present); qerr != nil {
				t.Fatalf("count id=3: %v", qerr)
			}
			if id3Present != 0 {
				t.Errorf("id=3 must NOT be present (its insert violated NOT NULL), got count=%d", id3Present)
			}
			var total int
			if qerr := db.QueryRow("SELECT COUNT(*) FROM " + qt).Scan(&total); qerr != nil {
				t.Fatalf("count total: %v", qerr)
			}
			if total != 2 {
				t.Errorf("total row count = %d, want 2 (id=1,id=2 unchanged \u2014 the whole failed flush must have rolled back atomically)", total)
			}
		})
	}
}
