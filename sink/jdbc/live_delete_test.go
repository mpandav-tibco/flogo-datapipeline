package jdbc

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestJDBCSink_LiveDeleteCapableMode exercises the full opColumn path against
// live Postgres: an initial batch of plain upserts, then a second batch that
// mixes an upsert, an update, and two deletes in the SAME batch, proving the
// mixed insert+delete transaction (flushMixed) commits both correctly.
func TestJDBCSink_LiveDeleteCapableMode(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC delete-capable sink test")
	}
	table := fmt.Sprintf("dp_sink_delete_test_%d", time.Now().UnixNano())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	sk, err := newJDBCSink(map[string]interface{}{
		"driver": "postgres", "dsn": dsn, "table": table,
		"autoMigrate": true, "batchSize": 100,
		"onConflict": "upsert", "conflictKey": []interface{}{"id"},
		"opColumn": "__op",
	})
	if err != nil {
		t.Fatalf("newJDBCSink: %v", err)
	}

	// __op is metadata: present in every batch, but must NOT become a table
	// column (ensureTable/autoMigrate must create only id/name/status).
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "name", Type: record.TypeString},
		{Name: "status", Type: record.TypeString},
		{Name: "__op", Type: record.TypeString},
	}
	row := func(id int64, name, status, op string) []interface{} {
		return []interface{}{id, name, status, op}
	}

	// Seed 3 rows (op values other than a delete marker are all upserts).
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		row(1, "alice", "open", "c"),
		row(2, "bob", "open", "c"),
		row(3, "carol", "open", "c"),
	}}); err != nil {
		t.Fatalf("WriteBatch (seed): %v", err)
	}
	// Flush the seed rows before the mixed batch below re-upserts id=1: Postgres
	// rejects "ON CONFLICT DO UPDATE" touching the same conflict key twice
	// within one statement, so the seed and the update must land in separate
	// statements — exactly like a real run's periodic checkpoint flush would.
	if err := sk.Flush(); err != nil {
		t.Fatalf("Flush (seed): %v", err)
	}

	// Mixed batch: update id=1, delete id=2 and id=3, insert id=4 — all in one
	// WriteBatch/flush, proving the mixed transaction handles both operations.
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		row(1, "alice", "closed", "u"),
		row(2, "bob", "open", "d"),
		row(3, "carol", "open", "delete"),
		row(4, "dave", "open", "c"),
	}}); err != nil {
		t.Fatalf("WriteBatch (mixed): %v", err)
	}
	if err := sk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify __op never became a table column.
	var colCount int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = '__op'", table,
	).Scan(&colCount); err != nil {
		t.Fatalf("check __op column: %v", err)
	}
	if colCount != 0 {
		t.Error("__op leaked into the target table as a real column")
	}

	rows, err := db.Query("SELECT id, name, status FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	type gotRow struct {
		id           int64
		name, status string
	}
	var got []gotRow
	for rows.Next() {
		var r gotRow
		if err := rows.Scan(&r.id, &r.name, &r.status); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	want := []gotRow{
		{1, "alice", "closed"}, // updated
		{4, "dave", "open"},    // inserted
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %+v, want %+v (id=2/3 should have been deleted)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
