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

// TestJDBCSink_LiveWrite exercises the full write path against live Postgres:
// newJDBCSink, autoMigrate ensureTable, WriteBatch (incl. the same-schema
// schemasEqual fast path on the 2nd batch), flush, Flush and Close.
func TestJDBCSink_LiveWrite(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC sink test")
	}
	table := fmt.Sprintf("dp_sink_test_%d", time.Now().UnixNano())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	sk, err := newJDBCSink(map[string]interface{}{
		"driver": "postgres", "dsn": dsn, "table": table,
		"autoMigrate": true, "batchSize": 100,
	})
	if err != nil {
		t.Fatalf("newJDBCSink: %v", err)
	}

	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "name", Type: record.TypeString},
		{Name: "amount", Type: record.TypeDecimal, Scale: 2},
	}
	mk := func(id int64, name, amt string) []interface{} {
		d, _ := record.ParseDecimal(amt)
		return []interface{}{id, name, d}
	}
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		mk(1, "alice", "10.50"), mk(2, "bob", "20.00"),
	}}); err != nil {
		t.Fatalf("WriteBatch 1: %v", err)
	}
	// A second batch with the same schema exercises the schemasEqual fast path.
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		mk(3, "carol", "30.25"),
	}}); err != nil {
		t.Fatalf("WriteBatch 2: %v", err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := sk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("row count = %d, want 3", count)
	}
	var amt string
	if err := db.QueryRow("SELECT amount::text FROM " + table + " WHERE id=1").Scan(&amt); err != nil {
		t.Fatalf("select amount: %v", err)
	}
	if amt != "10.50" {
		t.Errorf("amount = %q, want 10.50 (decimal fidelity)", amt)
	}
}

func TestNewJDBCSink_ConstructErrors(t *testing.T) {
	if _, err := newJDBCSink(map[string]interface{}{"table": "t"}); err == nil {
		t.Error("missing dsn expected an error")
	}
	if _, err := newJDBCSink(map[string]interface{}{"dsn": "postgres://x"}); err == nil {
		t.Error("missing table expected an error")
	}
}
