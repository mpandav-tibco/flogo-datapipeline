package jdbc

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func setupJDBCSrcTable(t *testing.T, dsn string, n int) string {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	table := fmt.Sprintf("dp_src_test_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE TABLE " + table + " (id INT PRIMARY KEY, label TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := db.Exec("INSERT INTO "+table+" (id,label) VALUES ($1,$2)", i, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table); _ = db.Close() })
	return table
}

func drainSource(t *testing.T, src source.Source) int {
	t.Helper()
	total := 0
	for {
		b, err := src.ReadBatch(10)
		if b != nil {
			total += b.Len()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if b == nil {
			break
		}
	}
	return total
}

func TestJDBCSource_LiveReadAndCursor(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC source test")
	}
	table := setupJDBCSrcTable(t, dsn, 25)

	src, err := newJDBCSource(map[string]interface{}{
		"driver": "postgres", "dsn": dsn, "table": table, "orderKey": "id",
	})
	if err != nil {
		t.Fatalf("newJDBCSource: %v", err)
	}
	defer src.Close()

	if len(src.Schema()) != 2 {
		t.Errorf("Schema() = %v, want 2 columns", src.Schema())
	}
	if got := drainSource(t, src); got != 25 {
		t.Errorf("read %d rows, want 25", got)
	}
	// Cursor returns a non-empty JSON snapshot for the checkpoint layer.
	js := src.(*jdbcSource)
	if len(js.Cursor()) == 0 {
		t.Error("Cursor() returned empty payload")
	}
}

func TestJDBCSource_Skip(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC source test")
	}
	table := setupJDBCSrcTable(t, dsn, 10)

	src, err := newJDBCSource(map[string]interface{}{
		"driver": "postgres", "dsn": dsn, "table": table, "orderKey": "id",
	})
	if err != nil {
		t.Fatalf("newJDBCSource: %v", err)
	}
	defer src.Close()

	if err := src.Skip(3); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	b, err := src.ReadBatch(100)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b == nil || b.Len() != 7 {
		t.Fatalf("after Skip(3) read %v rows, want 7", b)
	}
	if got := fmt.Sprintf("%v", b.Value(0, "label")); got != "row-4" {
		t.Errorf("first label after Skip(3) = %q, want row-4", got)
	}
}

func TestJDBCSource_Partitions(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC source test")
	}
	table := setupJDBCSrcTable(t, dsn, 25)

	src, err := newJDBCSource(map[string]interface{}{
		"driver": "postgres", "dsn": dsn, "table": table, "orderKey": "id",
	})
	if err != nil {
		t.Fatalf("newJDBCSource: %v", err)
	}
	defer src.Close()

	ps, ok := src.(source.PartitionableSource)
	if !ok {
		t.Fatal("jdbc source does not implement PartitionableSource")
	}
	parts, err := ps.Partitions(3)
	if err != nil {
		t.Fatalf("Partitions: %v", err)
	}
	if len(parts) < 2 {
		t.Fatalf("got %d partitions, want >= 2", len(parts))
	}

	total := 0
	for _, p := range parts {
		child, err := ps.OpenPartition(p)
		if err != nil {
			t.Fatalf("OpenPartition(%s): %v", p.ID, err)
		}
		total += drainSource(t, child)
		_ = child.Close()
	}
	if total != 25 {
		t.Errorf("partitioned read total = %d, want 25", total)
	}
}

func TestNewJDBCSource_Errors(t *testing.T) {
	if _, err := newJDBCSource(map[string]interface{}{"table": "t"}); err == nil {
		t.Error("missing dsn expected an error")
	}
	if _, err := newJDBCSource(map[string]interface{}{"dsn": "postgres://x"}); err == nil {
		t.Error("missing table/query expected an error")
	}
	// SQL Server / Oracle require an orderKey in table mode (OFFSET/FETCH needs ORDER BY).
	if _, err := newJDBCSource(map[string]interface{}{"driver": "oracle", "dsn": "oracle://x", "table": "t"}); err == nil {
		t.Error("oracle without orderKey expected an error")
	}
}

// TestJDBCSource_LiveMidStreamErrorReleasesCursor: a query that fails partway
// through (rows.Err) must close the cursor instead of holding it until Close.
func TestJDBCSource_LiveMidStreamErrorReleasesCursor(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC source test")
	}
	src, err := newJDBCSource(map[string]interface{}{
		"driver": "postgres", "dsn": dsn,
		"query": "SELECT x, 10/(x-50000) AS y FROM generate_series(1,100000) AS x",
	})
	if err != nil {
		t.Fatalf("newJDBCSource: %v", err)
	}
	defer src.Close()
	var readErr error
	for i := 0; i < 1000 && readErr == nil; i++ {
		_, readErr = src.ReadBatch(1000)
	}
	if readErr == nil || readErr == io.EOF {
		t.Fatalf("want division-by-zero error, got %v", readErr)
	}
	if js := src.(*jdbcSource); js.rows != nil {
		t.Fatal("cursor still open after a mid-stream error")
	}
}
