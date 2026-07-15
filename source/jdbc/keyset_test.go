package jdbc

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestKeysetPagination_ReadsAllRows is a regression test for the bug where a
// keyset (orderKey) jdbc source stopped after a single page: when a page filled
// to exactly `size` rows the per-page cursor was left open, so the next
// ReadBatch read the drained cursor and returned io.EOF instead of the next
// page. It reads a multi-page table via keyset and asserts every row is returned
// exactly once, in ascending key order.
//
// Skipped unless DP_TEST_PG_DSN is set, e.g.
//
//	host=localhost port=55432 dbname=warehouse user=app password=secret sslmode=disable
func TestKeysetPagination_ReadsAllRows(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the keyset pagination regression test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const table = "dp_keyset_test"
	const total = 275 // deliberately not a multiple of the page size
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE " + table + " (id INT PRIMARY KEY, label TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS " + table)
	for i := 1; i <= total; i++ {
		if _, err := db.Exec("INSERT INTO "+table+" (id, label) VALUES ($1, $2)", i, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	src, err := newJDBCSource(map[string]interface{}{
		"driver":    "postgres",
		"dsn":       dsn,
		"table":     table,
		"orderKey":  "id", // keyset mode
		"batchSize": 50,
	})
	if err != nil {
		t.Fatalf("newJDBCSource: %v", err)
	}
	defer src.Close()

	var got []int
	for {
		batch, rErr := src.ReadBatch(50)
		if batch != nil {
			idx := batch.ColIndex("id")
			for _, row := range batch.Rows {
				got = append(got, toInt(row[idx]))
			}
			record.Put(batch)
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			t.Fatalf("ReadBatch: %v", rErr)
		}
	}

	if len(got) != total {
		t.Fatalf("read %d rows, want %d (keyset pagination stopped early?)", len(got), total)
	}
	for i, id := range got {
		if id != i+1 {
			t.Fatalf("row %d = id %d, want %d (out of order or gap)", i, id, i+1)
		}
	}
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	default:
		return -1
	}
}
