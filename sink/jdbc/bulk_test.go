package jdbc

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestInsertSQLCacheMatchesBuilder(t *testing.T) {
	for _, drv := range []string{"postgres", "mysql", "sqlserver", "oracle"} {
		for _, oc := range []conflictStrategy{conflictInsert, conflictUpsert, conflictIgnore} {
			s := &jdbcSink{driver: drv, table: "t", columns: []string{"id", "v"}, onConflict: oc, conflictKey: []string{"id"}}
			rows := [][]interface{}{{int64(1), "a"}, {int64(2)}} // second row short → nil-padded
			for round := 0; round < 2; round++ {                  // round 1 hits the cache
				gotQ, gotA := s.insertSQL(rows)
				wantQ, wantA := s.buildInsert(rows)
				if gotQ != wantQ || !reflect.DeepEqual(gotA, wantA) {
					t.Fatalf("%s/%s round %d: cached SQL/args diverge from builder\n got %q %v\nwant %q %v", drv, oc, round, gotQ, gotA, wantQ, wantA)
				}
			}
			// Schema change must invalidate the cache.
			s.columns = append(s.columns, "extra")
			gotQ, _ := s.insertSQL(rows)
			if wantQ, _ := s.buildInsert(rows); gotQ != wantQ {
				t.Fatalf("%s/%s: stale SQL after column change: %q", drv, oc, gotQ)
			}
		}
	}
}

func TestAllocRowIsolation(t *testing.T) {
	s := &jdbcSink{batchSize: 2}
	a, b := s.allocRow(3), s.allocRow(3)
	a = append(a, "spill") // must reallocate, not overwrite b
	if b[0] != nil || len(a) != 4 {
		t.Fatalf("rows alias: a=%v b=%v", a, b)
	}
}

func TestBulkModeValidation(t *testing.T) {
	_, err := newJDBCSink(map[string]interface{}{"driver": "postgres", "dsn": "x", "table": "t", "bulkMode": "fast"})
	if err == nil {
		t.Fatal("invalid bulkMode accepted")
	}
}

// TestJDBCSink_LiveCopy writes the same data through COPY (bulkMode auto) and
// multi-row INSERT (bulkMode insert) and requires identical table contents,
// including NULLs, decimals, timestamps, quoting-sensitive text and a load
// larger than one INSERT chunk.
func TestJDBCSink_LiveCopy(t *testing.T) {
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live JDBC COPY test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "Name", Type: record.TypeString},
		{Name: "amount", Type: record.TypeDecimal, Scale: 2},
		{Name: "at", Type: record.TypeDate},
		{Name: "ok", Type: record.TypeBool},
	}
	const n = 30000 // > maxRowsPerInsert(postgres, 5) to exercise chunking on the INSERT side
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	var rows [][]interface{}
	for i := 0; i < n; i++ {
		d, _ := record.ParseDecimal(fmt.Sprintf("%d.%02d", i, i%100))
		var name interface{} = fmt.Sprintf("n\t%d \"q\" \\ ,%d", i, i)
		if i%7 == 0 {
			name = nil
		}
		rows = append(rows, []interface{}{int64(i), name, d, base.Add(time.Duration(i) * time.Second), i%2 == 0})
	}
	dump := func(table string) []string {
		r, err := db.Query(fmt.Sprintf(`SELECT id, COALESCE("Name",'<NULL>'), amount::text, at, ok FROM %q ORDER BY id`, table))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var out []string
		for r.Next() {
			var id int64
			var name, amt string
			var at time.Time
			var ok bool
			if err := r.Scan(&id, &name, &amt, &at, &ok); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%d|%s|%s|%s|%v", id, name, amt, at.UTC().Format(time.RFC3339), ok))
		}
		return out
	}
	results := map[string][]string{}
	for _, mode := range []string{"auto", "insert"} {
		table := fmt.Sprintf("dp_copy_%s_%d", mode, time.Now().UnixNano())
		t.Cleanup(func() { _, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %q", table)) })
		sk, err := newJDBCSink(map[string]interface{}{
			"driver": "postgres", "dsn": dsn, "table": table, "autoMigrate": true,
			"batchSize": 100000, "bulkMode": mode,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := sk.(*jdbcSink).useCopy; got != (mode == "auto") {
			t.Fatalf("mode %s: useCopy=%v", mode, got)
		}
		start := time.Now()
		for i := 0; i < n; i += 5000 {
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: rows[i : i+5000]}); err != nil {
				t.Fatalf("%s WriteBatch: %v", mode, err)
			}
		}
		if err := sk.Close(); err != nil {
			t.Fatalf("%s Close: %v", mode, err)
		}
		t.Logf("bulkMode=%s: %d rows in %v", mode, n, time.Since(start))
		results[mode] = dump(table)
	}
	if len(results["auto"]) != n || !reflect.DeepEqual(results["auto"], results["insert"]) {
		t.Fatalf("COPY and INSERT produced different tables (len %d vs %d)", len(results["auto"]), len(results["insert"]))
	}
}
