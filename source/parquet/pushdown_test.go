package parquet

import (
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestColumnProjection verifies that requesting a column subset yields a
// projected schema and rows of exactly that width, in the requested order.
func TestColumnProjection(t *testing.T) {
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "amount", Type: record.TypeFloat},
		{Name: "region", Type: record.TypeString},
	}
	rows := [][]interface{}{
		{int64(1), 10.0, "US"},
		{int64(2), 20.0, "EU"},
	}
	path := filepath.Join(t.TempDir(), "proj.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"columns":             []interface{}{"id", "region"},
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	defer src.Close()

	sch := src.Schema()
	if len(sch) != 2 || sch[0].Name != "id" || sch[1].Name != "region" {
		t.Fatalf("projected schema = %+v, want [id region]", sch)
	}
	got := readAllRows(t, src, 10)
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2", len(got))
	}
	if len(got[0]) != 2 {
		t.Fatalf("projected row width = %d, want 2", len(got[0]))
	}
	if got[0][0] != int64(1) || got[0][1] != "US" {
		t.Errorf("row0 = %v, want [1 US]", got[0])
	}
	if got[1][0] != int64(2) || got[1][1] != "EU" {
		t.Errorf("row1 = %v, want [2 EU]", got[1])
	}
}

// TestPredicateFilterNumeric exercises numeric predicate pushdown: two of three
// row groups have all values below the threshold (prunable by statistics) and
// only the matching row group's rows are emitted.
func TestPredicateFilterNumeric(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeInt}}
	var rows [][]interface{}
	for _, base := range []int64{0, 100, 20} { // group0: 0..9, group1: 100..109, group2: 20..29
		for i := int64(0); i < 10; i++ {
			rows = append(rows, []interface{}{base + i, base + i})
		}
	}
	path := filepath.Join(t.TempDir(), "filt.parquet")
	writeTestParquet(t, path, cols, rows, 10 /* 3 row groups */, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"filter":              "amount >= 100",
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	defer src.Close()

	got := readAllRows(t, src, 8)
	if len(got) != 10 {
		t.Fatalf("rows = %d, want 10 (only the 100..109 group)", len(got))
	}
	for _, r := range got {
		if amt, _ := r[1].(int64); amt < 100 {
			t.Errorf("emitted amount %d, want >= 100", amt)
		}
	}
}

// TestPredicateFilterOnUnprojectedColumn verifies the filter column is read for
// evaluation even when it is not part of the projection.
func TestPredicateFilterOnUnprojectedColumn(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeInt}}
	rows := [][]interface{}{
		{int64(1), int64(50)},
		{int64(2), int64(150)},
		{int64(3), int64(250)},
	}
	path := filepath.Join(t.TempDir(), "filtproj.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"columns":             []interface{}{"id"}, // amount is NOT projected
		"filter":              "amount > 100",
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	defer src.Close()

	if w := len(src.Schema()); w != 1 {
		t.Fatalf("schema width = %d, want 1", w)
	}
	got := readAllRows(t, src, 10)
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2 (ids 2,3)", len(got))
	}
	if len(got[0]) != 1 {
		t.Fatalf("row width = %d, want 1", len(got[0]))
	}
	if got[0][0] != int64(2) || got[1][0] != int64(3) {
		t.Errorf("ids = %v,%v want 2,3", got[0][0], got[1][0])
	}
}

// TestPredicateFilterString exercises a string-equality predicate.
func TestPredicateFilterString(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "region", Type: record.TypeString}}
	rows := [][]interface{}{
		{int64(1), "US"}, {int64(2), "EU"}, {int64(3), "US"},
	}
	path := filepath.Join(t.TempDir(), "filtstr.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"filter":              "region = 'US'",
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	defer src.Close()

	got := readAllRows(t, src, 10)
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2", len(got))
	}
	for _, r := range got {
		if r[1] != "US" {
			t.Errorf("region = %v, want US", r[1])
		}
	}
}

// TestInvalidFilter rejects an unknown column and an unparseable expression at
// construction (fail fast rather than silently ignore).
func TestInvalidFilter(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}}
	rows := [][]interface{}{{int64(1)}}
	path := filepath.Join(t.TempDir(), "bad.parquet")
	writeTestParquet(t, path, cols, rows, 0, 0)

	if _, err := newParquetSource(map[string]interface{}{"path": path, "filter": "nope > 1"}); err == nil {
		t.Error("expected error for filter on unknown column")
	}
	if _, err := newParquetSource(map[string]interface{}{"path": path, "filter": "garbage"}); err == nil {
		t.Error("expected error for unparseable filter")
	}
}

// TestFilterWithSkip verifies checkpoint-resume Skip counts emitted (post-filter)
// rows, so resume lands on the correct surviving row.
func TestFilterWithSkip(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeInt}}
	var rows [][]interface{}
	for i := int64(0); i < 20; i++ {
		rows = append(rows, []interface{}{i, i * 10}) // amount 0,10,...,190
	}
	path := filepath.Join(t.TempDir(), "fs.parquet")
	writeTestParquet(t, path, cols, rows, 5 /* 4 row groups */, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"filter":              "amount >= 50",
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	defer src.Close()

	// amount>=50 → ids 5..19 (15 rows). Skip(5) drops ids 5..9; resume at id 10.
	if err := src.Skip(5); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	got := readAllRows(t, src, 4)
	if len(got) != 10 {
		t.Fatalf("after Skip(5) rows = %d, want 10", len(got))
	}
	// Files written from a parquet.Group get alphabetically-ordered columns, so
	// locate "id" by name rather than assuming a position.
	idIdx := -1
	for i, c := range src.Schema() {
		if c.Name == "id" {
			idIdx = i
		}
	}
	if idIdx < 0 {
		t.Fatal("id column missing from schema")
	}
	if got[0][idIdx] != int64(10) {
		t.Errorf("first id = %v, want 10", got[0][idIdx])
	}
}

// TestRangeMayMatch locks the row-group pruning decision logic.
func TestRangeMayMatch(t *testing.T) {
	cases := []struct {
		min, max, lit float64
		op            string
		want          bool
	}{
		{0, 40, 50, ">=", false}, // whole group below threshold → prune
		{50, 90, 50, ">=", true},
		{0, 40, 50, ">", false},
		{100, 140, 50, ">", true},
		{0, 40, 50, "<", true},
		{60, 90, 50, "<", false},
		{0, 40, 20, "=", true},
		{0, 40, 99, "=", false},
		{5, 5, 5, "!=", false}, // all values equal the literal → none match !=
		{5, 9, 5, "!=", true},
	}
	for _, c := range cases {
		if got := rangeMayMatch(c.min, c.max, c.lit, c.op); got != c.want {
			t.Errorf("rangeMayMatch(%v,%v,%v,%q) = %v, want %v", c.min, c.max, c.lit, c.op, got, c.want)
		}
	}
}
