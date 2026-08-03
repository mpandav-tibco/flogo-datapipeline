package parquet

import (
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func colIndexByName(t *testing.T, src interface{ Schema() []record.Column }, name string) int {
	t.Helper()
	for i, c := range src.Schema() {
		if c.Name == name {
			return i
		}
	}
	t.Fatalf("column %q not found in schema", name)
	return -1
}

// TestPredicateFilterFloat exercises a float-column numeric predicate (a
// distinct coercion path from the int case).
func TestPredicateFilterFloat(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeFloat}}
	rows := [][]interface{}{
		{int64(1), 1.0}, {int64(2), 2.5}, {int64(3), 0.5}, {int64(4), 3.0},
	}
	path := filepath.Join(t.TempDir(), "flt.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"filter":              "amount > 1.5",
		"rowGroupParallelism": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	amtIdx := colIndexByName(t, src, "amount")
	got := readAllRows(t, src, 10)
	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2 (2.5, 3.0)", len(got))
	}
	for _, r := range got {
		if f, _ := r[amtIdx].(float64); f <= 1.5 {
			t.Errorf("emitted amount %v, want > 1.5", f)
		}
	}
}

// TestPredicateFilterParallel exercises the concurrent fill path (parallelism 4)
// together with statistics pruning: two of four row groups are prunable.
func TestPredicateFilterParallel(t *testing.T) {
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeInt}}
	var rows [][]interface{}
	for _, base := range []int64{0, 100, 200, 20} { // groups: 0..9, 100..109, 200..209, 20..29
		for i := int64(0); i < 10; i++ {
			rows = append(rows, []interface{}{base + i, base + i})
		}
	}
	path := filepath.Join(t.TempDir(), "par.parquet")
	writeTestParquet(t, path, cols, rows, 10 /* 4 row groups */, 0)

	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"filter":              "amount >= 100",
		"rowGroupParallelism": 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	amtIdx := colIndexByName(t, src, "amount")
	got := readAllRows(t, src, 8)
	if len(got) != 20 {
		t.Fatalf("rows = %d, want 20 (the 100..109 and 200..209 groups)", len(got))
	}
	for _, r := range got {
		if a, _ := r[amtIdx].(int64); a < 100 {
			t.Errorf("emitted amount %d, want >= 100", a)
		}
	}
}
