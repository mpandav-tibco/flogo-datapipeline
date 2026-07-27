package flatten

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func newFlatten(t *testing.T, cfg map[string]interface{}) *flattenTransform {
	t.Helper()
	tr, err := newFlattenTransform(cfg)
	if err != nil {
		t.Fatalf("newFlattenTransform: %v", err)
	}
	return tr.(*flattenTransform)
}

func colIdx(cols []record.Column, name string) int {
	for i, c := range cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

func TestFlatten_NestedObject(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "id", Type: record.TypeInt},
			{Name: "address", Type: record.TypeAny},
		},
		Rows: [][]interface{}{
			{int64(1), map[string]interface{}{"city": "NYC", "zip": "10001"}},
		},
	}
	out, err := newFlatten(t, map[string]interface{}{}).Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if colIdx(out.Columns, "address") != -1 {
		t.Error("nested 'address' column should be replaced by its leaves")
	}
	ci := colIdx(out.Columns, "address.city")
	zi := colIdx(out.Columns, "address.zip")
	idi := colIdx(out.Columns, "id")
	if ci < 0 || zi < 0 || idi < 0 {
		t.Fatalf("columns = %v", out.Columns)
	}
	if out.Rows[0][ci] != "NYC" || out.Rows[0][zi] != "10001" || out.Rows[0][idi] != int64(1) {
		t.Errorf("row = %v", out.Rows[0])
	}
}

func TestFlatten_UnionAcrossRows(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "m", Type: record.TypeAny}},
		Rows: [][]interface{}{
			{map[string]interface{}{"a": 1}},
			{map[string]interface{}{"b": 2}},
		},
	}
	out, _ := newFlatten(t, map[string]interface{}{}).Apply(in)
	ai := colIdx(out.Columns, "m.a")
	bi := colIdx(out.Columns, "m.b")
	if ai < 0 || bi < 0 {
		t.Fatalf("union columns missing: %v", out.Columns)
	}
	if out.Rows[0][ai] != 1 || out.Rows[0][bi] != nil {
		t.Errorf("row0 = %v; want a=1 b=nil", out.Rows[0])
	}
	if out.Rows[1][ai] != nil || out.Rows[1][bi] != 2 {
		t.Errorf("row1 = %v; want a=nil b=2", out.Rows[1])
	}
}

func TestFlatten_DepthAndSeparator(t *testing.T) {
	nested := map[string]interface{}{
		"a": map[string]interface{}{"b": map[string]interface{}{"c": 1}},
	}
	mk := func() *record.RecordBatch {
		return &record.RecordBatch{
			Columns: []record.Column{{Name: "m", Type: record.TypeAny}},
			Rows:    [][]interface{}{{nested}},
		}
	}
	// Unlimited depth with a custom separator.
	out, _ := newFlatten(t, map[string]interface{}{"separator": "_"}).Apply(mk())
	if colIdx(out.Columns, "m_a_b_c") < 0 {
		t.Fatalf("deep flatten columns = %v", out.Columns)
	}
	// depth 1 flattens only one level; m.a stays a map value.
	out2, _ := newFlatten(t, map[string]interface{}{"depth": float64(1)}).Apply(mk())
	ai := colIdx(out2.Columns, "m.a")
	if ai < 0 {
		t.Fatalf("depth-1 columns = %v", out2.Columns)
	}
	if _, ok := out2.Rows[0][ai].(map[string]interface{}); !ok {
		t.Errorf("depth-1 m.a should still be a map, got %T", out2.Rows[0][ai])
	}
}

func TestFlatten_ColumnsFilterAndNonMap(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "a", Type: record.TypeAny},
			{Name: "b", Type: record.TypeAny},
			{Name: "s", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{map[string]interface{}{"x": 1}, map[string]interface{}{"y": 2}, "text"},
		},
	}
	out, _ := newFlatten(t, map[string]interface{}{"columns": []interface{}{"a"}}).Apply(in)
	if colIdx(out.Columns, "a.x") < 0 {
		t.Error("selected column 'a' should be flattened")
	}
	if colIdx(out.Columns, "b") < 0 || colIdx(out.Columns, "b.y") >= 0 {
		t.Error("unselected column 'b' should be kept unflattened")
	}
	if colIdx(out.Columns, "s") < 0 {
		t.Error("non-map column 's' should be kept")
	}
}

func TestFlatten_ArraysLeftAsValues(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "m", Type: record.TypeAny}},
		Rows:    [][]interface{}{{map[string]interface{}{"tags": []interface{}{"a", "b"}}}},
	}
	out, _ := newFlatten(t, map[string]interface{}{}).Apply(in)
	ti := colIdx(out.Columns, "m.tags")
	if ti < 0 {
		t.Fatalf("m.tags missing: %v", out.Columns)
	}
	if arr, ok := out.Rows[0][ti].([]interface{}); !ok || len(arr) != 2 {
		t.Errorf("m.tags should stay an array, got %T %v", out.Rows[0][ti], out.Rows[0][ti])
	}
}

func TestFlatten_ConfigValidation(t *testing.T) {
	if _, err := newFlattenTransform(map[string]interface{}{"columns": []interface{}{}}); err == nil {
		t.Error("empty 'columns' should error")
	}
	if _, err := newFlattenTransform(map[string]interface{}{"depth": float64(-1)}); err == nil {
		t.Error("negative 'depth' should error")
	}
}
