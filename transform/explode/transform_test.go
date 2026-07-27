package explode

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func newExplode(t *testing.T, cfg map[string]interface{}) *explodeTransform {
	t.Helper()
	tr, err := newExplodeTransform(cfg)
	if err != nil {
		t.Fatalf("newExplodeTransform: %v", err)
	}
	return tr.(*explodeTransform)
}

func TestExplode_ConfigValidation(t *testing.T) {
	if _, err := newExplodeTransform(map[string]interface{}{}); err == nil {
		t.Error("missing 'column' should error")
	}
}

func TestExplode_ArrayToRows(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "tags", Type: record.TypeAny}},
		Rows: [][]interface{}{
			{int64(1), []interface{}{"a", "b", "c"}},
			{int64(2), []interface{}{"x"}},
		},
	}
	out, err := newExplode(t, map[string]interface{}{"column": "tags"}).Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 4 {
		t.Fatalf("rows = %d; want 4", out.Len())
	}
	if out.Rows[0][0] != int64(1) || out.Rows[0][1] != "a" {
		t.Errorf("row0 = %v; want [1 a]", out.Rows[0])
	}
	if out.Rows[2][1] != "c" || out.Rows[3][0] != int64(2) || out.Rows[3][1] != "x" {
		t.Errorf("rows = %v", out.Rows)
	}
	if len(in.Rows) != 2 {
		t.Error("input batch was mutated")
	}
}

func TestExplode_Into(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "items", Type: record.TypeAny}},
		Rows:    [][]interface{}{{[]interface{}{"a", "b"}}},
	}
	out, _ := newExplode(t, map[string]interface{}{"column": "items", "into": "item"}).Apply(in)
	if out.Columns[0].Name != "item" {
		t.Errorf("column name = %q; want item", out.Columns[0].Name)
	}
	if out.Rows[0][0] != "a" || out.Rows[1][0] != "b" {
		t.Errorf("rows = %v", out.Rows)
	}
}

func TestExplode_EmptyAndNil(t *testing.T) {
	mk := func() *record.RecordBatch {
		return &record.RecordBatch{
			Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "a", Type: record.TypeAny}},
			Rows: [][]interface{}{
				{int64(1), []interface{}{}},
				{int64(2), nil},
				{int64(3), []interface{}{"x"}},
			},
		}
	}
	// Default: empty array and nil are dropped → only the third row survives.
	out, _ := newExplode(t, map[string]interface{}{"column": "a"}).Apply(mk())
	if out.Len() != 1 || out.Rows[0][0] != int64(3) {
		t.Errorf("default drop = %v", out.Rows)
	}
	// keepEmpty: empty array and nil each emit one null row → 3 rows.
	out2, _ := newExplode(t, map[string]interface{}{"column": "a", "keepEmpty": true}).Apply(mk())
	if out2.Len() != 3 {
		t.Fatalf("keepEmpty rows = %d; want 3", out2.Len())
	}
	if out2.Rows[0][1] != nil || out2.Rows[1][1] != nil {
		t.Errorf("keepEmpty null rows = %v", out2.Rows)
	}
}

func TestExplode_Scalar(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "a", Type: record.TypeAny}},
		Rows:    [][]interface{}{{"solo"}},
	}
	out, _ := newExplode(t, map[string]interface{}{"column": "a"}).Apply(in)
	if out.Len() != 1 || out.Rows[0][0] != "solo" {
		t.Errorf("scalar explode = %v", out.Rows)
	}
}

func TestExplode_MissingColumn(t *testing.T) {
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}},
		Rows:    [][]interface{}{{int64(1)}},
	}
	out, _ := newExplode(t, map[string]interface{}{"column": "nope"}).Apply(in)
	if out != in {
		t.Error("missing column should pass the batch through unchanged")
	}
}
