package schema

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func col(name string, t record.ColumnType) record.Column { return record.Column{Name: name, Type: t} }

func TestDiff_NoChange(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	cur := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	if ch := Diff(base, cur); ch != nil {
		t.Errorf("expected nil, got %v", ch)
	}
}

func TestDiff_AddedDroppedRetyped(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	cur := []record.Column{col("a", record.TypeFloat), col("c", record.TypeString)}
	changes := Diff(base, cur)
	seen := map[string]string{}
	for _, c := range changes {
		seen[c.Column] = c.Kind
	}
	if seen["a"] != "retyped" {
		t.Errorf("a should be retyped, got %q", seen["a"])
	}
	if seen["c"] != "added" {
		t.Errorf("c should be added, got %q", seen["c"])
	}
	if seen["b"] != "dropped" {
		t.Errorf("b should be dropped, got %q", seen["b"])
	}
}

func TestProject_FillAndDrop(t *testing.T) {
	src := &record.RecordBatch{
		Columns: []record.Column{col("a", record.TypeInt), col("extra", record.TypeString)},
		Rows:    [][]interface{}{{1, "x"}, {2, "y"}},
	}
	target := []record.Column{col("a", record.TypeInt), col("missing", record.TypeString)}
	out := Project(src, target)
	defer record.Put(out)
	if len(out.Columns) != 2 || out.Columns[1].Name != "missing" {
		t.Fatalf("unexpected columns %+v", out.Columns)
	}
	if out.Rows[0][0] != 1 || out.Rows[0][1] != nil {
		t.Errorf("row0 = %v, want [1 <nil>]", out.Rows[0])
	}
	// 'extra' must be dropped, 'missing' filled with nil.
	if out.Rows[1][0] != 2 || out.Rows[1][1] != nil {
		t.Errorf("row1 = %v", out.Rows[1])
	}
}
