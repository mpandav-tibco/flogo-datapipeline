package schemadrift

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func newT(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := newSchemaDriftTransform(cfg)
	if err != nil {
		t.Fatalf("newSchemaDriftTransform: %v", err)
	}
	return tr
}

func cols(names ...string) []record.Column {
	c := make([]record.Column, len(names))
	for i, n := range names {
		c[i] = record.Column{Name: n, Type: record.TypeString}
	}
	return c
}

func rows2(colDefs []record.Column, data ...[]interface{}) *record.RecordBatch {
	return &record.RecordBatch{Columns: colDefs, Rows: data}
}

func TestSchemaDrift_ConfigValidation(t *testing.T) {
	if _, err := newSchemaDriftTransform(map[string]interface{}{"policy": "nonsense"}); err == nil {
		t.Error("expected error for invalid policy")
	}
	if _, err := newSchemaDriftTransform(map[string]interface{}{"onDrift": "nonsense"}); err == nil {
		t.Error("expected error for invalid onDrift")
	}
	// defaults
	tr := newT(t, map[string]interface{}{})
	if tr.Name() != "schemadrift" {
		t.Errorf("Name = %q", tr.Name())
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestSchemaDrift_BaselinePassthrough(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "project"})
	in := rows2(cols("a", "b"), []interface{}{"1", "2"})
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != in {
		t.Error("first batch should pass through unchanged (establishes baseline)")
	}
}

func TestSchemaDrift_NoDriftFastPath(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "strict"})
	_, _ = tr.Apply(rows2(cols("a", "b"), []interface{}{"1", "2"}))
	in := rows2(cols("a", "b"), []interface{}{"3", "4"})
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != in {
		t.Error("identical schema should return the same batch")
	}
}

func TestSchemaDrift_Strict(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "strict", "onDrift": "silent"})
	_, _ = tr.Apply(rows2(cols("a"), []interface{}{"1"}))
	_, err := tr.Apply(rows2(cols("a", "b"), []interface{}{"1", "2"}))
	if err == nil {
		t.Error("strict policy must error on drift")
	}
}

func TestSchemaDrift_Drop(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "drop", "onDrift": "silent"})
	_, _ = tr.Apply(rows2(cols("a"), []interface{}{"1"}))
	out, err := tr.Apply(rows2(cols("a", "b"), []interface{}{"1", "2"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Error("drop policy must return nil batch on drift")
	}
}

func TestSchemaDrift_Project(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "project", "onDrift": "log"})
	_, _ = tr.Apply(rows2(cols("a", "b"), []interface{}{"1", "2"}))
	// New column "c" appears; project must keep only a,b.
	out, err := tr.Apply(rows2(cols("a", "b", "c"), []interface{}{"3", "4", "5"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("projected columns = %d, want 2", len(out.Columns))
	}
	if out.Value(0, "a") != "3" || out.Value(0, "b") != "4" {
		t.Errorf("projected values wrong: %v", out.Rows[0])
	}
	if out.ColIndex("c") != -1 {
		t.Error("column c should have been projected out")
	}
}

func TestSchemaDrift_Additive(t *testing.T) {
	tr := newT(t, map[string]interface{}{"policy": "additive", "onDrift": "alert"})
	_, _ = tr.Apply(rows2(cols("a"), []interface{}{"1"}))
	out, err := tr.Apply(rows2(cols("a", "b"), []interface{}{"2", "3"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// Additive merges the new column into the baseline.
	if out.ColIndex("b") == -1 {
		t.Error("additive policy should propagate new column b")
	}
}
