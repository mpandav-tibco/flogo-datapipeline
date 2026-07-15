package schemadrift

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func build(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := transform.New("schemadrift", cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr
}

func b(cols []record.Column, rows ...[]interface{}) *record.RecordBatch {
	return &record.RecordBatch{Columns: cols, Rows: rows}
}

var baseCols = []record.Column{
	{Name: "id", Type: record.TypeInt},
	{Name: "name", Type: record.TypeString},
}

func TestSchemaDrift_ConfigValidation(t *testing.T) {
	if _, err := transform.New("schemadrift", map[string]interface{}{"policy": "bogus"}); err == nil {
		t.Error("expected error for bad policy")
	}
	if _, err := transform.New("schemadrift", map[string]interface{}{"onDrift": "bogus"}); err == nil {
		t.Error("expected error for bad onDrift")
	}
	// Defaults are valid.
	if _, err := transform.New("schemadrift", map[string]interface{}{}); err != nil {
		t.Errorf("defaults should be valid: %v", err)
	}
}

func TestSchemaDrift_Project(t *testing.T) {
	tr := build(t, map[string]interface{}{"policy": "project", "onDrift": "silent"})
	// First batch establishes baseline.
	_, _ = tr.Apply(b(baseCols, []interface{}{1, "a"}))
	// Second batch adds a column and drops name.
	drift := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "extra", Type: record.TypeString}}
	out, err := tr.Apply(b(drift, []interface{}{2, "x"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// Only baseline columns survive; name filled nil, extra dropped.
	if len(out.Columns) != 2 || out.Columns[0].Name != "id" || out.Columns[1].Name != "name" {
		t.Fatalf("columns = %+v", out.Columns)
	}
	if out.Rows[0][0] != 2 || out.Rows[0][1] != nil {
		t.Errorf("row = %v, want [2 <nil>]", out.Rows[0])
	}
}

func TestSchemaDrift_Additive(t *testing.T) {
	tr := build(t, map[string]interface{}{"policy": "additive", "onDrift": "log"})
	_, _ = tr.Apply(b(baseCols, []interface{}{1, "a"}))
	drift := append(append([]record.Column{}, baseCols...), record.Column{Name: "age", Type: record.TypeInt})
	out, err := tr.Apply(b(drift, []interface{}{2, "b", 30}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Columns) != 3 || out.Columns[2].Name != "age" {
		t.Fatalf("additive columns = %+v", out.Columns)
	}
	if out.Rows[0][2] != 30 {
		t.Errorf("age value = %v", out.Rows[0][2])
	}
}

func TestSchemaDrift_Strict(t *testing.T) {
	tr := build(t, map[string]interface{}{"policy": "strict"})
	_, _ = tr.Apply(b(baseCols, []interface{}{1, "a"}))
	drift := []record.Column{{Name: "id", Type: record.TypeInt}}
	if _, err := tr.Apply(b(drift, []interface{}{2})); err == nil {
		t.Error("strict policy should error on drift")
	}
}

func TestSchemaDrift_Drop(t *testing.T) {
	tr := build(t, map[string]interface{}{"policy": "drop"})
	_, _ = tr.Apply(b(baseCols, []interface{}{1, "a"}))
	drift := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "new", Type: record.TypeString}}
	out, err := tr.Apply(b(drift, []interface{}{2, "z"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Errorf("drop policy should return nil batch, got %+v", out)
	}
}

func TestSchemaDrift_NoDriftFastPath(t *testing.T) {
	tr := build(t, map[string]interface{}{"policy": "strict"})
	first := b(baseCols, []interface{}{1, "a"})
	_, _ = tr.Apply(first)
	same := b(baseCols, []interface{}{2, "b"})
	out, err := tr.Apply(same)
	if err != nil || out != same {
		t.Errorf("no-drift batch should pass through unchanged")
	}
	if tr.Name() != "schemadrift" || tr.Close() != nil {
		t.Error("Name/Close contract")
	}
}
