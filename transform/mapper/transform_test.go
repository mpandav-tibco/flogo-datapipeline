package mapper

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// helper: build a simple input batch
func inputBatch() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{
			{Name: "firstName", Type: record.TypeString},
			{Name: "lastName", Type: record.TypeString},
			{Name: "amount", Type: record.TypeFloat},
			{Name: "notes", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{"Alice", "Smith", 10000.0, "  trimme  "},
			{"Bob", "Jones", 5000.0, nil},
		},
	}
}

func buildMapper(t *testing.T, fields map[string]interface{}, fieldOrder []interface{}) transform.Transform {
	t.Helper()
	cfg := map[string]interface{}{"fields": fields}
	if fieldOrder != nil {
		cfg["fieldOrder"] = fieldOrder
	}
	m, err := transform.New("map", cfg)
	if err != nil {
		t.Fatalf("buildMapper: %v", err)
	}
	return m
}

// ─── Static literal ───────────────────────────────────────────────────────────

func TestMapper_StaticLiteral(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"source": "pipeline-v1",
	}, nil)
	in := inputBatch()
	out, err := m.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("rows = %d, want 2", out.Len())
	}
	for i := range out.Rows {
		if got := out.Value(i, "source"); got != "pipeline-v1" {
			t.Errorf("row %d source = %v, want pipeline-v1", i, got)
		}
	}
}

// ─── Field reference ──────────────────────────────────────────────────────────

func TestMapper_FieldReference(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"first": "=$firstName",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Value(0, "first") != "Alice" {
		t.Errorf("row 0 first = %v, want Alice", out.Value(0, "first"))
	}
	if out.Value(1, "first") != "Bob" {
		t.Errorf("row 1 first = %v, want Bob", out.Value(1, "first"))
	}
}

// ─── concat builtin ───────────────────────────────────────────────────────────

func TestMapper_Concat(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"fullName": "=concat(=$firstName, =' ', =$lastName)",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Value(0, "fullName"); got != "Alice Smith" {
		t.Errorf("fullName = %v, want 'Alice Smith'", got)
	}
	if got := out.Value(1, "fullName"); got != "Bob Jones" {
		t.Errorf("fullName = %v, want 'Bob Jones'", got)
	}
}

// ─── divide builtin ───────────────────────────────────────────────────────────

func TestMapper_Divide(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"amountUsd": "=divide(=$amount, 100)",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, _ := out.Value(0, "amountUsd").(float64); got != 100.0 {
		t.Errorf("amountUsd row 0 = %v, want 100.0", got)
	}
	if got, _ := out.Value(1, "amountUsd").(float64); got != 50.0 {
		t.Errorf("amountUsd row 1 = %v, want 50.0", got)
	}
}

// ─── upper/lower/trim builtins ────────────────────────────────────────────────

func TestMapper_Upper(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"upperFirst": "=upper(=$firstName)",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Value(0, "upperFirst"); got != "ALICE" {
		t.Errorf("upperFirst = %v, want ALICE", got)
	}
}

func TestMapper_Lower(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"lowerFirst": "=lower(=$firstName)",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Value(0, "lowerFirst"); got != "alice" {
		t.Errorf("lowerFirst = %v, want alice", got)
	}
}

func TestMapper_Trim(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"trimmedNotes": "=trim(=$notes)",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Value(0, "trimmedNotes"); got != "trimme" {
		t.Errorf("trimmedNotes = %v, want 'trimme'", got)
	}
}

// ─── coalesce builtin ─────────────────────────────────────────────────────────

func TestMapper_Coalesce(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{
		"safeNotes": "=coalesce(=$notes, ='N/A')",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// Row 0 has notes = "  trimme  " (non-nil) → coalesce returns it as-is.
	if got := out.Value(0, "safeNotes"); got != "  trimme  " {
		t.Errorf("row 0 safeNotes = %v, want '  trimme  '", got)
	}
	// Row 1 has notes = nil → coalesce returns "N/A".
	if got := out.Value(1, "safeNotes"); got != "N/A" {
		t.Errorf("row 1 safeNotes = %v, want 'N/A'", got)
	}
}

// ─── fieldOrder determinism ───────────────────────────────────────────────────

func TestMapper_FieldOrderDeterminism(t *testing.T) {
	order := []interface{}{"z", "a", "m"}
	m := buildMapper(t, map[string]interface{}{
		"a": "first",
		"m": "second",
		"z": "third",
	}, order)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(out.Columns))
	}
	wantOrder := []string{"z", "a", "m"}
	for i, col := range out.Columns {
		if col.Name != wantOrder[i] {
			t.Errorf("column[%d] = %q, want %q", i, col.Name, wantOrder[i])
		}
	}
}

func TestMapper_LexicographicDefaultOrder(t *testing.T) {
	// No fieldOrder — expect lexicographic sort.
	m := buildMapper(t, map[string]interface{}{
		"z": "last",
		"a": "first",
		"m": "middle",
	}, nil)
	out, err := m.Apply(inputBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	wantOrder := []string{"a", "m", "z"}
	for i, col := range out.Columns {
		if col.Name != wantOrder[i] {
			t.Errorf("column[%d] = %q, want %q (default lex order)", i, col.Name, wantOrder[i])
		}
	}
}

// ─── unknown function error ───────────────────────────────────────────────────

func TestMapper_UnknownFunctionError(t *testing.T) {
	_, err := transform.New("map", map[string]interface{}{
		"fields": map[string]interface{}{
			"x": "=noSuchFunc(=$firstName)",
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown function, got nil")
	}
}

// ─── missing 'fields' error ───────────────────────────────────────────────────

func TestMapper_MissingFieldsError(t *testing.T) {
	_, err := transform.New("map", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing fields, got nil")
	}
}

// ─── Close is a no-op ─────────────────────────────────────────────────────────

func TestMapper_Close(t *testing.T) {
	m := buildMapper(t, map[string]interface{}{"x": "val"}, nil)
	if err := m.Close(); err != nil {
		t.Errorf("Close(): %v", err)
	}
}
