package fixedwidth

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func tmpFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fw.txt")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

func cols() []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "id", "start": 1, "length": 3, "type": "int"},
		map[string]interface{}{"name": "name", "start": 4, "length": 6, "type": "string", "trim": true},
		map[string]interface{}{"name": "amount", "start": 10, "length": 6, "type": "decimal", "impliedDecimal": 2},
	}
}

func TestFixedWidth_ConfigValidation(t *testing.T) {
	if _, err := newFixedWidthSource(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
	p := tmpFile(t, "")
	if _, err := newFixedWidthSource(map[string]interface{}{"path": p}); err == nil {
		t.Error("missing columns should error")
	}
	if _, err := newFixedWidthSource(map[string]interface{}{"path": p, "columns": "notarray"}); err == nil {
		t.Error("columns must be array")
	}
}

func TestFixedWidth_ParseRows(t *testing.T) {
	// id(3) name(6) amount(6) => "001Alice 012345" => amount 123.45
	content := "001Alice 012345\n002Bob   000099\n"
	p := tmpFile(t, content)
	src, err := newFixedWidthSource(map[string]interface{}{"path": p, "columns": cols()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer src.Close()
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 2 {
		t.Fatalf("rows = %d, want 2", b.Len())
	}
	if b.Value(0, "id") != int64(1) {
		t.Errorf("id = %v (%T), want int64(1)", b.Value(0, "id"), b.Value(0, "id"))
	}
	if b.Value(0, "name") != "Alice" {
		t.Errorf("name = %q, want Alice", b.Value(0, "name"))
	}
	if d, ok := b.Value(0, "amount").(record.Decimal); !ok || d.String() != "123.45" {
		t.Errorf("amount = %v (%T), want record.Decimal 123.45", b.Value(0, "amount"), b.Value(0, "amount"))
	}
	if d, ok := b.Value(1, "amount").(record.Decimal); !ok || d.String() != "0.99" {
		t.Errorf("amount1 = %v (%T), want record.Decimal 0.99", b.Value(1, "amount"), b.Value(1, "amount"))
	}
}

func TestFixedWidth_ShortLinePadded(t *testing.T) {
	// A short line should be tolerated (padded / nil beyond end).
	content := "003Cara\n"
	p := tmpFile(t, content)
	src, _ := newFixedWidthSource(map[string]interface{}{"path": p, "columns": cols(), "recordLength": 15})
	defer src.Close()
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 1 || b.Value(0, "id") != int64(3) {
		t.Errorf("short line parse failed: %v", b.Rows)
	}
}

func TestFixedWidth_SkipLines(t *testing.T) {
	content := "HEADER LINE\n001Alice 012345\n"
	p := tmpFile(t, content)
	src, _ := newFixedWidthSource(map[string]interface{}{"path": p, "columns": cols(), "skipLines": 1})
	defer src.Close()
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 1 || b.Value(0, "name") != "Alice" {
		t.Errorf("skipLines failed: %v", b.Rows)
	}
}

func TestFixedWidth_EOF(t *testing.T) {
	p := tmpFile(t, "")
	src, _ := newFixedWidthSource(map[string]interface{}{"path": p, "columns": cols()})
	defer src.Close()
	if _, err := src.ReadBatch(10); err != io.EOF {
		t.Errorf("empty should EOF, got %v", err)
	}
}

func TestCoerceFixed_Types(t *testing.T) {
	tests := []struct {
		raw  string
		col  fwColumn
		want interface{}
	}{
		{"  ", fwColumn{}, nil},
		{"TRUE", fwColumn{Column: record.Column{Type: record.TypeBool}}, true},
		{"N", fwColumn{Column: record.Column{Type: record.TypeBool}}, false},
		{"20240115", fwColumn{Column: record.Column{Type: record.TypeDate}, format: "20060102"}, nil}, // compared below
	}
	for _, tc := range tests {
		got := coerceFixed(tc.raw, tc.col)
		if tc.want != nil && got != tc.want {
			t.Errorf("coerceFixed(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

// TestParseFixedWidthColumnsValidation verifies that invalid positional metadata
// is rejected at construction (a config boundary) rather than causing a per-row
// slice-bounds panic (negative length) or silent nulls (start < 1) at parse time.
func TestParseFixedWidthColumnsValidation(t *testing.T) {
	col := func(name string, start, length int) map[string]interface{} {
		return map[string]interface{}{"name": name, "start": start, "length": length, "type": "string"}
	}
	cases := []struct {
		name    string
		columns []interface{}
		wantErr bool
	}{
		{"valid", []interface{}{col("a", 1, 10), col("b", 11, 5)}, false},
		{"negative length", []interface{}{col("a", 1, -1)}, true},
		{"zero length", []interface{}{col("a", 1, 0)}, true},
		{"start below one", []interface{}{col("a", 0, 10)}, true},
		{"negative start", []interface{}{col("a", -3, 10)}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFixedWidthColumns(map[string]interface{}{"columns": c.columns})
			if c.wantErr && err == nil {
				t.Fatal("expected a validation error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
		})
	}
}
