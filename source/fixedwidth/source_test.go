package fixedwidth

import "testing"

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
