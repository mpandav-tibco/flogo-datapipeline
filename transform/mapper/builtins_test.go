package mapper

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestMapperPadAndScientific exercises the lpad/rpad (padArg/padStr) and
// scientific builtins through a real mapper transform, and the Name() accessor.
func TestMapperPadAndScientific(t *testing.T) {
	tr, err := newMapperTransform(map[string]interface{}{
		"fields": map[string]interface{}{
			"padded_l": "=lpad($code, 5, '0')",
			"padded_r": "=rpad($code, 5, '*')",
			"sci":      "=scientific($amount)",
		},
		"fieldOrder": []interface{}{"padded_l", "padded_r", "sci"},
	})
	if err != nil {
		t.Fatalf("newMapperTransform: %v", err)
	}
	if tr.Name() != "map" {
		t.Errorf("Name = %q, want map", tr.Name())
	}

	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "code", Type: record.TypeString},
			{Name: "amount", Type: record.TypeFloat},
		},
		Rows: [][]interface{}{{"ab", 13326.68}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// lpad left-justifies (pads on the right); rpad right-justifies (pads on the
	// left). Both fill with spaces — the mapper's lpad/rpad ignore a fill-char arg.
	if v := out.Value(0, "padded_l"); v != "ab   " {
		t.Errorf("padded_l = %q, want %q", v, "ab   ")
	}
	if v := out.Value(0, "padded_r"); v != "   ab" {
		t.Errorf("padded_r = %q, want %q", v, "   ab")
	}
	if v := out.Value(0, "sci"); v != "+1.332668E+04" {
		t.Errorf("sci = %v, want +1.332668E+04", v)
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
