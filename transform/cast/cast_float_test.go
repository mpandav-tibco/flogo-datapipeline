package cast

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestCastToFloatAndClose exercises the float cast path (castToFloat) across
// value types, the lenient null-on-error default, Name() and Close().
func TestCastToFloatAndClose(t *testing.T) {
	tr, err := newCastTransform(map[string]interface{}{
		"columns": []interface{}{
			map[string]interface{}{"name": "v", "type": "float"},
		},
	})
	if err != nil {
		t.Fatalf("newCastTransform: %v", err)
	}
	if tr.Name() != "cast" {
		t.Errorf("Name = %q, want cast", tr.Name())
	}

	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "v", Type: record.TypeAny}},
		Rows:    [][]interface{}{{int64(3)}, {"2.5"}, {true}, {float64(1.5)}, {"not-a-number"}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := []interface{}{float64(3), float64(2.5), float64(1), float64(1.5), nil}
	for i, w := range want {
		if got := out.Value(i, "v"); got != w {
			t.Errorf("row %d = %v (%T), want %v", i, got, got, w)
		}
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestCastOnErrorFail verifies onError:"fail" surfaces an error on an
// unconvertible value rather than nulling it out.
func TestCastOnErrorFail(t *testing.T) {
	tr, err := newCastTransform(map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "v", "type": "float"}},
		"onError": "fail",
	})
	if err != nil {
		t.Fatalf("newCastTransform: %v", err)
	}
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "v", Type: record.TypeAny}},
		Rows:    [][]interface{}{{"not-a-number"}},
	}
	if _, err := tr.Apply(in); err == nil {
		t.Error("onError=fail expected an error on an unconvertible value")
	}
}
