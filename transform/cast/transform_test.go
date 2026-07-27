package cast

import (
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func newCast(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := newCastTransform(cfg)
	if err != nil {
		t.Fatalf("newCastTransform: %v", err)
	}
	return tr
}

func TestCast_ConfigValidation(t *testing.T) {
	if _, err := newCastTransform(map[string]interface{}{}); err == nil {
		t.Error("expected error when 'columns' missing")
	}
	if _, err := newCastTransform(map[string]interface{}{"columns": []interface{}{}}); err == nil {
		t.Error("expected error for empty columns")
	}
	if _, err := newCastTransform(map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "x", "type": "bogus"},
	}}); err == nil {
		t.Error("expected error for unsupported type")
	}
	if _, err := newCastTransform(map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"type": "int"},
	}}); err == nil {
		t.Error("expected error for missing name")
	}
	if _, err := newCastTransform(map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "x", "type": "int"}},
		"onError": "explode",
	}); err == nil {
		t.Error("expected error for invalid onError")
	}
}

func TestCast_Conversions(t *testing.T) {
	tr := newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "n", "type": "int"},
		map[string]interface{}{"name": "amt", "type": "decimal", "scale": float64(2)},
		map[string]interface{}{"name": "flag", "type": "bool"},
	}})
	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "n", Type: record.TypeString},
			{Name: "amt", Type: record.TypeString},
			{Name: "flag", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{"42", "5", "yes"},
			{"7", "3.14159", "0"},
		},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// Output schema reflects the target types.
	if out.Columns[0].Type != record.TypeInt || out.Columns[1].Type != record.TypeDecimal || out.Columns[2].Type != record.TypeBool {
		t.Fatalf("schema not updated: %+v", out.Columns)
	}
	if out.Rows[0][0] != int64(42) {
		t.Errorf("n[0] = %v (%T); want int64(42)", out.Rows[0][0], out.Rows[0][0])
	}
	if d, ok := out.Rows[0][1].(record.Decimal); !ok || d.String() != "5.00" {
		t.Errorf("amt[0] = %v; want decimal 5.00 (padded to scale 2)", out.Rows[0][1])
	}
	if d, ok := out.Rows[1][1].(record.Decimal); !ok || d.String() != "3.14" {
		t.Errorf("amt[1] = %v; want decimal 3.14 (rounded to scale 2)", out.Rows[1][1])
	}
	if out.Rows[0][2] != true || out.Rows[1][2] != false {
		t.Errorf("flag = %v,%v; want true,false", out.Rows[0][2], out.Rows[1][2])
	}
	// The input batch must not be mutated (the engine may recycle it).
	if in.Columns[0].Type != record.TypeString || in.Rows[0][0] != "42" {
		t.Error("input batch was mutated")
	}
}

func TestCast_OnErrorNullVsFail(t *testing.T) {
	mkIn := func() *record.RecordBatch {
		return &record.RecordBatch{
			Columns: []record.Column{{Name: "n", Type: record.TypeString}},
			Rows:    [][]interface{}{{"42"}, {"notanumber"}},
		}
	}
	// Default "null": an unconvertible value becomes nil.
	trNull := newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "n", "type": "int"},
	}})
	out, err := trNull.Apply(mkIn())
	if err != nil {
		t.Fatalf("Apply(null): %v", err)
	}
	if out.Rows[0][0] != int64(42) || out.Rows[1][0] != nil {
		t.Errorf("null mode = %v,%v; want 42,nil", out.Rows[0][0], out.Rows[1][0])
	}
	// "fail": errors on the first unconvertible value.
	trFail := newCast(t, map[string]interface{}{
		"columns": []interface{}{map[string]interface{}{"name": "n", "type": "int"}},
		"onError": "fail",
	})
	if _, err := trFail.Apply(mkIn()); err == nil {
		t.Error("fail mode should error on an unconvertible value")
	}
}

func TestCast_AbsentColumnAndNilPassthrough(t *testing.T) {
	tr := newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "missing", "type": "int"}, // absent → skipped
		map[string]interface{}{"name": "n", "type": "int"},
	}})
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "n", Type: record.TypeString}},
		Rows:    [][]interface{}{{"5"}, {nil}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Rows[0][0] != int64(5) {
		t.Errorf("n[0] = %v; want 5", out.Rows[0][0])
	}
	if out.Rows[1][0] != nil {
		t.Errorf("nil should pass through, got %v", out.Rows[1][0])
	}
}

func TestCast_DateAndDecimalToString(t *testing.T) {
	tr := newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "ts", "type": "date", "format": "2006-01-02"},
		map[string]interface{}{"name": "amt", "type": "string"},
	}})
	amt, _ := record.ParseDecimal("123.40")
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "ts", Type: record.TypeString}, {Name: "amt", Type: record.TypeDecimal}},
		Rows:    [][]interface{}{{"2024-03-15", amt}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	ts, ok := out.Rows[0][0].(time.Time)
	if !ok || ts.Year() != 2024 || ts.Month() != time.March || ts.Day() != 15 {
		t.Errorf("ts = %v; want 2024-03-15", out.Rows[0][0])
	}
	// decimal → string keeps the exact scale-preserving text.
	if out.Rows[0][1] != "123.40" {
		t.Errorf("amt as string = %v; want exact 123.40", out.Rows[0][1])
	}
}

func TestCast_NumericToIntRounds(t *testing.T) {
	// All numeric → int casts round half-away-from-zero (SQL CAST), consistently
	// across float, decimal and numeric strings.
	amt, _ := record.ParseDecimal("3.9")
	nd, _ := record.ParseDecimal("-2.5")
	tr := newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "f", "type": "int"},
		map[string]interface{}{"name": "d", "type": "int"},
		map[string]interface{}{"name": "s", "type": "int"},
		map[string]interface{}{"name": "neg", "type": "int"},
	}})
	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "f", Type: record.TypeFloat},
			{Name: "d", Type: record.TypeDecimal},
			{Name: "s", Type: record.TypeString},
			{Name: "neg", Type: record.TypeDecimal},
		},
		Rows: [][]interface{}{{3.9, amt, "3.9", nd}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Rows[0][0] != int64(4) {
		t.Errorf("float 3.9 → int = %v; want 4", out.Rows[0][0])
	}
	if out.Rows[0][1] != int64(4) {
		t.Errorf("decimal 3.9 → int = %v; want 4", out.Rows[0][1])
	}
	if out.Rows[0][2] != int64(4) {
		t.Errorf("string 3.9 → int = %v; want 4", out.Rows[0][2])
	}
	if out.Rows[0][3] != int64(-3) {
		t.Errorf("decimal -2.5 → int = %v; want -3 (half away from zero)", out.Rows[0][3])
	}
}

func TestCast_Name(t *testing.T) {
	if newCast(t, map[string]interface{}{"columns": []interface{}{
		map[string]interface{}{"name": "n", "type": "int"},
	}}).Name() != "cast" {
		t.Error("Name should be 'cast'")
	}
}
