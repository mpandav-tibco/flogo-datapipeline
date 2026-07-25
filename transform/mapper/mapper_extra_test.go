package mapper

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func TestMapper_ConfigErrors(t *testing.T) {
	if _, err := newMapperTransform(map[string]interface{}{}); err == nil {
		t.Error("missing fields should error")
	}
	if _, err := newMapperTransform(map[string]interface{}{"fields": "notobject"}); err == nil {
		t.Error("fields must be object")
	}
	// bad expression
	if _, err := newMapperTransform(map[string]interface{}{"fields": map[string]interface{}{"x": "=unknownfn("}}); err == nil {
		t.Error("malformed expression should error")
	}
	if _, err := newMapperTransform(map[string]interface{}{"fields": map[string]interface{}{"x": "=nosuchfunc(1)"}}); err == nil {
		t.Error("unknown function should error")
	}
}

func TestMapper_FieldOrderValidation(t *testing.T) {
	fields := map[string]interface{}{"a": "=$a", "b": "=$b"}
	// wrong count
	if _, err := newMapperTransform(map[string]interface{}{"fields": fields, "fieldOrder": []interface{}{"a"}}); err == nil {
		t.Error("fieldOrder count mismatch should error")
	}
	// unknown key
	if _, err := newMapperTransform(map[string]interface{}{"fields": fields, "fieldOrder": []interface{}{"a", "z"}}); err == nil {
		t.Error("fieldOrder unknown key should error")
	}
	// duplicate
	if _, err := newMapperTransform(map[string]interface{}{"fields": fields, "fieldOrder": []interface{}{"a", "a"}}); err == nil {
		t.Error("fieldOrder duplicate should error")
	}
	// non-string element
	if _, err := newMapperTransform(map[string]interface{}{"fields": fields, "fieldOrder": []interface{}{"a", 5}}); err == nil {
		t.Error("fieldOrder non-string should error")
	}
	// valid explicit order
	tr, err := newMapperTransform(map[string]interface{}{"fields": fields, "fieldOrder": []interface{}{"b", "a"}})
	if err != nil {
		t.Fatalf("valid fieldOrder: %v", err)
	}
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "a", Type: record.TypeInt}, {Name: "b", Type: record.TypeInt}},
		Rows:    [][]interface{}{{1, 2}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Columns[0].Name != "b" || out.Columns[1].Name != "a" {
		t.Errorf("explicit fieldOrder not honoured: %v", out.Columns)
	}
}

func TestMapper_TypeInference(t *testing.T) {
	tr, _ := newMapperTransform(map[string]interface{}{"fields": map[string]interface{}{
		"i": "=$i", "f": "=$f", "s": "=$s", "b": "=$b",
	}})
	in := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "i", Type: record.TypeInt}, {Name: "f", Type: record.TypeFloat},
			{Name: "s", Type: record.TypeString}, {Name: "b", Type: record.TypeBool},
		},
		Rows: [][]interface{}{{int64(1), 2.5, "x", true}},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	types := map[string]record.ColumnType{}
	for _, c := range out.Columns {
		types[c.Name] = c.Type
	}
	if types["i"] != record.TypeInt || types["f"] != record.TypeFloat ||
		types["s"] != record.TypeString || types["b"] != record.TypeBool {
		t.Errorf("inferred types wrong: %v", types)
	}
}

func TestMapper_DottedPathResolve(t *testing.T) {
	tr, _ := newMapperTransform(map[string]interface{}{"fields": map[string]interface{}{
		"city": "=$address.city",
	}})
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "address", Type: record.TypeAny}},
		Rows: [][]interface{}{
			{map[string]interface{}{"city": "Oslo"}},
			{map[string]interface{}{"other": "x"}}, // missing → nil
		},
	}
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Value(0, "city") != "Oslo" {
		t.Errorf("dotted path = %v, want Oslo", out.Value(0, "city"))
	}
	if out.Value(1, "city") != nil {
		t.Errorf("missing nested = %v, want nil", out.Value(1, "city"))
	}
}

func TestMapper_QuotedAndNumericLiterals(t *testing.T) {
	tr, _ := newMapperTransform(map[string]interface{}{"fields": map[string]interface{}{
		"lit":   "='hello'",
		"num":   "=3.14",
		"plain": "STATIC",
	}})
	in := &record.RecordBatch{Columns: []record.Column{{Name: "x", Type: record.TypeInt}}, Rows: [][]interface{}{{1}}}
	out, _ := tr.Apply(in)
	if out.Value(0, "lit") != "hello" {
		t.Errorf("quoted literal = %v", out.Value(0, "lit"))
	}
	if out.Value(0, "num") != 3.14 {
		t.Errorf("numeric literal = %v", out.Value(0, "num"))
	}
	if out.Value(0, "plain") != "STATIC" {
		t.Errorf("plain constant = %v", out.Value(0, "plain"))
	}
}
