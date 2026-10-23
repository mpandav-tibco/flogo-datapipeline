package mapper

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// TestMapperSlotBinding covers the positional field binding: nested paths,
// absent columns, ragged rows, and a schema change between batches.
func TestMapperSlotBinding(t *testing.T) {
	tr, err := newMapperTransform(map[string]interface{}{
		"fields": map[string]interface{}{
			"a":     "=$amount",
			"city":  "=$.addr.city",
			"miss":  "=$nope",
			"deep":  "=$.nope.x",
			"upper": "=upper($name)",
		},
		"fieldOrder": []interface{}{"a", "city", "miss", "deep", "upper"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b1 := &record.RecordBatch{
		Columns: []record.Column{{Name: "amount"}, {Name: "addr"}, {Name: "name"}},
		Rows: [][]interface{}{
			{1.5, map[string]interface{}{"city": "Pune"}, "ann"},
			{2.5, "not-a-map", "bob"},
			{3.5}, // ragged
		},
	}
	want1 := [][]interface{}{
		{1.5, "Pune", nil, nil, "ANN"},
		{2.5, nil, nil, nil, "BOB"},
		{3.5, nil, nil, nil, nil},
	}
	// Same field names, different positions: the binding must follow the names.
	b2 := &record.RecordBatch{
		Columns: []record.Column{{Name: "name"}, {Name: "amount"}, {Name: "addr"}},
		Rows:    [][]interface{}{{"cy", 9.0, map[string]interface{}{"city": "Oslo"}}},
	}
	want2 := [][]interface{}{{9.0, "Oslo", nil, nil, "CY"}}

	for bi, tc := range []struct {
		in   *record.RecordBatch
		want [][]interface{}
	}{{b1, want1}, {b2, want2}} {
		out, err := tr.Apply(tc.in)
		if err != nil {
			t.Fatalf("batch %d: %v", bi, err)
		}
		for i, row := range tc.want {
			for j, w := range row {
				if got := out.Rows[i][j]; got != w {
					t.Errorf("batch %d row %d col %s: got %#v want %#v", bi, i, out.Columns[j].Name, got, w)
				}
			}
		}
	}
}
