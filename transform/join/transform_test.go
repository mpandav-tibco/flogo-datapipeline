package join

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"

	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
)

// customersCSV is the reference (right) dataset. Note id=2 appears twice so the
// many-to-many fan-out can be exercised.
const customersCSV = `id,name,tier
1,Alice,gold
2,Bob,silver
2,Bobby,bronze
`

func writeCSV(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "right.csv")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	return p
}

// leftOrders builds a left batch of orders keyed by customerId.
func leftOrders(ids ...string) *record.RecordBatch {
	rows := make([][]interface{}, len(ids))
	for i, id := range ids {
		rows[i] = []interface{}{id, "order-" + id}
	}
	return &record.RecordBatch{
		Columns: []record.Column{
			{Name: "customerId", Type: record.TypeString},
			{Name: "orderRef", Type: record.TypeString},
		},
		Rows: rows,
	}
}

// build constructs a join transform from cfg (adding the right CSV path).
func build(t *testing.T, csvPath string, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	cfg["right"] = map[string]interface{}{"type": "csv", "path": csvPath, "hasHeader": true}
	tr, err := newJoinTransform(cfg)
	if err != nil {
		t.Fatalf("newJoinTransform: %v", err)
	}
	return tr
}

func applyToMaps(t *testing.T, tr transform.Transform, in *record.RecordBatch) []map[string]interface{} {
	t.Helper()
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	res := make([]map[string]interface{}, out.Len())
	for i := 0; i < out.Len(); i++ {
		res[i] = out.RowMap(i)
	}
	return res
}

func TestJoin_Inner(t *testing.T) {
	csv := writeCSV(t, customersCSV)
	tr := build(t, csv, map[string]interface{}{"leftKey": "customerId", "rightKey": "id"})
	defer tr.Close()

	// customerId 3 has no match → dropped; 2 matches twice → fan-out.
	rows := applyToMaps(t, tr, leftOrders("1", "2", "3"))
	if len(rows) != 3 {
		t.Fatalf("inner join rows = %d, want 3 (Alice + Bob + Bobby; id=3 dropped)", len(rows))
	}
	if rows[0]["name"] != "Alice" || rows[0]["tier"] != "gold" {
		t.Fatalf("row0 = %v, want Alice/gold", rows[0])
	}
	// The two id=2 matches preserve the left order fields.
	if rows[1]["orderRef"] != "order-2" || rows[2]["orderRef"] != "order-2" {
		t.Fatalf("fan-out rows lost left fields: %v %v", rows[1], rows[2])
	}
}

func TestJoin_Left_KeepsUnmatched(t *testing.T) {
	csv := writeCSV(t, customersCSV)
	tr := build(t, csv, map[string]interface{}{"leftKey": "customerId", "rightKey": "id", "joinType": "left"})
	defer tr.Close()

	rows := applyToMaps(t, tr, leftOrders("1", "3"))
	if len(rows) != 2 {
		t.Fatalf("left join rows = %d, want 2 (Alice + unmatched id=3)", len(rows))
	}
	// id=3 kept with nil right columns.
	var unmatched map[string]interface{}
	for _, r := range rows {
		if r["customerId"] == "3" {
			unmatched = r
		}
	}
	if unmatched == nil {
		t.Fatalf("left join dropped the unmatched row: %v", rows)
	}
	if unmatched["name"] != nil {
		t.Fatalf("unmatched right column should be nil, got %v", unmatched["name"])
	}
}

func TestJoin_RightColumnsSubsetAndPrefix(t *testing.T) {
	csv := writeCSV(t, customersCSV)
	tr := build(t, csv, map[string]interface{}{
		"leftKey": "customerId", "rightKey": "id",
		"rightColumns": []interface{}{"name"}, "prefix": "cust_",
	})
	defer tr.Close()

	rows := applyToMaps(t, tr, leftOrders("1"))
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0]["cust_name"] != "Alice" {
		t.Fatalf("prefixed column cust_name = %v, want Alice", rows[0]["cust_name"])
	}
	if _, ok := rows[0]["tier"]; ok {
		t.Fatalf("tier should be excluded (only name requested): %v", rows[0])
	}
}

func TestJoin_RightKeyDefaultsToLeftKey(t *testing.T) {
	// Right CSV keyed by the same column name as the left key.
	csv := writeCSV(t, "customerId,name\n1,Alice\n")
	tr := build(t, csv, map[string]interface{}{"leftKey": "customerId"}) // no rightKey
	defer tr.Close()
	rows := applyToMaps(t, tr, leftOrders("1"))
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("default rightKey join failed: %v", rows)
	}
}

func TestJoin_MaxRowsExceeded(t *testing.T) {
	csv := writeCSV(t, customersCSV) // 3 right rows
	csvRight := map[string]interface{}{"type": "csv", "path": csv, "hasHeader": true}
	_, err := newJoinTransform(map[string]interface{}{
		"right": csvRight, "leftKey": "customerId", "rightKey": "id", "maxRows": float64(2),
	})
	if err == nil {
		t.Fatal("expected maxRows exceeded error, got nil")
	}
}

func TestJoin_ValidationErrors(t *testing.T) {
	csv := writeCSV(t, customersCSV)
	right := map[string]interface{}{"type": "csv", "path": csv, "hasHeader": true}
	cases := []struct {
		name string
		cfg  map[string]interface{}
	}{
		{"no right", map[string]interface{}{"leftKey": "customerId"}},
		{"no leftKey", map[string]interface{}{"right": right}},
		{"bad joinType", map[string]interface{}{"right": right, "leftKey": "customerId", "joinType": "outer"}},
		{"right no type", map[string]interface{}{"right": map[string]interface{}{"path": csv}, "leftKey": "customerId"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newJoinTransform(tc.cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
