package join

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// rightCustomers builds a right-side batch matching customersCSV's data
// directly, for the graph-wired (LoadRight/CloseRight) path.
func rightCustomers() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{
			{Name: "id", Type: record.TypeString},
			{Name: "name", Type: record.TypeString},
			{Name: "tier", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{"1", "Alice", "gold"},
			{"2", "Bob", "silver"},
			{"2", "Bobby", "bronze"},
		},
	}
}

// multiInputJoin narrows a Transform down to the graph-wired capability
// methods the graph runner (graphexec.MultiInputOp) expects — declared
// locally (structural typing) so this test doesn't need to import
// action/pipeline/graphexec.
type multiInputJoin interface {
	LoadRight(*record.RecordBatch) error
	CloseRight() error
	ApplyLeft(*record.RecordBatch) (*record.RecordBatch, error)
}

func TestJoin_GraphWired_LoadRightCloseRightApplyLeft(t *testing.T) {
	tr, err := newJoinTransform(map[string]interface{}{
		"right":   "graph", // sentinel: the right side is wired via a graph edge, not inline config.
		"leftKey": "customerId", "rightKey": "id",
	})
	if err != nil {
		t.Fatalf("newJoinTransform (graph-wired): %v", err)
	}
	defer tr.Close()

	mi, ok := tr.(multiInputJoin)
	if !ok {
		t.Fatalf("graph-wired join does not implement LoadRight/CloseRight/ApplyLeft")
	}

	if err := mi.LoadRight(rightCustomers()); err != nil {
		t.Fatalf("LoadRight: %v", err)
	}
	if err := mi.CloseRight(); err != nil {
		t.Fatalf("CloseRight: %v", err)
	}

	out, err := mi.ApplyLeft(leftOrders("1", "2", "3"))
	if err != nil {
		t.Fatalf("ApplyLeft: %v", err)
	}
	rows := make([]map[string]interface{}, out.Len())
	for i := 0; i < out.Len(); i++ {
		rows[i] = out.RowMap(i)
	}

	// Same expectation as TestJoin_Inner: id=3 has no match → dropped; id=2
	// matches twice → fan-out.
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (Alice + Bob + Bobby; id=3 dropped): %v", len(rows), rows)
	}
	if rows[0]["name"] != "Alice" || rows[0]["tier"] != "gold" {
		t.Fatalf("row0 = %v, want Alice/gold", rows[0])
	}
	if rows[1]["orderRef"] != "order-2" || rows[2]["orderRef"] != "order-2" {
		t.Fatalf("fan-out rows lost left fields: %v %v", rows[1], rows[2])
	}
}

func TestJoin_GraphWired_LeftJoinKeepsUnmatched(t *testing.T) {
	tr, err := newJoinTransform(map[string]interface{}{
		"right": "graph", "leftKey": "customerId", "rightKey": "id", "joinType": "left",
	})
	if err != nil {
		t.Fatalf("newJoinTransform (graph-wired): %v", err)
	}
	defer tr.Close()
	mi := tr.(multiInputJoin)
	if err := mi.LoadRight(rightCustomers()); err != nil {
		t.Fatalf("LoadRight: %v", err)
	}
	if err := mi.CloseRight(); err != nil {
		t.Fatalf("CloseRight: %v", err)
	}
	out, err := mi.ApplyLeft(leftOrders("1", "3"))
	if err != nil {
		t.Fatalf("ApplyLeft: %v", err)
	}
	if out.Len() != 2 {
		t.Fatalf("rows = %d, want 2 (Alice + unmatched id=3)", out.Len())
	}
	var unmatched map[string]interface{}
	for i := 0; i < out.Len(); i++ {
		r := out.RowMap(i)
		if r["customerId"] == "3" {
			unmatched = r
		}
	}
	if unmatched == nil {
		t.Fatalf("left join dropped the unmatched row")
	}
	if unmatched["name"] != nil {
		t.Fatalf("unmatched right column should be nil, got %v", unmatched["name"])
	}
}

// TestJoin_GraphWired_MissingRightConfigStillErrors proves omitting "right"
// ENTIRELY (not even the "graph" sentinel) is still a hard construction error
// for a plain linear-model join — only the explicit "graph" sentinel opts into
// lazy/graph-wired construction, so an accidental omission still fails loudly.
func TestJoin_GraphWired_MissingRightConfigStillErrors(t *testing.T) {
	_, err := newJoinTransform(map[string]interface{}{"leftKey": "customerId"})
	if err == nil {
		t.Fatal("expected an error when 'right' is entirely omitted")
	}
}

// TestJoin_GraphWired_ApplyLeftBeforeCloseRightErrors proves calling ApplyLeft
// before CloseRight is rejected rather than silently joining against a
// partially-loaded (or empty) table.
func TestJoin_GraphWired_ApplyLeftBeforeCloseRightErrors(t *testing.T) {
	tr, err := newJoinTransform(map[string]interface{}{
		"right": "graph", "leftKey": "customerId", "rightKey": "id",
	})
	if err != nil {
		t.Fatalf("newJoinTransform (graph-wired): %v", err)
	}
	defer tr.Close()
	mi := tr.(multiInputJoin)
	if err := mi.LoadRight(rightCustomers()); err != nil {
		t.Fatalf("LoadRight: %v", err)
	}
	if _, err := mi.ApplyLeft(leftOrders("1")); err == nil {
		t.Fatal("expected ApplyLeft to error when CloseRight has not been called yet")
	}
}
