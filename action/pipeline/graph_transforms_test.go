package pipeline

import (
	"path/filepath"
	"testing"

	dqtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"

	// Registry activation for the transforms the graph runner now supports.
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/join"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/union"
)

// nullSinkNode is a self-contained graph sink that discards its rows — used as a
// second fan-out branch so the graph is non-reducible (forcing the graph runner)
// without needing a second output file.
func nullSinkNode(id string) GraphNodeDef {
	return GraphNodeDef{ID: id, Kind: "sink", Type: "null",
		Extra: map[string]interface{}{"config": map[string]interface{}{}}}
}

// TestGraphRun_DQReportHarvested proves the graph runner surfaces the DQ gate
// report in Output.Metadata after the run — the harvest the linear engine does,
// now wired into runGraph. dq previously forced a fall-back to the linear path.
func TestGraphRun_DQReportHarvested(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outMain := filepath.Join(t.TempDir(), "main.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "dq", Extra: map[string]interface{}{
				"type": "dq",
				"expectations": []interface{}{
					map[string]interface{}{"type": "expectColumnNotNull", "column": "amount"},
				},
				"onFail":           "warn",
				"qualityThreshold": 0.95,
			}},
		},
		Sink: jsonlSink(outMain),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "dq"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				nullSinkNode("hi"),
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"}, // success: every row
				{From: "t0:out", To: "hi:in", Type: "expression", Value: "amount > 150"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q err=%s", out.Status, out.Error)
	}
	rep, ok := out.Metadata["dqReport"].(*dqtransform.DQReport)
	if !ok || rep == nil {
		t.Fatalf("expected *dq.DQReport in Metadata[dqReport], got %T", out.Metadata["dqReport"])
	}
	if rep.RowsEvaluated != 5 {
		t.Errorf("dqReport.RowsEvaluated=%d, want 5", rep.RowsEvaluated)
	}
}

// TestGraphRun_HWMWatermarkPersisted proves the graph runner harvests the hwm
// transform's observed maximum and persists it to the watermark store after a
// clean run — the incremental-load harvest the linear engine does.
func TestGraphRun_HWMWatermarkPersisted(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	wmDir := t.TempDir()

	def := &Definition{
		Source: csvSource(src),
		Incremental: IncrementalDef{
			Mode: "hwm", Column: "amount", ValueType: "int",
			StoreType: "file", StoreOptions: map[string]interface{}{"path": wmDir},
		},
		Transforms: []TransformDef{
			{Type: "hwm", Extra: map[string]interface{}{"type": "hwm", "column": "amount", "valueType": "int"}},
		},
		Sink: jsonlSink(filepath.Join(t.TempDir(), "main.jsonl")),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "hwm"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				nullSinkNode("hi"),
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"},
				{From: "t0:out", To: "hi:in", Type: "expression", Value: "amount > 150"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q err=%s", out.Status, out.Error)
	}

	store, err := watermark.New("file", map[string]interface{}{"path": wmDir})
	if err != nil {
		t.Fatalf("watermark.New: %v", err)
	}
	defer store.Close()
	mark, err := store.Load("sdk://test")
	if err != nil {
		t.Fatalf("watermark load: %v", err)
	}
	if mark == nil {
		t.Fatal("graph runner did not persist a watermark")
	}
	if mark.Value != "300" { // max amount in ordersCSV
		t.Errorf("persisted watermark = %q, want 300", mark.Value)
	}
}

// TestGraphRun_JoinEnriches proves a join transform runs under the graph runner
// (self-loading its bounded right side) and enriches the stream. join previously
// forced a fall-back to the linear path.
func TestGraphRun_JoinEnriches(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	ref := writeTemp(t, "tiers.csv", "id,tier\n1,gold\n2,silver\n3,gold\n4,bronze\n5,gold\n")
	outMain := filepath.Join(t.TempDir(), "main.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "join", Extra: map[string]interface{}{
				"type": "join",
				"right": map[string]interface{}{
					"type": "csv", "path": ref, "hasHeader": true,
					"columns": []interface{}{
						map[string]interface{}{"name": "id", "type": "string"},
						map[string]interface{}{"name": "tier", "type": "string"},
					},
				},
				"leftKey":      "id",
				"rightColumns": []interface{}{"tier"},
			}},
		},
		Sink: jsonlSink(outMain),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "join"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				nullSinkNode("hi"),
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"}, // success: every enriched row
				{From: "t0:out", To: "hi:in", Type: "expression", Value: "amount > 150"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q err=%s", out.Status, out.Error)
	}
	rows := readJSONL(t, outMain)
	if len(rows) != 5 {
		t.Fatalf("main sink rows=%d, want 5 (inner join, all match)", len(rows))
	}
	for _, r := range rows {
		if tier, ok := r["tier"].(string); !ok || tier == "" {
			t.Fatalf("joined row missing enriched 'tier' column: %v", r)
		}
	}
}

// TestGraphRun_UnionAddsRows proves a union transform runs under the graph runner
// and appends its bounded secondary feed: the source reads 5 rows and the sink
// receives 7. union previously forced a fall-back to the linear path.
func TestGraphRun_UnionAddsRows(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	extra := writeTemp(t, "more.csv", "id,name,amount,currency\n6,Frank,120,USD\n7,Grace,80,USD\n")
	outMain := filepath.Join(t.TempDir(), "main.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "union", Extra: map[string]interface{}{
				"type": "union",
				"with": []interface{}{
					map[string]interface{}{
						"type": "csv", "path": extra, "hasHeader": true,
						"columns": []interface{}{
							map[string]interface{}{"name": "id", "type": "string"},
							map[string]interface{}{"name": "name", "type": "string"},
							map[string]interface{}{"name": "amount", "type": "int"},
							map[string]interface{}{"name": "currency", "type": "string"},
						},
					},
				},
			}},
		},
		Sink: jsonlSink(outMain),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "union"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				nullSinkNode("hi"),
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"}, // success: every row (primary + unioned)
				{From: "t0:out", To: "hi:in", Type: "expression", Value: "amount > 100000"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q err=%s", out.Status, out.Error)
	}
	if out.RowsProcessed != 5 {
		t.Errorf("RowsProcessed=%d, want 5 (source rows only)", out.RowsProcessed)
	}
	if rows := readJSONL(t, outMain); len(rows) != 7 {
		t.Fatalf("main sink rows=%d, want 7 (5 primary + 2 unioned)", len(rows))
	}
}
