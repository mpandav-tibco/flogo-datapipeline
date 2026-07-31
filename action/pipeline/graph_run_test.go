package pipeline

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
)

func mkGraphDef(nTransforms int, txType string, g *GraphDef) *Definition {
	tr := make([]TransformDef, nTransforms)
	for i := range tr {
		tr[i] = TransformDef{Type: txType, Extra: map[string]interface{}{"type": txType, "expr": "amount > 0"}}
	}
	return &Definition{
		Source:     SourceDef{Type: "csv"},
		Transforms: tr,
		Sink:       SinkDef{Type: "null"},
		Graph:      g,
	}
}

func TestTransformIndex(t *testing.T) {
	cases := []struct {
		id  string
		idx int
		ok  bool
	}{
		{"t0", 0, true}, {"t12", 12, true}, {"source", 0, false},
		{"sink", 0, false}, {"t", 0, false}, {"tx", 0, false}, {"dlq", 0, false},
	}
	for _, c := range cases {
		idx, ok := transformIndex(c.id)
		if ok != c.ok || (ok && idx != c.idx) {
			t.Fatalf("transformIndex(%q) = (%d,%v), want (%d,%v)", c.id, idx, ok, c.idx, c.ok)
		}
	}
}

func TestGraphExecutable(t *testing.T) {
	stdNodes := []GraphNodeDef{
		{ID: "source", Kind: "source"}, {ID: "t0", Kind: "transform"}, {ID: "sink", Kind: "sink"},
	}
	chain := []GraphEdge{{From: "source:rows", To: "t0:in"}, {From: "t0:out", To: "sink:in"}}

	// A pure linear chain graph is reducible → must stay on the linear path.
	if graphExecutable(mkGraphDef(1, "filter", &GraphDef{Nodes: stdNodes, Edges: chain})) {
		t.Fatal("linear chain graph must NOT trigger the graph runner")
	}

	// A conditional edge makes it non-reducible → run via the graph.
	condEdges := append(append([]GraphEdge{}, chain...),
		GraphEdge{From: "source:rows", To: "sink:in", Type: "expression", Value: "amount > 100"})
	if !graphExecutable(mkGraphDef(1, "filter", &GraphDef{Nodes: stdNodes, Edges: condEdges})) {
		t.Fatal("conditional-edge graph must trigger the graph runner")
	}

	// A non-standard node (dlq branch) is deferred → fall back to linear.
	branchNodes := append(append([]GraphNodeDef{}, stdNodes...), GraphNodeDef{ID: "dlq", Kind: "sink"})
	branchEdges := append(append([]GraphEdge{}, chain...),
		GraphEdge{From: "source:rows", To: "dlq:in", Type: "expression", Value: "amount > 100"})
	if graphExecutable(mkGraphDef(1, "filter", &GraphDef{Nodes: branchNodes, Edges: branchEdges})) {
		t.Fatal("graph with a non-standard node (dlq) must fall back to linear")
	}

	// An unsupported transform (window) with a conditional edge → fall back.
	if graphExecutable(mkGraphDef(1, "window", &GraphDef{Nodes: stdNodes, Edges: condEdges})) {
		t.Fatal("graph containing an unsupported transform (window) must fall back to linear")
	}
}

// End-to-end through the real orchestration path: a conditional edge lets high
// value rows skip the filter and reach the sink, changing the observable output.
func TestGraphRun_ConditionalSkip(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 200"}},
		},
		Sink: jsonlSink(outPath),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "filter"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in", Type: "otherwise"},
				{From: "t0:out", To: "sink:in"},
				{From: "source:rows", To: "sink:in", Type: "expression", Value: "amount > 100"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", out.Status, out.Error)
	}
	// amount>100 rows (Carol 200, Eve 300) skip straight to the sink; the rest
	// fall through the filter(amount>200), which drops them all — so the sink
	// receives exactly those 2. (Linear would keep only Eve/300.)
	if out.RowsProcessed != 5 || out.RowsWritten != 2 {
		t.Fatalf("processed=%d written=%d, want 5/2", out.RowsProcessed, out.RowsWritten)
	}
	rows := readJSONL(t, outPath)
	names := map[string]bool{}
	for _, r := range rows {
		names[fmt.Sprint(r["name"])] = true
	}
	if len(rows) != 2 || !names["Carol"] || !names["Eve"] {
		t.Fatalf("sink rows = %v, want exactly Carol + Eve", rows)
	}
}

// End-to-end with a self-contained multi-sink graph: a condition edge and an
// otherwise edge partition the source into two DISTINCT sinks that each carry
// their own config (no linear "sink" node is involved).
func TestGraphRun_MultiSink(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	bigPath := filepath.Join(t.TempDir(), "big.jsonl")
	restPath := filepath.Join(t.TempDir(), "rest.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Sink:   SinkDef{Type: "null"}, // required by the schema; unused by the graph
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "big", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": bigPath}}},
				{ID: "rest", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": restPath}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "big:in", Type: "expression", Value: "amount > 100"},
				{From: "source:rows", To: "rest:in", Type: "otherwise"},
			},
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", out.Status, out.Error)
	}
	if out.RowsProcessed != 5 || out.RowsWritten != 5 {
		t.Fatalf("processed=%d written=%d, want 5/5", out.RowsProcessed, out.RowsWritten)
	}
	// amount>100 → Carol(200), Eve(300); otherwise → Alice(100), Bob(-50), Dave(0).
	if big := readJSONL(t, bigPath); len(big) != 2 {
		t.Fatalf("big sink = %d rows, want 2 (Carol, Eve)", len(big))
	}
	if rest := readJSONL(t, restPath); len(rest) != 3 {
		t.Fatalf("rest sink = %d rows, want 3 (Alice, Bob, Dave)", len(rest))
	}
}

// The studio emits an extra sink node as {id,kind,type,ports,config} with a typed
// edge. Parse that EXACT JSON shape and run it — proving studio output executes
// on the engine end-to-end (GraphNodeDef.UnmarshalJSON → Extra["config"]).
func TestGraphRun_ExtraSinkJSON(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	xsinkPath := filepath.Join(t.TempDir(), "errors.jsonl")
	raw := fmt.Sprintf(`{
      "source": {"type":"csv","path":%q,"hasHeader":true},
      "sink": {"type":"null"},
      "graph": {
        "nodes": [
          {"id":"source","kind":"source","type":"csv"},
          {"id":"sink","kind":"sink","type":"null"},
          {"id":"xsink-1","kind":"sink","type":"jsonlines","ports":{"in":["in"]},"config":{"path":%q}}
        ],
        "edges": [
          {"from":"source:rows","to":"xsink-1:in","type":"expression","value":"amount > 100"},
          {"from":"source:rows","to":"sink:in","type":"otherwise"}
        ]
      }
    }`, src, xsinkPath)
	var def Definition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := runInline(t, &def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", out.Status, out.Error)
	}
	// amount>100 → Carol(200), Eve(300) reach the extra jsonlines sink; the rest
	// go to the null sink (discarded).
	if rows := readJSONL(t, xsinkPath); len(rows) != 2 {
		t.Fatalf("extra sink = %d rows, want 2 (Carol, Eve)", len(rows))
	}
}
