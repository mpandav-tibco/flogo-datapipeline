package pipeline

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"

	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/validate"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

// TestGraphRun_NativeEdgeRouting proves the "router"/"split" transform's
// routing semantics (named routes, checked in priority order, a default
// fallback) are FULLY replicable with native conditional/otherwise graph
// edges wired straight to sink nodes — the pattern DAG-authored pipelines
// should prefer (see the recommendation in transform/router's package doc and
// Phase E notes): no node owns an external sink directly, so every route's
// row count is visible in graphStats/the canvas, and the node is no longer
// forced single-threaded by SinkOwner (moot for the graph runner today, since
// it never parallelizes within one node anyway — but relevant if that changes).
//
// Route priority (mirrors router's own doc example, "high" listed before
// "eu"): a row matching BOTH conditions must land ONLY in "high" — the "eu"
// edge's condition is written to explicitly EXCLUDE what "high" already
// claims (amount<=1000), which is the pattern authors must follow to replicate
// exclusive first-match routing with the graph's inherently TEE-style
// conditional edges (a row can match multiple conditions and gets a copy on
// each — only correct as "exclusive routing" when predicates are mutually
// exclusive, as here).
func TestGraphRun_NativeEdgeRouting(t *testing.T) {
	src := writeTemp(t, "in.csv", `id,amount,region
1,2000,US
2,500,EU
3,300,US
4,1500,EU
`)
	highPath := filepath.Join(t.TempDir(), "high.jsonl")
	euPath := filepath.Join(t.TempDir(), "eu.jsonl")
	defaultPath := filepath.Join(t.TempDir(), "default.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Sink:   SinkDef{Type: "null"}, // required by the schema; unused by the graph
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "high", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": highPath}}},
				{ID: "eu", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": euPath}}},
				{ID: "def", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": defaultPath}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "high:in", Type: "expression", Value: "amount > 1000"},
				{From: "source:rows", To: "eu:in", Type: "expression", Value: "region = 'EU' AND amount <= 1000"},
				{From: "source:rows", To: "def:in", Type: "otherwise"},
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
	if out.RowsProcessed != 4 {
		t.Fatalf("RowsProcessed = %d, want 4", out.RowsProcessed)
	}
	// id=1 (amount>1000) and id=4 (amount>1000 AND region=EU) both go to "high"
	// ONLY — id=4 must NOT also leak into "eu" despite matching region='EU'.
	if rows := readJSONL(t, highPath); len(rows) != 2 {
		t.Fatalf("high sink = %d rows, want 2 (id 1 and 4): %v", len(rows), rows)
	}
	if rows := readJSONL(t, euPath); len(rows) != 1 || fmt.Sprint(rows[0]["id"]) != "2" {
		t.Fatalf("eu sink = %v, want exactly id=2", rows)
	}
	if rows := readJSONL(t, defaultPath); len(rows) != 1 || fmt.Sprint(rows[0]["id"]) != "3" {
		t.Fatalf("default sink = %v, want exactly id=3", rows)
	}
	// graphStats must show the source's RowsOut split across all 3 sinks — the
	// concrete observability win over router's owned (graph-invisible) sinks.
	gs, ok := out.Metadata["graphStats"].([]graphexec.NodeStat)
	if !ok {
		t.Fatalf("Metadata[\"graphStats\"] missing or wrong type: %T", out.Metadata["graphStats"])
	}
	written := map[string]int64{}
	for _, ns := range gs {
		written[ns.ID] = ns.RowsWritten
	}
	if written["high"] != 2 || written["eu"] != 1 || written["def"] != 1 {
		t.Fatalf("graphStats written counts = %v, want high=2 eu=1 def=1", written)
	}
}

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

	// An unsupported transform type would fall back to linear — but the set is
	// now empty. hwm / dq / join / union (and window) are ALL supported by the
	// graph runner, so a non-reducible graph using any of them must run via the
	// graph, not fall back.
	for _, txType := range []string{"hwm", "dq", "join", "union", "window"} {
		if !graphExecutable(mkGraphDef(1, txType, &GraphDef{Nodes: stdNodes, Edges: condEdges})) {
			t.Fatalf("graph with a %q transform must run via the graph runner", txType)
		}
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

// TestGraphRun_TwoPortJoin proves a join transform can be wired from TWO
// distinct source nodes via graph edges targeting its named "left"/"right"
// ports, end-to-end through the real orchestration path — the join's "right"
// side is no longer embedded inline config but a separate graph node/subgraph.
func TestGraphRun_TwoPortJoin(t *testing.T) {
	leftCSV := writeTemp(t, "orders.csv", `customerId,orderRef
1,order-1
2,order-2
3,order-3
`)
	rightCSV := writeTemp(t, "customers.csv", `id,name,tier
1,Alice,gold
2,Bob,silver
`)
	outPath := filepath.Join(t.TempDir(), "joined.jsonl")

	def := &Definition{
		Source: SourceDef{Type: "csv"}, // required by the schema; unused by the graph
		Sink:   SinkDef{Type: "null"},  // required by the schema; unused by the graph
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "leftSrc", Kind: "source", Type: "csv", Extra: map[string]interface{}{
					"config": map[string]interface{}{"path": leftCSV, "hasHeader": true},
				}},
				{ID: "rightSrc", Kind: "source", Type: "csv", Extra: map[string]interface{}{
					"config": map[string]interface{}{"path": rightCSV, "hasHeader": true},
				}},
				{ID: "j", Kind: "transform", Type: "join", Extra: map[string]interface{}{
					"config": map[string]interface{}{"leftKey": "customerId", "rightKey": "id", "right": "graph"},
				}},
				{ID: "out", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{
					"config": map[string]interface{}{"path": outPath},
				}},
			},
			Edges: []GraphEdge{
				{From: "leftSrc:rows", To: "j:left"},
				{From: "rightSrc:rows", To: "j:right"},
				{From: "j:out", To: "out:in"},
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
	if out.RowsProcessed != 5 { // 3 left + 2 right
		t.Fatalf("RowsProcessed = %d, want 5", out.RowsProcessed)
	}
	rows := readJSONL(t, outPath)
	// customerId=3 has no match on the right → dropped (inner join default).
	if len(rows) != 2 {
		t.Fatalf("joined rows = %d, want 2 (customerId 1 and 2): %v", len(rows), rows)
	}
	byRef := map[string]map[string]interface{}{}
	for _, r := range rows {
		byRef[fmt.Sprint(r["orderRef"])] = r
	}
	if byRef["order-1"]["name"] != "Alice" || byRef["order-1"]["tier"] != "gold" {
		t.Fatalf("order-1 row = %v, want Alice/gold", byRef["order-1"])
	}
	if byRef["order-2"]["name"] != "Bob" || byRef["order-2"]["tier"] != "silver" {
		t.Fatalf("order-2 row = %v, want Bob/silver", byRef["order-2"])
	}
}

// Per-row error routing: a validate node with an error edge sends its violating
// rows down the edge to an error sink row-by-row (not its internal sink).
func TestGraphRun_ValidateReject(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	rejectPath := filepath.Join(t.TempDir(), "rejects.jsonl")
	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "validate", Extra: map[string]interface{}{
				"type":        "validate",
				"rules":       []interface{}{map[string]interface{}{"field": "amount", "min": 1}},
				"onViolation": "drop",
			}},
		},
		Sink: SinkDef{Type: "null"},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "validate"},
				{ID: "sink", Kind: "sink", Type: "null"},
				{ID: "xsink-1", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": rejectPath}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"},
				{From: "t0:out", To: "xsink-1:in", Type: "error"},
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
	// amount < 1 violates: Bob(-50) and Dave(0) → error edge; the other 3 pass.
	if rej := readJSONL(t, rejectPath); len(rej) != 2 {
		t.Fatalf("reject sink = %d rows, want 2 (Bob, Dave)", len(rej))
	}
}

const windowCSV = `ts,sensor
2020-01-01T00:00:00Z,A
2020-01-01T00:00:10Z,A
2020-01-01T00:00:20Z,A
`

// A window transform runs as a graph node: it buffers rows and the runner
// flushes its open aggregates at end-of-input (the Flusher hook). Fan-out to two
// sinks makes the graph non-reducible so the graph runner is used.
func TestGraphRun_Window(t *testing.T) {
	src := writeTemp(t, "win.csv", windowCSV)
	out1 := filepath.Join(t.TempDir(), "agg1.jsonl")
	out2 := filepath.Join(t.TempDir(), "agg2.jsonl")
	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "window", Extra: map[string]interface{}{
				"type":         "window",
				"eventTime":    map[string]interface{}{"column": "ts", "format": "RFC3339"},
				"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(60000)},
				"groupBy":      []interface{}{"sensor"},
				"aggregations": []interface{}{map[string]interface{}{"as": "n", "fn": "count"}},
			}},
		},
		Sink: jsonlSink(out1),
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "window"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				{ID: "sink2", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": out2}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"},
				{From: "t0:out", To: "sink2:in"},
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
	// 3 rows in one 60s tumbling window for sensor A → 1 aggregate row (n=3),
	// teed to both sinks by the fan-out (proves window flushed on the graph path).
	if r1 := readJSONL(t, out1); len(r1) != 1 {
		t.Fatalf("sink1 = %d rows, want 1 aggregate", len(r1))
	}
	if r2 := readJSONL(t, out2); len(r2) != 1 {
		t.Fatalf("sink2 = %d rows, want 1 aggregate", len(r2))
	}
}

// ─── checkpoint / resume on the graph path ──────────────────────────────────

// A non-reducible (branching) graph gets full no-loss checkpointing: the source
// pauses at each interval, a barrier flushes every sink, and the position is
// persisted only once all sinks are durable. This mirrors the linear
// TestRunDirect_Checkpoint_SaveAndResume but exercises the graph runner: run 1
// processes everything and persists the checkpoint; run 2 (same key) resumes,
// skips the already-processed rows, and writes nothing.
func TestGraphRun_CheckpointSaveAndResume(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	ckptDir := t.TempDir()

	mkDef := func() *Definition {
		bigPath := filepath.Join(t.TempDir(), "big.jsonl")
		restPath := filepath.Join(t.TempDir(), "rest.jsonl")
		return &Definition{
			Source: csvSource(src),
			Sink:   SinkDef{Type: "null"}, // required by the schema; unused by the graph
			Checkpoint: CheckpointDef{
				Type:            "file",
				Path:            ckptDir,
				Key:             "graph-resume-key",
				IntervalRows:    1, // checkpoint aggressively
				DeleteOnSuccess: false,
			},
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
	}

	// Run 1: processes all 5 via the graph runner and persists a checkpoint.
	out1, err := runInline(t, mkDef())
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	if out1.Status != "completed" {
		t.Fatalf("run1 status=%q, want completed (err=%s)", out1.Status, out1.Error)
	}
	if out1.RowsProcessed != 5 || out1.RowsWritten != 5 {
		t.Fatalf("run1 processed=%d written=%d, want 5/5", out1.RowsProcessed, out1.RowsWritten)
	}

	// Run 2 (same key): the graph runner must resume, skip the 5 processed rows,
	// and write nothing — proving checkpoint load→skip is wired on the graph path.
	out2, err := runInline(t, mkDef())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if out2.RowsWritten != 0 {
		t.Fatalf("run2 written=%d, want 0 (all rows already checkpointed → skipped)", out2.RowsWritten)
	}
}

// A windowed (stateful) graph with checkpoint configured now checkpoints
// correctly (see TestGraphRun_StatefulCheckpoint_ResumesWindowState for the
// resume proof) — this test just guards that a normal, uninterrupted run still
// completes with the right aggregate when checkpointing is configured.
func TestGraphRun_CheckpointStatefulGuard(t *testing.T) {
	src := writeTemp(t, "win.csv", windowCSV)
	outp := filepath.Join(t.TempDir(), "agg.jsonl")
	out2 := filepath.Join(t.TempDir(), "agg2.jsonl")
	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "window", Extra: map[string]interface{}{
				"type":         "window",
				"eventTime":    map[string]interface{}{"column": "ts", "format": "RFC3339"},
				"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(60000)},
				"groupBy":      []interface{}{"sensor"},
				"aggregations": []interface{}{map[string]interface{}{"as": "n", "fn": "count"}},
			}},
		},
		Sink:       jsonlSink(outp),
		Checkpoint: CheckpointDef{Type: "file", Path: t.TempDir(), Key: "win-key", IntervalRows: 1},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "csv"},
				{ID: "t0", Kind: "transform", Type: "window"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				{ID: "sink2", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": out2}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"},
				{From: "t0:out", To: "sink2:in"},
			},
		},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status=%q, want completed (err=%s)", out.Status, out.Error)
	}
	if r := readJSONL(t, outp); len(r) != 1 {
		t.Fatalf("sink = %d rows, want 1 aggregate (window stays correct with checkpointing enabled)", len(r))
	}
}
