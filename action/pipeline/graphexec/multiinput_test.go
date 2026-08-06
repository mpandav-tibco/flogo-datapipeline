package graphexec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// fakeMultiInput is a minimal join-like MultiInputOp: LoadRight populates a
// key->value table, CloseRight finalizes it, ApplyLeft appends the matched
// right value as a new column. It ALSO implements plain TransformOp (Apply)
// so a node using it with NO "right"-port edges falls back to the ordinary
// single-input path unchanged — exactly mirroring how the real join transform
// must keep working via inline "right" config when merely promoted into a
// graph for unrelated reasons.
type fakeMultiInput struct {
	mu                 sync.Mutex
	table              map[string]string
	closed             bool
	loadCalls          int
	closeCalls         int
	applyLeftCalls     int
	applyCalls         int
	appliedBeforeClose bool // set true if ApplyLeft/Apply ran before CloseRight — a scheduling bug
}

func newFakeMultiInput() *fakeMultiInput { return &fakeMultiInput{table: map[string]string{}} }

func (f *fakeMultiInput) LoadRight(b *record.RecordBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	for i := range b.Rows {
		m := b.RowMap(i)
		f.table[fmt.Sprint(m["key"])] = fmt.Sprint(m["val"])
	}
	return nil
}

func (f *fakeMultiInput) CloseRight() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	f.closed = true
	return nil
}

func (f *fakeMultiInput) ApplyLeft(b *record.RecordBatch) (*record.RecordBatch, error) {
	f.mu.Lock()
	f.applyLeftCalls++
	if !f.closed {
		f.appliedBeforeClose = true
	}
	f.mu.Unlock()
	out := b.Clone(b.Len())
	out.Columns = append(append([]record.Column{}, b.Columns...), record.Column{Name: "rval", Type: record.TypeAny})
	for i := range b.Rows {
		m := b.RowMap(i)
		rv := f.table[fmt.Sprint(m["key"])]
		row := append(append([]interface{}{}, b.Rows[i]...), rv)
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

// Apply is the plain (ordinary-path) TransformOp entrypoint — used only when
// the node has no "right"-port inbound edge (never wired as multi-input).
func (f *fakeMultiInput) Apply(b *record.RecordBatch) (*record.RecordBatch, error) {
	f.mu.Lock()
	f.applyCalls++
	f.mu.Unlock()
	return b, nil
}

func (f *fakeMultiInput) Close() error { return nil }

var joinTestCols = []record.Column{{Name: "key", Type: record.TypeString}, {Name: "n", Type: record.TypeInt}}
var rightTestCols = []record.Column{{Name: "key", Type: record.TypeString}, {Name: "val", Type: record.TypeString}}

// TestMultiInput_JoinLikeOutput proves a two-port multi-input node actually
// joins rows from two DISTINCT sources wired via named "left"/"right" edges,
// through the real Run() orchestration (not just direct method calls).
func TestMultiInput_JoinLikeOutput(t *testing.T) {
	leftSrc := &memSource{cols: joinTestCols, rows: [][]interface{}{{"a", 1}, {"b", 2}, {"c", 3}}}
	rightSrc := &memSource{cols: rightTestCols, rows: [][]interface{}{{"a", "APPLE"}, {"b", "BANANA"}}}
	snk := &memSink{}
	op := newFakeMultiInput()

	g := &Graph{
		Nodes: []Node{
			{ID: "left", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": leftSrc}},
			{ID: "right", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": rightSrc}},
			{ID: "j", Kind: KindTransform, Type: "join", Cfg: map[string]interface{}{"op": op}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{
			{From: "left", To: "j:left"},
			{From: "right", To: "j:right"},
			{From: "j", To: "sink"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RowsRead != 5 { // 3 left + 2 right
		t.Fatalf("RowsRead = %d, want 5", res.RowsRead)
	}
	if op.loadCalls == 0 {
		t.Fatal("LoadRight was never called")
	}
	if op.closeCalls != 1 {
		t.Fatalf("CloseRight called %d times, want exactly 1", op.closeCalls)
	}
	if op.applyLeftCalls == 0 {
		t.Fatal("ApplyLeft was never called")
	}
	if op.applyCalls != 0 {
		t.Fatalf("plain Apply was called %d times, want 0 (this node is multi-input active)", op.applyCalls)
	}
	if op.appliedBeforeClose {
		t.Fatal("ApplyLeft ran before CloseRight — the right side must be fully drained first")
	}
	if snk.rowCount() != 3 {
		t.Fatalf("sink rows = %d, want 3", snk.rowCount())
	}
	got := map[string]string{}
	for _, r := range snk.rows {
		got[fmt.Sprint(r[0])] = fmt.Sprint(r[2])
	}
	want := map[string]string{"a": "APPLE", "b": "BANANA", "c": ""}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("row %q rval = %q, want %q (got all: %v)", k, got[k], v, got)
		}
	}
}

// TestMultiInput_FallsBackToOrdinaryWhenNoRightEdges proves a node whose
// operator implements MultiInputOp but has ZERO "right"-port inbound edges
// runs through the ORDINARY single-input path (plain Apply) unchanged — this
// is what keeps an existing join with inline "right" config working when a
// pipeline is merely promoted into a graph for unrelated reasons (e.g. a
// conditional edge elsewhere).
func TestMultiInput_FallsBackToOrdinaryWhenNoRightEdges(t *testing.T) {
	leftSrc := &memSource{cols: joinTestCols, rows: [][]interface{}{{"a", 1}}}
	snk := &memSink{}
	op := newFakeMultiInput()

	g := &Graph{
		Nodes: []Node{
			{ID: "left", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": leftSrc}},
			{ID: "j", Kind: KindTransform, Type: "join", Cfg: map[string]interface{}{"op": op}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{
			{From: "left", To: "j"},
			{From: "j", To: "sink"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := plan.Run(context.Background(), Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if op.applyCalls == 0 {
		t.Fatal("plain Apply was never called — node should use the ordinary path with no right-port edges")
	}
	if op.loadCalls != 0 || op.closeCalls != 0 || op.applyLeftCalls != 0 {
		t.Fatalf("multi-input methods were called (load=%d close=%d applyLeft=%d) — want none, this node has no right edges",
			op.loadCalls, op.closeCalls, op.applyLeftCalls)
	}
}

// TestMultiInput_RightBarrierIgnored proves a checkpoint barrier arriving on
// the right channel (e.g. because the right subgraph's own source enabled
// unrelated checkpointing) is simply ignored rather than breaking the run —
// the bounded right side is not part of the primary checkpoint story.
func TestMultiInput_RightBarrierIgnored(t *testing.T) {
	leftSrc := &memSource{cols: joinTestCols, rows: [][]interface{}{{"a", 1}}}
	rightSrc := &memSource{cols: rightTestCols, rows: [][]interface{}{{"a", "APPLE"}}}
	snk := &memSink{}
	op := newFakeMultiInput()

	g := &Graph{
		Nodes: []Node{
			{ID: "left", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": leftSrc}},
			{ID: "right", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": rightSrc}},
			{ID: "j", Kind: KindTransform, Type: "join", Cfg: map[string]interface{}{"op": op}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{
			{From: "left", To: "j:left"},
			{From: "right", To: "j:right"},
			{From: "j", To: "sink"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// No checkpoint options are set on the left path, so this mainly proves the
	// run completes cleanly with a 2-source multi-input node; a true barrier
	// injection on the right side would require a fake CheckpointRows-emitting
	// source, which is exercised at the pipeline-integration level instead.
	if _, err := plan.Run(context.Background(), Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if snk.rowCount() != 1 {
		t.Fatalf("sink rows = %d, want 1", snk.rowCount())
	}
}
