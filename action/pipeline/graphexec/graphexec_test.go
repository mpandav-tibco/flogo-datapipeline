package graphexec

import (
	"context"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

// --- in-memory fakes (white-box: same package, so they satisfy the operator
// interfaces and the test can reach subset/cloneRow directly) ---

type memSource struct {
	cols []record.Column
	rows [][]interface{}
	pos  int
	err  error
}

func (m *memSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.pos >= len(m.rows) {
		return nil, io.EOF
	}
	end := m.pos + size
	if end > len(m.rows) {
		end = len(m.rows)
	}
	b := record.Get()
	b.Columns = m.cols
	b.Rows = b.Rows[:0]
	b.Rows = append(b.Rows, m.rows[m.pos:end]...)
	m.pos = end
	if m.pos >= len(m.rows) {
		return b, io.EOF
	}
	return b, nil
}
func (m *memSource) Close() error { return nil }

type memSink struct {
	mu      sync.Mutex
	rows    [][]interface{}
	flushed bool
	closed  bool
}

func (s *memSink) WriteBatch(b *record.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range b.Rows {
		s.rows = append(s.rows, cloneRow(b.Rows[i]))
	}
	return nil
}
func (s *memSink) Flush() error { s.mu.Lock(); s.flushed = true; s.mu.Unlock(); return nil }
func (s *memSink) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }

func (s *memSink) amounts() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r[1].(int))
	}
	sort.Ints(out)
	return out
}

// idTransform passes its input batch straight through (returns the same batch).
type idTransform struct{}

func (idTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) { return in, nil }
func (idTransform) Close() error                                              { return nil }

// testBuilders wires each node's fake operator (stored under cfg["op"]) and uses
// the real filter compiler for edge conditions.
func testBuilders() Builders {
	return Builders{
		Source:    func(_ string, cfg map[string]interface{}) (SourceOp, error) { return cfg["op"].(SourceOp), nil },
		Transform: func(_ string, cfg map[string]interface{}) (TransformOp, error) { return cfg["op"].(TransformOp), nil },
		Sink:      func(_ string, cfg map[string]interface{}) (SinkOp, error) { return cfg["op"].(SinkOp), nil },
		Predicate: func(expr string) (Pred, error) {
			p, err := filter.Compile(expr)
			if err != nil {
				return nil, err
			}
			return p.Eval, nil
		},
	}
}

var testCols = []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "amount", Type: record.TypeInt}}

func TestRunLinear(t *testing.T) {
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}}}
	snk := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{{From: "source:rows", To: "t0:in"}, {From: "t0:out", To: "sink:in"}},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{BatchSize: 2})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := snk.amounts(); len(got) != 3 || got[0] != 50 || got[2] != 250 {
		t.Fatalf("sink rows = %v, want [50 150 250]", got)
	}
	if res.RowsRead != 3 || res.RowsWritten != 3 {
		t.Fatalf("RowsRead=%d RowsWritten=%d, want 3/3", res.RowsRead, res.RowsWritten)
	}
	if !snk.flushed || !snk.closed {
		t.Fatalf("sink not flushed/closed: flushed=%v closed=%v", snk.flushed, snk.closed)
	}
}

func TestRunConditionalFork(t *testing.T) {
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}, {4, 80}}}
	big := &memSink{}
	small := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "big", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": big}},
			{ID: "small", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": small}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "big:in", Cond: "amount > 100"},
			{From: "source:rows", To: "small:in", Cond: "amount <= 100"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{BatchSize: 3})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := big.amounts(); len(got) != 2 || got[0] != 150 || got[1] != 250 {
		t.Fatalf("big sink = %v, want [150 250]", got)
	}
	if got := small.amounts(); len(got) != 2 || got[0] != 50 || got[1] != 80 {
		t.Fatalf("small sink = %v, want [50 80]", got)
	}
	// Each source row is copied per matching edge; both branches together see 4.
	if res.RowsRead != 4 || res.RowsWritten != 4 {
		t.Fatalf("RowsRead=%d RowsWritten=%d, want 4/4", res.RowsRead, res.RowsWritten)
	}
}

func TestCompileRejectsCycle(t *testing.T) {
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": &memSource{}}},
			{ID: "a", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "b", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": &memSink{}}},
		},
		Edges: []Edge{
			{From: "source", To: "a"},
			{From: "a", To: "b"},
			{From: "b", To: "a"}, // cycle
			{From: "b", To: "sink"},
		},
	}
	if _, err := Compile(g, testBuilders()); err == nil {
		t.Fatal("expected cycle to be rejected, got nil error")
	}
}

func TestCompileRejectsUnknownEndpoint(t *testing.T) {
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": &memSource{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": &memSink{}}},
		},
		Edges: []Edge{{From: "source", To: "ghost"}},
	}
	if _, err := Compile(g, testBuilders()); err == nil {
		t.Fatal("expected unknown endpoint to be rejected, got nil error")
	}
}

func TestFirstMatchRouting(t *testing.T) {
	// source fans out to a conditional skip edge (amount > 100 → straight to
	// sink) and an unconditional default edge (→ t0 → sink). First-match routing
	// must divert matching rows to the skip edge and send the rest through the
	// chain, so every row reaches the sink exactly once (no double-write), and
	// the sink's two inbound edges (skip + chain) fan in.
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}, {4, 80}}}
	snk := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "t0:in"},                       // default / main chain
			{From: "source:rows", To: "sink:in", Cond: "amount > 100"}, // conditional skip
			{From: "t0:out", To: "sink:in"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{BatchSize: 3})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := snk.amounts(); len(got) != 4 || got[0] != 50 || got[1] != 80 || got[2] != 150 || got[3] != 250 {
		t.Fatalf("sink = %v, want [50 80 150 250] each exactly once (no double-write)", got)
	}
	if res.RowsWritten != 4 {
		t.Fatalf("RowsWritten=%d, want 4 (rows conserved, not duplicated)", res.RowsWritten)
	}
}

func TestSubsetRowIsolation(t *testing.T) {
	// appendRow must copy rows so a fan-out branch never aliases the source.
	b := &record.RecordBatch{Columns: testCols, Rows: [][]interface{}{{1, 50}, {2, 150}}}
	var bucket *record.RecordBatch
	appendRow(&bucket, b, 1)
	if bucket.Len() != 1 || bucket.Rows[0][0].(int) != 2 {
		t.Fatalf("appendRow = %v, want single row id=2", bucket.Rows)
	}
	bucket.Rows[0][1] = 999
	if b.Rows[1][1].(int) != 150 {
		t.Fatal("appendRow must not alias source rows")
	}
}
