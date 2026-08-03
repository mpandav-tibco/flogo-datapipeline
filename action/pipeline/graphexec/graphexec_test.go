package graphexec

import (
	"context"
	"fmt"
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
func (m *memSource) Skip(n int64) error { m.pos += int(n); return nil }
func (m *memSource) Close() error       { return nil }

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

func (s *memSink) rowCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.rows) }

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

// failTransform always errors, to exercise the error edge.
type failTransform struct{}

func (failTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	return nil, fmt.Errorf("failTransform: boom")
}
func (failTransform) Close() error { return nil }

// splitTransform partitions rows by amount>100 (pass) vs <=100 (reject) — a fake
// Splitter for per-row error routing.
type splitTransform struct{}

func (splitTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) { return in, nil }
func (splitTransform) Close() error                                              { return nil }
func (splitTransform) ApplySplit(in *record.RecordBatch) (*record.RecordBatch, *record.RecordBatch, error) {
	pass := in.Clone(in.Len())
	reject := in.Clone(in.Len())
	for i := range in.Rows {
		if in.Rows[i][1].(int) > 100 {
			pass.Rows = append(pass.Rows, in.Rows[i])
		} else {
			reject.Rows = append(reject.Rows, in.Rows[i])
		}
	}
	return pass, reject, nil
}

// bufferTransform buffers every row and emits them only on Flush — a fake
// stateful operator (like window) for the end-of-input flush hook.
type bufferTransform struct {
	buf  [][]interface{}
	cols []record.Column
}

func (bt *bufferTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	bt.cols = in.Columns
	bt.buf = append(bt.buf, in.Rows...)
	return nil, nil
}
func (bt *bufferTransform) Close() error { return nil }
func (bt *bufferTransform) Flush() (*record.RecordBatch, error) {
	if len(bt.buf) == 0 {
		return nil, nil
	}
	out := &record.RecordBatch{Columns: bt.cols, Rows: bt.buf}
	bt.buf = nil
	return out, nil
}

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
			{From: "source:rows", To: "big:in", Type: EdgeCondition, Cond: "amount > 100"},
			{From: "source:rows", To: "small:in", Type: EdgeCondition, Cond: "amount <= 100"},
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

func TestSuccessTee(t *testing.T) {
	// A success (default) edge carries ALL rows through the chain; a coexisting
	// condition edge ADDITIONALLY tees matching rows straight to the sink. So the
	// matching rows are written twice — the explicit Flogo "success + condition".
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}, {4, 80}}}
	snk := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "t0:in"},                                              // success (all rows)
			{From: "source:rows", To: "sink:in", Type: EdgeCondition, Cond: "amount > 100"}, // tee matching
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
	// 4 rows via the chain + 2 matching rows teed directly = 6, with 150 and 250
	// appearing twice.
	if got := snk.amounts(); len(got) != 6 || got[0] != 50 || got[1] != 80 || got[2] != 150 || got[3] != 150 || got[4] != 250 || got[5] != 250 {
		t.Fatalf("sink = %v, want [50 80 150 150 250 250] (matching rows teed)", got)
	}
	if res.RowsWritten != 6 {
		t.Fatalf("RowsWritten=%d, want 6", res.RowsWritten)
	}
}

func TestConditionDivert(t *testing.T) {
	// A condition edge + an OTHERWISE edge partition the stream: matching rows go
	// one way, everything else the other — each row exactly once (no tee, because
	// there is no success edge).
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}, {4, 80}}}
	big := &memSink{}
	rest := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "big", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": big}},
			{ID: "rest", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": rest}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "big:in", Type: EdgeCondition, Cond: "amount > 100"},
			{From: "source:rows", To: "rest:in", Type: EdgeOtherwise},
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
		t.Fatalf("big = %v, want [150 250]", got)
	}
	if got := rest.amounts(); len(got) != 2 || got[0] != 50 || got[1] != 80 {
		t.Fatalf("rest = %v, want [50 80] (otherwise = unmatched)", got)
	}
	if res.RowsWritten != 4 {
		t.Fatalf("RowsWritten=%d, want 4 (no duplication)", res.RowsWritten)
	}
}

func TestErrorEdge(t *testing.T) {
	// A transform that fails routes its failed batch to the error edge instead of
	// aborting the run; the success edge to the main sink gets nothing.
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}}}
	snk := &memSink{}
	errSink := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "fail", Cfg: map[string]interface{}{"op": failTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
			{ID: "errors", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": errSink}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "t0:in"},
			{From: "t0:out", To: "sink:in"},
			{From: "t0:out", To: "errors:in", Type: EdgeError},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{BatchSize: 5})
	if err != nil {
		t.Fatalf("run must NOT fail when an error edge handles the failure: %v", err)
	}
	if got := errSink.amounts(); len(got) != 3 {
		t.Fatalf("error sink = %v, want 3 failed rows routed", got)
	}
	if got := snk.amounts(); len(got) != 0 {
		t.Fatalf("main sink = %v, want 0 (all rows errored)", got)
	}
	if res.Nodes["t0"].RowsErrored != 3 {
		t.Fatalf("t0 RowsErrored=%d, want 3", res.Nodes["t0"].RowsErrored)
	}
}

func TestSplitterErrorRouting(t *testing.T) {
	// A Splitter transform + an error edge routes rejected rows down the error
	// edge row-by-row, while passing rows continue on the success edge.
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}, {4, 80}}}
	snk := &memSink{}
	errSink := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "split", Cfg: map[string]interface{}{"op": splitTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
			{ID: "errors", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": errSink}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "t0:in"},
			{From: "t0:out", To: "sink:in"},
			{From: "t0:out", To: "errors:in", Type: EdgeError},
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
	if got := snk.amounts(); len(got) != 2 || got[0] != 150 || got[1] != 250 {
		t.Fatalf("pass sink = %v, want [150 250]", got)
	}
	if got := errSink.amounts(); len(got) != 2 || got[0] != 50 || got[1] != 80 {
		t.Fatalf("error sink = %v, want [50 80] (rejected rows)", got)
	}
	if res.Nodes["t0"].RowsErrored != 2 {
		t.Fatalf("t0 RowsErrored=%d, want 2", res.Nodes["t0"].RowsErrored)
	}
}

func TestFlusher(t *testing.T) {
	// A buffering transform emits nothing per-batch and flushes all rows at
	// end-of-input; the flushed batch must reach the sink.
	src := &memSource{cols: testCols, rows: [][]interface{}{{1, 50}, {2, 150}, {3, 250}}}
	snk := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "buffer", Cfg: map[string]interface{}{"op": &bufferTransform{}}},
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
		t.Fatalf("sink = %v, want [50 150 250] (flushed at end)", got)
	}
	if res.RowsWritten != 3 {
		t.Fatalf("RowsWritten=%d, want 3", res.RowsWritten)
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

// seqRows builds n rows {i, i*10} for checkpoint tests.
func seqRows(n int) [][]interface{} {
	rows := make([][]interface{}, 0, n)
	for i := 1; i <= n; i++ {
		rows = append(rows, []interface{}{i, i * 10})
	}
	return rows
}

func linearGraph(src *memSource, snk *memSink) *Graph {
	return &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{{From: "source:rows", To: "t0:in"}, {From: "t0:out", To: "sink:in"}},
	}
}

// TestCheckpointBarrierNoLoss verifies the core no-loss invariant: whenever the
// coordinator persists a source position, every row up to that position is
// already durably written (the sink flushed at the barrier before the save).
func TestCheckpointBarrierNoLoss(t *testing.T) {
	src := &memSource{cols: testCols, rows: seqRows(20)}
	snk := &memSink{}
	plan, err := Compile(linearGraph(src, snk), testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	var mu sync.Mutex
	var saved []int64
	opts := Options{
		BatchSize:      3,
		CheckpointRows: 5,
		OnCheckpoint: func(pos int64) error {
			if got := int64(snk.rowCount()); got < pos {
				t.Errorf("checkpoint pos=%d but only %d rows durable (data loss window)", pos, got)
			}
			mu.Lock()
			saved = append(saved, pos)
			mu.Unlock()
			return nil
		},
	}
	res, err := plan.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.RowsRead != 20 || res.RowsWritten != 20 {
		t.Fatalf("RowsRead=%d RowsWritten=%d, want 20/20", res.RowsRead, res.RowsWritten)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(saved) == 0 {
		t.Fatal("expected at least one checkpoint to be saved")
	}
	// Barriers fire at the batch boundary that first crosses each interval, so a
	// position is >= its interval multiple (not exactly it), and strictly rising.
	for i, p := range saved {
		if p < int64(5*(i+1)) {
			t.Errorf("checkpoint %d pos=%d, want >= %d", i, p, 5*(i+1))
		}
		if i > 0 && p <= saved[i-1] {
			t.Fatalf("checkpoint positions not strictly increasing: %v", saved)
		}
	}
}

// TestCheckpointResume verifies the source skips already-processed rows on
// resume, so only the remaining rows are re-emitted (at-least-once, no loss).
func TestCheckpointResume(t *testing.T) {
	src := &memSource{cols: testCols, rows: seqRows(10)}
	snk := &memSink{}
	plan, err := Compile(linearGraph(src, snk), testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	res, err := plan.Run(context.Background(), Options{BatchSize: 3, ResumeFrom: 4})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.RowsRead != 6 || res.RowsWritten != 6 {
		t.Fatalf("RowsRead=%d RowsWritten=%d, want 6/6 after ResumeFrom=4", res.RowsRead, res.RowsWritten)
	}
	got := snk.amounts()
	want := []int{50, 60, 70, 80, 90, 100}
	if len(got) != len(want) {
		t.Fatalf("sink = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sink = %v, want %v", got, want)
		}
	}
}

// TestCheckpointForkNoLoss verifies the coordinator waits for EVERY sink of a
// fan-out to flush at a barrier before persisting the position — so the no-loss
// invariant holds across all branches, not just one.
func TestCheckpointForkNoLoss(t *testing.T) {
	src := &memSource{cols: testCols, rows: seqRows(20)} // amount = i*10; >100 for i>10
	big := &memSink{}
	small := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "big", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": big}},
			{ID: "small", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": small}},
		},
		Edges: []Edge{
			{From: "source:rows", To: "big:in", Type: EdgeCondition, Cond: "amount > 100"},
			{From: "source:rows", To: "small:in", Type: EdgeCondition, Cond: "amount <= 100"},
		},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opts := Options{
		BatchSize:      3,
		CheckpointRows: 4,
		OnCheckpoint: func(pos int64) error {
			// Every row before the barrier lands in exactly one branch; both must
			// have drained+flushed before the coordinator saves.
			if got := int64(big.rowCount() + small.rowCount()); got < pos {
				t.Errorf("checkpoint pos=%d but only %d rows durable across branches", pos, got)
			}
			return nil
		},
	}
	res, err := plan.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.RowsWritten != 20 {
		t.Fatalf("RowsWritten=%d, want 20", res.RowsWritten)
	}
	if big.rowCount() != 10 || small.rowCount() != 10 {
		t.Fatalf("branch counts big=%d small=%d, want 10/10", big.rowCount(), small.rowCount())
	}
}

// TestHasStateful confirms a Flusher node marks the plan stateful (so callers
// disable checkpointing for it) while a stateless plan does not.
func TestHasStateful(t *testing.T) {
	stateless := &memSource{cols: testCols, rows: seqRows(3)}
	plan, err := Compile(linearGraph(stateless, &memSink{}), testBuilders())
	if err != nil {
		t.Fatalf("compile stateless: %v", err)
	}
	if plan.HasStateful() {
		t.Fatal("stateless plan reported HasStateful=true")
	}
	_ = plan.Close()

	src := &memSource{cols: testCols, rows: seqRows(3)}
	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "buf", Cfg: map[string]interface{}{"op": &bufferTransform{}}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": &memSink{}}},
		},
		Edges: []Edge{{From: "source:rows", To: "t0:in"}, {From: "t0:out", To: "sink:in"}},
	}
	plan2, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("compile stateful: %v", err)
	}
	if !plan2.HasStateful() {
		t.Fatal("plan with a Flusher node reported HasStateful=false")
	}
	_ = plan2.Close()
}
