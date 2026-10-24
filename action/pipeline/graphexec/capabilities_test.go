package graphexec

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func seqRowsN(n int) [][]interface{} {
	rows := make([][]interface{}, n)
	for i := range rows {
		rows[i] = []interface{}{i, i}
	}
	return rows
}

// jitterTransform copies its input after a random delay, so parallel replicas
// finish out of order.
type jitterTransform struct{}

func (jitterTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	out := in.Clone(in.Len())
	out.Rows = append(out.Rows, in.Rows...)
	return out, nil
}
func (jitterTransform) Close() error { return nil }

func chainGraph(src SourceOp, tx Node, snk SinkOp) *Graph {
	return &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", PreBuilt: src},
			tx,
			{ID: "sink", Kind: KindSink, Type: "mem", Operator: snk},
		},
		Edges: []Edge{{From: "source", To: tx.ID}, {From: tx.ID, To: "sink"}},
	}
}

func TestParallelReplicasPreserveOrder(t *testing.T) {
	src := &memSource{cols: testCols, rows: seqRowsN(5000)}
	snk := &memSink{}
	tx := Node{ID: "t", Kind: KindTransform, Operator: jitterTransform{},
		Replicas: []TransformOp{jitterTransform{}, jitterTransform{}, jitterTransform{}}}
	plan, err := Compile(chainGraph(src, tx, snk), testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Run(context.Background(), Options{BatchSize: 37}); err != nil {
		t.Fatal(err)
	}
	if len(snk.rows) != 5000 {
		t.Fatalf("rows=%d want 5000", len(snk.rows))
	}
	for i, r := range snk.rows {
		if r[0].(int) != i {
			t.Fatalf("row %d out of order: %v", i, r[0])
		}
	}
}

// Barriers through a parallel node must still mean "everything before me is
// written": at each checkpoint the sink holds exactly pos rows.
func TestParallelReplicasBarrierNoLoss(t *testing.T) {
	src := &memSource{cols: testCols, rows: seqRowsN(3000)}
	snk := &memSink{}
	tx := Node{ID: "t", Kind: KindTransform, Operator: jitterTransform{},
		Replicas: []TransformOp{jitterTransform{}, jitterTransform{}}}
	plan, err := Compile(chainGraph(src, tx, snk), testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	var bad atomic.Int64
	_, err = plan.Run(context.Background(), Options{BatchSize: 50, CheckpointRows: 400, OnCheckpoint: func(pos int64) error {
		if int64(snk.rowCount()) != pos {
			bad.Add(1)
		}
		return nil
	}})
	if err != nil || bad.Load() != 0 {
		t.Fatalf("err=%v; %d checkpoints saw unwritten rows", err, bad.Load())
	}
}

// idleSource returns a few rows, then empty polls (an idle stream) for a
// while, then EOF.
type idleSource struct {
	sent  bool
	start time.Time
}

func (s *idleSource) ReadBatch(int) (*record.RecordBatch, error) {
	b := record.Get()
	b.Columns = testCols
	if !s.sent {
		s.sent, s.start = true, time.Now()
		b.Rows = append(b.Rows, []interface{}{1, 1}, []interface{}{2, 2})
		return b, nil
	}
	if time.Since(s.start) > 600*time.Millisecond {
		record.Put(b)
		return nil, io.EOF
	}
	time.Sleep(10 * time.Millisecond)
	return b, nil
}
func (s *idleSource) Skip(int64) error { return nil }
func (s *idleSource) Close() error     { return nil }

func TestTimeBarrierFiresOnIdleStream(t *testing.T) {
	snk := &memSink{}
	plan, err := Compile(chainGraph(&idleSource{}, Node{ID: "t", Kind: KindTransform, Operator: idTransform{}}, snk), testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	var positions []int64
	var mu sync.Mutex
	_, err = plan.Run(context.Background(), Options{CheckpointRows: 1000, CheckpointInterval: 100 * time.Millisecond,
		OnCheckpoint: func(pos int64) error { mu.Lock(); positions = append(positions, pos); mu.Unlock(); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0] != 2 {
		t.Fatalf("checkpoints=%v; want exactly one time-triggered checkpoint at 2 (none while idle with nothing pending)", positions)
	}
}

func TestTimeBarrierHonoursHasUncommitted(t *testing.T) {
	snk := &memSink{}
	plan, err := Compile(chainGraph(&idleSource{}, Node{ID: "t", Kind: KindTransform, Operator: idTransform{}}, snk), testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	_, err = plan.Run(context.Background(), Options{CheckpointInterval: 100 * time.Millisecond,
		HasUncommitted: func() bool { return true },
		OnCheckpoint:   func(int64) error { n.Add(1); return nil }})
	if err != nil || n.Load() < 3 {
		t.Fatalf("err=%v checkpoints=%d; want repeated idle checkpoints while uncommitted", err, n.Load())
	}
}

// slowSink holds each batch for a while; the memory budget must stop sources
// from reading far ahead of it.
type slowSink struct {
	memSink
	budget  *MemoryBudget
	maxUsed int64
}

func (s *slowSink) WriteBatch(b *record.RecordBatch) error {
	if u := s.budget.Used(); u > s.maxUsed {
		s.maxUsed = u
	}
	time.Sleep(2 * time.Millisecond)
	return s.memSink.WriteBatch(b)
}

func TestMemoryBudgetBoundsReadAhead(t *testing.T) {
	budget := NewMemoryBudget(1000)
	snk := &slowSink{budget: budget}
	src := &memSource{cols: testCols, rows: seqRowsN(2000)}
	plan, err := Compile(chainGraph(src, Node{ID: "t", Kind: KindTransform, Operator: idTransform{}}, snk), testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.Run(context.Background(), Options{BatchSize: 20, BufferDepth: 64, Budget: budget,
		SizeOf: func(b *record.RecordBatch) int64 { return int64(b.Len()) * 10 }})
	if err != nil {
		t.Fatal(err)
	}
	if snk.rowCount() != 2000 {
		t.Fatalf("rows=%d", snk.rowCount())
	}
	// One batch is 200 bytes: the budget admits at most limit + one batch.
	if snk.maxUsed > 1200 {
		t.Fatalf("budget exceeded: %d bytes buffered (limit 1000)", snk.maxUsed)
	}
	if budget.Used() != 0 {
		t.Fatalf("budget leak: %d bytes still charged after the run", budget.Used())
	}
}

// Two sources with checkpointing used to panic (both closed the barrier
// channel). Multi-source graphs now checkpoint only at end of run.
func TestMultiSourceCheckpointDoesNotPanic(t *testing.T) {
	snk := &memSink{}
	g := &Graph{
		Nodes: []Node{
			{ID: "a", Kind: KindSource, PreBuilt: &memSource{cols: testCols, rows: seqRowsN(100)}},
			{ID: "b", Kind: KindSource, PreBuilt: &memSource{cols: testCols, rows: seqRowsN(100)}},
			{ID: "sink", Kind: KindSink, Operator: snk},
		},
		Edges: []Edge{{From: "a", To: "sink"}, {From: "b", To: "sink"}},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Run(context.Background(), Options{BatchSize: 10, CheckpointRows: 20,
		OnCheckpoint: func(int64) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if snk.rowCount() != 200 {
		t.Fatalf("rows=%d want 200", snk.rowCount())
	}
}
