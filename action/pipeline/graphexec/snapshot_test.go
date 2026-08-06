package graphexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// statefulCounter is a fake stateful transform (like window): it accumulates a
// running sum per key across Apply calls, implements Flusher (emit on
// end-of-input) and Snapshotter (serialize/restore the running sums).
type statefulCounter struct {
	sums    map[string]int
	cols    []record.Column
	flushed bool
}

func newStatefulCounter() *statefulCounter { return &statefulCounter{sums: map[string]int{}} }

func (s *statefulCounter) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	s.cols = in.Columns
	for i := range in.Rows {
		m := in.RowMap(i)
		k := fmt.Sprint(m["key"])
		s.sums[k] += m["n"].(int)
	}
	return nil, nil // buffers everything; emits only on Flush.
}
func (s *statefulCounter) Close() error { return nil }

func (s *statefulCounter) Flush() (*record.RecordBatch, error) {
	if s.flushed || len(s.sums) == 0 {
		return nil, nil
	}
	s.flushed = true
	out := &record.RecordBatch{Columns: []record.Column{{Name: "key", Type: record.TypeString}, {Name: "sum", Type: record.TypeInt}}}
	for k, v := range s.sums {
		out.Rows = append(out.Rows, []interface{}{k, v})
	}
	return out, nil
}

// Snapshot/Restore round-trip via a tiny "key=sum;key=sum" encoding — real
// enough to prove state survives a Snapshot->Restore->Apply->Flush cycle
// without depending on encoding/json for this fake.
func (s *statefulCounter) Snapshot() ([]byte, error) {
	out := []byte{}
	for k, v := range s.sums {
		out = append(out, []byte(fmt.Sprintf("%s=%d;", k, v))...)
	}
	return out, nil
}
func (s *statefulCounter) Restore(data []byte) error {
	s.sums = map[string]int{}
	cur := ""
	var k string
	for _, c := range data {
		switch c {
		case '=':
			k = cur
			cur = ""
		case ';':
			var v int
			fmt.Sscanf(cur, "%d", &v)
			s.sums[k] = v
			cur = ""
		default:
			cur += string(c)
		}
	}
	return nil
}

var counterCols = []record.Column{{Name: "key", Type: record.TypeString}, {Name: "n", Type: record.TypeInt}}

// TestSnapshotters_ReturnsStatefulNodesOnly proves Plan.Snapshotters() finds
// exactly the nodes whose operator implements Snapshotter, keyed by node id.
func TestSnapshotters_ReturnsStatefulNodesOnly(t *testing.T) {
	src := &memSource{cols: counterCols, rows: [][]interface{}{{"a", 1}}}
	snk := &memSink{}
	counter := newStatefulCounter()

	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
			{ID: "t0", Kind: KindTransform, Type: "id", Cfg: map[string]interface{}{"op": idTransform{}}},
			{ID: "t1", Kind: KindTransform, Type: "counter", Cfg: map[string]interface{}{"op": counter}},
			{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
		},
		Edges: []Edge{{From: "source", To: "t0"}, {From: "t0", To: "t1"}, {From: "t1", To: "sink"}},
	}
	plan, err := Compile(g, testBuilders())
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	snaps := plan.Snapshotters()
	if len(snaps) != 1 {
		t.Fatalf("Snapshotters() = %d entries, want 1: %v", len(snaps), snaps)
	}
	if _, ok := snaps["t1"]; !ok {
		t.Fatalf("Snapshotters() missing \"t1\": %v", snaps)
	}
	if _, ok := snaps["t0"]; ok {
		t.Fatal("Snapshotters() should not include the plain idTransform node \"t0\"")
	}
}

// TestSnapshotRestore_PreservesStateAcrossRuns proves a Snapshot taken after
// one Run, fed into Restore before a second Run on a FRESH plan, carries the
// running sums forward — the core guarantee stateful graph checkpointing
// needs (a resumed run continues aggregating instead of starting from zero).
func TestSnapshotRestore_PreservesStateAcrossRuns(t *testing.T) {
	build := func(src *memSource, counter *statefulCounter) (*Plan, *memSink) {
		snk := &memSink{}
		g := &Graph{
			Nodes: []Node{
				{ID: "source", Kind: KindSource, Type: "mem", Cfg: map[string]interface{}{"op": src}},
				{ID: "t1", Kind: KindTransform, Type: "counter", Cfg: map[string]interface{}{"op": counter}},
				{ID: "sink", Kind: KindSink, Type: "mem", Cfg: map[string]interface{}{"op": snk}},
			},
			Edges: []Edge{{From: "source", To: "t1"}, {From: "t1", To: "sink"}},
		}
		plan, err := Compile(g, testBuilders())
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}
		return plan, snk
	}

	// Run 1: accumulate a=1, a=2 (sum=3), then snapshot before flush would clear it.
	c1 := newStatefulCounter()
	src1 := &memSource{cols: counterCols, rows: [][]interface{}{{"a", 1}, {"a", 2}}}
	plan1, _ := build(src1, c1)
	if _, err := plan1.Run(context.Background(), Options{KeepOpen: true}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	snap, err := c1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	_ = plan1.Close()

	// Run 2: a FRESH plan/counter, restored from run 1's snapshot, then fed one
	// more row (a=5) before flushing — the emitted sum must be 3+5=8, proving
	// the restored state was actually used, not a fresh zero.
	c2 := newStatefulCounter()
	if err := c2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	src2 := &memSource{cols: counterCols, rows: [][]interface{}{{"a", 5}}}
	plan2, snk2 := build(src2, c2)
	if _, err := plan2.Run(context.Background(), Options{}); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if snk2.rowCount() != 1 {
		t.Fatalf("sink rows = %d, want 1", snk2.rowCount())
	}
	got := snk2.rows[0][1].(int)
	if got != 8 {
		t.Fatalf("restored+continued sum = %d, want 8 (3 from run 1 + 5 from run 2)", got)
	}
}
