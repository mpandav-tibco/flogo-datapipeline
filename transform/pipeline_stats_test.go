package transform

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// keepN is a minimal filtering stage that keeps the first n rows of each batch,
// so a pipeline of them yields predictable per-stage row counters.
type keepN struct {
	name string
	n    int
}

func (k *keepN) Name() string { return k.name }
func (k *keepN) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	if in.Len() <= k.n {
		return in, nil
	}
	out := record.Get()
	out.Columns = in.Columns
	out.Rows = make([][]interface{}, k.n)
	copy(out.Rows, in.Rows[:k.n])
	return out, nil
}
func (k *keepN) Close() error { return nil }

func statsBatch(n int) *record.RecordBatch {
	b := record.Get()
	b.Columns = []record.Column{{Name: "x"}}
	b.Rows = make([][]interface{}, n)
	for i := 0; i < n; i++ {
		b.Rows[i] = []interface{}{i}
	}
	return b
}

// TestPipelineStageStats checks that per-stage RowsIn/RowsOut accumulate across
// batches and reflect each stage's actual input (the previous stage's output).
func TestPipelineStageStats(t *testing.T) {
	p := NewPipeline([]Transform{&keepN{"a", 2}, &keepN{"b", 1}})
	for r := 0; r < 2; r++ { // two batches of 3 rows
		if _, err := p.Apply(statsBatch(3)); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	stats := p.StageStats()
	if len(stats) != 2 {
		t.Fatalf("want 2 stage stats, got %d", len(stats))
	}
	if stats[0].Name != "a" || stats[0].RowsIn != 6 || stats[0].RowsOut != 4 {
		t.Errorf("stage a = %+v; want {a 6 4}", stats[0])
	}
	if stats[1].Name != "b" || stats[1].RowsIn != 4 || stats[1].RowsOut != 2 {
		t.Errorf("stage b = %+v; want {b 4 2}", stats[1])
	}
}

// TestPipelineStageStatsShortCircuit checks that when a stage empties the batch,
// later stages are not counted (they never run).
func TestPipelineStageStatsShortCircuit(t *testing.T) {
	p := NewPipeline([]Transform{&keepN{"a", 0}, &keepN{"b", 5}})
	if _, err := p.Apply(statsBatch(3)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	stats := p.StageStats()
	if stats[0].RowsIn != 3 || stats[0].RowsOut != 0 {
		t.Errorf("stage a = %+v; want in 3 out 0", stats[0])
	}
	if stats[1].RowsIn != 0 || stats[1].RowsOut != 0 {
		t.Errorf("stage b = %+v; want in 0 out 0 (short-circuited)", stats[1])
	}
}
