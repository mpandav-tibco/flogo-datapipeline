package pipeline

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// fakePartAllRows is the fixed dataset the "fakepart" test source serves,
// split into 2 fixed partitions of 2 rows each by OpenPartition.
var fakePartAllRows = []map[string]interface{}{
	{"id": 1, "name": "Alice", "amount": 50},
	{"id": 2, "name": "Bob", "amount": 150},
	{"id": 3, "name": "Carol", "amount": 90},
	{"id": 4, "name": "Dave", "amount": 200},
}

// fakePartSource is a minimal in-memory source.PartitionableSource used to
// exercise runGraphScaled's sharded/distributed wiring without needing a real
// jdbc/objstore backend (only they implement partitioning today).
type fakePartSource struct {
	rows []map[string]interface{}
	pos  int
}

func (f *fakePartSource) Schema() []record.Column {
	return []record.Column{{Name: "id", Type: "int"}, {Name: "name", Type: "string"}, {Name: "amount", Type: "int"}}
}

func (f *fakePartSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if f.pos >= len(f.rows) {
		return nil, io.EOF
	}
	end := f.pos + size
	if end > len(f.rows) {
		end = len(f.rows)
	}
	b := record.Get()
	b.Columns = f.Schema()
	for _, r := range f.rows[f.pos:end] {
		b.Rows = append(b.Rows, []interface{}{r["id"], r["name"], r["amount"]})
	}
	f.pos = end
	if f.pos >= len(f.rows) {
		return b, io.EOF
	}
	return b, nil
}
func (f *fakePartSource) Skip(n int64) error { f.pos += int(n); return nil }
func (f *fakePartSource) Close() error       { return nil }

func (f *fakePartSource) Partitions(targetCount int) ([]source.Partition, error) {
	return []source.Partition{{ID: "p0", Token: 0}, {ID: "p1", Token: 1}}, nil
}

func (f *fakePartSource) OpenPartition(p source.Partition) (source.Source, error) {
	idx, _ := p.Token.(int)
	half := len(fakePartAllRows) / 2
	var rows []map[string]interface{}
	if idx == 0 {
		rows = fakePartAllRows[:half]
	} else {
		rows = fakePartAllRows[half:]
	}
	return &fakePartSource{rows: rows}, nil
}

func init() {
	source.Register("fakepart", func(cfg map[string]interface{}) (source.Source, error) {
		return &fakePartSource{rows: fakePartAllRows}, nil
	})
}

// TestGraphRun_ScaledSharded proves runGraphScaled actually drives the graph
// runner across every partition a "sharded" instance owns (via the SAME
// scaling.Coordinator machinery the linear engine's runScaled uses), rather
// than silently running only the primary (un-partitioned) source once.
func TestGraphRun_ScaledSharded(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:  SourceDef{Type: "fakepart"},
		Sink:    jsonlSink(outPath),
		Scaling: ScalingDef{Mode: "sharded", InstanceIndex: 0, InstanceCount: 1},
		Transforms: []TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 40"}},
		},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "fakepart"},
				{ID: "t0", Kind: "transform", Type: "filter"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
			},
			Edges: []GraphEdge{
				// A conditional edge (amount>1000, never true here) makes the
				// graph non-reducible so it runs via the graph runner, while the
				// otherwise edge keeps the normal filter->sink chain intact —
				// every row is expected to reach the sink through t0.
				{From: "source:rows", To: "t0:in", Type: "otherwise"},
				{From: "t0:out", To: "sink:in"},
				{From: "source:rows", To: "sink:in", Type: "expression", Value: "amount > 1000"},
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
	// Both fixed partitions (2 rows each) must be processed by this single
	// instance — proves partitions were actually enumerated and claimed, not
	// just the primary un-partitioned source read once.
	if out.RowsProcessed != 4 {
		t.Fatalf("RowsProcessed = %d, want 4 (both partitions)", out.RowsProcessed)
	}
	rows := readJSONL(t, outPath)
	// amount>40 keeps Alice(50), Bob(150), Carol(90), Dave(200) — all 4.
	if len(rows) != 4 {
		t.Fatalf("sink rows = %d, want 4 — got %v", len(rows), rows)
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[fmt.Sprint(r["name"])] = true
	}
	for _, want := range []string{"Alice", "Bob", "Carol", "Dave"} {
		if !names[want] {
			t.Fatalf("sink rows missing %q — got %v", want, rows)
		}
	}
	// graphStats must reflect BOTH partitions merged (4 rows in/out at t0, not 2).
	gs, ok := out.Metadata["graphStats"].([]graphexec.NodeStat)
	if !ok {
		t.Fatalf("Output.Metadata[\"graphStats\"] missing or wrong type: %T", out.Metadata["graphStats"])
	}
	if len(gs) == 0 {
		t.Fatal("graphStats is empty")
	}
	for _, ns := range gs {
		if ns.ID == "t0" && ns.RowsIn != 4 {
			t.Fatalf("t0 RowsIn = %d, want 4 (merged across both partitions)", ns.RowsIn)
		}
	}
}
