package pipeline

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func scaledWindowTransform() TransformDef {
	return TransformDef{Type: "window", Extra: map[string]interface{}{
		"type":         "window",
		"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(3600000)},
		"groupBy":      []interface{}{"name"},
		"aggregations": []interface{}{map[string]interface{}{"as": "n", "fn": "count"}},
	}}
}

// A scaled linear windowed pipeline flushes its windows once after the last
// partition; those rows must be counted in RowsWritten.
func TestScaledLinearWindow_FlushCounted(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:     SourceDef{Type: "fakepart"},
		Sink:       jsonlSink(outPath),
		Scaling:    ScalingDef{Mode: "sharded", InstanceIndex: 0, InstanceCount: 1},
		Transforms: []TransformDef{scaledWindowTransform()},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if n := len(readJSONL(t, outPath)); n != 4 {
		t.Fatalf("sink rows = %d, want 4 (one window row per name)", n)
	}
	if out.RowsWritten != 4 {
		t.Fatalf("RowsWritten = %d, want 4 (deferred flush must be counted)", out.RowsWritten)
	}
}

// A scaled DAG with a window node must still emit its windows (each
// partition's plan run flushes at end of input).
func TestScaledGraphWindow_Flushes(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:     SourceDef{Type: "fakepart"},
		Sink:       jsonlSink(outPath),
		Scaling:    ScalingDef{Mode: "sharded", InstanceIndex: 0, InstanceCount: 1},
		Transforms: []TransformDef{scaledWindowTransform()},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "fakepart"},
				{ID: "t0", Kind: "transform", Type: "window"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
			},
			Edges: []GraphEdge{
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
	if n := len(readJSONL(t, outPath)); n != 4 {
		t.Fatalf("sink rows = %d, want 4 — scaled DAG window state was never flushed", n)
	}
	if out.RowsWritten != 4 {
		t.Fatalf("RowsWritten = %d, want 4", out.RowsWritten)
	}
}

type ctxProbe struct{ ctx context.Context }

func (p *ctxProbe) Apply(b *record.RecordBatch) (*record.RecordBatch, error) { return b, nil }
func (p *ctxProbe) Close() error                                             { return nil }
func (p *ctxProbe) Name() string                                             { return "ctxprobe" }
func (p *ctxProbe) BindContext(ctx context.Context)                          { p.ctx = ctx }

var lastCtxProbe *ctxProbe

func init() {
	transform.Register("ctxprobe", func(map[string]interface{}) (transform.Transform, error) {
		lastCtxProbe = &ctxProbe{}
		return lastCtxProbe, nil
	})
}

// Linear-chain transforms must receive the run context so cancellation
// aborts their in-flight I/O (e.g. lookup HTTP calls).
func TestLinearChain_BindsRunContext(t *testing.T) {
	def := &Definition{
		Source:     csvSource(writeTemp(t, "in.csv", ordersCSV)),
		Sink:       jsonlSink(filepath.Join(t.TempDir(), "out.jsonl")),
		Transforms: []TransformDef{{Type: "ctxprobe", Extra: map[string]interface{}{"type": "ctxprobe"}}},
	}
	if _, err := runInline(t, def); err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if lastCtxProbe == nil || lastCtxProbe.ctx == nil || lastCtxProbe.ctx.Done() == nil {
		t.Fatal("chain transform was not bound to a cancellable run context")
	}
}
