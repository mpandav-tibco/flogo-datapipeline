package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
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

// ctxGuard fails a batch when its bound context is cancelled, like lookup.
type ctxGuard struct{ ctx context.Context }

func (g *ctxGuard) Apply(b *record.RecordBatch) (*record.RecordBatch, error) {
	if g.ctx != nil && g.ctx.Err() != nil {
		return nil, g.ctx.Err()
	}
	return b, nil
}
func (g *ctxGuard) Close() error                    { return nil }
func (g *ctxGuard) Name() string                    { return "ctxguard" }
func (g *ctxGuard) BindContext(ctx context.Context) { g.ctx = ctx }

func init() {
	transform.Register("ctxguard", func(map[string]interface{}) (transform.Transform, error) { return &ctxGuard{}, nil })
}

// The deferred window flush of a scaled linear run runs after the last
// Plan.Run returned; stages after the window must not see a cancelled context.
func TestScaledLinearWindow_FlushThroughContextBoundStage(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:  SourceDef{Type: "fakepart"},
		Sink:    jsonlSink(outPath),
		Scaling: ScalingDef{Mode: "sharded", InstanceIndex: 0, InstanceCount: 1},
		Transforms: []TransformDef{scaledWindowTransform(),
			{Type: "ctxguard", Extra: map[string]interface{}{"type": "ctxguard"}}},
	}
	out, err := runInline(t, def)
	if err != nil || out.Status != "completed" {
		t.Fatalf("status=%q err=%v", out.Status, err)
	}
	if n := len(readJSONL(t, outPath)); n != 4 {
		t.Fatalf("sink rows = %d, want 4", n)
	}
}

// failOnAmount errors on any batch containing amount == 0, like a map
// expression failing on one malformed row.
type failOnAmount struct{}

func (failOnAmount) Name() string { return "failonamount" }
func (failOnAmount) Close() error { return nil }
func (failOnAmount) Apply(b *record.RecordBatch) (*record.RecordBatch, error) {
	for i := range b.Rows {
		if fmt.Sprint(b.Value(i, "amount")) == "0" {
			return nil, fmt.Errorf("cannot process amount 0")
		}
	}
	return b, nil
}

func init() {
	transform.Register("failonamount", func(map[string]interface{}) (transform.Transform, error) { return failOnAmount{}, nil })
}

// A transform error must not silently discard the batch: with a DLQ
// configured, its rows are dead-lettered (and counted as failed).
func TestTransformErrorBatchDeadLettered(t *testing.T) {
	dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:      csvSource(writeTemp(t, "in.csv", ordersCSV)),
		Sink:        jsonlSink(outPath),
		Transforms:  []TransformDef{{Type: "failonamount", Extra: map[string]interface{}{"type": "failonamount"}}},
		DLQ:         DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": dlqPath}},
		Performance: PerformanceDef{BatchSize: 2},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatal(err)
	}
	written, dead := len(readJSONL(t, outPath)), len(readJSONL(t, dlqPath))
	if written+dead != 5 || dead == 0 {
		t.Fatalf("written=%d dead-lettered=%d, want all 5 rows accounted for with the failing batch in the DLQ", written, dead)
	}
	if out.RowsFailed != int64(dead) || out.Status != "partial" {
		t.Fatalf("RowsFailed=%d status=%q, want %d/partial", out.RowsFailed, out.Status, dead)
	}
}

// validate with onViolation "route" and no errorSink sends rejected rows to
// the pipeline DLQ (with the reason) instead of discarding them.
func TestValidateRouteWithoutErrorSinkUsesDLQ(t *testing.T) {
	dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")
	outPath := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source: csvSource(writeTemp(t, "in.csv", ordersCSV)),
		Sink:   jsonlSink(outPath),
		Transforms: []TransformDef{{Type: "validate", Extra: map[string]interface{}{
			"type": "validate", "onViolation": "route",
			"rules": []interface{}{map[string]interface{}{"field": "amount", "min": float64(1)}},
		}}},
		DLQ: DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": dlqPath}},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatal(err)
	}
	dead := readJSONL(t, dlqPath)
	if len(readJSONL(t, outPath)) != 3 || len(dead) != 2 {
		t.Fatalf("written=%d dlq=%d, want 3 valid rows written and the 2 rejected (amount -50, 0) in the DLQ", len(readJSONL(t, outPath)), len(dead))
	}
	if !strings.Contains(fmt.Sprint(dead[0]), "validate") {
		t.Errorf("DLQ row carries no rejection reason: %v", dead[0])
	}
	if out.RowsFailed != 2 {
		t.Errorf("RowsFailed=%d, want 2 (rejected rows are accounted)", out.RowsFailed)
	}
}

var failOnceOpens int32

// failOnceSource fails the first run (like an unreachable dependency) and
// delivers 3 rows on the next one.
type failOnceSource struct{ fail, done bool }

func (s *failOnceSource) Schema() []record.Column { return []record.Column{{Name: "v"}} }
func (s *failOnceSource) ReadBatch(int) (*record.RecordBatch, error) {
	if s.fail {
		return nil, fmt.Errorf("dependency unavailable")
	}
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	b := record.Get()
	b.Columns = s.Schema()
	b.Rows = append(b.Rows, []interface{}{1}, []interface{}{2}, []interface{}{3})
	return b, io.EOF
}
func (s *failOnceSource) Skip(int64) error { return nil }
func (s *failOnceSource) Close() error     { return nil }

func init() {
	source.Register("failoncetest", func(map[string]interface{}) (source.Source, error) {
		n := atomic.AddInt32(&failOnceOpens, 1)
		return &failOnceSource{fail: n == 1}, nil
	})
}

// restart.onFailure re-runs a failed triggered run after a backoff.
func TestRestartOnFailure(t *testing.T) {
	atomic.StoreInt32(&failOnceOpens, 0)
	out := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:  SourceDef{Type: "failoncetest", Options: map[string]interface{}{}},
		Sink:    jsonlSink(out),
		Restart: RestartDef{OnFailure: true, InitialBackoffMs: 10},
	}
	def.Defaults()
	act, err := NewInline(def, "sdk://test")
	if err != nil {
		t.Fatal(err)
	}
	o, err := act.runWithRestart(context.Background(), &Input{}, newRunLog("sdk://test", "r"), "r")
	if err != nil || o.RowsWritten != 3 {
		t.Fatalf("err=%v written=%d; want the restarted run to succeed with 3 rows", err, o.RowsWritten)
	}
	if n := atomic.LoadInt32(&failOnceOpens); n != 2 {
		t.Fatalf("source opened %d times, want 2 (one failed attempt + one restart)", n)
	}

	// Without the policy the failure is returned.
	atomic.StoreInt32(&failOnceOpens, 0)
	def.Restart = RestartDef{}
	act2, _ := NewInline(def, "sdk://test")
	if _, err := act2.runWithRestart(context.Background(), &Input{}, newRunLog("sdk://test", "r"), "r"); err == nil {
		t.Fatal("expected the failure without restart.onFailure")
	}
}

// Rows validate routed to the DLQ must not be dead-lettered or counted again
// when a later stage fails on the batch, nor counted as dropped when every
// row of a batch is rejected.
func TestRejectsNotDoubleCounted(t *testing.T) {
	run := func(min float64, extra ...TransformDef) (*Output, int, int) {
		dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")
		outPath := filepath.Join(t.TempDir(), "out.jsonl")
		tx := append([]TransformDef{{Type: "validate", Extra: map[string]interface{}{
			"type": "validate", "onViolation": "route",
			"rules": []interface{}{map[string]interface{}{"field": "amount", "min": min}},
		}}}, extra...)
		def := &Definition{
			Source: csvSource(writeTemp(t, "in.csv", ordersCSV)), Sink: jsonlSink(outPath), Transforms: tx,
			DLQ: DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": dlqPath}},
		}
		out, err := runInline(t, def)
		if err != nil {
			t.Fatal(err)
		}
		written := 0
		if _, err := os.Stat(outPath); err == nil {
			written = len(readJSONL(t, outPath))
		}
		return out, written, len(readJSONL(t, dlqPath))
	}
	// validate rejects -50; failonamount then fails the batch (amount 0).
	out, written, dead := run(0, TransformDef{Type: "failonamount", Extra: map[string]interface{}{"type": "failonamount"}})
	if written != 0 || dead != 5 || out.RowsFailed != 5 {
		t.Fatalf("written=%d dlq=%d failed=%d; want 0/5/5 (each row dead-lettered once)", written, dead, out.RowsFailed)
	}
	// Every row rejected: failed, not also dropped.
	out, _, dead = run(1000)
	if dead != 5 || out.RowsFailed != 5 || out.RowsDropped != 0 {
		t.Fatalf("dlq=%d failed=%d dropped=%d; want 5/5/0", dead, out.RowsFailed, out.RowsDropped)
	}
}
