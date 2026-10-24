package pipeline

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	wintransform "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

// This file holds the operator wrappers through which the single execution
// engine (graphexec) provides the pipeline's run semantics — metrics, stall
// detection, transform-error accounting, DLQ isolation — independent of the
// graph's shape. See docs/design/engine-unification.md.

// runCounters accumulates row outcomes across every node goroutine of a run.
type runCounters struct {
	written atomic.Int64
	failed  atomic.Int64
	dropped atomic.Int64
}

func (c *runCounters) addTo(out *Output) {
	out.RowsWritten += c.written.Load()
	out.RowsFailed += c.failed.Load()
	out.RowsDropped += c.dropped.Load()
}

// runEnv is what wrappers need from the running action.
type runEnv struct {
	a      *DataPipelineAction
	runLog corelog.Logger
	runID  string
	counts *runCounters
	// drain is closed when shutdown asks the run to finish cleanly; nil when
	// the plan cannot drain (stateful plans resume from checkpointed state).
	drain     <-chan struct{}
	drainOnce sync.Once
	drained   atomic.Bool
}

// errDrained ends a run that shutdown stopped after draining. It wraps
// context.Canceled: the run is reported "cancelled" and never restarted.
var errDrained = fmt.Errorf("stopped by shutdown after draining (final checkpoint saved): %w", context.Canceled)

// wasDrained reports whether a source stopped reading because of shutdown.
func (e *runEnv) wasDrained() bool { return e.drained.Load() }

// draining reports whether shutdown asked the run to stop reading.
func (e *runEnv) draining() bool {
	if e.drain == nil {
		return false
	}
	select {
	case <-e.drain:
		e.drainOnce.Do(func() { e.runLog.Infof("shutdown: stopping reads and draining the run (final checkpoint follows)") })
		e.drained.Store(true)
		return true
	default:
		return false
	}
}

// ─── source ──────────────────────────────────────────────────────────────────

// meteredSource reports BatchRead metrics and read stalls.
type meteredSource struct {
	source.Source
	env *runEnv
	typ string
}

func (m *meteredSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if m.env.draining() {
		// Shutdown asked the run to finish cleanly: report end of input so
		// the plan drains, flushes and takes its final checkpoint.
		return nil, io.EOF
	}
	t0 := time.Now()
	b, err := m.Source.ReadBatch(size)
	if b != nil && b.Len() > 0 {
		d := time.Since(t0)
		m.env.a.recorder.BatchRead(m.env.a.ref, m.typ, b.Len(), d)
		m.env.a.checkStall(m.env.runLog, "read", d)
	}
	return b, err
}

// Unwrap returns the wrapped source (for capability type assertions).
func (m *meteredSource) Unwrap() source.Source { return m.Source }

// ─── transform chain ─────────────────────────────────────────────────────────

// chainOp runs a linear pipeline's whole transform chain as ONE graph node, so
// a linear definition keeps exactly the per-batch behaviour it always had
// (pooling, stage statistics, ordering) while running on the graph engine.
// A transform error counts the batch as failed and the run continues — the
// linear semantics (an explicit graph node without an error edge aborts). The
// failed batch's rows go to the DLQ when one is configured, so they are
// accounted for rather than lost.
type chainOp struct {
	p   *transform.Pipeline
	env *runEnv
	dlq dlqWriteSink
	rej *dlqRejects // rows routed to the DLQ by stages (validate), per batch
}

func newChainOp(p *transform.Pipeline, env *runEnv, dlq dlqWriteSink) graphexec.TransformOp {
	c := &chainOp{p: p, env: env, dlq: dlq}
	if dlq != nil {
		c.rej = &dlqRejects{dlq: dlq, env: env}
		bindRejects(p, c.rej)
	}
	if w, idx := windowStage(p); w != nil {
		return &windowedChainOp{chainOp: c, win: w, winIdx: idx}
	}
	return c
}

func (c *chainOp) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	n := in.Len()
	if c.rej != nil {
		c.rej.reset()
	}
	t1 := time.Now()
	out, err := c.p.Apply(in)
	d := time.Since(t1)
	a, rl := c.env.a, c.env.runLog
	switch {
	case err != nil:
		a.recorder.BatchFailed(a.ref, n)
		c.env.counts.failed.Add(int64(n))
		if c.dlq == nil {
			rl.Errorf("transform error on batch of %d rows — rows NOT delivered (configure a dlq to keep them): %v", n, err)
			return nil, nil // consumed: the engine recycles the input
		}
		// The input batch is still valid (the transform pipeline never
		// recycles its input); it may hold in-place edits of earlier stages.
		// Rows a stage already routed to the DLQ (and counted) are skipped.
		failedBatch := in
		if routed := c.rej.routedCount(); routed > 0 {
			c.env.counts.failed.Add(-int64(routed))
			failedBatch = &record.RecordBatch{Columns: in.Columns}
			for _, r := range in.Rows {
				if !c.rej.wasRouted(r) {
					failedBatch.Rows = append(failedBatch.Rows, r)
				}
			}
		}
		reasons := make([]string, failedBatch.Len())
		for i := range reasons {
			reasons[i] = "transform: " + err.Error()
		}
		if derr := c.dlq.WriteBatchErrors(failedBatch, reasons); derr != nil {
			// Neither delivered nor dead-lettered: abort, so the source is not
			// checkpointed past these rows and they are replayed.
			return nil, fmt.Errorf("transform error (%v) and dead-lettering the batch failed: %w", err, derr)
		}
		rl.Warnf("transform error on batch of %d rows — dead-lettered: %v", n, err)
		return nil, nil
	case out == nil:
		// Rows routed to the DLQ are already counted as failed.
		c.env.counts.dropped.Add(int64(n - c.rej.routedCount()))
		a.recorder.BatchTransformed(a.ref, n, 0, d)
	default:
		a.recorder.BatchTransformed(a.ref, n, out.Len(), d)
	}
	a.checkStall(rl, "transform", d)
	return out, nil
}

func (c *chainOp) Close() error { return c.p.Close() }

// BindContext hands the run context to the chain's I/O-performing transforms
// (e.g. lookup) so cancelling the run aborts their in-flight calls.
func (c *chainOp) BindContext(ctx context.Context) { c.p.BindContext(ctx) }

// Pipeline exposes the wrapped transform pipeline (capability lookups).
func (c *chainOp) Pipeline() *transform.Pipeline { return c.p }

// windowedChainOp is a chain containing a window stage: it flushes the open
// windows at end of input — through the stages AFTER the window, so later
// mapping/filtering applies to the final windows too — and snapshots the
// window state at checkpoints.
type windowedChainOp struct {
	*chainOp
	win    wintransform.WindowedOperator
	winIdx int
}

func (w *windowedChainOp) Flush() (*record.RecordBatch, error) {
	flushed, err := w.win.Flush()
	if err != nil || flushed == nil || flushed.Len() == 0 {
		if flushed != nil && flushed.Len() == 0 {
			record.Put(flushed)
			flushed = nil
		}
		return flushed, err
	}
	return w.p.ApplyFrom(w.winIdx+1, flushed)
}

func (w *windowedChainOp) Snapshot() ([]byte, error) { return w.win.Snapshot() }
func (w *windowedChainOp) Restore(b []byte) error    { return w.win.Restore(b) }

// Window exposes the window operator (stats/metrics).
func (w *windowedChainOp) Window() wintransform.WindowedOperator { return w.win }

func windowStage(p *transform.Pipeline) (wintransform.WindowedOperator, int) {
	for i, st := range p.Stages() {
		if w, ok := st.(wintransform.WindowedOperator); ok {
			return w, i
		}
	}
	return nil, -1
}

// ─── sink ────────────────────────────────────────────────────────────────────

// managedSink gives every sink node the pipeline's write semantics: per-row
// isolation and DLQ routing of failed rows (bisection for atomic sinks),
// recovery of rows a buffering sink failed to flush, write metrics and stall
// detection. It only returns an error for infrastructure failures (e.g. the
// DLQ itself failing), which abort the run.
type managedSink struct {
	snk sink.Sink
	dlq dlqWriteSink
	env *runEnv
	typ string
}

func (m *managedSink) WriteBatch(b *record.RecordBatch) error {
	t := time.Now()
	var o Output
	n, err := m.env.a.rowLevelWrite(m.env.runLog, m.snk, m.dlq, b, m.env.runID, &o)
	m.merge(n, &o)
	if err != nil {
		return err
	}
	d := time.Since(t)
	m.env.a.recorder.BatchWritten(m.env.a.ref, m.typ, n, d)
	m.env.a.checkStall(m.env.runLog, "write", d)
	return nil
}

func (m *managedSink) Flush() error {
	var o Output
	err := m.env.a.flushSink(m.env.runLog, m.snk, m.dlq, &o)
	m.merge(0, &o)
	// Rows dead-lettered before a checkpoint must be durable when it is
	// taken, like the rows written to the sink.
	if err == nil && m.dlq != nil {
		if derr := m.dlq.Flush(); derr != nil {
			err = fmt.Errorf("dlq flush: %w", derr)
		}
	}
	return err
}

func (m *managedSink) merge(written int, o *Output) {
	// o.RowsWritten carries (negative) corrections for previously accepted
	// rows a buffering sink later failed to persist.
	m.env.counts.written.Add(int64(written) + o.RowsWritten)
	m.env.counts.failed.Add(o.RowsFailed)
}

func (m *managedSink) Close() error { return m.snk.Close() }

// Unwrap returns the wrapped sink (StatsReporter / Reconciler lookups).
func (m *managedSink) Unwrap() sink.Sink { return m.snk }

// dlqRejects adapts the pipeline DLQ for transforms that route rejected rows
// (transform.RejectRouter), counting them as failed so read = written + failed
// reconciles.
type dlqRejects struct {
	dlq dlqWriteSink
	env *runEnv
	// routed holds the rows routed during the current batch (identified by
	// their backing array), so the chain neither dead-letters nor counts
	// them twice. Each chain (and replica) has its own instance.
	routed map[*interface{}]struct{}
}

func (d *dlqRejects) WriteRejects(b *record.RecordBatch, reasons []string) error {
	if err := d.dlq.WriteBatchErrors(b, reasons); err != nil {
		return err
	}
	d.env.counts.failed.Add(int64(b.Len()))
	if d.routed == nil {
		d.routed = map[*interface{}]struct{}{}
	}
	for _, r := range b.Rows {
		if len(r) > 0 {
			d.routed[&r[0]] = struct{}{}
		}
	}
	return nil
}

func (d *dlqRejects) reset() {
	for k := range d.routed {
		delete(d.routed, k)
	}
}

func (d *dlqRejects) routedCount() int {
	if d == nil {
		return 0
	}
	return len(d.routed)
}

func (d *dlqRejects) wasRouted(r []interface{}) bool {
	if d == nil || len(r) == 0 {
		return false
	}
	_, ok := d.routed[&r[0]]
	return ok
}

// bindRejects offers the DLQ to every stage that routes rejected rows and has
// no destination of its own.
func bindRejects(p *transform.Pipeline, w transform.RejectWriter) {
	for _, st := range p.Stages() {
		if rr, ok := st.(transform.RejectRouter); ok {
			rr.UseDefaultRejects(w)
		}
	}
}

// lockedDLQ serialises a DLQ shared by several sink nodes, which run on their
// own goroutines.
type lockedDLQ struct {
	mu    sync.Mutex
	inner dlqWriteSink
}

func (l *lockedDLQ) WriteBatch(b *record.RecordBatch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.WriteBatch(b)
}
func (l *lockedDLQ) WriteBatchErrors(b *record.RecordBatch, errs []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.WriteBatchErrors(b, errs)
}
func (l *lockedDLQ) InjectContext(ref, runID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inner.InjectContext(ref, runID)
}
func (l *lockedDLQ) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Flush()
}
func (l *lockedDLQ) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Close()
}

// unwrapSource / unwrapSink peel the engine wrappers off an operator.
func unwrapSource(op graphexec.SourceOp) interface{} {
	if m, ok := op.(*meteredSource); ok {
		return m.Unwrap()
	}
	return op
}

func unwrapSink(op graphexec.SinkOp) interface{} {
	if m, ok := op.(*managedSink); ok {
		return m.Unwrap()
	}
	return op
}
