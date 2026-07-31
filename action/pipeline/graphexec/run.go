package graphexec

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Run executes the plan as a streaming operator network: one goroutine per
// node, one bounded channel per consumer (its merged inbound edges). Sources
// push batches; transforms apply and fan out; sinks drain. Conditional edges
// partition rows by their predicate at the producer, and each consumer receives
// its own batch so batch ownership stays single-writer.
//
// Run blocks until every node finishes (all sources reach EOF and the drain
// propagates to the sinks) or a node errors — the first error cancels the run
// and is returned. Operators are closed before Run returns; do not call
// Plan.Close afterwards.
func (p *Plan) Run(ctx context.Context, opts Options) (*Result, error) {
	res := &Result{Nodes: make(map[string]*NodeStat, len(p.nodes))}
	for _, n := range p.nodes {
		res.Nodes[n.id] = &NodeStat{ID: n.id, Kind: n.kind}
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var runErr error
	var errOnce sync.Once
	fail := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			runErr = err
			cancel()
		})
	}

	// One input channel per non-source node; a WaitGroup counting its inbound
	// producers so the channel is closed exactly once they have all finished.
	inCh := make(map[string]chan *record.RecordBatch, len(p.nodes))
	inWG := make(map[string]*sync.WaitGroup, len(p.nodes))
	for _, n := range p.nodes {
		if n.kind == KindSource {
			continue
		}
		inCh[n.id] = make(chan *record.RecordBatch, opts.bufferDepth())
		wg := &sync.WaitGroup{}
		wg.Add(n.inDeg)
		inWG[n.id] = wg
	}
	for id, ch := range inCh {
		go func(ch chan *record.RecordBatch, wg *sync.WaitGroup) {
			wg.Wait()
			close(ch)
		}(ch, inWG[id])
	}

	rt := &runtime{res: res, opts: opts, inCh: inCh, inWG: inWG, fail: fail}

	var nodes sync.WaitGroup
	for _, n := range p.nodes {
		nodes.Add(1)
		go func(n *pnode) {
			defer nodes.Done()
			switch n.kind {
			case KindSource:
				rt.runSource(rctx, n)
			case KindTransform:
				rt.runTransform(rctx, n)
			case KindSink:
				rt.runSink(rctx, n)
			}
		}(n)
	}
	nodes.Wait()
	_ = p.Close()
	return res, runErr
}

// runtime carries the shared per-run wiring passed to each node goroutine.
type runtime struct {
	res  *Result
	opts Options
	inCh map[string]chan *record.RecordBatch
	inWG map[string]*sync.WaitGroup
	fail func(error)
}

// finishProducers signals, for every outbound edge of n, that this producer has
// finished — decrementing each consumer's inbound WaitGroup so its channel is
// eventually closed. Deferred by producers so it runs even on early return.
func (rt *runtime) finishProducers(n *pnode) {
	for _, e := range n.out {
		if wg := rt.inWG[e.to.id]; wg != nil {
			wg.Done()
		}
	}
}

func (rt *runtime) runSource(ctx context.Context, n *pnode) {
	defer rt.finishProducers(n)
	stat := rt.res.Nodes[n.id]
	size := rt.opts.batchSize()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		b, err := n.src.ReadBatch(size)
		if b != nil && b.Len() > 0 {
			atomic.AddInt64(&stat.RowsOut, int64(b.Len()))
			atomic.AddInt64(&rt.res.RowsRead, int64(b.Len()))
			if !rt.emit(ctx, n, b) {
				return
			}
		} else if b != nil {
			record.Put(b)
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			rt.fail(fmt.Errorf("source %q: %w", n.id, err))
			return
		}
	}
}

func (rt *runtime) runTransform(ctx context.Context, n *pnode) {
	defer rt.finishProducers(n)
	stat := rt.res.Nodes[n.id]
	ch := rt.inCh[n.id]
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-ch:
			if !ok {
				return
			}
			atomic.AddInt64(&stat.RowsIn, int64(b.Len()))
			out, err := n.tx.Apply(b)
			// Per the transform contract the caller owns the input; recycle it
			// once the stage produced a DISTINCT output (pass-through returns
			// the same batch, which is forwarded instead).
			if out != b {
				record.Put(b)
			}
			if err != nil {
				if out != nil && out != b {
					record.Put(out)
				}
				rt.fail(fmt.Errorf("transform %q: %w", n.id, err))
				return
			}
			if out == nil {
				continue
			}
			if out.Len() == 0 {
				record.Put(out)
				continue
			}
			atomic.AddInt64(&stat.RowsOut, int64(out.Len()))
			if !rt.emit(ctx, n, out) {
				return
			}
		}
	}
}

func (rt *runtime) runSink(ctx context.Context, n *pnode) {
	stat := rt.res.Nodes[n.id]
	ch := rt.inCh[n.id]
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-ch:
			if !ok {
				if err := n.snk.Flush(); err != nil {
					rt.fail(fmt.Errorf("sink %q flush: %w", n.id, err))
				}
				return
			}
			atomic.AddInt64(&stat.RowsIn, int64(b.Len()))
			if err := n.snk.WriteBatch(b); err != nil {
				record.Put(b)
				rt.fail(fmt.Errorf("sink %q write: %w", n.id, err))
				return
			}
			atomic.AddInt64(&stat.RowsWritten, int64(b.Len()))
			atomic.AddInt64(&rt.res.RowsWritten, int64(b.Len()))
			record.Put(b)
		}
	}
}

// emit fans an output batch out to every outbound edge. A single unconditional
// edge transfers ownership directly (no copy); otherwise each edge receives its
// own batch of the rows matching its predicate.
func (rt *runtime) emit(ctx context.Context, n *pnode, b *record.RecordBatch) bool {
	if b == nil {
		return true
	}
	if len(n.out) == 0 {
		record.Put(b)
		return true
	}
	if len(n.out) == 1 && n.out[0].pred == nil {
		return rt.send(ctx, n.out[0], b)
	}
	for _, e := range n.out {
		sub := subset(b, e.pred)
		if sub.Len() == 0 {
			record.Put(sub)
			continue
		}
		if !rt.send(ctx, e, sub) {
			record.Put(b)
			return false
		}
	}
	record.Put(b)
	return true
}

// send delivers a batch to an edge's consumer, honouring cancellation. On
// cancellation it recycles the batch it was handed.
func (rt *runtime) send(ctx context.Context, e *pedge, b *record.RecordBatch) bool {
	select {
	case rt.inCh[e.to.id] <- b:
		return true
	case <-ctx.Done():
		record.Put(b)
		return false
	}
}

// subset builds a new batch containing the rows of b that satisfy pred (all rows
// when pred is nil). Rows are copied so each fan-out branch owns its data.
func subset(b *record.RecordBatch, pred Pred) *record.RecordBatch {
	out := b.Clone(b.Len())
	if pred == nil {
		for _, row := range b.Rows {
			out.Rows = append(out.Rows, cloneRow(row))
		}
		return out
	}
	for i := range b.Rows {
		if pred(b.RowMap(i)) {
			out.Rows = append(out.Rows, cloneRow(b.Rows[i]))
		}
	}
	return out
}

func cloneRow(r []interface{}) []interface{} {
	c := make([]interface{}, len(r))
	copy(c, r)
	return c
}
