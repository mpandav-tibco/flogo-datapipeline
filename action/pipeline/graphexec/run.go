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
				// End-of-input: let the operator emit any rows it buffered (e.g. a
				// window's still-open aggregates) before this node finishes.
				if f, hasFlush := n.tx.(Flusher); hasFlush {
					fb, ferr := f.Flush()
					if ferr != nil {
						rt.fail(fmt.Errorf("transform %q flush: %w", n.id, ferr))
						return
					}
					if fb != nil {
						if fb.Len() > 0 {
							atomic.AddInt64(&stat.RowsOut, int64(fb.Len()))
							rt.emit(ctx, n, fb)
						} else {
							record.Put(fb)
						}
					}
				}
				return
			}
			atomic.AddInt64(&stat.RowsIn, int64(b.Len()))
			// Per-row error routing: when this node both can split and has an
			// error edge, partition the batch and send the rejected rows down the
			// error edge instead of failing the whole batch.
			if sp, ok := n.tx.(Splitter); ok && n.hasErr {
				pass, reject, serr := sp.ApplySplit(b)
				record.Put(b)
				if serr != nil {
					if pass != nil {
						record.Put(pass)
					}
					if reject != nil {
						record.Put(reject)
					}
					rt.fail(fmt.Errorf("transform %q: %w", n.id, serr))
					return
				}
				if reject != nil {
					if reject.Len() > 0 {
						rt.emitError(ctx, n, reject)
					}
					record.Put(reject)
				}
				if pass == nil || pass.Len() == 0 {
					if pass != nil {
						record.Put(pass)
					}
					continue
				}
				atomic.AddInt64(&stat.RowsOut, int64(pass.Len()))
				if !rt.emit(ctx, n, pass) {
					return
				}
				continue
			}
			out, err := n.tx.Apply(b)
			if err != nil {
				// Route the failed batch to the node's error edge(s); if it has
				// none, the error aborts the run (matching the linear engine).
				handled := rt.emitError(ctx, n, b)
				if out != nil && out != b {
					record.Put(out)
				}
				record.Put(b)
				if !handled {
					rt.fail(fmt.Errorf("transform %q: %w", n.id, err))
					return
				}
				continue
			}
			// Per the transform contract the caller owns the input; recycle it
			// once the stage produced a DISTINCT output (pass-through returns
			// the same batch, which is forwarded instead).
			if out != b {
				record.Put(b)
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

// emit routes a batch of SUCCESSFULLY-processed rows to the node's outbound
// edges using Flogo-style link semantics:
//
//   - every EdgeSuccess edge receives every row (tee);
//   - each EdgeCondition edge receives the rows for which its predicate is true;
//   - EdgeOtherwise edges receive the rows that matched no condition edge.
//
// Error edges are not touched here (they carry failed rows — see emitError). A
// single success edge is the fast path: the batch is forwarded without copying.
func (rt *runtime) emit(ctx context.Context, n *pnode, b *record.RecordBatch) bool {
	if b == nil {
		return true
	}
	if len(n.out) == 1 && n.out[0].etype == EdgeSuccess {
		return rt.send(ctx, n.out[0], b)
	}
	buckets := make([]*record.RecordBatch, len(n.out))
	for ri := range b.Rows {
		row := b.RowMap(ri)
		matched := false
		for i, e := range n.out {
			switch e.etype {
			case EdgeSuccess:
				appendRow(&buckets[i], b, ri)
			case EdgeCondition:
				if e.pred(row) {
					appendRow(&buckets[i], b, ri)
					matched = true
				}
			}
		}
		if !matched {
			for i, e := range n.out {
				if e.etype == EdgeOtherwise {
					appendRow(&buckets[i], b, ri)
				}
			}
		}
	}
	record.Put(b)
	for i, e := range n.out {
		if buckets[i] == nil {
			continue
		}
		if !rt.send(ctx, e, buckets[i]) {
			for k := i + 1; k < len(buckets); k++ {
				if buckets[k] != nil {
					record.Put(buckets[k])
				}
			}
			return false
		}
	}
	return true
}

// emitError routes a failed batch to the node's error edge(s). It returns false
// when the node declares no error edge, so the caller aborts the run (matching
// the linear engine, which fails on an unhandled transform error).
func (rt *runtime) emitError(ctx context.Context, n *pnode, b *record.RecordBatch) bool {
	stat := rt.res.Nodes[n.id]
	handled := false
	for _, e := range n.out {
		if e.etype != EdgeError {
			continue
		}
		handled = true
		cp := copyBatch(b)
		atomic.AddInt64(&stat.RowsErrored, int64(cp.Len()))
		if !rt.send(ctx, e, cp) {
			return true // cancelled; the run is already tearing down
		}
	}
	return handled
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

// appendRow lazily allocates the per-edge bucket (a pooled batch that shares the
// source schema) and appends a COPY of row i, so each downstream branch owns its
// rows independently of the source batch and of sibling branches.
func appendRow(bucket **record.RecordBatch, src *record.RecordBatch, i int) {
	if *bucket == nil {
		*bucket = src.Clone(src.Len())
	}
	(*bucket).Rows = append((*bucket).Rows, cloneRow(src.Rows[i]))
}

func cloneRow(r []interface{}) []interface{} {
	c := make([]interface{}, len(r))
	copy(c, r)
	return c
}

// copyBatch returns a copy of b with an independent Rows slice and copied row
// slices, so the original can be recycled while the copy travels an edge.
func copyBatch(b *record.RecordBatch) *record.RecordBatch {
	out := b.Clone(b.Len())
	for i := range b.Rows {
		out.Rows = append(out.Rows, cloneRow(b.Rows[i]))
	}
	return out
}
