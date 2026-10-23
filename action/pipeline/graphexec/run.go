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
	// producers so the channel is closed exactly once they have all finished. A
	// multi-input-active node (see pnode.multiIn) gets TWO dedicated channels
	// instead — rightCh/leftCh — each with its own producer WaitGroup, so its
	// right-side producers can be drained to completion before any left batch
	// is delivered.
	inCh := make(map[string]chan pmsg, len(p.nodes))
	inWG := make(map[string]*sync.WaitGroup, len(p.nodes))
	rightCh := make(map[string]chan pmsg, len(p.nodes))
	rightWG := make(map[string]*sync.WaitGroup, len(p.nodes))
	leftCh := make(map[string]chan pmsg, len(p.nodes))
	leftWG := make(map[string]*sync.WaitGroup, len(p.nodes))
	for _, n := range p.nodes {
		if n.kind == KindSource {
			continue
		}
		if n.multiIn != nil {
			rightCh[n.id] = make(chan pmsg, opts.bufferDepth())
			rwg := &sync.WaitGroup{}
			rwg.Add(n.rightInDeg)
			rightWG[n.id] = rwg

			leftCh[n.id] = make(chan pmsg, opts.bufferDepth())
			lwg := &sync.WaitGroup{}
			lwg.Add(n.leftInDeg)
			leftWG[n.id] = lwg
			continue
		}
		inCh[n.id] = make(chan pmsg, opts.bufferDepth())
		wg := &sync.WaitGroup{}
		wg.Add(n.inDeg)
		inWG[n.id] = wg
	}
	for id, ch := range inCh {
		go func(ch chan pmsg, wg *sync.WaitGroup) {
			wg.Wait()
			close(ch)
		}(ch, inWG[id])
	}
	for id, ch := range rightCh {
		go func(ch chan pmsg, wg *sync.WaitGroup) {
			wg.Wait()
			close(ch)
		}(ch, rightWG[id])
	}
	for id, ch := range leftCh {
		go func(ch chan pmsg, wg *sync.WaitGroup) {
			wg.Wait()
			close(ch)
		}(ch, leftWG[id])
	}

	rt := &runtime{
		res: res, opts: opts,
		inCh: inCh, inWG: inWG,
		rightCh: rightCh, rightWG: rightWG,
		leftCh: leftCh, leftWG: leftWG,
		fail: fail,
	}

	// Checkpoint coordinator: active only when an interval + callback are set.
	// The source emits a drain barrier every CheckpointRows rows; the coordinator
	// waits for every sink to flush at the barrier, persists the source position,
	// then releases the source to continue — so a crash resumes with no data loss.
	if opts.CheckpointRows > 0 && opts.OnCheckpoint != nil {
		rt.cpOn = true
		rt.cpSinks = p.sinkCount()
		rt.cpBarrierCh = make(chan int64)
		rt.cpAckCh = make(chan struct{}, rt.cpSinks)
		rt.cpResumeCh = make(chan struct{})
		go rt.runCheckpointCoord(rctx)
	}

	var nodes sync.WaitGroup
	for _, n := range p.nodes {
		nodes.Add(1)
		go func(n *pnode) {
			defer nodes.Done()
			switch n.kind {
			case KindSource:
				rt.runSource(rctx, n)
			case KindTransform:
				if n.multiIn != nil {
					rt.runMultiInputTransform(rctx, n)
				} else {
					rt.runTransform(rctx, n)
				}
			case KindSink:
				rt.runSink(rctx, n)
			}
		}(n)
	}
	nodes.Wait()
	if !opts.KeepOpen {
		_ = p.Close()
	}
	return res, runErr
}

// runtime carries the shared per-run wiring passed to each node goroutine.
type runtime struct {
	res  *Result
	opts Options
	inCh map[string]chan pmsg
	inWG map[string]*sync.WaitGroup
	fail func(error)

	// Dedicated channels for a multi-input-active node's two named ports (see
	// pnode.multiIn) — populated only for such nodes; absent for every other node.
	rightCh map[string]chan pmsg
	rightWG map[string]*sync.WaitGroup
	leftCh  map[string]chan pmsg
	leftWG  map[string]*sync.WaitGroup

	// Checkpoint coordination (active only when opts.CheckpointRows > 0).
	cpOn        bool
	cpBarrierCh chan int64    // source → coordinator: barrier emitted at this position
	cpAckCh     chan struct{} // sink → coordinator: flushed at the barrier
	cpResumeCh  chan struct{} // coordinator → source: checkpoint saved, resume
	cpSinks     int
}

// pmsg is what flows on a node's input channel: a data batch, or a checkpoint
// barrier (batch == nil, barrier holds the epoch number > 0).
type pmsg struct {
	batch   *record.RecordBatch
	barrier int64
}

// runCheckpointCoord drives one checkpoint round at a time: it reads the source
// position a barrier was emitted at, waits for every sink to flush at that
// barrier (so all rows before it are durable), persists the position, then
// releases the source to resume.
func (rt *runtime) runCheckpointCoord(ctx context.Context) {
	for {
		var pos int64
		select {
		case p, ok := <-rt.cpBarrierCh:
			if !ok {
				return
			}
			pos = p
		case <-ctx.Done():
			return
		}
		for i := 0; i < rt.cpSinks; i++ {
			select {
			case <-rt.cpAckCh:
			case <-ctx.Done():
				return
			}
		}
		if err := rt.opts.OnCheckpoint(pos); err != nil {
			rt.fail(fmt.Errorf("checkpoint save: %w", err))
			return
		}
		select {
		case rt.cpResumeCh <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
}

// emitBarrier forwards a checkpoint barrier to EVERY outbound edge. Barriers are
// control signals — they ignore edge conditions so every downstream path drains.
// A multi-input-active target receives it on whichever dedicated channel the
// edge targets (see runMultiInputTransform — a right-channel barrier is simply
// ignored there; the bounded right side is not part of the checkpoint story).
func (rt *runtime) emitBarrier(ctx context.Context, n *pnode, epoch int64) bool {
	for _, e := range n.out {
		ch := rt.inCh[e.to.id]
		if e.to.multiIn != nil {
			if e.toPort == "right" {
				ch = rt.rightCh[e.to.id]
			} else {
				ch = rt.leftCh[e.to.id]
			}
		}
		select {
		case ch <- pmsg{barrier: epoch}:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// finishProducers signals, for every outbound edge of n, that this producer has
// finished — decrementing the consumer's inbound WaitGroup (the ordinary merged
// one, or the right/left one for a multi-input-active target) so its channel is
// eventually closed. Deferred by producers so it runs even on early return.
func (rt *runtime) finishProducers(n *pnode) {
	for _, e := range n.out {
		if e.to.multiIn != nil {
			if e.toPort == "right" {
				if wg := rt.rightWG[e.to.id]; wg != nil {
					wg.Done()
				}
			} else if wg := rt.leftWG[e.to.id]; wg != nil {
				wg.Done()
			}
			continue
		}
		if wg := rt.inWG[e.to.id]; wg != nil {
			wg.Done()
		}
	}
}

func (rt *runtime) runSource(ctx context.Context, n *pnode) {
	defer rt.finishProducers(n)
	// Let the coordinator exit once the source is done producing barriers.
	if rt.cpOn {
		defer close(rt.cpBarrierCh)
	}
	stat := rt.res.Nodes[n.id]
	size := rt.opts.batchSize()

	// Resume: skip rows already durable before the last saved checkpoint.
	if rt.opts.ResumeFrom > 0 {
		if err := n.src.Skip(rt.opts.ResumeFrom); err != nil {
			rt.fail(fmt.Errorf("source %q resume skip: %w", n.id, err))
			return
		}
	}

	var rows, nextBarrier int64
	var epoch int64
	if rt.cpOn {
		nextBarrier = int64(rt.opts.CheckpointRows)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		b, err := n.src.ReadBatch(size)
		if b != nil && b.Len() > 0 {
			rows += int64(b.Len())
			atomic.AddInt64(&stat.RowsOut, int64(b.Len()))
			atomic.AddInt64(&rt.res.RowsRead, int64(b.Len()))
			if !rt.emit(ctx, n, b) {
				return
			}
		} else if b != nil {
			record.Put(b)
		}
		// Drain barrier: once we cross a checkpoint boundary, hand the coordinator
		// our position, broadcast the barrier, then PAUSE until it has flushed every
		// sink and persisted the position. Pausing means fan-in nodes never have to
		// buffer to align barriers — no more rows are in flight behind this one.
		if rt.cpOn && err == nil && rows >= nextBarrier {
			nextBarrier += int64(rt.opts.CheckpointRows)
			epoch++
			pos := rt.opts.ResumeFrom + rows
			select {
			case rt.cpBarrierCh <- pos:
			case <-ctx.Done():
				return
			}
			if !rt.emitBarrier(ctx, n, epoch) {
				return
			}
			select {
			case <-rt.cpResumeCh:
			case <-ctx.Done():
				return
			}
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
	var barrierSeen int64 // barriers of the current epoch seen across inbound edges
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
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
			// Checkpoint barrier: forward it once this node has seen it on every
			// inbound edge (the source paused, so no data rows trail the barrier).
			if m.barrier > 0 {
				barrierSeen++
				if barrierSeen >= int64(n.inDeg) {
					barrierSeen = 0
					if !rt.emitBarrier(ctx, n, m.barrier) {
						return
					}
				}
				continue
			}
			b := m.batch
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

// runMultiInputTransform drives a multi-input-active node (see pnode.multiIn):
// it fully drains the right channel to LoadRight, calls CloseRight exactly
// once, THEN streams the left channel through ApplyLeft — mirroring
// runTransform's main loop (barrier handling, Flusher end-of-input) but
// against leftCh instead of the merged inCh, and counting barriers against
// leftInDeg instead of inDeg. A barrier arriving on the right channel is
// ignored (the bounded right side is not part of the checkpoint story); a
// multi-input node has no error-edge/Splitter path since none of today's
// MultiInputOp implementers (just join) need one.
func (rt *runtime) runMultiInputTransform(ctx context.Context, n *pnode) {
	defer rt.finishProducers(n)
	stat := rt.res.Nodes[n.id]

	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-rt.rightCh[n.id]:
			if !ok {
				goto rightDone
			}
			if m.batch == nil {
				continue // barrier on the right side — not part of the checkpoint story.
			}
			atomic.AddInt64(&stat.RowsIn, int64(m.batch.Len()))
			if err := n.multiIn.LoadRight(m.batch); err != nil {
				record.Put(m.batch)
				rt.fail(fmt.Errorf("transform %q: load right: %w", n.id, err))
				return
			}
			record.Put(m.batch)
		}
	}
rightDone:
	if err := n.multiIn.CloseRight(); err != nil {
		rt.fail(fmt.Errorf("transform %q: close right: %w", n.id, err))
		return
	}

	ch := rt.leftCh[n.id]
	var barrierSeen int64
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
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
			if m.barrier > 0 {
				barrierSeen++
				if barrierSeen >= int64(n.leftInDeg) {
					barrierSeen = 0
					if !rt.emitBarrier(ctx, n, m.barrier) {
						return
					}
				}
				continue
			}
			b := m.batch
			atomic.AddInt64(&stat.RowsIn, int64(b.Len()))
			out, err := n.multiIn.ApplyLeft(b)
			if err != nil {
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
	var barrierSeen int64 // barriers of the current epoch seen across inbound edges
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				if err := n.snk.Flush(); err != nil {
					rt.fail(fmt.Errorf("sink %q flush: %w", n.id, err))
				}
				return
			}
			// Checkpoint barrier: once seen on every inbound edge, flush everything
			// written before it durable, then ack the coordinator so it can persist
			// the source position for this epoch.
			if m.barrier > 0 {
				barrierSeen++
				if barrierSeen >= int64(n.inDeg) {
					barrierSeen = 0
					if err := n.snk.Flush(); err != nil {
						rt.fail(fmt.Errorf("sink %q flush: %w", n.id, err))
						return
					}
					select {
					case rt.cpAckCh <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
				continue
			}
			b := m.batch
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
	hasCond := false
	for _, e := range n.out {
		if e.etype == EdgeCondition {
			hasCond = true
			break
		}
	}
	var row map[string]interface{}
	for ri := range b.Rows {
		if hasCond {
			row = b.RowMapInto(ri, row)
		}
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
// cancellation it recycles the batch it was handed. A multi-input-active
// target routes to its dedicated right/left channel instead of the ordinary
// merged one.
func (rt *runtime) send(ctx context.Context, e *pedge, b *record.RecordBatch) bool {
	ch := rt.inCh[e.to.id]
	if e.to.multiIn != nil {
		if e.toPort == "right" {
			ch = rt.rightCh[e.to.id]
		} else {
			ch = rt.leftCh[e.to.id]
		}
	}
	select {
	case ch <- pmsg{batch: b}:
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
