package graphexec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// runParallelTransform runs a transform node whose operator has replicas
// (Node.Replicas): a dispatcher hands sequence-numbered batches to one worker
// per operator instance, and a collector re-sequences the results so the node
// emits in exactly the order it received. A checkpoint barrier is forwarded
// only after every batch dispatched before it has been emitted, preserving
// the barrier's "nothing in flight behind me" guarantee.
func (rt *runtime) runParallelTransform(ctx context.Context, n *pnode) {
	defer rt.finishProducers(n)
	stat := rt.res.Nodes[n.id]
	ops := append([]TransformOp{n.tx}, n.replicas...)

	type job struct {
		seq int64
		b   *record.RecordBatch
	}
	type result struct {
		seq int64
		in  *record.RecordBatch
		out *record.RecordBatch
		err error
	}
	jobs := make(chan job, len(ops))
	results := make(chan result, len(ops)*2)

	var workers sync.WaitGroup
	for _, op := range ops {
		workers.Add(1)
		go func(op TransformOp) {
			defer workers.Done()
			for j := range jobs {
				out, err := op.Apply(j.b)
				results <- result{seq: j.seq, in: j.b, out: out, err: err}
			}
		}(op)
	}
	go func() { workers.Wait(); close(results) }()

	// emitted counts batches the collector has fully handled; the dispatcher
	// waits on it before forwarding a barrier.
	var mu sync.Mutex
	progress := sync.NewCond(&mu)
	var emitted int64
	stopped := false

	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		pending := map[int64]result{}
		var next int64
		failed := false
		for r := range results {
			pending[r.seq] = r
			for {
				cur, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				next++
				if !failed && !rt.finishParallelBatch(ctx, n, stat, cur.in, cur.out, cur.err) {
					failed = true // keep draining so workers can exit
				}
				mu.Lock()
				emitted++
				progress.Broadcast()
				mu.Unlock()
			}
		}
		mu.Lock()
		stopped = true
		progress.Broadcast()
		mu.Unlock()
	}()

	waitEmitted := func(target int64) bool {
		stop := context.AfterFunc(ctx, func() {
			mu.Lock()
			progress.Broadcast()
			mu.Unlock()
		})
		defer stop()
		mu.Lock()
		defer mu.Unlock()
		for emitted < target && !stopped && ctx.Err() == nil {
			progress.Wait()
		}
		return emitted >= target
	}

	ch := rt.inCh[n.id]
	var dispatched, barrierSeen int64
	func() {
		defer close(jobs)
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				rt.release(m)
				if !ok {
					return
				}
				if m.barrier > 0 {
					barrierSeen++
					if barrierSeen >= int64(n.inDeg) {
						barrierSeen = 0
						if !waitEmitted(dispatched) || !rt.emitBarrier(ctx, n, m.barrier) {
							return
						}
					}
					continue
				}
				atomic.AddInt64(&stat.RowsIn, int64(m.batch.Len()))
				select {
				case jobs <- job{seq: dispatched, b: m.batch}:
					dispatched++
				case <-ctx.Done():
					record.Put(m.batch)
					return
				}
			}
		}
	}()
	<-collectorDone
}

// finishParallelBatch applies the ordinary single-input post-processing to
// one result, in order: error routing, recycling and emission. It returns
// false once the run must stop.
func (rt *runtime) finishParallelBatch(ctx context.Context, n *pnode, stat *NodeStat, in, out *record.RecordBatch, err error) bool {
	if err != nil {
		handled := rt.emitError(ctx, n, in)
		if out != nil && out != in {
			record.Put(out)
		}
		record.Put(in)
		if !handled {
			rt.fail(fmt.Errorf("transform %q: %w", n.id, err))
			return false
		}
		return true
	}
	if out != in {
		record.Put(in)
	}
	if out == nil {
		return true
	}
	if out.Len() == 0 {
		record.Put(out)
		return true
	}
	atomic.AddInt64(&stat.RowsOut, int64(out.Len()))
	return rt.emit(ctx, n, out)
}
