package pipeline

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// writeStage runs sink writes on a dedicated goroutine so the linear engine
// transforms batch N+1 while batch N is being written. A single writer keeps
// sink calls strictly sequential and in submission order, so sinks need no
// extra synchronisation. sync() is a barrier: it returns only once every
// previously submitted batch has been written, which the engine calls before
// any sink Flush / checkpoint and before the drain returns, preserving the
// "everything before the checkpoint is durable" contract.
type writeStage struct {
	ch    chan writeReq
	done  chan struct{}
	write func(b *record.RecordBatch, out *Output) (int, error)

	failedFlag atomic.Bool // set once err is recorded; lets submit fail fast
	discard    atomic.Bool // drop queued batches instead of writing them

	// Owned by the writer goroutine; read by the engine only after a barrier
	// (channel synchronisation provides the happens-before edge).
	err     error
	written int64
	failed  int64
}

type writeReq struct {
	batch   *record.RecordBatch
	barrier chan struct{}
}

// newWriteStage starts the writer. write performs one batch write and records
// row failures in the supplied Output (only RowsFailed is touched). depth is
// the number of transformed batches that may wait for the writer.
func newWriteStage(depth int, write func(*record.RecordBatch, *Output) (int, error)) *writeStage {
	if depth < 1 {
		depth = 1
	}
	s := &writeStage{ch: make(chan writeReq, depth), done: make(chan struct{}), write: write}
	go s.run()
	return s
}

func (s *writeStage) run() {
	defer close(s.done)
	for req := range s.ch {
		if req.barrier != nil {
			close(req.barrier)
			continue
		}
		if s.err != nil || s.discard.Load() {
			record.Put(req.batch)
			continue
		}
		var o Output
		n, err := s.safeWrite(req.batch, &o)
		record.Put(req.batch)
		s.written += int64(n)
		s.failed += o.RowsFailed
		if err != nil {
			s.err = err
			s.failedFlag.Store(true)
		}
	}
}

// safeWrite runs one write, converting a panic in the sink/DLQ into an error.
// The engine's own recover only covers the run goroutine, so without this a
// sink panic on the writer goroutine would crash the whole process instead of
// failing the run.
func (s *writeStage) safeWrite(b *record.RecordBatch, o *Output) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			n, err = 0, fmt.Errorf("sink panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return s.write(b, o)
}

// submit queues b for writing; the stage takes ownership of b. It returns
// false once an earlier write has failed — the caller should then sync() to
// collect the error.
func (s *writeStage) submit(b *record.RecordBatch) bool {
	if s.failedFlag.Load() {
		record.Put(b)
		return false
	}
	s.ch <- writeReq{batch: b}
	return true
}

// sync waits for all submitted batches to be written and returns the rows
// written / failed since the previous sync, plus the first write error.
func (s *writeStage) sync() (written, failed int64, err error) {
	b := make(chan struct{})
	s.ch <- writeReq{barrier: b}
	<-b
	written, failed = s.written, s.failed
	s.written, s.failed = 0, 0
	return written, failed, s.err
}

// close stops the writer and returns the rows written / failed since the last
// sync, so an aborting run still reports what was actually delivered. With
// discard=true queued batches are recycled without being written.
func (s *writeStage) close(discard bool) (written, failed int64) {
	if discard {
		s.discard.Store(true)
	}
	close(s.ch)
	<-s.done
	return s.written, s.failed
}
