package pipeline

import (
	"context"
	"sync"
	"time"
)

// runTracker records every in-flight pipeline run so process shutdown can
// cancel them and wait for them to unwind (sinks flushed and closed, cleanup
// run) instead of exiting underneath a streaming pipeline mid-write.
type runTracker struct {
	mu      sync.Mutex
	nextID  int
	cancels map[int]context.CancelFunc
	wg      sync.WaitGroup
	closing bool
}

var activeRuns = &runTracker{cancels: map[int]context.CancelFunc{}}

// start registers a run and returns its context (cancelled on shutdown) and a
// done func the run must call when it has fully finished. During shutdown new
// runs get an already-cancelled context.
func (t *runTracker) start(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		cancel()
		return ctx, func() {}
	}
	id := t.nextID
	t.nextID++
	t.cancels[id] = cancel
	t.wg.Add(1)
	return ctx, func() {
		t.mu.Lock()
		delete(t.cancels, id)
		t.mu.Unlock()
		cancel()
		t.wg.Done()
	}
}

// ShutdownRuns cancels every in-flight pipeline run and waits up to timeout
// for them to finish unwinding. It returns how many runs were in flight and
// how many were still running when the timeout expired. Call it on process
// shutdown before stopping the engine; runs started afterwards are cancelled
// immediately. A cancelled run saves no checkpoint, so a restart resumes from
// the last durable checkpoint (at-least-once) rather than mid-batch.
func ShutdownRuns(timeout time.Duration) (inFlight, stillRunning int) {
	t := activeRuns
	t.mu.Lock()
	t.closing = true
	inFlight = len(t.cancels)
	for _, cancel := range t.cancels {
		cancel()
	}
	t.mu.Unlock()

	done := make(chan struct{})
	go func() { t.wg.Wait(); close(done) }()
	select {
	case <-done:
		return inFlight, 0
	case <-time.After(timeout):
		t.mu.Lock()
		defer t.mu.Unlock()
		return inFlight, len(t.cancels)
	}
}

// InFlightRuns reports how many pipeline runs are currently executing.
func InFlightRuns() int {
	activeRuns.mu.Lock()
	defer activeRuns.mu.Unlock()
	return len(activeRuns.cancels)
}
