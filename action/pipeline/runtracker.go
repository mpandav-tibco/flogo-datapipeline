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
	drains  map[int]chan struct{}
	wg      sync.WaitGroup
	closing bool
}

var activeRuns = &runTracker{cancels: map[int]context.CancelFunc{}, drains: map[int]chan struct{}{}}

type drainKey struct{}

// drainSignal returns the channel closed when shutdown asks the run in ctx to
// stop reading and finish cleanly (nil when the run is not tracked).
func drainSignal(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(drainKey{}).(chan struct{})
	return ch
}

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
	drain := make(chan struct{})
	t.drains[id] = drain
	ctx = context.WithValue(ctx, drainKey{}, drain)
	t.wg.Add(1)
	return ctx, func() {
		t.mu.Lock()
		delete(t.cancels, id)
		delete(t.drains, id)
		t.mu.Unlock()
		cancel()
		t.wg.Done()
	}
}

// ShutdownRuns stops every in-flight pipeline run and waits up to timeout
// for them to finish. It returns how many runs were in flight and how many
// were still running when the timeout expired. Call it on process shutdown
// before stopping the engine; runs started afterwards are cancelled
// immediately.
//
// Runs are first asked to drain: their sources stop reading and report end of
// input, so everything already read is written, the sinks are flushed and the
// run ends with its normal final checkpoint — a restart resumes exactly where
// it stopped, without replaying (duplicating) the last checkpoint interval.
// Runs that have not finished after 3/4 of the timeout (or that cannot drain,
// e.g. stateful windowed plans, which resume from their checkpointed state)
// are cancelled; a cancelled run saves no checkpoint, so a restart resumes
// from the last durable one (at-least-once).
func ShutdownRuns(timeout time.Duration) (inFlight, stillRunning int) {
	t := activeRuns
	t.mu.Lock()
	t.closing = true
	inFlight = len(t.cancels)
	for _, ch := range t.drains {
		close(ch)
	}
	t.drains = map[int]chan struct{}{}
	t.mu.Unlock()

	done := make(chan struct{})
	go func() { t.wg.Wait(); close(done) }()
	deadline := time.After(timeout)
	select {
	case <-done:
		return inFlight, 0
	case <-time.After(timeout * 3 / 4):
	}
	t.mu.Lock()
	for _, cancel := range t.cancels {
		cancel()
	}
	t.mu.Unlock()
	select {
	case <-done:
		return inFlight, 0
	case <-deadline:
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
