package podlog

import (
	"context"
	"sync"
)

// Memory bounds between the tailer and the pipeline. Record counts alone do
// not bound memory: an application that logs multi-MB payloads (common for
// BW "log the message" activities) would otherwise let 1,024 queued records
// plus a 1,000-record batch take gigabytes and OOM-kill the collector — which
// then replays the same lines and crash-loops.
const (
	inflightBudgetBytes = 16 << 20 // message bytes queued between tailer and ReadBatch
	maxBatchBytes       = 8 << 20  // message bytes per ReadBatch (at least one record)
)

// byteBudget is a counting semaphore over bytes. A single record larger than
// the whole budget is admitted when nothing else is queued, so it can never
// deadlock.
type byteBudget struct {
	mu    sync.Mutex
	limit int
	used  int
	freed chan struct{} // closed and replaced on every release
}

func newByteBudget(limit int) *byteBudget {
	return &byteBudget{limit: limit, freed: make(chan struct{})}
}

// acquire blocks until n bytes fit (or ctx is done: false).
func (b *byteBudget) acquire(ctx context.Context, n int) bool {
	if b == nil {
		return true
	}
	for {
		b.mu.Lock()
		if b.used == 0 || b.used+n <= b.limit {
			b.used += n
			b.mu.Unlock()
			return true
		}
		freed := b.freed
		b.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return false
		}
	}
}

func (b *byteBudget) release(n int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
	close(b.freed)
	b.freed = make(chan struct{})
	b.mu.Unlock()
}
