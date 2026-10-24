package graphexec

import (
	"context"
	"sync"
)

// MemoryBudget bounds the estimated bytes of batches that sources have read but
// no consumer has taken yet (performance.maxMemoryMB). A source waits for room
// before each read; a batch's bytes are released when its first consumer
// receives it. A single oversized batch still passes when nothing is buffered.
type MemoryBudget struct {
	limit int64
	mu    sync.Mutex
	cond  *sync.Cond
	used  int64
}

// NewMemoryBudget returns a budget of limit bytes; limit <= 0 disables it.
func NewMemoryBudget(limit int64) *MemoryBudget {
	b := &MemoryBudget{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *MemoryBudget) enabled() bool { return b != nil && b.limit > 0 }

// WaitForRoom blocks while the budget is full; false when ctx ends first.
func (b *MemoryBudget) WaitForRoom(ctx context.Context) bool {
	if !b.enabled() {
		return ctx.Err() == nil
	}
	stop := context.AfterFunc(ctx, func() {
		b.mu.Lock()
		b.cond.Broadcast()
		b.mu.Unlock()
	})
	defer stop()
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.used >= b.limit && ctx.Err() == nil {
		b.cond.Wait()
	}
	return ctx.Err() == nil
}

// Acquire charges n bytes.
func (b *MemoryBudget) Acquire(n int64) {
	if !b.enabled() || n <= 0 {
		return
	}
	b.mu.Lock()
	b.used += n
	b.mu.Unlock()
}

// Release returns n bytes and wakes waiting sources.
func (b *MemoryBudget) Release(n int64) {
	if !b.enabled() || n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
	b.cond.Broadcast()
}

// Used reports the bytes currently charged (for tests and diagnostics).
func (b *MemoryBudget) Used() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
