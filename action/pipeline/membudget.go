package pipeline

import (
	"context"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// memBudget bounds the estimated bytes of batches buffered between the source
// reader and the transform/write stage (performance.maxMemoryMB). The reader
// blocks on a condition variable — not a sleep/poll loop — until enough
// buffered batches are released, and wakes immediately on cancellation.
type memBudget struct {
	limit int64 // <= 0 disables the budget

	mu   sync.Mutex
	cond *sync.Cond
	used int64
}

func newMemBudget(limit int64) *memBudget {
	b := &memBudget{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// waitForRoom blocks while the buffer already holds at least the limit (so a
// single oversized batch can still pass when the buffer is empty — the
// historical semantics). It returns false if ctx is cancelled first.
func (b *memBudget) waitForRoom(ctx context.Context) bool {
	if b.limit <= 0 {
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

func (b *memBudget) acquire(n int64) {
	if b.limit <= 0 || n == 0 {
		return
	}
	b.mu.Lock()
	b.used += n
	b.mu.Unlock()
}

func (b *memBudget) release(n int64) {
	if b.limit <= 0 || n == 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	b.mu.Unlock()
	b.cond.Broadcast()
}

// batchBytes estimates the heap held by a batch's cell values. It sizes a
// bounded sample of rows by their actual dynamic types (strings by length,
// nested maps/slices by element count) and extrapolates, so wide text/JSON
// rows are no longer under-counted by a flat per-cell constant. Callers
// compute it once per batch and carry the value with it, so in-place
// transforms cannot make acquire/release disagree.
func batchBytes(b *record.RecordBatch) int64 {
	if b == nil || len(b.Rows) == 0 {
		return 0
	}
	const sampleRows = 32
	n := len(b.Rows)
	step := 1
	if n > sampleRows {
		step = n / sampleRows
	}
	var sampled, bytes int64
	for i := 0; i < n; i += step {
		bytes += rowBytes(b.Rows[i])
		sampled++
	}
	return bytes * int64(n) / sampled
}

// Per-value overhead: a 16-byte interface slot plus the boxed payload.
const ifaceBytes = 16

func rowBytes(row []interface{}) int64 {
	sz := int64(24) // slice header
	for _, v := range row {
		sz += valueBytes(v, 0)
	}
	return sz
}

func valueBytes(v interface{}, depth int) int64 {
	switch t := v.(type) {
	case nil:
		return ifaceBytes
	case string:
		return ifaceBytes + 16 + int64(len(t))
	case []byte:
		return ifaceBytes + 24 + int64(len(t))
	case bool, int8, uint8:
		return ifaceBytes + 1
	case int, int64, uint64, float64, uint, int32, uint32, float32, int16, uint16:
		return ifaceBytes + 8
	case map[string]interface{}:
		sz := int64(ifaceBytes + 48)
		if depth < 4 {
			for k, e := range t {
				sz += 16 + int64(len(k)) + valueBytes(e, depth+1)
			}
		} else {
			sz += int64(len(t)) * 64
		}
		return sz
	case []interface{}:
		sz := int64(ifaceBytes + 24)
		if depth < 4 {
			for _, e := range t {
				sz += valueBytes(e, depth+1)
			}
		} else {
			sz += int64(len(t)) * 32
		}
		return sz
	default:
		return ifaceBytes + 32 // time.Time, Decimal and other small structs
	}
}
