package scaling

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/partition/workqueue"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// ─── ShardedPartitions ────────────────────────────────────────────────────────

func allPartitions(n int) []source.Partition {
	parts := make([]source.Partition, n)
	for i := range parts {
		parts[i] = source.Partition{ID: string(rune('A' + i))}
	}
	return parts
}

func TestShardedPartitions_SingleInstance(t *testing.T) {
	all := allPartitions(5) // A B C D E
	got := ShardedPartitions(all, 0, 1)
	if len(got) != 5 {
		t.Errorf("instanceCount=1 should return all 5 partitions, got %d", len(got))
	}
}

func TestShardedPartitions_ThreeInstances(t *testing.T) {
	all := allPartitions(9) // indices 0..8
	// Instance 0 owns: 0, 3, 6
	// Instance 1 owns: 1, 4, 7
	// Instance 2 owns: 2, 5, 8
	got0 := ShardedPartitions(all, 0, 3)
	got1 := ShardedPartitions(all, 1, 3)
	got2 := ShardedPartitions(all, 2, 3)

	if len(got0) != 3 {
		t.Errorf("instance 0: got %d partitions, want 3", len(got0))
	}
	if len(got1) != 3 {
		t.Errorf("instance 1: got %d partitions, want 3", len(got1))
	}
	if len(got2) != 3 {
		t.Errorf("instance 2: got %d partitions, want 3", len(got2))
	}

	// Verify no overlap and full coverage.
	seen := make(map[string]bool)
	for _, p := range append(append(got0, got1...), got2...) {
		if seen[p.ID] {
			t.Errorf("partition %q assigned to multiple instances", p.ID)
		}
		seen[p.ID] = true
	}
	if len(seen) != 9 {
		t.Errorf("total coverage = %d, want 9", len(seen))
	}
}

func TestShardedPartitions_InstanceCountZero(t *testing.T) {
	all := allPartitions(3)
	// instanceCount <= 1 returns everything.
	got := ShardedPartitions(all, 0, 0)
	if len(got) != 3 {
		t.Errorf("instanceCount=0 should return all 3 partitions, got %d", len(got))
	}
}

// ─── Coordinator Enqueue + ClaimLoop ─────────────────────────────────────────

func newTestCoordinator() *Coordinator {
	cfg := Config{MaxRetries: 3}
	q := workqueue.NewMemoryQueue()
	return NewCoordinator(cfg, q)
}

func makePartitions(ids ...string) []source.Partition {
	parts := make([]source.Partition, len(ids))
	for i, id := range ids {
		parts[i] = source.Partition{ID: id}
	}
	return parts
}

func TestCoordinator_Enqueue_And_ClaimLoop_AllSuccess(t *testing.T) {
	coord := newTestCoordinator()
	ctx := context.Background()

	parts := makePartitions("p1", "p2", "p3")
	if err := coord.Enqueue(ctx, parts); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	processed := make(map[string]bool)
	err := coord.ClaimLoop(ctx, func(_ context.Context, p source.Partition) error {
		processed[p.ID] = true
		return nil
	})
	if err != nil {
		t.Fatalf("ClaimLoop: %v", err)
	}

	for _, id := range []string{"p1", "p2", "p3"} {
		if !processed[id] {
			t.Errorf("partition %q was not processed", id)
		}
	}
}

func TestCoordinator_ClaimLoop_ProcessError_RoutesToFail(t *testing.T) {
	// Use a queue with maxRetries=1 so after one error it goes to FAILED.
	cfg := Config{MaxRetries: 1}
	q := workqueue.NewMemoryQueue()
	coord := NewCoordinator(cfg, q)

	ctx := context.Background()
	parts := makePartitions("bad-partition")
	if err := coord.Enqueue(ctx, parts); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	callCount := 0
	err := coord.ClaimLoop(ctx, func(_ context.Context, p source.Partition) error {
		callCount++
		return errors.New("simulated processing error")
	})
	// ClaimLoop itself does not return an error for process failures;
	// it routes via Fail. After exhausting retries the queue is complete.
	if err != nil {
		t.Fatalf("ClaimLoop returned unexpected error: %v", err)
	}

	// The partition should have been processed exactly once (maxRetries=1 means
	// 0 re-tries after the first fail: retryCount(1) >= maxRetries(1) → FAILED).
	if callCount != 1 {
		t.Errorf("processFunc called %d times, want 1 (no retries)", callCount)
	}

	stats, err := coord.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[workqueue.StatusFailed] != 1 {
		t.Errorf("failed = %d, want 1", stats[workqueue.StatusFailed])
	}
}

// TestCoordinator_ClaimLoop_PanicDoesNotLeakHeartbeat verifies that a panic in
// processFunc still stops the per-item heartbeat goroutine (cancelHB runs via a
// scoped defer) instead of leaking it for the life of the pipeline context.
func TestCoordinator_ClaimLoop_PanicDoesNotLeakHeartbeat(t *testing.T) {
	coord := newTestCoordinator()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coord.Enqueue(ctx, makePartitions("boom")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Baseline after letting startup goroutines settle.
	settle := func() int {
		var n int
		for i := 0; i < 40; i++ {
			runtime.GC()
			n = runtime.NumGoroutine()
			time.Sleep(5 * time.Millisecond)
		}
		return n
	}
	before := settle()

	func() {
		// The processFunc panic must propagate out of ClaimLoop.
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected the processFunc panic to propagate out of ClaimLoop")
			}
		}()
		_ = coord.ClaimLoop(ctx, func(_ context.Context, _ source.Partition) error {
			panic("boom in processFunc")
		})
	}()

	// With the fix, cancelHB runs on the panic path, so the heartbeat goroutine
	// exits and the count settles back to the baseline. Without it, the goroutine
	// blocks on the heartbeat ticker / ctx until the pipeline context is
	// cancelled, so the count stays elevated.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("heartbeat goroutine leaked after processFunc panic: before=%d after=%d", before, got)
	}
}

func TestCoordinator_Enqueue_IsIdempotent(t *testing.T) {
	coord := newTestCoordinator()
	ctx := context.Background()
	parts := makePartitions("dup1", "dup2")

	if err := coord.Enqueue(ctx, parts); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Second enqueue — same IDs, should be a no-op.
	if err := coord.Enqueue(ctx, parts); err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}

	stats, err := coord.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[workqueue.StatusPending] != 2 {
		t.Errorf("pending = %d after double-enqueue, want 2 (idempotent)", stats[workqueue.StatusPending])
	}
}
