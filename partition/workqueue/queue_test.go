package workqueue

import (
	"context"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// helper: enqueue a list of partition IDs into q
func enqueueIDs(t *testing.T, q *MemoryQueue, ids []string) {
	t.Helper()
	parts := make([]source.Partition, len(ids))
	for i, id := range ids {
		parts[i] = source.Partition{ID: id}
	}
	if err := q.Enqueue(context.Background(), parts, 3); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// ─── Enqueue idempotency ──────────────────────────────────────────────────────

func TestMemoryQueue_Enqueue_Idempotent(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"p1", "p2"})
	// Enqueue same IDs again — should not create duplicates.
	enqueueIDs(t, q, []string{"p1", "p2"})

	stats, err := q.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[StatusPending] != 2 {
		t.Errorf("pending = %d, want 2 (idempotent enqueue)", stats[StatusPending])
	}
}

// ─── Claim FIFO ordering ──────────────────────────────────────────────────────

func TestMemoryQueue_Claim_FIFOOrdering(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"first", "second", "third"})

	ctx := context.Background()
	wantOrder := []string{"first", "second", "third"}
	for _, want := range wantOrder {
		item, err := q.Claim(ctx, "w1", time.Minute)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if item == nil {
			t.Fatal("Claim returned nil unexpectedly")
		}
		if item.ID != want {
			t.Errorf("claimed %q, want %q (FIFO violated)", item.ID, want)
		}
		if err := q.Complete(ctx, item); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
}

// ─── Lease expiry re-queue ────────────────────────────────────────────────────

func TestMemoryQueue_LeaseExpiry_ReQueue(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"requeue-me"})

	ctx := context.Background()
	// Use a short lease so expiry is fast.
	const lease = 20 * time.Millisecond

	// Claim with a short lease.
	item, err := q.Claim(ctx, "w1", lease)
	if err != nil || item == nil {
		t.Fatalf("initial Claim failed: err=%v item=%v", err, item)
	}
	if item.ID != "requeue-me" {
		t.Fatalf("unexpected item: %v", item.ID)
	}

	// Wait for the lease to expire (3x the lease duration is sufficient).
	time.Sleep(3 * lease)

	// A second Claim using the same lease duration so the expiry check fires.
	item2, err := q.Claim(ctx, "w2", lease)
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if item2 == nil {
		t.Fatal("expected re-claim after lease expiry, got nil")
	}
	if item2.ID != "requeue-me" {
		t.Errorf("re-claimed item ID = %q, want 'requeue-me'", item2.ID)
	}
}

// ─── Fail with retry decrement ────────────────────────────────────────────────

func TestMemoryQueue_Fail_RetryAndFailed(t *testing.T) {
	q := NewMemoryQueue()
	// maxRetries = 2: fails on the 2nd Fail call.
	parts := []source.Partition{{ID: "flaky"}}
	if err := q.Enqueue(context.Background(), parts, 2); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx := context.Background()

	// First attempt → Fail → should re-queue (retryCount=1 < maxRetries=2).
	item, _ := q.Claim(ctx, "w1", time.Minute)
	if err := q.Fail(ctx, item, "first failure"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	stats, _ := q.Stats(ctx)
	if stats[StatusPending] != 1 {
		t.Errorf("after first fail: pending = %d, want 1", stats[StatusPending])
	}

	// Second attempt → Fail → should be FAILED (retryCount=2 >= maxRetries=2).
	item, _ = q.Claim(ctx, "w1", time.Minute)
	if err := q.Fail(ctx, item, "second failure"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	stats, _ = q.Stats(ctx)
	if stats[StatusFailed] != 1 {
		t.Errorf("after second fail: failed = %d, want 1", stats[StatusFailed])
	}
	if stats[StatusPending] != 0 {
		t.Errorf("after second fail: pending = %d, want 0", stats[StatusPending])
	}
}

// ─── AllComplete ──────────────────────────────────────────────────────────────

func TestMemoryQueue_AllComplete(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"a", "b"})

	ctx := context.Background()
	done, err := q.AllComplete(ctx)
	if err != nil {
		t.Fatalf("AllComplete: %v", err)
	}
	if done {
		t.Error("AllComplete should be false before claiming items")
	}

	// Claim and complete both items.
	for i := 0; i < 2; i++ {
		item, _ := q.Claim(ctx, "w1", time.Minute)
		_ = q.Complete(ctx, item)
	}

	done, err = q.AllComplete(ctx)
	if err != nil {
		t.Fatalf("AllComplete after complete: %v", err)
	}
	if !done {
		t.Error("AllComplete should be true after completing all items")
	}
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func TestMemoryQueue_Stats(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"s1", "s2", "s3"})

	ctx := context.Background()
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats[StatusPending] != 3 {
		t.Errorf("pending = %d, want 3", stats[StatusPending])
	}
	if stats[StatusInProgress] != 0 {
		t.Errorf("in_progress = %d, want 0", stats[StatusInProgress])
	}

	// Claim one.
	_, _ = q.Claim(ctx, "w1", time.Minute)
	stats, _ = q.Stats(ctx)
	if stats[StatusInProgress] != 1 {
		t.Errorf("in_progress after claim = %d, want 1", stats[StatusInProgress])
	}
	if stats[StatusPending] != 2 {
		t.Errorf("pending after claim = %d, want 2", stats[StatusPending])
	}
}

// ─── Close ────────────────────────────────────────────────────────────────────

func TestMemoryQueue_Close(t *testing.T) {
	q := NewMemoryQueue()
	if err := q.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

// ─── Claim returns nil when all done ─────────────────────────────────────────

func TestMemoryQueue_Claim_NilWhenAllDone(t *testing.T) {
	q := NewMemoryQueue()
	enqueueIDs(t, q, []string{"only"})

	ctx := context.Background()
	item, _ := q.Claim(ctx, "w1", time.Minute)
	_ = q.Complete(ctx, item)

	// Now queue is fully done — Claim should return (nil, nil).
	item2, err := q.Claim(ctx, "w2", time.Minute)
	if err != nil {
		t.Fatalf("Claim on completed queue: %v", err)
	}
	if item2 != nil {
		t.Errorf("expected nil item on completed queue, got %v", item2.ID)
	}
}
