package workqueue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// TestRedisQueue_Lifecycle exercises the full enqueue → claim → complete cycle
// plus lease reclaim and re-run reset against a live Redis. It is skipped unless
// DP_TEST_REDIS_ADDR is set (e.g. "localhost:56379"), so it never breaks CI.
func TestRedisQueue_Lifecycle(t *testing.T) {
	addr := os.Getenv("DP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set DP_TEST_REDIS_ADDR to run the Redis work-queue test")
	}
	ctx := context.Background()
	// Unique namespace per run so the test is fully isolated from any prior run's
	// leftover state (a persistent Redis in CI is shared across runs).
	q, err := NewRedisQueue(addr, fmt.Sprintf("flogo:test:wq:%d", time.Now().UnixNano()), "", "redis-qtest")
	if err != nil {
		t.Fatalf("NewRedisQueue: %v", err)
	}
	defer q.Close()
	// Clean slate on entry, and full teardown on exit (including the enqueue-lock,
	// which is not one of the three data keys).
	cleanup := func() {
		q.client.Del(ctx, q.kItems(), q.kPending(), q.kInProgress(), q.ns+":enqueue-lock")
	}
	cleanup()
	defer cleanup()

	parts := []source.Partition{{ID: "p0"}, {ID: "p1"}, {ID: "p2"}}
	if err := q.Enqueue(ctx, parts, 3); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Claim and complete all three.
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		item, err := q.Claim(ctx, "w1", 30*time.Second)
		if err != nil {
			t.Fatalf("Claim %d: %v", i, err)
		}
		if item == nil {
			t.Fatalf("Claim %d returned nil with work remaining", i)
		}
		seen[item.Partition.ID] = true
		if err := q.Complete(ctx, item); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct partitions, got %v", seen)
	}

	// Now the queue must report complete and Claim returns nil,nil.
	done, err := q.AllComplete(ctx)
	if err != nil || !done {
		t.Fatalf("AllComplete = %v,%v want true,nil", done, err)
	}
	if item, _ := q.Claim(ctx, "w1", time.Second); item != nil {
		t.Fatalf("Claim after completion returned %v, want nil", item)
	}

	// Re-run: Enqueue again must clear terminal items and reseed.
	if err := q.Enqueue(ctx, parts, 3); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}
	if item, err := q.Claim(ctx, "w1", 30*time.Second); err != nil || item == nil {
		t.Fatalf("Claim after re-enqueue = %v,%v want a partition", item, err)
	}
}

// TestRedisQueue_LeaseReclaim verifies an expired lease is re-queued.
func TestRedisQueue_LeaseReclaim(t *testing.T) {
	addr := os.Getenv("DP_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set DP_TEST_REDIS_ADDR to run the Redis work-queue test")
	}
	ctx := context.Background()
	q, err := NewRedisQueue(addr, fmt.Sprintf("flogo:test:wq2:%d", time.Now().UnixNano()), "", "redis-lease")
	if err != nil {
		t.Fatalf("NewRedisQueue: %v", err)
	}
	defer q.Close()
	cleanup := func() {
		q.client.Del(ctx, q.kItems(), q.kPending(), q.kInProgress(), q.ns+":enqueue-lock")
	}
	cleanup()
	defer cleanup()

	if err := q.Enqueue(ctx, []source.Partition{{ID: "p0"}}, 3); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Claim with a tiny lease and DON'T complete — simulating a dead worker.
	item, err := q.Claim(ctx, "dead", 300*time.Millisecond)
	if err != nil || item == nil {
		t.Fatalf("Claim: %v,%v", item, err)
	}
	time.Sleep(500 * time.Millisecond)
	// A fresh worker must reclaim the expired partition.
	item2, err := q.Claim(ctx, "live", 30*time.Second)
	if err != nil || item2 == nil {
		t.Fatalf("reclaim Claim = %v,%v want the partition", item2, err)
	}
	if item2.Partition.ID != "p0" {
		t.Fatalf("reclaimed %q, want p0", item2.Partition.ID)
	}
}
