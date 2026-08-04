package workqueue

import (
	"context"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// TestMemoryQueue_Heartbeat renews the lease timestamp on a claimed item.
func TestMemoryQueue_Heartbeat(t *testing.T) {
	q := NewMemoryQueue()
	if err := q.Enqueue(context.Background(), []source.Partition{{ID: "p1"}}, 3); err != nil {
		t.Fatal(err)
	}
	item, err := q.Claim(context.Background(), "w1", time.Minute)
	if err != nil || item == nil {
		t.Fatalf("Claim = %v, %v", item, err)
	}
	before := item.ClaimedAt
	time.Sleep(2 * time.Millisecond)
	if err := q.Heartbeat(context.Background(), item); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !item.ClaimedAt.After(before) {
		t.Error("Heartbeat did not renew ClaimedAt")
	}
	// Heartbeat on an unknown item is a no-op (no panic, no error).
	if err := q.Heartbeat(context.Background(), &WorkItem{ID: "ghost"}); err != nil {
		t.Errorf("Heartbeat(ghost): %v", err)
	}
}

// TestMemoryQueue_ClaimCancelledContext returns the context error rather than
// blocking when no PENDING item is available and the context is cancelled.
func TestMemoryQueue_ClaimCancelledContext(t *testing.T) {
	q := NewMemoryQueue()
	if err := q.Enqueue(context.Background(), []source.Partition{{ID: "p1"}}, 3); err != nil {
		t.Fatal(err)
	}
	// Claim p1 so it is IN_PROGRESS and no PENDING item remains.
	if _, err := q.Claim(context.Background(), "w1", time.Minute); err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Claim(ctx, "w2", time.Minute); err == nil {
		t.Fatal("Claim with cancelled context expected an error")
	}
}
