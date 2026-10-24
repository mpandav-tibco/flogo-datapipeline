package workqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func parts(ids ...string) []source.Partition {
	ps := make([]source.Partition, len(ids))
	for i, id := range ids {
		ps[i] = source.Partition{ID: id}
	}
	return ps
}

// A requeued item is claimed before later, never-claimed items (earliest
// PENDING in insertion order), exactly as the scan-based queue behaved.
func TestMemoryQueue_RequeuedClaimedInInsertionOrder(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue()
	_ = q.Enqueue(ctx, parts("a", "b", "c", "d", "e"), 3)
	claim := func() string {
		it, err := q.Claim(ctx, "w", time.Hour)
		if err != nil || it == nil {
			t.Fatalf("claim: %v %v", it, err)
		}
		return it.ID
	}
	a, b, c := claim(), claim(), claim()
	_ = q.Fail(ctx, &WorkItem{ID: c}, "x") // c back to pending
	_ = q.Fail(ctx, &WorkItem{ID: b}, "x") // b back to pending
	if got := claim(); got != "b" {
		t.Fatalf("claimed %s; want b (earliest pending)", got)
	}
	if got := claim(); got != "c" {
		t.Fatalf("claimed %s; want c", got)
	}
	_ = q.Complete(ctx, &WorkItem{ID: a})
	if got := claim(); got != "d" {
		t.Fatalf("claimed %s; want d", got)
	}
	st, _ := q.Stats(ctx)
	if st[StatusDone] != 1 || st[StatusInProgress] != 3 || st[StatusPending] != 1 {
		t.Fatalf("stats=%v", st)
	}
}

func TestMemoryQueue_ScalesToManyPartitions(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue()
	const n = 50000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("p%06d", i)
	}
	_ = q.Enqueue(ctx, parts(ids...), 1)
	start := time.Now()
	for i := 0; i < n; i++ {
		it, err := q.Claim(ctx, "w", time.Hour)
		if err != nil || it == nil || it.ID != ids[i] {
			t.Fatalf("claim %d: %v %v", i, it, err)
		}
		_ = q.Complete(ctx, it)
	}
	if it, _ := q.Claim(ctx, "w", time.Hour); it != nil {
		t.Fatalf("expected nil after all done, got %v", it.ID)
	}
	if done, _ := q.AllComplete(ctx); !done {
		t.Fatal("AllComplete=false")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("50k claim/complete took %v; claim must not scan all partitions", d)
	}
}
