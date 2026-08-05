package workqueue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// livePGQueue builds a PostgresQueue against DP_TEST_PG_DSN with a unique
// pipeline ID per run (the queue table is shared), skipping when unset.
func livePGQueue(t *testing.T) (*PostgresQueue, func()) {
	t.Helper()
	dsn := os.Getenv("DP_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set DP_TEST_PG_DSN to run the live Postgres work-queue test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	pipelineID := fmt.Sprintf("wq-test-%d", time.Now().UnixNano())
	q, err := NewPostgresQueue(db, pipelineID)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewPostgresQueue: %v", err)
	}
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM flogo_pipeline_queue WHERE pipeline_id=$1`, pipelineID)
		_ = q.Close()
	}
	return q, cleanup
}

func TestPostgresQueue_Lifecycle(t *testing.T) {
	q, cleanup := livePGQueue(t)
	defer cleanup()
	ctx := context.Background()

	parts := []source.Partition{{ID: "p1"}, {ID: "p2"}, {ID: "p3"}}
	if err := q.Enqueue(ctx, parts, 2); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Idempotent re-enqueue (ON CONFLICT DO NOTHING).
	if err := q.Enqueue(ctx, parts, 2); err != nil {
		t.Fatalf("Enqueue idempotent: %v", err)
	}
	if stats, _ := q.Stats(ctx); stats[StatusPending] != 3 {
		t.Fatalf("pending = %d, want 3", stats[StatusPending])
	}

	// Claim → Heartbeat → Complete the first item.
	item, err := q.Claim(ctx, "w1", time.Minute)
	if err != nil || item == nil {
		t.Fatalf("Claim = %v, %v", item, err)
	}
	if err := q.Heartbeat(ctx, item); err != nil {
		t.Errorf("Heartbeat: %v", err)
	}
	if err := q.Complete(ctx, item); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Claim + Fail twice → the item transitions PENDING → FAILED (maxRetries 2).
	itemF, _ := q.Claim(ctx, "w1", time.Minute)
	if err := q.Fail(ctx, itemF, "boom"); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	itemF2, _ := q.Claim(ctx, "w1", time.Minute)
	if err := q.Fail(ctx, itemF2, "boom again"); err != nil {
		t.Fatalf("Fail 2: %v", err)
	}

	// Drain the last item.
	if last, _ := q.Claim(ctx, "w1", time.Minute); last != nil {
		_ = q.Complete(ctx, last)
	}

	done, err := q.AllComplete(ctx)
	if err != nil {
		t.Fatalf("AllComplete: %v", err)
	}
	if !done {
		st, _ := q.Stats(ctx)
		t.Errorf("AllComplete = false, stats=%v", st)
	}
}

func TestPostgresQueue_EnqueueWithLock(t *testing.T) {
	q, cleanup := livePGQueue(t)
	defer cleanup()
	ctx := context.Background()
	if err := q.EnqueueWithLock(ctx, []source.Partition{{ID: "a"}, {ID: "b"}}, 3); err != nil {
		t.Fatalf("EnqueueWithLock: %v", err)
	}
	if stats, _ := q.Stats(ctx); stats[StatusPending] != 2 {
		t.Errorf("pending after EnqueueWithLock = %d, want 2", stats[StatusPending])
	}
}
