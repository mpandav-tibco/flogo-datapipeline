// Package workqueue defines the WorkQueue interface and provides three
// implementations for multi-instance pipeline coordination:
//   - MemoryQueue:   in-process, single-instance (dev/test)
//   - RedisQueue:    distributed via RPOPLPUSH (atomic claim, TTL lease)
//   - PostgresQueue: distributed via SELECT FOR UPDATE SKIP LOCKED
//
// The WorkQueue is the coordination layer for the distributed scaling mode.
// A coordinator (the first instance to acquire leader election) enumerates
// partitions and enqueues them as WorkItems. Workers compete to Claim items,
// process them, and call Complete or Fail. If a worker dies mid-partition,
// the lease expires and the item is automatically re-queued.
package workqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// Status values for a WorkItem.
const (
	StatusPending    = "PENDING"
	StatusInProgress = "IN_PROGRESS"
	StatusDone       = "DONE"
	StatusFailed     = "FAILED"
)

// postgresQueueTable is the fixed name of the Postgres work-queue table.
// Declared as a package-level constant (not a configurable field) to prevent
// any future refactor from accidentally opening a SQL-injection path by
// interpolating a caller-supplied identifier into raw SQL strings.
const postgresQueueTable = "flogo_pipeline_queue"

// WorkItem represents a single unit of pipeline work — typically one partition.
type WorkItem struct {
	ID          string           `json:"id"`
	Partition   source.Partition `json:"partition"`
	Status      string           `json:"status"`
	WorkerID    string           `json:"workerId,omitempty"`
	RetryCount  int              `json:"retryCount"`
	MaxRetries  int              `json:"maxRetries"`
	ClaimedAt   time.Time        `json:"claimedAt,omitempty"`
	CompletedAt time.Time        `json:"completedAt,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// WorkQueue manages the lifecycle of WorkItems for distributed pipeline execution.
type WorkQueue interface {
	// Enqueue adds a batch of partitions to the queue as PENDING items.
	// Idempotent: if an item with the same partition ID already exists, it is skipped.
	Enqueue(ctx context.Context, partitions []source.Partition, maxRetries int) error

	// Claim atomically moves a PENDING item to IN_PROGRESS and returns it.
	// Blocks until an item is available or ctx is cancelled.
	// Returns (nil, nil) when all items are DONE or FAILED.
	Claim(ctx context.Context, workerID string, leaseDuration time.Duration) (*WorkItem, error)

	// Heartbeat renews the lease on a claimed item, preventing it from expiring.
	// Must be called at least once per leaseDuration/2 while processing.
	Heartbeat(ctx context.Context, item *WorkItem) error

	// Complete marks a WorkItem as DONE.
	Complete(ctx context.Context, item *WorkItem) error

	// Fail marks a WorkItem as FAILED or returns it to PENDING if retries remain.
	Fail(ctx context.Context, item *WorkItem, reason string) error

	// AllComplete returns true when all items in the queue are DONE or FAILED.
	AllComplete(ctx context.Context) (bool, error)

	// Stats returns counts by status for observability.
	Stats(ctx context.Context) (map[string]int, error)

	// Close releases queue resources.
	Close() error
}

// ─── Memory Queue (single-instance, dev/test) ────────────────────────────────

// MemoryQueue is an in-process work queue for development and single-instance
// pipelines. Not suitable for distributed use — state is lost on restart.
//
// items has no eviction policy — entries are only ever added (by Enqueue),
// never removed, even after Complete/Fail. This is safe in practice: a fresh
// MemoryQueue is constructed per pipeline run (see scaling.BuildQueue) and
// scoped to that run's own partition count, so it is bounded by one run's
// partition count and reclaimed by the GC once the run finishes — it is not
// a long-lived singleton that accumulates across runs. Genuinely long-lived,
// unbounded-scale distributed coordination should use RedisQueue or
// PostgresQueue instead (both have lease/TTL-based cleanup), which is what
// production deployments are documented to use (see docs/scaling.md).
type MemoryQueue struct {
	mu    sync.Mutex
	items map[string]*WorkItem
	order []string // insertion order for FIFO claim
	cond  *sync.Cond
}

// NewMemoryQueue creates a new in-process work queue.
func NewMemoryQueue() *MemoryQueue {
	q := &MemoryQueue{
		items: make(map[string]*WorkItem),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *MemoryQueue) Enqueue(_ context.Context, partitions []source.Partition, maxRetries int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, p := range partitions {
		if _, ok := q.items[p.ID]; ok {
			continue // idempotent
		}
		item := &WorkItem{
			ID:         p.ID,
			Partition:  p,
			Status:     StatusPending,
			MaxRetries: maxRetries,
		}
		q.items[p.ID] = item
		q.order = append(q.order, p.ID)
	}
	q.cond.Broadcast()
	return nil
}

func (q *MemoryQueue) Claim(ctx context.Context, workerID string, lease time.Duration) (*WorkItem, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Expire stale leases.
		now := time.Now()
		for _, id := range q.order {
			item := q.items[id]
			if item.Status == StatusInProgress && !item.ClaimedAt.IsZero() &&
				now.Sub(item.ClaimedAt) > lease {
				item.Status = StatusPending
				item.WorkerID = ""
			}
		}
		// Claim first PENDING item.
		for _, id := range q.order {
			item := q.items[id]
			if item.Status == StatusPending {
				item.Status = StatusInProgress
				item.WorkerID = workerID
				item.ClaimedAt = now
				return item, nil
			}
		}
		// Check if all done.
		done, _ := q.allComplete()
		if done {
			return nil, nil
		}
		// Wait for a new item or lease expiry. Use an explicit timer (not
		// time.After) and Stop it on the common early-wake path (waitDone fires
		// first), so the runtime timer is released immediately instead of
		// sitting in the timer heap for up to lease/4 on every wait iteration.
		waitDone := make(chan struct{})
		timer := time.NewTimer(lease / 4)
		go func() {
			select {
			case <-ctx.Done():
				q.cond.Broadcast()
			case <-timer.C:
				q.cond.Broadcast()
			case <-waitDone:
			}
		}()
		q.cond.Wait()
		close(waitDone)
		timer.Stop()
	}
}

func (q *MemoryQueue) Heartbeat(_ context.Context, item *WorkItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if stored, ok := q.items[item.ID]; ok && stored.Status == StatusInProgress {
		stored.ClaimedAt = time.Now()
	}
	return nil
}

func (q *MemoryQueue) Complete(_ context.Context, item *WorkItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if stored, ok := q.items[item.ID]; ok {
		stored.Status = StatusDone
		stored.CompletedAt = time.Now()
	}
	q.cond.Broadcast()
	return nil
}

func (q *MemoryQueue) Fail(_ context.Context, item *WorkItem, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if stored, ok := q.items[item.ID]; ok {
		stored.RetryCount++
		stored.Error = reason
		if stored.RetryCount >= stored.MaxRetries {
			stored.Status = StatusFailed
		} else {
			stored.Status = StatusPending
			stored.WorkerID = ""
		}
	}
	q.cond.Broadcast()
	return nil
}

func (q *MemoryQueue) AllComplete(_ context.Context) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	done, _ := q.allComplete()
	return done, nil
}

func (q *MemoryQueue) allComplete() (bool, error) {
	for _, item := range q.items {
		if item.Status == StatusPending || item.Status == StatusInProgress {
			return false, nil
		}
	}
	return true, nil
}

func (q *MemoryQueue) Stats(_ context.Context) (map[string]int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	counts := map[string]int{StatusPending: 0, StatusInProgress: 0, StatusDone: 0, StatusFailed: 0}
	for _, item := range q.items {
		counts[item.Status]++
	}
	return counts, nil
}

func (q *MemoryQueue) Close() error { return nil }

// ─── PostgreSQL Queue (SELECT FOR UPDATE SKIP LOCKED) ────────────────────────

// PostgresQueue uses Postgres advisory locking (SELECT FOR UPDATE SKIP LOCKED)
// for atomic partition claiming. Ideal when the pipeline sink is also Postgres —
// claim and write happen in the same database, enabling exactly-once semantics
// via a wrapping transaction.
type PostgresQueue struct {
	db         *sql.DB
	pipelineID string
}

// NewPostgresQueue creates and initializes the work queue table if it does not exist.
func NewPostgresQueue(db *sql.DB, pipelineID string) (*PostgresQueue, error) {
	q := &PostgresQueue{db: db, pipelineID: pipelineID}
	if err := q.ensureTable(); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *PostgresQueue) ensureTable() error {
	_, err := q.db.Exec(`
		CREATE TABLE IF NOT EXISTS ` + postgresQueueTable + ` (
			id            TEXT NOT NULL,
			pipeline_id   TEXT NOT NULL,
			partition     JSONB NOT NULL,
			status        TEXT NOT NULL DEFAULT 'PENDING',
			worker_id     TEXT,
			retry_count   INT NOT NULL DEFAULT 0,
			max_retries   INT NOT NULL DEFAULT 3,
			claimed_at    TIMESTAMPTZ,
			completed_at  TIMESTAMPTZ,
			error         TEXT,
			PRIMARY KEY (pipeline_id, id)
		)
	`)
	return err
}

func (q *PostgresQueue) Enqueue(ctx context.Context, partitions []source.Partition, maxRetries int) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, p := range partitions {
		data, err := json.Marshal(p)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO `+postgresQueueTable+` (id, pipeline_id, partition, max_retries)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (pipeline_id, id) DO NOTHING`,
			p.ID, q.pipelineID, data, maxRetries,
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (q *PostgresQueue) Claim(ctx context.Context, workerID string, lease time.Duration) (*WorkItem, error) {
	for {
		item, err := q.tryClaim(ctx, workerID, lease)
		if err != nil {
			return nil, err
		}
		if item != nil {
			return item, nil
		}
		done, err := q.AllComplete(ctx)
		if err != nil {
			return nil, err
		}
		if done {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (q *PostgresQueue) tryClaim(ctx context.Context, workerID string, lease time.Duration) (*WorkItem, error) {
	// Re-queue expired leases first.
	_, _ = q.db.ExecContext(ctx, `
		UPDATE `+postgresQueueTable+` SET status='PENDING', worker_id=NULL
		WHERE pipeline_id=$1 AND status='IN_PROGRESS'
		  AND claimed_at < NOW() - $2::interval`,
		q.pipelineID, fmt.Sprintf("%.0f seconds", lease.Seconds()),
	)

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT id, partition, retry_count, max_retries
		FROM `+postgresQueueTable+`
		WHERE pipeline_id=$1 AND status='PENDING'
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		q.pipelineID,
	)

	var id string
	var partitionJSON []byte
	var retryCount, maxRetries int
	if err := row.Scan(&id, &partitionJSON, &retryCount, &maxRetries); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var p source.Partition
	if err := json.Unmarshal(partitionJSON, &p); err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE `+postgresQueueTable+`
		SET status='IN_PROGRESS', worker_id=$1, claimed_at=NOW()
		WHERE pipeline_id=$2 AND id=$3`,
		workerID, q.pipelineID, id,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &WorkItem{
		ID:         id,
		Partition:  p,
		Status:     StatusInProgress,
		WorkerID:   workerID,
		ClaimedAt:  time.Now(),
		RetryCount: retryCount,
		MaxRetries: maxRetries,
	}, nil
}

func (q *PostgresQueue) Heartbeat(ctx context.Context, item *WorkItem) error {
	_, err := q.db.ExecContext(ctx, `
		UPDATE `+postgresQueueTable+` SET claimed_at=NOW()
		WHERE pipeline_id=$1 AND id=$2 AND worker_id=$3`,
		q.pipelineID, item.ID, item.WorkerID,
	)
	return err
}

func (q *PostgresQueue) Complete(ctx context.Context, item *WorkItem) error {
	_, err := q.db.ExecContext(ctx, `
		UPDATE `+postgresQueueTable+`
		SET status='DONE', completed_at=NOW()
		WHERE pipeline_id=$1 AND id=$2`,
		q.pipelineID, item.ID,
	)
	return err
}

func (q *PostgresQueue) Fail(ctx context.Context, item *WorkItem, reason string) error {
	_, err := q.db.ExecContext(ctx, `
		UPDATE `+postgresQueueTable+`
		SET status = CASE WHEN retry_count+1 >= max_retries THEN 'FAILED' ELSE 'PENDING' END,
		    retry_count = retry_count+1,
		    error = $3,
		    worker_id = NULL
		WHERE pipeline_id=$1 AND id=$2`,
		q.pipelineID, item.ID, reason,
	)
	return err
}

func (q *PostgresQueue) AllComplete(ctx context.Context) (bool, error) {
	var count int
	err := q.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+postgresQueueTable+`
		 WHERE pipeline_id=$1 AND status IN ('PENDING','IN_PROGRESS')`,
		q.pipelineID,
	).Scan(&count)
	return count == 0, err
}

func (q *PostgresQueue) Stats(ctx context.Context) (map[string]int, error) {
	rows, err := q.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM `+postgresQueueTable+`
		 WHERE pipeline_id=$1 GROUP BY status`,
		q.pipelineID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, rows.Err()
}

func (q *PostgresQueue) Close() error { return q.db.Close() }

// EnqueueWithLock is the leader-election-aware variant of Enqueue for
// distributed deployments. It uses a Postgres transaction-level advisory lock
// derived from the pipeline ID so that exactly one instance performs the
// partition enumeration and INSERT work per pipeline run. All other instances
// that arrive concurrently return immediately with a nil error and proceed
// directly to ClaimLoop, where they will block on Claim until rows appear.
//
// Advisory lock key: ('x' || md5(pipelineID))::bit(64)::bigint
// This produces a deterministic 64-bit integer from the pipeline ID without
// requiring a dedicated lock-key table, and without risk of collision with
// other pg_try_advisory_xact_lock callers that use integer literals.
//
// The lock is transaction-scoped (pg_try_advisory_xact_lock) and is released
// automatically when the transaction commits or rolls back — no explicit
// pg_advisory_unlock call is needed.
func (q *PostgresQueue) EnqueueWithLock(ctx context.Context, partitions []source.Partition, maxRetries int) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Step 1: attempt to acquire the advisory lock. If another instance holds
	// it, it is actively enqueuing right now — skip and return nil so this
	// instance can proceed to ClaimLoop.
	var locked bool
	if err := tx.QueryRowContext(ctx,
		`SELECT pg_try_advisory_xact_lock(('x' || md5($1))::bit(64)::bigint)`,
		q.pipelineID,
	).Scan(&locked); err != nil {
		return fmt.Errorf("workqueue: advisory lock probe: %w", err)
	}
	if !locked {
		// Another instance is enqueuing. Workers will pick up items via ClaimLoop.
		return nil
	}

	// Step 2: check whether an ACTIVE run already owns this queue. Only PENDING
	// or IN_PROGRESS items mean work is still outstanding — in that case a prior
	// enqueue (from this or an earlier instance) has already seeded the queue, so
	// release the lock (by committing) and proceed to ClaimLoop to help drain it.
	var activeCount int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+postgresQueueTable+`
		 WHERE pipeline_id=$1 AND status IN ('PENDING','IN_PROGRESS')`,
		q.pipelineID,
	).Scan(&activeCount); err != nil {
		return fmt.Errorf("workqueue: queue presence check: %w", err)
	}
	if activeCount > 0 {
		// Queue actively being drained by a previous enqueue or a concurrent
		// winner. Commit to release the advisory lock and proceed to ClaimLoop.
		return tx.Commit()
	}

	// Step 2b: no active items — this is a fresh run (the queue is either empty
	// or holds only terminal DONE/FAILED items from a previous completed run).
	// Clear any terminal leftovers so this run starts from a clean slate; without
	// this, a re-run of the same pipeline would find every partition already DONE
	// and process zero rows. Safe under the advisory lock: no other instance can
	// be enqueuing, and there are no active claims to disturb.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+postgresQueueTable+`
		 WHERE pipeline_id=$1 AND status IN ('DONE','FAILED')`,
		q.pipelineID,
	); err != nil {
		return fmt.Errorf("workqueue: clear terminal items: %w", err)
	}

	// Step 3: we hold the lock and the queue is clean — this instance is the
	// enqueue leader. Insert all partitions using the same ON CONFLICT DO NOTHING
	// semantics as the base Enqueue method.
	for _, p := range partitions {
		data, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO `+postgresQueueTable+` (id, pipeline_id, partition, max_retries)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (pipeline_id, id) DO NOTHING`,
			p.ID, q.pipelineID, data, maxRetries,
		); err != nil {
			return err
		}
	}

	// Committing releases the transaction-scoped advisory lock automatically.
	return tx.Commit()
}
