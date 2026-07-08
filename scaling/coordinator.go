// Package scaling provides the multi-instance coordination layer.
//
// Three modes are supported:
//
//	single      One Flogo instance processes all data. Default.
//	sharded     Static partitioning: each instance processes partitions where
//	            partitionIndex % instanceCount == INSTANCE_INDEX. No coordination
//	            required — instances are independent. Best for Kubernetes StatefulSets
//	            with predictable replica counts.
//	distributed Dynamic partitioning via a shared WorkQueue. A leader enumerates
//	            partitions and enqueues them; all instances compete to claim and
//	            process partitions. Failed partitions are automatically re-queued
//	            up to maxRetries. Best for Kubernetes Deployments with HPA.
//
// Leader election (distributed mode only) uses the work queue itself as the
// synchronisation primitive — the first instance to successfully Enqueue
// is the leader. Subsequent Enqueue calls are idempotent (ON CONFLICT DO NOTHING).
package scaling

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/partition/workqueue"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// Mode enumerates the supported scaling modes.
type Mode string

const (
	ModeSingle      Mode = "single"
	ModeSharded     Mode = "sharded"
	ModeDistributed Mode = "distributed"
)

// Config holds scaling configuration, typically loaded from the pipeline
// resource definition's "scaling" block.
type Config struct {
	Mode              Mode
	InstanceIndex     int    // sharded: 0-based index of this instance
	InstanceCount     int    // sharded: total replica count
	PartitionStrategy string // rowgroup | file | byterange
	PartitionsPerClaim int
	// Distributed-mode coordinator config.
	CoordinatorType      string // postgres | memory
	CoordinatorDSN       string // postgres connection string
	LeaseSeconds         int
	HeartbeatSeconds     int
	MaxRetries           int
}

// ResolveFromEnv fills in InstanceIndex and InstanceCount from environment
// variables when running in a Kubernetes StatefulSet:
//
//	INSTANCE_INDEX  — pod ordinal (0-based)
//	INSTANCE_COUNT  — desired replica count
func (c *Config) ResolveFromEnv() {
	if idx := os.Getenv("INSTANCE_INDEX"); idx != "" {
		if n, err := strconv.Atoi(idx); err == nil {
			c.InstanceIndex = n
		}
	}
	if cnt := os.Getenv("INSTANCE_COUNT"); cnt != "" {
		if n, err := strconv.Atoi(cnt); err == nil {
			c.InstanceCount = n
		}
	}
}

// ShardedPartitions returns the subset of partitions assigned to this instance
// in sharded mode. The assignment is deterministic: instance i owns partitions
// at indices where index % instanceCount == instanceIndex.
func ShardedPartitions(all []source.Partition, instanceIndex, instanceCount int) []source.Partition {
	if instanceCount <= 1 {
		return all
	}
	var mine []source.Partition
	for i, p := range all {
		if i%instanceCount == instanceIndex {
			mine = append(mine, p)
		}
	}
	return mine
}

// Coordinator runs the distributed coordination loop for one pipeline run.
// It enqueues partitions (leader step), claims and processes each assigned
// partition, and reports completion.
type Coordinator struct {
	cfg      Config
	queue    workqueue.WorkQueue
	workerID string
}

// NewCoordinator creates a Coordinator with the given queue implementation.
func NewCoordinator(cfg Config, queue workqueue.WorkQueue) *Coordinator {
	workerID := os.Getenv("POD_NAME")
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	return &Coordinator{cfg: cfg, queue: queue, workerID: workerID}
}

// Enqueue populates the work queue with partitions.
//
// For PostgresQueue-backed coordinators (distributed mode) the underlying queue
// is wrapped by leaderAwareQueue, which calls EnqueueWithLock instead of plain
// Enqueue. EnqueueWithLock acquires a Postgres transaction-level advisory lock
// keyed on the pipeline ID before inserting:
//   - If another instance currently holds the lock it is actively enqueuing;
//     this call returns immediately so the instance can proceed to ClaimLoop.
//   - If the queue already contains rows from a prior run the lock is released
//     and this call returns immediately.
//   - Otherwise this instance becomes the enqueue leader and performs all INSERTs
//     inside the same transaction that holds the lock; the lock releases on commit.
//
// MemoryQueue-backed coordinators (single-instance / dev mode) call plain Enqueue,
// which is already serialised by an in-process sync.Cond.
func (c *Coordinator) Enqueue(ctx context.Context, partitions []source.Partition) error {
	maxRetries := c.cfg.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	return c.queue.Enqueue(ctx, partitions, maxRetries)
}

// ClaimLoop claims partitions from the queue one at a time, calls processFunc
// for each, and marks them complete or failed. Returns when all partitions are
// processed or ctx is cancelled.
//
// processFunc receives the claimed partition and should return an error if
// processing fails (causing the partition to be re-queued up to maxRetries).
func (c *Coordinator) ClaimLoop(
	ctx context.Context,
	processFunc func(ctx context.Context, p source.Partition) error,
) error {
	lease := time.Duration(c.cfg.LeaseSeconds) * time.Second
	if lease == 0 {
		lease = 120 * time.Second
	}
	heartbeatInterval := time.Duration(c.cfg.HeartbeatSeconds) * time.Second
	if heartbeatInterval == 0 {
		heartbeatInterval = 30 * time.Second
	}

	for {
		item, err := c.queue.Claim(ctx, c.workerID, lease)
		if err != nil {
			return fmt.Errorf("coordinator: claim: %w", err)
		}
		if item == nil {
			// All partitions done.
			return nil
		}

		// Start heartbeat goroutine.
		// Note: cancelHB is called explicitly below on every exit path; do NOT
		// use defer here — defer inside a loop accumulates until ClaimLoop returns,
		// leaving heartbeat goroutines alive for the entire pipeline run.
		heartbeatCtx, cancelHB := context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					_ = c.queue.Heartbeat(heartbeatCtx, item)
				}
			}
		}()

		// Process the partition.
		processErr := processFunc(ctx, item.Partition)
		cancelHB() // stop the heartbeat goroutine before marking the item done/failed

		if processErr != nil {
			_ = c.queue.Fail(ctx, item, processErr.Error())
		} else {
			_ = c.queue.Complete(ctx, item)
		}
	}
}

// WaitForCompletion polls until all queue items are DONE/FAILED or ctx expires.
func (c *Coordinator) WaitForCompletion(ctx context.Context) error {
	for {
		done, err := c.queue.AllComplete(ctx)
		if err != nil {
			return fmt.Errorf("coordinator: check completion: %w", err)
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// Stats returns queue statistics for observability.
func (c *Coordinator) Stats(ctx context.Context) (map[string]int, error) {
	return c.queue.Stats(ctx)
}

// leaderAwareQueue wraps a *workqueue.PostgresQueue and overrides Enqueue to
// call EnqueueWithLock, enabling advisory-lock-based leader election so that
// exactly one distributed instance performs the partition enumeration and INSERT
// work per pipeline run. All other WorkQueue methods are delegated unchanged.
//
// This is a private wrapper — callers continue to use the WorkQueue interface
// and are unaware of the leader-election mechanism.
type leaderAwareQueue struct {
	*workqueue.PostgresQueue
}

// Enqueue delegates to EnqueueWithLock, acquiring a Postgres transaction-level
// advisory lock before inserting partitions. See PostgresQueue.EnqueueWithLock
// for the full locking protocol.
func (w leaderAwareQueue) Enqueue(ctx context.Context, partitions []source.Partition, maxRetries int) error {
	return w.EnqueueWithLock(ctx, partitions, maxRetries)
}

// BuildQueue constructs a WorkQueue from the coordinator config.
// Supported types: "memory" (default), "postgres".
//
// For "postgres", the returned queue is wrapped in leaderAwareQueue so that
// concurrent Enqueue calls across instances are serialised via a Postgres
// advisory lock rather than relying solely on ON CONFLICT DO NOTHING.
func BuildQueue(cfg Config, pipelineID string) (workqueue.WorkQueue, error) {
	switch cfg.CoordinatorType {
	case "postgres":
		pq, err := BuildPostgresQueue(cfg, pipelineID)
		if err != nil {
			return nil, err
		}
		// BuildPostgresQueue returns *workqueue.PostgresQueue directly; wrap it
		// in leaderAwareQueue to activate advisory-lock leader election.
		return leaderAwareQueue{pq}, nil
	default:
		return workqueue.NewMemoryQueue(), nil
	}
}

// BuildPostgresQueue opens a *sql.DB from cfg.CoordinatorDSN and returns a
// *workqueue.PostgresQueue. The concrete type is returned (not the WorkQueue
// interface) so that BuildQueue can wrap it in leaderAwareQueue without a
// runtime type assertion. The caller must have imported a Postgres driver as a
// side-effect, e.g. _ "github.com/lib/pq" or _ "github.com/jackc/pgx/v5/stdlib".
func BuildPostgresQueue(cfg Config, pipelineID string) (*workqueue.PostgresQueue, error) {
	if cfg.CoordinatorDSN == "" {
		return nil, fmt.Errorf("scaling: postgres coordinator requires CoordinatorDSN")
	}
	db, err := sql.Open("postgres", cfg.CoordinatorDSN)
	if err != nil {
		return nil, fmt.Errorf("scaling: open postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("scaling: ping postgres: %w", err)
	}
	return workqueue.NewPostgresQueue(db, pipelineID)
}
