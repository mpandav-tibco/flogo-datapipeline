// Package workqueue — Redis-backed distributed work queue.
//
// RedisQueue is a lighter-weight alternative to PostgresQueue for distributed
// scaling: it needs only a Redis server (no shared SQL database) and is a good
// fit when the pipeline sink is NOT Postgres. Claiming is atomic via a Lua
// script (compare-and-set on the item hash), and expired leases are reclaimed
// lazily on the next Claim by any worker.
//
// Data model (all keys namespaced by keyPrefix + pipelineID):
//
//	{ns}:items          HASH   itemID -> JSON(WorkItem)          (source of truth)
//	{ns}:pending        LIST   itemIDs available to claim         (FIFO queue)
//	{ns}:inprogress     ZSET   itemID -> lease-expiry unix millis (for reclaim)
//
// A worker RPOPs from :pending, then CAS-marks the item IN_PROGRESS in :items
// and records its lease in :inprogress. On Complete/Fail the item hash is
// updated and the ZSET entry removed; a failed-with-retries item is re-pushed
// to :pending. Before each claim, any :inprogress entries whose lease has
// expired are moved back to :pending so a dead worker's partition is retried.
package workqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// RedisQueue implements WorkQueue over a single Redis server.
type RedisQueue struct {
	client     *goredis.Client
	ns         string // key namespace: {keyPrefix}:{pipelineID}
	pipelineID string
}

// NewRedisQueue connects to Redis at addr and returns a work queue scoped to
// pipelineID. keyPrefix defaults to "flogo:pipeline:wq" and passwordEnv, when
// set, names the environment variable holding the Redis password.
func NewRedisQueue(addr, keyPrefix, passwordEnv, pipelineID string) (*RedisQueue, error) {
	if addr == "" {
		addr = "localhost:6379"
	}
	if keyPrefix == "" {
		keyPrefix = "flogo:pipeline:wq"
	}
	password := ""
	if passwordEnv != "" {
		password = os.Getenv(passwordEnv)
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr, Password: password})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis workqueue: ping %s: %w", addr, err)
	}
	return &RedisQueue{
		client:     client,
		ns:         keyPrefix + ":" + pipelineID,
		pipelineID: pipelineID,
	}, nil
}

func (q *RedisQueue) kItems() string      { return q.ns + ":items" }
func (q *RedisQueue) kPending() string    { return q.ns + ":pending" }
func (q *RedisQueue) kInProgress() string { return q.ns + ":inprogress" }

// Enqueue seeds the queue with partitions. Terminal (DONE/FAILED) items from a
// previous completed run are cleared first so a re-run reprocesses cleanly —
// mirroring the PostgresQueue reset semantics. Enqueue is idempotent: an item
// that already exists in a non-terminal state is left untouched.
//
// The whole reset+seed runs inside a per-pipeline distributed lock so exactly
// one instance seeds the queue; late instances find the lock held, return, and
// proceed to ClaimLoop.
func (q *RedisQueue) Enqueue(ctx context.Context, partitions []source.Partition, maxRetries int) error {
	lockKey := q.ns + ":enqueue-lock"
	ok, err := q.client.SetNX(ctx, lockKey, "1", 30*time.Second).Result()
	if err != nil {
		return fmt.Errorf("redis workqueue: acquire enqueue lock: %w", err)
	}
	if !ok {
		// Another instance is enqueuing; proceed straight to ClaimLoop.
		return nil
	}
	defer func() { _ = q.client.Del(context.Background(), lockKey).Err() }()

	// If an ACTIVE run already owns the queue (pending or in-progress items),
	// don't reseed — just help drain it.
	pendingLen, _ := q.client.LLen(ctx, q.kPending()).Result()
	inProg, _ := q.client.ZCard(ctx, q.kInProgress()).Result()
	if pendingLen > 0 || inProg > 0 {
		return nil
	}

	// Fresh run: clear any terminal leftovers and seed from scratch.
	pipe := q.client.TxPipeline()
	pipe.Del(ctx, q.kItems(), q.kPending(), q.kInProgress())
	for _, p := range partitions {
		item := &WorkItem{ID: p.ID, Partition: p, Status: StatusPending, MaxRetries: maxRetries}
		data, mErr := json.Marshal(item)
		if mErr != nil {
			return mErr
		}
		pipe.HSet(ctx, q.kItems(), p.ID, data)
		pipe.RPush(ctx, q.kPending(), p.ID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis workqueue: seed: %w", err)
	}
	return nil
}

// claimScript atomically marks the given itemID's hash IN_PROGRESS with the
// given worker and records the lease expiry in the inprogress ZSET. The lease
// (ZSET score) is the authoritative claim record; ClaimedAt on the struct is
// set by the caller. Returns the updated item JSON, or "" if it vanished.
// KEYS[1]=items KEYS[2]=inprogress ; ARGV[1]=itemID ARGV[2]=workerID ARGV[3]=leaseExpiryMillis
var claimScript = goredis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return '' end
local item = cjson.decode(raw)
item.status = 'IN_PROGRESS'
item.workerId = ARGV[2]
local enc = cjson.encode(item)
redis.call('HSET', KEYS[1], ARGV[1], enc)
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return enc
`)

// reclaimExpired moves any in-progress items whose lease expired back onto the
// pending list, so a crashed worker's partition is retried.
func (q *RedisQueue) reclaimExpired(ctx context.Context, nowMillis int64) error {
	// ZRANGE <key> -inf <nowMillis> BYSCORE — members whose lease expiry score
	// is at or before now. (Replaces the deprecated ZRangeByScore as of Redis 6.2.0.)
	expired, err := q.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:     q.kInProgress(),
		Start:   "-inf",
		Stop:    nowMillis,
		ByScore: true,
	}).Result()
	if err != nil {
		return err
	}
	for _, id := range expired {
		// Remove from inprogress and re-queue as pending (idempotent RPUSH).
		pipe := q.client.TxPipeline()
		pipe.ZRem(ctx, q.kInProgress(), id)
		pipe.RPush(ctx, q.kPending(), id)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Claim pops the next pending partition and marks it IN_PROGRESS. Blocks until
// an item is available or all items are terminal (returns nil,nil) or ctx ends.
func (q *RedisQueue) Claim(ctx context.Context, workerID string, lease time.Duration) (*WorkItem, error) {
	if lease <= 0 {
		lease = 120 * time.Second
	}
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		now := time.Now()
		if err := q.reclaimExpired(ctx, now.UnixMilli()); err != nil {
			return nil, fmt.Errorf("redis workqueue: reclaim: %w", err)
		}
		// Try to grab a pending item id.
		id, err := q.client.LPop(ctx, q.kPending()).Result()
		if err == goredis.Nil || id == "" {
			// Nothing pending — done if nothing is in progress either.
			inProg, _ := q.client.ZCard(ctx, q.kInProgress()).Result()
			if inProg == 0 {
				return nil, nil
			}
			// Work still in flight on other workers; wait briefly and retry.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		} else if err != nil {
			return nil, fmt.Errorf("redis workqueue: lpop: %w", err)
		}
		leaseExpiry := now.Add(lease).UnixMilli()
		res, err := claimScript.Run(ctx, q.client,
			[]string{q.kItems(), q.kInProgress()},
			id, workerID, leaseExpiry,
		).Text()
		if err != nil {
			return nil, fmt.Errorf("redis workqueue: claim script: %w", err)
		}
		if res == "" {
			continue // item vanished; try the next
		}
		var item WorkItem
		if err := json.Unmarshal([]byte(res), &item); err != nil {
			return nil, err
		}
		item.ClaimedAt = now
		return &item, nil
	}
}

// Heartbeat extends the lease on a claimed item.
func (q *RedisQueue) Heartbeat(ctx context.Context, item *WorkItem) error {
	// Only renew if still in progress (present in the ZSET).
	score := time.Now().Add(120 * time.Second).UnixMilli()
	return q.client.ZAdd(ctx, q.kInProgress(), goredis.Z{Score: float64(score), Member: item.ID}).Err()
}

// Complete marks an item DONE and removes its in-progress lease.
func (q *RedisQueue) Complete(ctx context.Context, item *WorkItem) error {
	return q.updateTerminal(ctx, item.ID, StatusDone, "")
}

// Fail returns the item to PENDING if retries remain, else marks it FAILED.
func (q *RedisQueue) Fail(ctx context.Context, item *WorkItem, reason string) error {
	raw, err := q.client.HGet(ctx, q.kItems(), item.ID).Result()
	if err != nil {
		return err
	}
	var stored WorkItem
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return err
	}
	stored.RetryCount++
	stored.Error = reason
	_ = q.client.ZRem(ctx, q.kInProgress(), item.ID).Err()
	if stored.RetryCount >= stored.MaxRetries {
		stored.Status = StatusFailed
		stored.CompletedAt = time.Now()
	} else {
		stored.Status = StatusPending
		stored.WorkerID = ""
	}
	data, _ := json.Marshal(&stored)
	if err := q.client.HSet(ctx, q.kItems(), item.ID, data).Err(); err != nil {
		return err
	}
	if stored.Status == StatusPending {
		return q.client.RPush(ctx, q.kPending(), item.ID).Err()
	}
	return nil
}

func (q *RedisQueue) updateTerminal(ctx context.Context, id, status, reason string) error {
	raw, err := q.client.HGet(ctx, q.kItems(), id).Result()
	if err != nil {
		return err
	}
	var stored WorkItem
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return err
	}
	stored.Status = status
	stored.CompletedAt = time.Now()
	if reason != "" {
		stored.Error = reason
	}
	data, _ := json.Marshal(&stored)
	pipe := q.client.TxPipeline()
	pipe.HSet(ctx, q.kItems(), id, data)
	pipe.ZRem(ctx, q.kInProgress(), id)
	_, err = pipe.Exec(ctx)
	return err
}

// AllComplete returns true when no items remain PENDING or IN_PROGRESS.
func (q *RedisQueue) AllComplete(ctx context.Context) (bool, error) {
	pendingLen, err := q.client.LLen(ctx, q.kPending()).Result()
	if err != nil {
		return false, err
	}
	inProg, err := q.client.ZCard(ctx, q.kInProgress()).Result()
	if err != nil {
		return false, err
	}
	return pendingLen == 0 && inProg == 0, nil
}

// Stats returns counts by status from the items hash.
func (q *RedisQueue) Stats(ctx context.Context) (map[string]int, error) {
	all, err := q.client.HGetAll(ctx, q.kItems()).Result()
	if err != nil {
		return nil, err
	}
	counts := map[string]int{StatusPending: 0, StatusInProgress: 0, StatusDone: 0, StatusFailed: 0}
	for _, raw := range all {
		var item WorkItem
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			continue
		}
		counts[item.Status]++
	}
	return counts, nil
}

// Close releases the Redis client.
func (q *RedisQueue) Close() error { return q.client.Close() }
