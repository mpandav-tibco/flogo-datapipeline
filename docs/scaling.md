# Scaling & coordination

Most workloads run as a **single** process. When one instance is not enough, the same
`.datapipeline` runs across several instances that divide the source into
**partitions**. Only partitionable sources (`parquet`, `jdbc`, object store) split;
others always run single.

Implementation: [`scaling/coordinator.go`](../scaling/coordinator.go) +
[`partition/workqueue`](../partition/workqueue). `ResolveFromEnv()` reads
`INSTANCE_INDEX` / `INSTANCE_COUNT` — Kubernetes StatefulSet friendly.

## Modes

| Mode | Behavior |
|---|---|
| `single` (default) | one instance processes everything |
| `sharded` | static partitioning: instance *i* claims partitions where `index % count == i` |
| `distributed` | dynamic work-queue: a leader enqueues partitions, workers claim / heartbeat / complete |

## Sharded

No coordinator, no shared state — each instance is told *who it is* and computes its
share.

```json
"scaling": {
  "mode": "sharded",
  "instanceIndex": 0,
  "instanceCount": 4,
  "partitionStrategy": "rowgroup"
}
```

Set `instanceIndex`/`instanceCount` per instance (or via `INSTANCE_INDEX` /
`INSTANCE_COUNT`). `partitionStrategy` is `rowgroup` (Parquet), `file` (object
store), or `byterange`. Simplest to operate; best when the partition set is stable and
evenly sized. See [`examples/16-scaling-sharded`](../examples/16-scaling-sharded).

## Distributed

A coordinator hands out partitions dynamically, so a slow or failed worker doesn't
strand its share — another worker re-claims after the lease expires.

```json
"scaling": {
  "mode": "distributed",
  "coordinatorType": "redis",
  "coordinatorAddr": "localhost:6379",
  "coordinatorKeyPrefix": "flogo:pipeline:wq",
  "partitionsPerClaim": 4,
  "leaseSeconds": 60,
  "heartbeatSeconds": 15,
  "maxRetries": 3
}
```

| Key | Notes |
|---|---|
| `coordinatorType` | `redis`, `memory` (dev/test), 🚧 `postgres` |
| `coordinatorDSN` / `coordinatorAddr` / `coordinatorKeyPrefix` | backend location |
| `partitionsPerClaim` | how many partitions a worker leases at once |
| `leaseSeconds` | claim lease before another worker may steal it |
| `heartbeatSeconds` | how often a worker renews its lease |
| `maxRetries` | attempts before a partition is marked failed |

### Work-queue backends

| Queue | Use |
|---|---|
| **MemoryQueue** | in-process dev / test |
| **RedisQueue** | multi-instance; items HASH + pending LIST + inprogress ZSET, Lua CAS claim |
| 🚧 **PostgresQueue** | `FOR UPDATE SKIP LOCKED`, design-stage |

## State backends with scale

Multiple instances must **share** checkpoint and watermark state — use `redis` or
`postgres`, never `file`. See [Reliability](reliability.md).

## See also

- [Sources](sources.md) — which sources are partitionable
- [Deployment](deployment.md) — Kubernetes StatefulSet wiring
- [Feature reference](FEATURES.md#scaling--coordination)
