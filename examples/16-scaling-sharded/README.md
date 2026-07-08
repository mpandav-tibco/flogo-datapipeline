# Example 16 — Multi-Instance Sharding

**Mode:** ETL (Extract, Transform, Load)
**Source:** CSV files — one file per shard, selected by `INSTANCE_INDEX`
**Transforms:** `filter` — keep only orders with `value > 10`
**Sink:** Null (discard — focus is on the sharding mechanism, not the destination)
**Scaling:** `sharded` — static index-based partitioning, no coordinator required

This example shows how to split a folder of CSV files across N independent
pipeline instances using **static sharding**. Each instance is assigned an
ordinal index (0, 1, 2, ...) and processes only the file(s) that belong to
that shard. There is no coordinator process, no shared lock, and no external
dependency such as Redis or Postgres. Parallelism is entirely deterministic:
given the same `INSTANCE_INDEX` and `INSTANCE_COUNT`, the same instance will
always claim the same files.

---

## Directory Layout

```
16-scaling-sharded/
├── sharded.datapipeline          # Single pipeline definition used by all instances
├── run-sharded.sh                # Launches 3 instances concurrently in the background
├── data/
│   ├── partition_0.csv           # 20 order rows  (ids  1-20,  Jan 2024)
│   ├── partition_1.csv           # 20 order rows  (ids 21-40,  Jan-Feb 2024)
│   └── partition_2.csv           # 20 order rows  (ids 41-60,  Feb-Mar 2024)
├── logs/                         # Per-instance log files created by run-sharded.sh
│   └── .gitkeep
└── README.md                     # This file
```

---

## What is Static Sharding?

Static sharding assigns each data-source partition to an instance by a simple
formula:

```
owned = (partitionIndex % INSTANCE_COUNT) == INSTANCE_INDEX
```

Because the assignment is a pure arithmetic operation on the instance's ordinal,
every instance independently determines which files it should read — no
election, no lease, no distributed lock.

### Properties

| Property | Value |
|----------|-------|
| Coordinator required | No |
| External state (Redis / Postgres) | No |
| Deterministic assignment | Yes — same index always owns same shards |
| Safe to restart individual instances | Yes — each instance re-processes its own shard |
| Suitable for dynamic replica counts | No — all instances must agree on `INSTANCE_COUNT` |
| Suitable for Kubernetes StatefulSets | Yes — pod ordinal maps directly to `INSTANCE_INDEX` |

---

## How the Engine Resolves `{index}` in the Path

The source path `./data/partition_{index}.csv` is a **template**. When
`scaling.mode` is `sharded`, the engine substitutes `{index}` with the
resolved `INSTANCE_INDEX` value before opening any files.

Resolution order for `instanceIndex` and `instanceCount`:

1. Environment variables `INSTANCE_INDEX` and `INSTANCE_COUNT` (highest priority)
2. `scaling.instanceIndex` and `scaling.instanceCount` in the `.datapipeline` file
3. Defaults: `instanceIndex=0`, `instanceCount=1` (single-instance fallback)

This means **one `.datapipeline` file is shared by all instances**. You do not
need to create a separate file per shard. Only the env vars differ between
instances.

---

## Pipeline Definition Explained

```json
{
  "name": "sharded-orders",
  "mode": "etl",
  "source": {
    "type": "csv",
    "path": "./data/partition_{index}.csv",
    "hasHeader": true,
    "columns": [
      { "name": "id",          "type": "int"    },
      { "name": "customer_id", "type": "string" },
      { "name": "product_sku", "type": "string" },
      { "name": "quantity",    "type": "int"    },
      { "name": "unit_price",  "type": "float"  },
      { "name": "value",       "type": "float"  },
      { "name": "region",      "type": "string" },
      { "name": "status",      "type": "string" },
      { "name": "order_date",  "type": "string" }
    ]
  },
  "transforms": [
    { "type": "filter", "expr": "value > 10" }
  ],
  "sink": { "type": "null" },
  "scaling": {
    "mode": "sharded",
    "instanceIndex": 0,
    "instanceCount": 3
  },
  "checkpoint": { "type": "none" }
}
```

### Key Fields

| Field | Value | Purpose |
|-------|-------|---------|
| `source.path` | `"./data/partition_{index}.csv"` | `{index}` is replaced at runtime with `INSTANCE_INDEX` |
| `scaling.mode` | `"sharded"` | Activates static partition assignment |
| `scaling.instanceIndex` | `0` | Fallback index if `INSTANCE_INDEX` env var is absent |
| `scaling.instanceCount` | `3` | Fallback count if `INSTANCE_COUNT` env var is absent |
| `transforms[0].type` | `"filter"` | Drops rows where `value <= 10` (low-value cancelled orders) |
| `sink.type` | `"null"` | Discards all passing rows — swap for `jdbc` or `parquet` in production |
| `checkpoint.type` | `"none"` | No resume state; each run starts from row 1 of the partition file |

---

## Sample Data

Each CSV file has the same 9-column schema:

```
id,customer_id,product_sku,quantity,unit_price,value,region,status,order_date
```

| Column | Type | Notes |
|--------|------|-------|
| `id` | `int` | Globally unique order ID (1-60 across all partitions) |
| `customer_id` | `string` | Customer reference (C1001–C1060) |
| `product_sku` | `string` | Product SKU code |
| `quantity` | `int` | Units ordered |
| `unit_price` | `float` | Price per unit in USD |
| `value` | `float` | Total order value (`quantity * unit_price`) |
| `region` | `string` | NORTH / SOUTH / EAST / WEST |
| `status` | `string` | SHIPPED / DELIVERED / PROCESSING / CANCELLED |
| `order_date` | `string` | ISO-8601 date |

Partition file summary:

| File | IDs | Date range | Rows | Rows passing `value > 10` | Filtered IDs |
|------|-----|------------|------|---------------------------|--------------|
| `partition_0.csv` | 1-20 | 2024-01-03 to 2024-01-22 | 20 | 17 | 5 (8.50), 8 (9.99), 16 (7.25) |
| `partition_1.csv` | 21-40 | 2024-01-23 to 2024-02-11 | 20 | 19 | 23 (6.00) |
| `partition_2.csv` | 41-60 | 2024-02-12 to 2024-03-02 | 20 | 18 | 48 (5.50), 56 (8.75) |

---

## How to Run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`.
- Run from the `16-scaling-sharded/` directory so that relative paths in the
  pipeline file resolve correctly.

### Option A — run all three instances via script

```bash
chmod +x run-sharded.sh
./run-sharded.sh
```

The script launches three background processes, waits for all to finish, then
prints a results table. Per-instance logs are written to `logs/`.

### Option B — run a single instance manually

To test just one shard without running the others:

```bash
# Instance 0
INSTANCE_INDEX=0 INSTANCE_COUNT=3 flogo-datapipeline -f sharded.datapipeline

# Instance 1
INSTANCE_INDEX=1 INSTANCE_COUNT=3 flogo-datapipeline -f sharded.datapipeline

# Instance 2
INSTANCE_INDEX=2 INSTANCE_COUNT=3 flogo-datapipeline -f sharded.datapipeline
```

### Option C — override INSTANCE_COUNT to scale to 2 or 4 instances

You can change the shard count without editing the pipeline file. To split the
work across 2 instances instead of 3:

```bash
INSTANCE_INDEX=0 INSTANCE_COUNT=2 flogo-datapipeline -f sharded.datapipeline &
INSTANCE_INDEX=1 INSTANCE_COUNT=2 flogo-datapipeline -f sharded.datapipeline &
wait
```

With `INSTANCE_COUNT=2`, instance 0 reads `partition_0.csv` and instance 1
reads `partition_1.csv`. `partition_2.csv` would need to be assigned to one of
the existing instances or an additional instance must be added for full
coverage.

> **Note:** With static sharding you are responsible for ensuring that
> `INSTANCE_COUNT` matches the number of partition files and the number of
> running instances. If counts differ, some partitions may be skipped or
> processed more than once.

---

## Expected Output

### Instance 0

```
INFO  pipeline starting      name=sharded-orders mode=etl instance=0/3
INFO  shard resolved         path=./data/partition_0.csv instance_index=0 instance_count=3
INFO  csv source opened      path=./data/partition_0.csv rows=20
INFO  filter applied         passed=17 dropped=3 expr="value > 10"
INFO  batch processed        rows=17 batch=1
INFO  pipeline complete      rows_read=20 rows_written=0 rows_filtered=3 duration=9ms
```

(Rows 5, 8, and 16 have `value` of 8.50, 9.99, and 7.25 respectively — all
below the 10.00 threshold.)

### Instance 1

```
INFO  pipeline starting      name=sharded-orders mode=etl instance=1/3
INFO  shard resolved         path=./data/partition_1.csv instance_index=1 instance_count=3
INFO  csv source opened      path=./data/partition_1.csv rows=20
INFO  filter applied         passed=19 dropped=1 expr="value > 10"
INFO  batch processed        rows=19 batch=1
INFO  pipeline complete      rows_read=20 rows_written=0 rows_filtered=1 duration=11ms
```

(Row 23 has `value` of 6.00 — a single-item low-price order.)

### Instance 2

```
INFO  pipeline starting      name=sharded-orders mode=etl instance=2/3
INFO  shard resolved         path=./data/partition_2.csv instance_index=2 instance_count=3
INFO  csv source opened      path=./data/partition_2.csv rows=20
INFO  filter applied         passed=18 dropped=2 expr="value > 10"
INFO  batch processed        rows=18 batch=1
INFO  pipeline complete      rows_read=20 rows_written=0 rows_filtered=2 duration=8ms
```

(Rows 48 and 56 have `value` of 5.50 and 8.75 respectively.)

All three instances run concurrently. Total wall-clock time is approximately the
duration of the slowest single instance, not the sum of all three.

---

## How to Verify

After running `./run-sharded.sh`:

1. **All three exit codes are 0:**
   ```bash
   echo $?
   ```
   (The script exits non-zero and prints an error if any instance fails.)

2. **Each log confirms the correct partition file was opened:**
   ```bash
   grep "shard resolved" logs/instance-*.log
   ```
   Expected:
   ```
   logs/instance-0.log: shard resolved  path=./data/partition_0.csv
   logs/instance-1.log: shard resolved  path=./data/partition_1.csv
   logs/instance-2.log: shard resolved  path=./data/partition_2.csv
   ```

3. **Each instance processed exactly 20 rows:**
   ```bash
   grep "rows_read" logs/instance-*.log
   ```
   Expected: `rows_read=20` in each log.

4. **No partition was double-processed:**
   The three log files reference three distinct paths — `partition_0.csv`,
   `partition_1.csv`, and `partition_2.csv`. If any two logs reference the same
   path, `INSTANCE_INDEX` was duplicated.

5. **Filter dropped low-value rows per instance:**
   ```bash
   grep "rows_filtered" logs/instance-*.log
   ```
   Expected:
   ```
   logs/instance-0.log: rows_filtered=3   (ids 5, 8, 16  — values 8.50, 9.99, 7.25)
   logs/instance-1.log: rows_filtered=1   (id  23        — value  6.00)
   logs/instance-2.log: rows_filtered=2   (ids 48, 56    — values 5.50, 8.75)
   ```
   Total rows filtered across all instances: 6 out of 60 (10%).

---

## Sharded vs. Distributed Mode

| Aspect | `sharded` (this example) | `distributed` (example 17) |
|--------|--------------------------|----------------------------|
| Coordinator | None | Yes (Postgres or Redis) |
| Assignment | Static: `partitionIndex % count == index` | Dynamic: coordinator assigns partitions |
| Shared state | None | Lease table in coordinator backend |
| Late-joining instances | Not supported | Coordinator reassigns work |
| Straggler rebalancing | Not supported | Coordinator detects and reassigns |
| Suitable for | Kubernetes StatefulSets, fixed replica counts | Long-running jobs, auto-scaling, spot instances |
| Complexity | Low | Medium |
| External dependency | None | Postgres or Redis |

**Use `sharded` when:**
- You control the replica count (StatefulSet, fixed deployment).
- Partitions are pre-split into known files or ranges.
- You want zero external dependencies.
- Deterministic, reproducible shard assignment is important (e.g., auditable pipelines).

**Use `distributed` when:**
- The number of instances varies at runtime (auto-scaling, spot instances).
- You need a straggler to be automatically picked up by a healthy instance.
- Partitions arrive dynamically (e.g., Kafka, object store with unknown file count).

---

## Kubernetes StatefulSet Integration

In a Kubernetes StatefulSet, each pod gets a stable DNS name and a numeric
ordinal suffix. Map the ordinal directly to `INSTANCE_INDEX`:

```yaml
# statefulset.yaml (excerpt)
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sharded-orders
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: pipeline
          image: my-registry/flogo-datapipeline:latest
          args: ["-f", "/config/sharded.datapipeline"]
          env:
            - name: INSTANCE_INDEX
              valueFrom:
                fieldRef:
                  fieldPath: metadata.annotations['apps.kubernetes.io/pod-index']
            - name: INSTANCE_COUNT
              value: "3"
          volumeMounts:
            - name: pipeline-config
              mountPath: /config
            - name: data
              mountPath: /app/data
```

When you scale the StatefulSet from 3 to 6 replicas (and add more partition
files), update `INSTANCE_COUNT` to `6`. Each pod will re-read the env var on
restart and claim its new shard.

---

## Configuration Reference

### Scaling Block

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `mode` | string | Yes | — | `"sharded"` activates static partition assignment |
| `instanceIndex` | int | No | `0` | 0-based ordinal of this instance; overridden by `INSTANCE_INDEX` env var |
| `instanceCount` | int | No | `1` | Total number of instances; overridden by `INSTANCE_COUNT` env var |

### Environment Variable Overrides

| Variable | Overrides | Description |
|----------|-----------|-------------|
| `INSTANCE_INDEX` | `scaling.instanceIndex` | 0-based index of this instance (0 to `INSTANCE_COUNT - 1`) |
| `INSTANCE_COUNT` | `scaling.instanceCount` | Total number of instances sharing the workload |

### Source Path Template

| Token | Replaced with | Example |
|-------|---------------|---------|
| `{index}` | Value of `INSTANCE_INDEX` | `./data/partition_{index}.csv` + index 1 = `./data/partition_1.csv` |

Only one token is supported in the path. The token is case-sensitive.

### CSV Source

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `path` | string | Yes | — | May contain `{index}` for shard-aware file selection |
| `delimiter` | string | No | `","` | Single character |
| `hasHeader` | bool | No | `false` | First row is column names, not data |
| `columns` | array | Yes | — | `[{name, type}]`; types: `string`, `int`, `float`, `bool` |

### Filter Transform

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | string | Yes | `"filter"` |
| `expr` | string | Yes | Boolean expression; rows where this is `false` are dropped |

### Performance

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows per internal buffer flush |
| `maxMemoryMB` | int | `512` | Soft heap limit per instance |

---

## Adapting This Example for Production

### Replace the Null Sink with a Real Destination

Swap `"type": "null"` with `"type": "jdbc"` (or `"parquet"`, `"objstore"`, etc.)
to write processed rows to a real destination. Each instance writes independently
so there is no write contention — each instance can even write to a different
table or file:

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "postgres://user:pass@host/db",
  "table": "orders_shard_${INSTANCE_INDEX}",
  "batchSize": 1000,
  "onConflict": "upsert",
  "conflictKey": ["id"]
}
```

### Use Glob Patterns for Multiple Files per Shard

If you have more files than instances, a glob pattern such as
`./data/region_*.csv` combined with the `index % count` formula lets the engine
claim a subset of the matched files. Set `source.path` to the glob and the
engine will internally iterate the file list, processing only files where
`fileIndex % instanceCount == instanceIndex`.

### Add a Checkpoint for Long-Running Partitions

When partition files are large enough that a restart mid-run is possible, add
a file checkpoint so each instance can resume where it left off:

```json
"checkpoint": {
  "type": "file",
  "path": "./checkpoints",
  "intervalRows": 10000,
  "intervalSeconds": 30,
  "deleteOnSuccess": true
}
```

Each instance writes a checkpoint file named after its `INSTANCE_INDEX`, so
there is no path collision between instances.

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [15-checkpoint-resume](../15-checkpoint-resume/) | Interrupt and resume a long-running pipeline using file checkpoints |
| 17-scaling-distributed | Dynamic partition assignment with a Postgres/Redis coordinator (no static count required) |
| 18-kafka-parallel | Shard Kafka partitions across instances; `INSTANCE_INDEX` maps to Kafka consumer group member |
