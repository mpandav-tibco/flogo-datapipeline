# Example 12 — Incremental Load (High-Water Mark)

This example demonstrates **incremental change data capture (CDC) without CDC
infrastructure** — a pattern that is ubiquitous in nightly delta loads, event
pipeline back-fills, and audit-log streaming. The pipeline reads from a PostgreSQL
table that grows over time and uses a **high-water mark (HWM) transform** to
track the newest row it has seen. On every subsequent run, only rows newer than
the recorded mark are processed.

---

## What you will learn

| Concept | Covered here |
|---|---|
| HWM transform | `type: "hwm"` — filters rows by a monotonic timestamp column |
| File backend | Watermark persisted to a local `.hwm` JSON file between runs |
| First-run semantics | No watermark file = no lower bound; all rows pass through |
| Watermark advancement | After each run the file is updated to the MAX value seen |
| Incremental CDC pattern | Only rows with `created_at > last_hwm` are processed |
| JDBC source with keyset pagination | `keyColumn: "event_id"` for reliable ordered reads |
| ETL mode with null sink | Transform chain runs; output is discarded (swap for real sink) |

---

## Directory layout

```
12-incremental-hwm/
├── incremental-hwm.datapipeline   # pipeline definition
├── docker-compose.yml             # local PostgreSQL service
├── setup.sh                       # start Postgres + create/seed raw_events (100 rows)
├── run1.sh                        # first run — full load, watermark written
├── add-new-events.sh              # insert 20 new rows with newer timestamps
├── run2.sh                        # second run — only 20 new rows processed
└── watermarks/
    └── events.hwm                 # watermark file (created by run1.sh)
```

---

## The high-water mark pattern

### The problem with full table scans

The naive approach to reading new data from a growing table is:

```sql
SELECT * FROM raw_events ORDER BY event_id;
```

This works on day one. By month three, when the table holds 50 million rows, a
full scan wastes I/O, CPU, and time re-reading data that has already been
processed. The pipeline re-processes rows it has already seen, which is
wasteful at best and causes duplicate writes at worst.

### The solution: track the newest value seen

A high-water mark maintains a single persistent value — the maximum value of a
monotonic column (`created_at`, `event_id`, `updated_at`) observed so far. On
the next run, only rows with a value strictly greater than the mark are read.

```
Run 1 (no prior HWM):
  SELECT * FROM raw_events
  -- reads 100 rows; HWM saved as "2025-06-05 15:30:00"

Run 2 (HWM = "2025-06-05 15:30:00"):
  SELECT * FROM raw_events WHERE created_at > '2025-06-05 15:30:00'
  -- reads 20 rows; HWM advanced to "2025-06-06 14:00:00"

Run 3 (no new data):
  SELECT * FROM raw_events WHERE created_at > '2025-06-06 14:00:00'
  -- reads 0 rows; HWM unchanged
```

The HWM is strictly greater-than, not greater-than-or-equal. This prevents
re-processing the boundary row.

### Why this is better than a full table scan

| Dimension | Full scan | HWM incremental |
|---|---|---|
| Rows read per run | All rows (grows unbounded) | Only new rows (constant) |
| Duplicate processing | Yes (every run) | No |
| Infrastructure needed | None | A small state file or Redis key |
| Suitable for | One-time loads | Daily/hourly/continuous delta loads |
| Risk | Reprocessing, duplicates in sink | Late-arriving data may be missed |

### When to use HWM vs. full CDC

Use HWM when:
- Your source table has a reliable monotonic `created_at` or `updated_at` column.
- Rows are never updated after insert (append-only log, event table).
- You do not have access to the database binlog/WAL (no Debezium/Maxwell).
- You need a low-infrastructure incremental pattern.

Use full CDC (Debezium, DMS, etc.) when:
- Rows are updated or deleted after insert.
- You need sub-second latency.
- You need to capture `UPDATE` and `DELETE` as well as `INSERT`.

---

## The dataset — `raw_events` (PostgreSQL)

The table is created by `setup.sh` inside the PostgreSQL container and holds one
table:

```sql
CREATE TABLE raw_events (
    event_id    SERIAL  PRIMARY KEY,
    event_type  TEXT    NOT NULL,  -- 'page_view' | 'click' | 'api_call' | 'checkout'
    user_id     INTEGER NOT NULL,
    session_id  TEXT    NOT NULL,
    page        TEXT    NOT NULL,  -- e.g. '/home', '/products', '/api/search'
    duration_ms INTEGER NOT NULL,
    status      TEXT    NOT NULL,  -- 'ok' | 'error' | 'timeout'
    created_at  TEXT    NOT NULL   -- ISO-8601 datetime, e.g. '2025-06-01 08:01:00'
);
```

Initial data (100 rows, inserted by `setup.sh`):

| Date range | Row count | Purpose |
|---|---|---|
| `2025-06-01` to `2025-06-03` | 50 rows | Older events — the "historical backlog" |
| `2025-06-04` to `2025-06-05` | 50 rows | More recent events — "full initial dataset" |

After `add-new-events.sh`:

| Date | Row count | Purpose |
|---|---|---|
| `2025-06-06` | 20 rows | New events arriving after the first pipeline run |

---

## Pipeline walkthrough

### 1. Source — JDBC (PostgreSQL)

```json
"source": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "postgres://postgres:postgres@localhost:5432/pipelinedemo?sslmode=disable",
  "table": "raw_events",
  "keyColumn": "event_id",
  "batchSize": 200
}
```

The JDBC source reads `raw_events` using keyset pagination on `event_id`. Each
page fetches up to 200 rows using a query equivalent to:

```sql
SELECT * FROM raw_events WHERE event_id > ? ORDER BY event_id LIMIT 200
```

Keyset pagination guarantees stable, consistent reads even if new rows are
inserted during the pipeline run. The source reads the full table and passes
all rows to the transform chain — the HWM transform (not the source) decides
which rows are new.

`partitions` is not set, so the source runs in single-partition mode. For
large tables, set `partitions: 4` (or more) to spread the reads across
parallel goroutines.

### 2. HWM transform

```json
{
  "type": "hwm",
  "column": "created_at",
  "backend": "file",
  "backendConfig": {
    "path": "./watermarks/events.hwm"
  }
}
```

The HWM transform does three things:

1. **Load the watermark** — reads the stored value from the file backend at
   pipeline startup. If the file does not exist, the watermark is treated as
   negative infinity (all rows pass).

2. **Filter rows** — for each row in the batch, it compares `created_at`
   against the stored watermark. Rows where `created_at <= watermark` are
   dropped. Rows where `created_at > watermark` pass through.

3. **Advance the watermark** — at pipeline shutdown, the maximum `created_at`
   value seen across all surviving rows is written back to the file. The next
   run reads this new value as its lower bound.

The watermark file is plain JSON:

```json
{
  "column": "created_at",
  "value": "2025-06-05 15:30:00",
  "updatedAt": "2025-06-05T15:30:00Z"
}
```

- `column` — the column name being tracked (matches the `column` field in the
  pipeline definition).
- `value` — the high-water mark itself: the MAX value of `column` seen in the
  last successful run.
- `updatedAt` — the wall-clock time the watermark was last written (useful for
  debugging stale watermarks).

### 3. Sink — null

```json
"sink": { "type": "null" }
```

The null sink discards all rows. In a real pipeline, replace this with a JDBC
sink writing to a target table, or a Parquet sink writing delta files to object
storage. The HWM pattern works with any sink type.

---

## How to run

### Prerequisites

- `flogo-datapipeline` on your `PATH`. Build from the repo root:

  ```bash
  go build -o flogo-datapipeline ./cmd/flogo-datapipeline
  export PATH="$PATH:$(pwd)"
  ```

- Docker + Docker Compose for the local PostgreSQL service used by the setup
  and verification scripts:

  ```bash
  # macOS
  brew install --cask docker

  # Debian / Ubuntu
  # Install Docker Engine + the Compose plugin:
  # https://docs.docker.com/engine/install/

  # Verify the Compose plugin is available
  docker compose version
  ```

- Run from inside the example directory:

  ```bash
  cd examples/12-incremental-hwm
  ```

### Step-by-step walkthrough

#### Step 1 — Start PostgreSQL and seed the database

```bash
./setup.sh
```

Expected output:

```
==> Starting PostgreSQL (docker compose up -d) ...
==> Waiting for PostgreSQL to become ready ...
    PostgreSQL is ready.
==> Creating schema in database pipelinedemo ...
==> Inserting 50 older events (2025-06-01 to 2025-06-03) ...
==> Inserting 50 more recent events (2025-06-04 to 2025-06-05) ...

==> Database ready.
100 rows in raw_events

    Timestamp range:
    earliest: 2025-06-01 08:01:00
    latest:   2025-06-05 15:30:00

    (No watermark file exists yet — run1.sh will process all 100 rows)
```

#### Step 2 — First pipeline run (full initial load)

```bash
./run1.sh
```

Expected output:

```
==> Run 1: Full initial load (no watermark on record)

    Current watermark: <none — this is the first run>

    Rows in raw_events: 100
    Expected rows processed: 100  (all rows pass — no prior HWM)

--- Running pipeline ---
[engine] pipeline "incremental-hwm" started
[source/jdbc] driver=postgres dsn=postgres://postgres:postgres@localhost:5432/pipelinedemo table=raw_events keyColumn=event_id
[transform/hwm] backend=file path=./watermarks/events.hwm
[transform/hwm] no prior watermark — all rows will pass
[sink/null] discarding all rows
[engine] pipeline finished  rows_in=100 rows_out=100 rows_dropped=0
--- Pipeline finished ---

==> Watermark file written at: ./watermarks/events.hwm
    Content:
    {"column":"created_at","value":"2025-06-05 15:30:00","updatedAt":"2025-06-05T15:30:00Z"}

    The next run will only process rows with created_at > the value above.

    Next step: run ./add-new-events.sh to insert 20 new rows, then ./run2.sh
```

After this run, `watermarks/events.hwm` contains the timestamp of the most
recent row in the initial dataset.

#### Step 3 — Simulate new data arriving

```bash
./add-new-events.sh
```

Expected output:

```
==> Current watermark:
    {"column":"created_at","value":"2025-06-05 15:30:00","updatedAt":"2025-06-05T15:30:00Z"}

    Rows before insert: 100
    Rows after insert:  120  (+20 new rows)

    New rows span: 2025-06-06 08:00:00 → 2025-06-06 14:00:00
    All new timestamps are AFTER the recorded watermark.

    Next step: run ./run2.sh — the pipeline will process only these 20 new rows.
```

#### Step 4 — Second pipeline run (incremental delta load)

```bash
./run2.sh
```

Expected output:

```
==> Run 2: Incremental delta load (HWM resume)

    Watermark BEFORE run:
    {"column":"created_at","value":"2025-06-05 15:30:00","updatedAt":"2025-06-05T15:30:00Z"}

    Total rows in table:  120
    Rows after watermark: 20   (these will be processed)
    Rows skipped by HWM:  100

--- Running pipeline ---
[engine] pipeline "incremental-hwm" started
[source/jdbc] driver=postgres dsn=postgres://postgres:postgres@localhost:5432/pipelinedemo table=raw_events keyColumn=event_id
[transform/hwm] backend=file path=./watermarks/events.hwm
[transform/hwm] loaded watermark: column=created_at value="2025-06-05 15:30:00"
[transform/hwm] filtering rows with created_at > "2025-06-05 15:30:00"
[sink/null] discarding all rows
[engine] pipeline finished  rows_in=120 rows_out=20 rows_dropped=100
--- Pipeline finished ---

==> Watermark file AFTER run:
    {"column":"created_at","value":"2025-06-06 14:00:00","updatedAt":"2025-06-06T14:00:00Z"}

    The watermark has advanced to 2025-06-06 14:00:00.
    A third run with no new data would process 0 rows.
```

Only the 20 new rows are processed. The original 100 rows are dropped by the
HWM transform. The watermark is advanced to `2025-06-06 14:00:00`.

---

## Verifying the watermark file manually

After `run1.sh` completes, inspect the watermark:

```bash
cat ./watermarks/events.hwm
```

```json
{
  "column": "created_at",
  "value": "2025-06-05 15:30:00",
  "updatedAt": "2025-06-05T15:30:00Z"
}
```

After `run2.sh`, the value advances:

```json
{
  "column": "created_at",
  "value": "2025-06-06 14:00:00",
  "updatedAt": "2025-06-06T14:00:00Z"
}
```

To manually verify which rows would pass the HWM filter in run 2:

```bash
docker compose exec -T postgres psql -U postgres -d pipelinedemo -tAc \
  "SELECT COUNT(*) FROM raw_events WHERE created_at > '2025-06-05 15:30:00';"
# Expected: 20
```

To confirm the boundary row (the last row from run 1) is excluded:

```bash
docker compose exec -T postgres psql -U postgres -d pipelinedemo -c \
  "SELECT event_id, created_at FROM raw_events
   WHERE created_at >= '2025-06-05 15:00:00'
   ORDER BY created_at LIMIT 5;"
```

```
 event_id |     created_at
----------+---------------------
      100 | 2025-06-05 15:30:00   ← boundary row (processed in run 1, excluded in run 2)
      101 | 2025-06-06 08:00:00   ← first new row (processed in run 2)
      102 | 2025-06-06 08:01:10
```

---

## Resetting the example

To start over from scratch:

```bash
docker compose down -v            # stop Postgres and drop its data volume
rm -f ./watermarks/events.hwm
./setup.sh
```

To reset just the watermark (re-process all rows without recreating the DB):

```bash
rm -f ./watermarks/events.hwm
```

Deleting the watermark file causes the next pipeline run to behave like a first
run — it will process all rows and write a new watermark.

---

## Common use cases for the HWM pattern

| Use case | HWM column | Notes |
|---|---|---|
| Nightly delta load from OLTP | `updated_at` | Picks up only rows modified today |
| Event log streaming | `event_timestamp` | Append-only; HWM is monotonically increasing |
| Audit trail extraction | `log_id` (integer) | Use integer HWM for auto-increment keys |
| API response archiving | `created_date` | Date-granular HWM for daily batch jobs |
| CDC without Debezium | `created_at` | Works when rows are never updated after insert |

---

## HWM backend options

Incremental loading is configured in the pipeline's top-level `incremental`
block. The `storeType` selects where the watermark is persisted between runs,
and `storeOptions` carries the backend connection details.

### File backend (this example)

```json
"incremental": {
  "mode": "hwm",
  "column": "created_at",
  "valueType": "datetime",
  "storeType": "file",
  "storeOptions": { "path": "./watermarks" }
}
```

Good for: local development, single-node pipelines, simple batch jobs.
Not suitable for: multi-node deployments (the file is local to one machine).
When `path` is omitted the OS temp directory is used.

### Redis backend

```json
"incremental": {
  "mode": "hwm",
  "column": "created_at",
  "valueType": "datetime",
  "storeType": "redis",
  "storeOptions": {
    "addr": "localhost:6379",
    "keyPrefix": "flogo:pipeline:hwm",
    "passwordEnv": "REDIS_PASSWORD"
  }
}
```

Good for: distributed deployments where multiple pipeline instances share the
same watermark. The mark is stored at `{keyPrefix}:{pipelineRef}`.

### PostgreSQL backend

```json
"incremental": {
  "mode": "hwm",
  "column": "created_at",
  "valueType": "datetime",
  "storeType": "postgres",
  "storeOptions": {
    "dsn": "host=localhost port=5432 dbname=pipeline_state user=etl password=secret sslmode=disable"
  }
}
```

Good for: teams already running PostgreSQL who want watermarks stored alongside
other pipeline metadata, with full ACID guarantees. The backend upserts into a
`pipeline_watermarks` table (created automatically).

> **Note:** `valueType` (`datetime` | `int` | `string`) tells the engine how to
> order the watermark column. It must match the column's real type for the
> `column > mark` predicate to compare correctly.

---

## Adapting this example

### Use an integer key as the watermark column

If your table uses an auto-increment primary key instead of a timestamp, use
that key as the `column`:

```json
{
  "type": "hwm",
  "column": "event_id",
  "backend": "file",
  "backendConfig": {
    "path": "./watermarks/events.hwm"
  }
}
```

Integer HWMs are simpler and avoid timezone ambiguity. Use them when the
integer key is guaranteed to increase monotonically with insertion order.

### Add a map transform after the HWM filter

Because the HWM transform only filters — it does not change the schema — you
can chain any subsequent transforms on the surviving rows:

```json
"transforms": [
  {
    "type": "hwm",
    "column": "created_at",
    "backend": "file",
    "backendConfig": { "path": "./watermarks/events.hwm" }
  },
  {
    "type": "map",
    "fields": {
      "id":          "=$event_id",
      "type":        "=$event_type",
      "uid":         "=$user_id",
      "page":        "=$page",
      "duration_s":  "=divide($.duration_ms, 1000)",
      "ts":          "=$created_at"
    }
  }
]
```

### Write to a real sink

Replace the null sink with a JDBC sink to upsert the new rows into a target
table:

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "postgres://postgres:postgres@localhost:5432/pipelinedemo?sslmode=disable",
  "table": "events_processed",
  "batchSize": 200,
  "onConflict": "upsert",
  "conflictKey": ["id"]
}
```

### Add a validate step before the sink

Enforce data quality on the incoming events before writing them downstream:

```json
{
  "type": "validate",
  "rules": [
    { "field": "event_id",    "notNull": true },
    { "field": "user_id",     "notNull": true },
    { "field": "duration_ms", "min": 0 },
    { "field": "status",      "in": ["ok", "error", "timeout"] }
  ],
  "onViolation": "drop"
}
```

---

## Key configuration reference

### Source (JDBC)

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `type` | string | yes | — | `"jdbc"` |
| `driver` | string | yes | — | `"postgres"` or `"mysql"` |
| `dsn` | string | yes | — | Connection string or file path |
| `table` | string | yes | — | Source table name |
| `keyColumn` | string | yes | — | Column used for keyset pagination (must be indexed) |
| `batchSize` | int | no | `1000` | Rows per page in the keyset cursor |
| `partitions` | int | no | `1` | Parallel read partitions (requires unique key distribution) |

### HWM transform

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `type` | string | yes | — | `"hwm"` |
| `column` | string | yes | — | Column to track; must be monotonically increasing |
| `backend` | string | yes | — | `"file"`, `"redis"`, or `"postgres"` |
| `backendConfig.path` | string | yes (file) | — | Filesystem path for the `.hwm` JSON file |
| `backendConfig.addr` | string | yes (redis) | — | Redis server address (`host:port`) |
| `backendConfig.key` | string | yes (redis) | — | Redis key name for the watermark |
| `backendConfig.db` | int | no (redis) | `0` | Redis database index |
| `backendConfig.dsn` | string | yes (postgres) | — | PostgreSQL connection string |
| `backendConfig.table` | string | yes (postgres) | — | Table name for watermark storage |
| `backendConfig.key` | string | yes (postgres) | — | Row key identifying this pipeline's watermark |

### Pipeline-level fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `mode` | string | yes | — | `"etl"` — source → transform chain → sink |
| `performance.batchSize` | int | no | `5000` | RecordBatch size (rows in memory per batch) |
| `performance.maxMemoryMB` | int | no | `512` | Soft memory ceiling for the pipeline engine |
| `scaling.mode` | string | no | `"single"` | `"single"` = one goroutine, no partitioning |
| `checkpoint.type` | string | no | `"none"` | `"none"` = no restart-resume; HWM itself serves as the resume point |

---

## Troubleshooting

### The second run processes all 100 rows again

The watermark file was deleted or is missing. Check:

```bash
ls -la ./watermarks/
cat ./watermarks/events.hwm
```

If the file is absent, the transform treats this as a first run. Re-run
`run1.sh` to re-establish the watermark.

### The second run processes 0 rows

The new events were inserted with timestamps that are at or before the
recorded watermark. Verify the `created_at` values of the new rows:

```bash
docker compose exec -T postgres psql -U postgres -d pipelinedemo -c \
  "SELECT MIN(created_at), MAX(created_at)
   FROM raw_events
   WHERE event_id > 100;"
```

All values must be strictly greater than the watermark value stored in
`events.hwm`.

### Late-arriving data is missed

If a row is inserted with a `created_at` timestamp in the past (earlier than
the current HWM), the HWM transform will drop it — it appears to be "already
processed". This is a fundamental limitation of the HWM pattern.

Mitigations:
- Add a configurable lookback window: `"lookbackDuration": "PT1H"` causes the
  HWM filter to use `max(hwm - 1 hour, stored_hwm)` as the lower bound.
- Switch to a full CDC system (Debezium) that captures the database WAL and
  can recover late arrivals.
- Use an `updated_at` column (instead of `created_at`) if your system updates
  that field whenever a row is modified; late-arriving corrections will then
  have a fresh `updated_at` and will not be missed.
