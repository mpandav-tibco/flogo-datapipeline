# Example 19 — Full Production Pipeline (Kitchen Sink)

**Mode:** ETL (Extract, Transform, Load)
**Source:** JDBC — PostgreSQL (production RDBMS claims intake table `raw_claims`)
**Sink:** JDBC — PostgreSQL with upsert (enriched claims warehouse table `claims_processed`)
**DLQ Sink:** Null (invalid claims discarded; swap to a DLQ table in production)
**Database:** A single local PostgreSQL 16 instance (database `pipelinedemo`) holds both tables, started via `docker-compose.yml`.
**Features demonstrated:** All major pipeline features combined — HWM, validate + DLQ routing, data quality, HTTP lookup enrichment, field mapping, JDBC upsert, file checkpoint, and the interaction between HWM and checkpoint for reliable incremental loads.

---

## Overview

This example is the reference architecture for enterprise insurance claims ETL. It deliberately uses every significant feature of the flogo-datapipeline engine in a realistic, end-to-end workflow. Use it as:

- A **starting template** when building a new production pipeline.
- A **learning reference** to see how individual features compose.
- A **test harness** to validate that all features work together on your target platform.

### The business scenario

An insurance company receives raw claims from field agents into a `raw_claims` table. Claims arrive continuously throughout the day. A scheduled pipeline must:

1. **Pick up only new claims** since the last run (HWM on `submitted_at`).
2. **Reject malformed or out-of-range claims** (validate + DLQ routing) so bad data never pollutes the warehouse.
3. **Measure dataset quality** (DQ) to alert the operations team if the incoming data quality drops below 98%.
4. **Enrich each claim** with policyholder identity (name, email) from an internal policy registry — simulated here with JSONPlaceholder.
5. **Normalize the schema** — amounts arrive in cents, the warehouse expects dollars; apply coalesce defaults for optional lookup fields.
6. **Upsert into the warehouse** — re-running the pipeline for the same claim must not create duplicates.
7. **Survive failure** — if the pipeline crashes mid-batch, a file checkpoint allows it to resume from the last safe point without reprocessing already-committed rows.

---

## Directory Layout

```
19-full-production/
├── full-production.datapipeline   # Pipeline definition (JSON)
├── docker-compose.yml             # Local PostgreSQL 16 service (database: pipelinedemo)
├── watermarks/                    # HWM state files (written by the engine)
├── checkpoints/                   # Checkpoint tokens (written by the engine)
├── setup.sh                       # Start PostgreSQL and seed both tables
├── run.sh                         # Execute the pipeline
├── add-claims.sh                  # Insert 20 new claims (incremental load demo)
├── verify.sh                      # Inspect the enriched output
└── README.md                      # This file
```

> `watermarks/` and `checkpoints/` contents are generated at runtime and are not committed to version control. The PostgreSQL data lives in a Docker volume managed by `docker-compose.yml`.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    full-production.datapipeline                             │
│                                                                             │
│  ┌──────────────────┐                                                       │
│  │   JDBC Source    │  driver: postgres                                     │
│  │   pipelinedemo   │  table: raw_claims    keyColumn: claim_id             │
│  │   raw_claims     │  batchSize: 500                                       │
│  └────────┬─────────┘                                                       │
│           │ 100 rows (80 valid + 20 invalid)                                │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  1. HWM Filter   │  column: submitted_at   backend: file                │
│  │  (incremental)   │  → First run: all rows pass (no watermark yet)       │
│  │                  │  → Subsequent runs: only rows AFTER last max          │
│  └────────┬─────────┘                                                       │
│           │ up to 100 rows (first run)                                      │
│           ▼                                                                 │
│  ┌──────────────────┐          ┌───────────────┐                           │
│  │  2. Validate     │─ FAIL ──▶│   DLQ Sink    │  type: null               │
│  │  claim_id pattern│          │ (20 bad rows) │  (swap to jdbc/csv in     │
│  │  amount range    │          └───────────────┘   production)             │
│  │  status enum     │                                                       │
│  └────────┬─────────┘                                                       │
│           │ 80 valid rows                                                   │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  3. DQ Check     │  expectations: claim_id not_null, amount range       │
│  │  (measurement)   │  threshold: 0.98   onFail: report                    │
│  │                  │  → Emits quality ratio to log; does NOT drop rows    │
│  └────────┬─────────┘                                                       │
│           │ 80 rows (DQ does not filter)                                    │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  4. HTTP Lookup  │  url: .../users/{policy_id}                          │
│  │  (enrichment)    │  concurrency: 10   cacheSize: 5000   onError: null   │
│  │                  │  → Adds: name, email from policy registry             │
│  └────────┬─────────┘                                                       │
│           │ 80 rows + name, email columns                                   │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │  5. Map          │  amount_usd = divide($.amount, 100)                  │
│  │  (normalize)     │  claimant_name  = coalesce($.name,  'UNKNOWN')       │
│  │                  │  claimant_email = coalesce($.email, 'UNKNOWN')       │
│  └────────┬─────────┘                                                       │
│           │ 80 rows (canonical output schema)                               │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │   JDBC Sink      │  driver: postgres table: claims_processed            │
│  │  pipelinedemo     │  onConflict: upsert   conflictKey: [claim_id]       │
│  │  claims_processed│  batchSize: 200                                       │
│  └──────────────────┘                                                       │
│                                                                             │
│  Checkpoint: file   path: ./checkpoints   intervalRows: 500                 │
│  HWM watermark is updated AFTER each batch commits to the sink.             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Source Schema

Table `raw_claims` in the `pipelinedemo` PostgreSQL database:

```sql
CREATE TABLE raw_claims (
    claim_id      TEXT    NOT NULL,   -- CLM-XXXXXXXX format; 8 digits after the prefix
    policy_id     INTEGER NOT NULL,   -- FK to policyholder (1–10 in this example)
    amount        INTEGER NOT NULL,   -- Claim value in CENTS (pipeline converts to dollars)
    status        TEXT    NOT NULL,   -- PENDING | APPROVED | REJECTED | UNDER_REVIEW
    incident_date TEXT    NOT NULL,   -- YYYY-MM-DD
    submitted_at  TEXT    NOT NULL,   -- ISO-8601 datetime; used as the HWM column
    adjuster_id   INTEGER             -- Internal staff ID; NULL for new PENDING claims
);
```

### Sample data composition

| Batch | Count | Description | Pipeline outcome |
|-------|-------|-------------|-----------------|
| Valid claims | 80 | CLM-00000001 to CLM-00000080; amount 100 to 9,000,000 cents; submitted Jan–Jun 2025 | Enriched and upserted into `claims_processed` |
| Invalid claim_id | 10 | Patterns like `bad_id_01`, `BAD-00000001`, `CLM-1234` that fail `^CLM-[0-9]{8}$` | Routed to DLQ sink |
| Out-of-range amount | 10 | CLM-00000091 to CLM-00000100; amount=0 or amount>10,000,000 | Routed to DLQ sink |

---

## Sink Schema

Table `claims_processed` in the `pipelinedemo` PostgreSQL database:

```sql
CREATE TABLE claims_processed (
    claim_id       TEXT  NOT NULL PRIMARY KEY,
    policy_id      INTEGER NOT NULL,
    amount_usd     REAL    NOT NULL,   -- amount / 100 (dollars)
    claimant_name  TEXT    NOT NULL,   -- from HTTP lookup; 'UNKNOWN' on error
    claimant_email TEXT    NOT NULL,   -- from HTTP lookup; 'UNKNOWN' on error
    status         TEXT    NOT NULL,
    processed_at   TEXT    NOT NULL    -- copied from submitted_at
);
```

---

## Transform-by-Transform Explanation

### 1. HWM (High-Water Mark) — Incremental Load Gate

```json
{
  "type": "hwm",
  "column": "submitted_at",
  "backend": "file",
  "backendConfig": { "path": "./watermarks" }
}
```

**What it does:** Before the first batch is read, the HWM transform checks `./watermarks/` for a persisted watermark value. If none exists, all rows pass through. After the run completes successfully, it writes the maximum `submitted_at` value seen in the batch to the watermark file.

**On subsequent runs:** The engine injects a `WHERE submitted_at > '<last_watermark>'` clause into the source query. Only rows with a `submitted_at` newer than the watermark are fetched. This makes the pipeline naturally idempotent and incremental.

**Interaction with checkpoint:** The checkpoint and HWM work at different granularities:

| Mechanism | Granularity | Purpose |
|-----------|-------------|---------|
| Checkpoint | Row-level | Resume mid-batch after crash; re-uses the same HWM window |
| HWM | Batch-level | Skip entire previous batches on the next scheduled run |

On a crash mid-batch, the HWM is NOT updated (it is only updated on successful completion). The checkpoint allows the engine to skip the rows it already committed within the current HWM window, without re-opening the previous window. This means you get exactly-once delivery within a scheduling period and at-least-once delivery across scheduling periods (the current interrupted batch is safely retried).

### 2. Validate — Field-Level Quality Gate + DLQ Routing

```json
{
  "type": "validate",
  "rules": [
    { "field": "claim_id",  "notNull": true, "pattern": "^CLM-[0-9]{8}$" },
    { "field": "policy_id", "notNull": true },
    { "field": "amount",    "notNull": true, "min": 1, "max": 10000000 },
    { "field": "status",    "in": ["PENDING", "APPROVED", "REJECTED", "UNDER_REVIEW"] }
  ],
  "onViolation": "route",
  "errorSinkType": "null",
  "errorSinkConfig": {}
}
```

**What it does:** Each row is tested against all four rules. The first rule violation causes the row to be routed to the `dlqSink` (defined at the top level of the pipeline file) instead of the main sink. The remaining transforms are skipped for that row. Rows that pass all rules continue to the DQ transform.

**Why `onViolation: "route"` instead of `"drop"` or `"fail"`:**

| `onViolation` | Behavior |
|---------------|---------|
| `"drop"` | Invalid row is silently discarded; no record of it |
| `"route"` | Invalid row is written to `dlqSink` with the violation reason attached; auditable |
| `"fail"` | Pipeline aborts immediately; use for zero-tolerance data contracts |

In production, the `dlqSink` would point to a `raw_claims_dlq` table so operations teams can inspect, fix, and replay rejected claims. This example uses `type: null` to keep the demo dependency-free.

**The 20 bad rows in this example:**

- 10 rows with claim_id that does not match `^CLM-[0-9]{8}$` (wrong prefix, wrong length, non-digit characters).
- 10 rows with amount outside the valid range [1, 10,000,000] (zero, negative, or over the policy maximum).

### 3. DQ (Data Quality) — Dataset-Level Measurement

```json
{
  "type": "dq",
  "expectations": [
    { "column": "claim_id", "check": "not_null", "value": null },
    { "column": "amount",   "check": "range",    "value": { "min": 1, "max": 10000000 } }
  ],
  "threshold": 0.98,
  "onFail": "report",
  "reportSinkType": "null"
}
```

**What it does:** After validate has already removed invalid rows, DQ runs a second measurement pass over the surviving rows. It scores each row against each expectation and computes a dataset-level quality ratio: `(passing_checks / total_checks)`. If the ratio falls below `0.98`, it triggers the `onFail` action.

**DQ vs. Validate — what is the difference?**

| Feature | Validate | DQ |
|---------|----------|----|
| Scope | Row-level gate | Dataset-level measurement |
| Failed rows | Routed to DLQ or dropped | All rows continue to sink |
| Failure action | Per-row routing | Batch-level report/abort |
| Primary use | Enforce data contracts | Monitor data health trends |

In this pipeline, DQ is positioned AFTER validate so it measures only the rows that passed validation. Its threshold of 0.98 means the pipeline will log a quality warning if more than 2% of post-validation rows fail a DQ expectation. With the 80 valid rows in this example, the expected quality ratio is 100% (all 80 pass both expectations).

In production, you would set `onFail: "abort"` for mission-critical pipelines or `onFail: "report"` for observability-only monitoring where you want the pipeline to continue but alert on degradation.

### 4. Lookup — HTTP Enrichment with LRU Cache

```json
{
  "type": "lookup",
  "url": "https://jsonplaceholder.typicode.com/users/{policy_id}",
  "method": "GET",
  "keyField": "policy_id",
  "outputFields": ["name", "email"],
  "concurrency": 10,
  "cacheSize": 5000,
  "timeoutMs": 3000,
  "onError": "null"
}
```

**What it does:** For each row, the lookup transform calls the URL with the `policy_id` substituted into the `{policy_id}` placeholder. The JSON response is parsed and the fields listed in `outputFields` (`name`, `email`) are added to the row.

**LRU cache:** The engine maintains an in-memory LRU cache of size `cacheSize`. The first time `policy_id=3` is seen, the engine fetches `https://jsonplaceholder.typicode.com/users/3` and caches the result. Every subsequent row with `policy_id=3` in the same pipeline run is served from the cache without an HTTP call. With 80 valid claims distributed across 10 policy IDs, at most 10 HTTP calls are made regardless of how many rows exist.

**Concurrency:** Up to 10 HTTP requests can be in-flight simultaneously. The engine uses a semaphore to cap concurrency. For a cold cache with 10 unique policy IDs and concurrency=10, all 10 fetches may execute in parallel, then every subsequent row is served from the cache.

**`onError: "null"`:** If the HTTP call fails (network timeout, 404, 500), the lookup fields are set to JSON `null`. The downstream map transform uses `coalesce` to replace `null` with the string `"UNKNOWN"`, so the row is never lost due to an enrichment failure.

### 5. Map — Schema Normalization

```json
{
  "type": "map",
  "fields": {
    "claim_id":       "=$claim_id",
    "policy_id":      "=$policy_id",
    "amount_usd":     "=divide($.amount,100)",
    "claimant_name":  "=coalesce($.name,'UNKNOWN')",
    "claimant_email": "=coalesce($.email,'UNKNOWN')",
    "status":         "=$status",
    "processed_at":   "=$submitted_at"
  }
}
```

**What it does:** Projects and transforms the enriched row into the canonical output schema expected by `claims_processed`. Only the named output fields are written to the sink; all other source fields (including `incident_date`, `adjuster_id`, and the raw `amount` in cents) are dropped.

**Key expressions:**

| Output field | Expression | Explanation |
|---|---|---|
| `amount_usd` | `=divide($.amount,100)` | Converts cents to dollars using the built-in `divide` function |
| `claimant_name` | `=coalesce($.name,'UNKNOWN')` | Uses lookup result if present; falls back to `"UNKNOWN"` |
| `claimant_email` | `=coalesce($.email,'UNKNOWN')` | Same pattern for email |
| `processed_at` | `=$submitted_at` | Preserves the original submission timestamp as the warehouse timestamp |

---

## Checkpoint and Resume

The pipeline is configured with a file-based checkpoint:

```json
"checkpoint": {
  "type": "file",
  "path": "./checkpoints",
  "intervalRows": 500,
  "intervalSeconds": 30,
  "deleteOnSuccess": false
}
```

**How it works:** After every 500 rows are committed to the sink (or every 30 seconds, whichever comes first), the engine writes a checkpoint token to `./checkpoints/`. The token encodes the last successfully committed position in the source stream.

**On restart after failure:** The engine reads the most recent checkpoint token, seeks the source to the recorded position, and resumes from there. Rows that were already committed before the checkpoint are skipped; rows after the checkpoint are reprocessed. No row is written twice to the sink because the JDBC sink uses `onConflict: upsert` — if a row is reprocessed, the upsert simply overwrites the existing row with identical data.

**`deleteOnSuccess: false`:** The checkpoint file is preserved after a successful run. This is useful for:
- Inspecting the checkpoint format for debugging.
- Manually rolling back to a previous checkpoint if a bad batch was committed.

In production, set `deleteOnSuccess: true` to keep the `checkpoints/` directory clean.

---

## How to Run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`.
- Docker and Docker Compose (the local PostgreSQL 16 service is defined in `docker-compose.yml`).
- Network access to `jsonplaceholder.typicode.com` (used by the lookup transform).

### Step 1 — Start PostgreSQL and seed both tables

```bash
chmod +x setup.sh run.sh add-claims.sh verify.sh
./setup.sh
```

Expected output:

```
==> Starting PostgreSQL (docker compose up -d) ...
==> Waiting for PostgreSQL to become ready ...
    PostgreSQL is ready.
==> Creating source table raw_claims in database pipelinedemo ...

==> Source table seeded successfully.

    Total claims inserted : 100
    Valid claims           : 80  (will be enriched and upserted)
    Invalid claims         : 20 (will be routed to DLQ)

    Status breakdown (all rows):
        status    | count
    --------------+-------
     APPROVED     |    26
     PENDING      |    38
     REJECTED     |    18
     UNDER_REVIEW |    18
    (4 rows)

    Date range:
    earliest submitted_at: 2025-01-03 08:14:22
    latest   submitted_at: 2025-06-30 17:00:00

==> Creating sink table claims_processed in database pipelinedemo ...
    Sink table 'claims_processed' created (empty — will be populated by the pipeline).
```

### Step 2 — First run (full initial load)

```bash
./run.sh
```

The pipeline processes all 100 rows. 80 valid claims are enriched and written to `claims_processed`. 20 invalid claims are routed to the DLQ sink (discarded in this example). The HWM watermark is set to `2025-06-30 17:00:00`.

Expected log output:

```
INFO  pipeline starting          name=full-production mode=etl
INFO  jdbc source opened         driver=postgres dsn=postgres://postgres:***@localhost:5432/pipelinedemo table=raw_claims
INFO  hwm filter                 column=submitted_at backend=file no_watermark=true
INFO  source batch               page=1 rows=100 last_key=CLM-00000100
INFO  validate                   rows_checked=100 rows_passed=80 rows_failed=20
INFO  dlq routed                 rows=20 reason="claim_id pattern|amount range"
INFO  dq check                   rows=80 ratio=1.000 threshold=0.98 status=PASS
INFO  lookup                     cache_hits=70 cache_misses=10 fetches=10
INFO  map                        rows=80
INFO  jdbc sink batch            rows=80 table=claims_processed conflict=upsert
INFO  checkpoint written         path=./checkpoints/checkpoint-001.json rows_committed=80
INFO  hwm updated                column=submitted_at new_value=2025-06-30T17:00:00
INFO  pipeline complete          rows_read=100 rows_valid=80 rows_dlq=20 rows_written=80 duration=2.1s
```

### Step 3 — Second run (HWM idempotence)

```bash
./run.sh
```

Because the watermark is now `2025-06-30 17:00:00` and all source rows have `submitted_at <= 2025-06-30 17:00:00`, zero rows pass the HWM filter. The pipeline exits immediately with:

```
INFO  pipeline starting          name=full-production mode=etl
INFO  hwm filter                 column=submitted_at watermark=2025-06-30T17:00:00
INFO  source                     rows_read=0 (all rows filtered by HWM)
INFO  pipeline complete          rows_read=0 rows_written=0 duration=42ms
```

This confirms that the pipeline is idempotent — running it again without new data produces no output and costs only the time to query the source.

### Step 4 — Add new claims (incremental load demo)

```bash
./add-claims.sh
```

This inserts 20 new claims (CLM-00000101 to CLM-00000120) with `submitted_at = 2025-07-15`, which is after the current HWM of `2025-06-30 17:00:00`.

### Step 5 — Third run (incremental load)

```bash
./run.sh
```

Only the 20 new rows pass the HWM filter. They are validated (all 20 are valid), DQ-checked, enriched, mapped, and upserted. The HWM advances to `2025-07-15 12:45:00`.

```
INFO  hwm filter                 column=submitted_at watermark=2025-06-30T17:00:00
INFO  source batch               page=1 rows=20 last_key=CLM-00000120
INFO  validate                   rows_checked=20 rows_passed=20 rows_failed=0
INFO  dq check                   rows=20 ratio=1.000 threshold=0.98 status=PASS
INFO  lookup                     cache_hits=10 cache_misses=10 fetches=10
INFO  map                        rows=20
INFO  jdbc sink batch            rows=20 table=claims_processed conflict=upsert
INFO  hwm updated                column=submitted_at new_value=2025-07-15T12:45:00
INFO  pipeline complete          rows_read=20 rows_valid=20 rows_dlq=0 rows_written=20 duration=1.2s
```

### Step 6 — Verify

```bash
./verify.sh
```

Sample output:

```
── 1. ROW COUNT & STATUS BREAKDOWN ─────────────────────────

status        claim_count  avg_usd      min_usd    max_usd
------------  -----------  -----------  ---------  ---------
PENDING       38           26844.74     1000.00    98000.00
APPROVED      24           32156.25     1200.00    90000.00
UNDER_REVIEW  18           38722.22     2000.00    98000.00
REJECTED      20           31850.00     1500.00    92000.00

    Total rows in claims_processed: 100

── 2. SAMPLE ENRICHED ROWS (first 10) ──────────────────────

claim_id      policy_id  amount_usd  claimant_name  claimant_email          status
------------  ---------  ----------  -------------  ----------------------  --------
CLM-00000001  3          2500.0      Clementine ...  Nathan@yesenia.net     PENDING
CLM-00000002  7          18000.0     Kurtis Weiss    Telly.Hoeger@billy...  APPROVED
...
```

### Teardown

Stop the PostgreSQL container and delete its data volume (all seeded and processed rows are discarded):

```bash
docker compose down -v
```

The `watermarks/` and `checkpoints/` directories are left in place; delete them manually or re-run `./setup.sh` to reset them.

---

## Adapting for Production

### Replace the DLQ sink with a real table

In this example `dlqSink.type` is `null` for simplicity. In production, route bad claims to an audit table:

```json
"dlqSink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "${CLAIMS_DB_DSN}",
  "table": "raw_claims_dlq",
  "batchSize": 200,
  "onConflict": "insert"
}
```

The engine appends a `_violation_reason` column to each DLQ row explaining which rule failed. Operations teams can query this table, fix the source data, and replay the corrected rows.

### Point at your production database

The example already uses PostgreSQL locally. To point at a production database (PostgreSQL or MySQL), change only `driver` and `dsn` in the source and sink:

```json
"source": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "${CLAIMS_SOURCE_DSN}",
  "table": "raw_claims",
  "keyColumn": "claim_id",
  "batchSize": 5000
}
```

Increase `batchSize` from 500 to 5,000–50,000 for network-attached databases where query round-trips are more expensive than local database reads.

### Replace the JSON placeholder lookup with your real policy registry

```json
{
  "type": "lookup",
  "url": "https://policy-registry.internal/api/v1/policies/{policy_id}",
  "method": "GET",
  "keyField": "policy_id",
  "outputFields": ["holder_name", "holder_email", "coverage_tier", "risk_score"],
  "concurrency": 20,
  "cacheSize": 100000,
  "timeoutMs": 5000,
  "onError": "null"
}
```

Increase `cacheSize` to match your cardinality. If your policy registry has 50,000 active policies and your batch contains claims for many of them, a `cacheSize` of 100,000 ensures the first batch warms the cache and all subsequent batches are served entirely from memory.

### Scale to distributed mode

Swap the `scaling` section to distribute the load across multiple engine instances:

```json
"scaling": {
  "mode": "sharded",
  "shards": 4,
  "coordinatorDsn": "${REDIS_URL}"
}
```

Each shard handles a non-overlapping `claim_id` key range. The HWM and checkpoint backends must also be switched to a shared store (Redis or PostgreSQL) so all shards read and write the same watermark:

```json
{
  "type": "hwm",
  "column": "submitted_at",
  "backend": "redis",
  "backendConfig": {
    "url": "${REDIS_URL}",
    "key": "claims:hwm:submitted_at"
  }
}
```

### Add Prometheus metrics

The engine exposes a `/metrics` endpoint on port 9091 by default when `performance.metrics` is enabled:

```json
"performance": {
  "batchSize": 500,
  "maxMemoryMB": 256,
  "transformWorkers": 2,
  "metrics": {
    "enabled": true,
    "port": 9091
  }
}
```

Scrape metrics at `http://localhost:9091/metrics`. Key counters to alert on:

| Metric | Alert condition |
|--------|----------------|
| `pipeline_rows_dlq_total` | > 0 (unexpected DLQ traffic) |
| `pipeline_dq_ratio` | < 0.98 (quality below threshold) |
| `pipeline_lookup_errors_total` | > 0 (policy registry degraded) |
| `pipeline_duration_seconds` | > SLA (pipeline taking too long) |

### Split into sub-pipelines

For very high volumes or independent failure domains, split this monolithic pipeline into three:

1. **Ingest pipeline** — JDBC source → validate → DLQ → raw validated table (no enrichment).
2. **Enrichment pipeline** — raw validated table → lookup → map → enriched table.
3. **Aggregation pipeline** — enriched table → aggregate transforms → summary table.

Each sub-pipeline can be scaled, checkpointed, and monitored independently. The coupling is through shared JDBC tables, which act as durable queues between stages.

---

## Configuration Reference

### Top-level fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | Yes | Pipeline identifier; used in log lines and metrics labels |
| `description` | string | No | Human-readable description |
| `version` | string | No | Semantic version for tracking config changes |
| `mode` | `"etl"` / `"elt"` / `"el"` | Yes | `"etl"` applies transforms between source and sink |
| `source` | object | Yes | Source connector configuration |
| `transforms` | array | No | Ordered list of transform steps; applied left to right |
| `sink` | object | Yes | Primary sink connector |
| `dlqSink` | object | No | Dead-letter sink; required when any validate uses `onViolation: "route"` |
| `performance` | object | No | Batch sizing, memory limits, worker counts |
| `scaling` | object | No | `mode: "single"` (default) or `mode: "sharded"` |
| `checkpoint` | object | No | Resume-after-failure configuration |

### JDBC source fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `driver` | `"postgres"` / `"mysql"` | — | Required |
| `dsn` | string | — | Connection string; env var substitution (`${VAR}`) supported |
| `table` | string | — | Required when `query` is absent |
| `query` | string | — | Custom SQL; overrides `table`; engine wraps as subquery for keyset pagination |
| `keyColumn` | string | — | Must be a unique, indexed, monotonically increasing column |
| `batchSize` | int | `5000` | Rows per paginated SELECT |
| `partitions` | int | `1` | Parallel read workers |

### HWM transform fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `column` | string | — | Source column to track; must be monotonically non-decreasing |
| `backend` | `"file"` / `"redis"` / `"postgres"` | — | Where the watermark is persisted |
| `backendConfig.path` | string | — | Directory (file backend); engine creates the file automatically |
| `backendConfig.url` | string | — | Redis URL or PostgreSQL DSN for shared backends |
| `backendConfig.key` | string | — | Redis key name (Redis backend only) |

### Validate transform fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `rules[].field` | string | — | Column name to check |
| `rules[].notNull` | bool | `false` | Fail if value is null or empty string |
| `rules[].min` | number | — | Minimum numeric value (inclusive) |
| `rules[].max` | number | — | Maximum numeric value (inclusive) |
| `rules[].minLength` | int | — | Minimum string length |
| `rules[].maxLength` | int | — | Maximum string length |
| `rules[].pattern` | string | — | Go regular expression; must match the full value |
| `rules[].in` | array | — | Value must be one of the listed strings |
| `onViolation` | `"route"` / `"drop"` / `"fail"` | — | Required |
| `errorSinkType` | string | — | Required when `onViolation: "route"` |
| `errorSinkConfig` | object | `{}` | Connector config for the error sink |

### DQ transform fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `expectations[].column` | string | — | Column to evaluate |
| `expectations[].check` | `"not_null"` / `"range"` / `"in_set"` | — | Type of check |
| `expectations[].value` | varies | `null` | `null` for `not_null`; `{min,max}` for `range`; `[...]` for `in_set` |
| `threshold` | float | — | Quality ratio below which `onFail` is triggered (0.0–1.0) |
| `onFail` | `"abort"` / `"report"` / `"pass"` | — | Action when ratio < threshold |
| `reportSinkType` | string | — | Optional; where to write the quality report |

### Lookup transform fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `url` | string | — | URL template; `{fieldName}` is replaced with the row's field value |
| `method` | `"GET"` | `"GET"` | HTTP method |
| `keyField` | string | — | Row field substituted into the URL template |
| `outputFields` | array | — | JSON response fields to extract and add to the row |
| `concurrency` | int | `5` | Max simultaneous in-flight HTTP requests |
| `cacheSize` | int | `1000` | LRU cache capacity (number of unique keys) |
| `timeoutMs` | int | `5000` | Per-request HTTP timeout in milliseconds |
| `onError` | `"null"` / `"skip"` / `"fail"` | — | Row behavior when HTTP call fails |

### Map transform fields

| Field | Type | Notes |
|-------|------|-------|
| `fields` | object | Map of `outputFieldName → expression`; expressions start with `=` |

Common expression patterns:

| Expression | Description |
|---|---|
| `=$fieldName` | Copy source field by name |
| `=$.fieldName` | Same; `$.` is the row object accessor |
| `=divide($.amount,100)` | Call built-in `divide` function |
| `=coalesce($.name,'UNKNOWN')` | Return first non-null value |
| `=upper($.status)` | String built-in |
| `=concat($.first,' ',$.last)` | String concatenation |

### JDBC sink fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `driver` | `"postgres"` / `"mysql"` | — | Required |
| `dsn` | string | — | Connection string |
| `table` | string | — | Target table; must exist before the pipeline runs |
| `batchSize` | int | `5000` | Rows per INSERT/UPSERT batch |
| `onConflict` | `"insert"` / `"upsert"` / `"replace"` / `"ignore"` | `"insert"` | Conflict resolution |
| `conflictKey` | array of strings | — | Required for `"upsert"` and `"replace"`; column(s) that define uniqueness |

### Checkpoint fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `type` | `"file"` / `"redis"` / `"none"` | `"none"` | Persistence backend |
| `path` | string | — | Directory for checkpoint files (file backend) |
| `intervalRows` | int | `10000` | Write checkpoint every N committed rows |
| `intervalSeconds` | int | `60` | Write checkpoint every N seconds (whichever fires first) |
| `deleteOnSuccess` | bool | `true` | Delete checkpoint files on clean pipeline exit |

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [12-incremental-hwm](../12-incremental-hwm/) | HWM in isolation — see how the watermark file is managed |
| [03-validate-and-dlq](../03-validate-and-dlq/) | Validate + DLQ routing with a CSV file DLQ output |
| [13-data-quality](../13-data-quality/) | DQ in isolation with multiple check types |
| [04-http-lookup-enrich](../04-http-lookup-enrich/) | Lookup in isolation — understand the cache and concurrency settings |
| [15-checkpoint-resume](../15-checkpoint-resume/) | Checkpoint + resume in isolation — interrupt and resume a long run |
| [16-scaling-sharded](../16-scaling-sharded/) | Distributed mode — split the key range across multiple engine instances |
