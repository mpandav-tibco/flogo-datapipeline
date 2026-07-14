# Pipeline Settings — Behavior Reference

How every property in the Studio **⚙ Pipeline Settings** panel maps to engine
behavior, and how each was validated. All configuration below is set through the
UI panel (no hand-edited JSON).

Every run emits a one-line **run-config echo** so operators can confirm what was
honored, e.g.:

```
run config — mode=etl batchSize=5000 bufferBatches=4 maxMemoryMB=512 transformWorkers=1 scaling=single checkpoint=file(rows=10000,secs=30) incremental=none metrics=prometheus
```

---

## Execution mode

| Value | Behavior | Observable |
|-------|----------|-----------|
| `etl` (default) | Transform chain runs in-flight between source and sink. | `written` reflects filtered/transformed rows. |
| `el` | Raw extract-load. Transform pipeline (incl. schema-drift) is **bypassed**; declared transforms are ignored. | `WARN mode=el — N declared transform(s) will be ignored`; `written == processed`. |
| `elt` | Load raw now, transform in-target later. In-flight transforms bypassed. | `INFO mode=elt — N transform(s) not run in-flight`. |

**Validated (UI):** `etl` → written=4 (filter dropped 3, map/lookup applied);
`el` → written=7 with the ignore warning. Both echoed in the run-config line.

---

## Reliability · Checkpoint & resume

| Property | Behavior |
|----------|----------|
| `type` = `none` / `file` / `redis` | Selects the checkpoint store. `none` disables resume. |
| `path` (file) | Directory for `{runId}.ckpt.json`. Defaults to the OS temp dir. |
| `addr` / `keyPrefix` / `passwordEnv` (redis) | Redis connection; key = `{keyPrefix}:{runId}`. |
| `intervalRows` (default 10000) | Save progress after N rows. |
| `intervalSeconds` (default 30) | **Also** save on this wall-clock interval — bounds re-processed work for slow, low-volume streams that never reach the row threshold. |
| `deleteOnSuccess` | Remove the checkpoint on a clean finish (fresh load next run). |

**Validated (UI):** checkpoint file written as
`{"format":"sectioned","payload":null,"rowsProcessed":7}`; `intervalSeconds`
wired as a time-based trigger alongside `intervalRows`.

---

## Reliability · Incremental load (HWM)

| Property | Behavior |
|----------|----------|
| `mode` = `hwm` | Loads the last watermark, injects `hwmColumn/hwmValue/hwmValueType` into the source so HWM-aware sources (jdbc) append a `WHERE col > mark` predicate. Requires an `hwm` transform to track the in-batch max. |
| `column` / `valueType` / `initialValue` | Watermark column, ordering type, and first-run start value. |
| `storeType` = `file` / `redis` / `postgres` | Where the mark is persisted between runs. |

Observable: `INFO incremental load — {col} > {value}` on the 2nd run;
processed drops to only the new rows.

---

## Load strategy · Performance

| Property | Status | Behavior |
|----------|--------|----------|
| `batchSize` (5000) | **Wired** | Rows per `ReadBatch`. Echoed and drives batch metrics. |
| `sourceBufferBatches` (4) | **Wired** | Depth of the read-ahead buffer. A dedicated reader goroutine reads ahead so the source overlaps with transform+write (source never blocks the pipeline). |
| `maxMemoryMB` (512) | **Wired** | Backpressure: the reader blocks when buffered in-flight bytes exceed this budget, bounding memory. |
| `transformWorkers` (1) | **Ordered-safe** | Reads are overlapped via read-ahead. True multi-core transform parallelism is **not** enabled because out-of-order batch completion would corrupt the checkpoint cursor and stateful transforms (HWM/DQ/drift). A notice is logged when >1 so the behavior is explicit. |
| `sinkBufferBatches` (4) | Reserved | Accepted; sink writes stay ordered for checkpoint correctness. |
| `rowGroupParallelism` (4) | Source-internal | Consumed by the Parquet source for concurrent row-group reads, not the engine loop. |

**Validated (UI):** `batchSize`, `bufferBatches`, `maxMemoryMB`, `transformWorkers`
all appear in the run-config echo; read-ahead + backpressure exercised by the
integration suite.

---

## Scaling

| Value | Behavior |
|-------|----------|
| `single` (default) | One instance processes all data. |
| `sharded` | Static assignment: this instance owns partitions where `index % instanceCount == instanceIndex`. No coordinator needed. Reveals `instanceIndex`, `instanceCount`, `partitionStrategy`. |
| `distributed` | Dynamic assignment via a shared **Postgres** work queue: every instance competes to claim partitions; failed partitions are re-queued up to `maxRetries`. Reveals the coordinator block (type, DSN, lease/heartbeat/retries). |

Only **jdbc** and **object-store** (s3/gcs/adls) sources are partitionable.
Non-partitionable sources (csv, sectioned, …) log a graceful fallback and run
single-instance. Redis coordinator is not yet implemented (use Postgres).

**Validated (UI):** `scaling=sharded` echoed and executed; sectioned source →
`WARN … not partitionable — falling back to single-instance execution`.
Positive multi-partition assignment (`instance i/N owns M partitions`) applies to
jdbc/object-store sources; `ShardedPartitions` math is unit-tested.

---

## Observability · Metrics

| Value | Behavior |
|-------|----------|
| `none` (default) | Zero-cost no-op recorder. |
| `prometheus` | Starts an HTTP scrape endpoint at `listenAddr` (default `:9090`) + `path` (default `/metrics`). |
| `otel` | OTLP/gRPC export to `endpoint` (default `localhost:4317`). |
| `stallSeconds` (30) | Warn + `flogo_pipeline_stalls_total` when a stage batch exceeds the threshold. `-1` disables. |

Exposed metrics include `flogo_pipeline_rows_read/transformed/written_total`,
`flogo_pipeline_batch_duration_seconds`, `flogo_pipeline_runs_total`,
`flogo_pipeline_throughput_rows_per_second`, `flogo_pipeline_checkpoints_saved_total`.

**Validated (UI):** `prometheus` with `listenAddr=:9095` →
`curl http://localhost:9095/metrics` returned live
`flogo_pipeline_batch_duration_seconds` histograms — the custom port set in the
UI was honored end-to-end.
