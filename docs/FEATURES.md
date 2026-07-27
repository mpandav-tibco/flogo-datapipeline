# Flogo Data Pipeline — Feature matrix

> **Per-activity detail now lives in its own page.** This page is a one-screen matrix
> linking to the authoritative reference for every source, sink, and transform (under
> [`docs/sources/`](sources/), [`docs/transforms/`](transforms/),
> [`docs/sinks/`](sinks/)), followed by the engine subsystems (checkpoint, watermark,
> metrics, scaling, pipeline definition). Everything is code-derived from the
> `init()`/`Register(...)` calls and config structs.

**At a glance:** 10 source families · 9 sink families (incl. DLQ) · 8 transforms ·
2 checkpoint backends · 3 watermark backends · 2 metrics backends · 3 scaling modes.

## Sources → [index](sources.md)

| Type(s) | Reads | Partitionable |
|---|---|:--:|
| [`csv`, `tsv`](sources/csv.md) | delimited text | — |
| [`jsonlines`, `ndjson`](sources/jsonlines.md) | newline-delimited JSON | — |
| [`xml`](sources/xml.md) | element-per-record XML | — |
| [`fixedwidth`, `flatfile`](sources/fixedwidth.md) | positional / EBCDIC | — |
| [`sectioned`](sources/sectioned.md) | header block + delimited body | — |
| [`parquet`](sources/parquet.md) | columnar Parquet | ✅ |
| [`jdbc`](sources/jdbc.md) | PostgreSQL / MySQL / SQLite | ✅ |
| [`kafka`](sources/kafka.md) | consumer-group topic | — |
| [`s3`, `gcs`, `adls`](sources/objstore.md) | cloud object storage | ✅ |
| [`podlog`](sources/podlog.md) | Kubernetes / CRI logs | — |

## Transforms → [index](transforms.md)

| Type(s) | Does |
|---|---|
| [`filter`](transforms/filter.md) | drop rows by a boolean expression |
| [`map`, `mapper`](transforms/map.md) | project / rename / coerce / derive |
| [`validate`](transforms/validate.md) | per-field rules → route / fail / drop |
| [`lookup`, `enrich`](transforms/lookup.md) | HTTP enrichment (LRU-cached) |
| [`parse`](transforms/parse.md) | text/JSON → columns (regex named groups) |
| [`dq`](transforms/dq.md) | data-quality expectations + score |
| [`schemadrift`](transforms/schemadrift.md) | detect add/remove/retype columns |
| [`hwm`](transforms/hwm.md) | track a column max for incremental loads |
| [`cast`](transforms/cast.md) | convert column types (string ↔ int / float / decimal / bool / date) |
| [`explode`](transforms/explode.md) | expand an array column into one row per element |
| [`flatten`](transforms/flatten.md) | flatten nested-object columns into dotted columns |

Plus the shared [expression-language reference](transforms/expression-language.md).

## Sinks → [index](sinks.md)

| Type(s) | Writes |
|---|---|
| [`csv`, `tsv`](sinks/csv.md) | delimited text |
| [`jsonlines`, `ndjson`](sinks/jsonlines.md) | one JSON object per line |
| [`parquet`](sinks/parquet.md) | typed, row-group-aligned Parquet |
| [`jdbc`](sinks/jdbc.md) | insert / upsert into SQL |
| [`kafka`](sinks/kafka.md) | producer with acks & serialization |
| [`template`](sinks/template.md) | Go text/template (fixed-width / legacy) |
| [`s3`, `gcs`, `adls`](sinks/objstore.md) | cloud object storage (macros, rolling) |
| [`null`, `discard`](sinks/null.md) | drop all rows |
| [`dlq`](sinks/dlq.md) | wrap any sink; capture rejects |

---

## Pipeline model

A pipeline is a single-source → transform-chain → single-sink dataflow, with an
optional dead-letter branch for rejected rows:

```
Source  →  [ Transform₁ → Transform₂ → … ]  →  Sink
                                                 ↓ (rejected / failed rows)
                                                DLQ Sink
```

The action (`action/pipeline`) streams the source in `RecordBatch`es of
`performance.batchSize` rows through the transform chain into the sink, saving
checkpoints and advancing watermarks as configured. There is no explicit
ETL/ELT/EL switch — the *mode* is emergent from which transforms you declare:

| Mode | Shape | Typical use |
|---|---|---|
| **ETL** | Source → transforms → Sink | filter/map/validate/dq before load |
| **ELT** | Source → minimal transform → Sink, logic in DB | load raw, transform in-database |
| **EL** | Source → Sink (no transforms) | bulk copy, format conversion |

---

## Sources

Registered via `source.Register(name, factory)`. **Per-source detail — every key,
default, behavior, and example — lives on the [per-source pages](sources/)** linked in
the matrix above. Sources marked ✅ in the matrix implement `PartitionableSource` and can
be split across [scaling](scaling.md) instances.

---

## Sinks

Registered via `sink.Register(name, factory)`. **Per-sink detail — every key, default,
key-template macro, partitioning/rolling behavior, credential resolution, and example —
lives on the [per-sink pages](sinks/)** linked in the matrix above. Any sink can be
wrapped by the [`dlq`](sinks/dlq.md) sink to capture rejected rows.

---

## Transforms

Registered via `transform.Register(name, factory)`, executed in declared order.
**Per-transform detail — every config key, rule/expectation type, routing behavior, and
example — lives on the [per-transform pages](transforms/)** linked in the matrix above,
and the full 60+-function map/filter expression catalogue is in the
[expression-language reference](transforms/expression-language.md).

---

## Checkpoint backends

Registered per backend; save/resume the source position so an interrupted run can
continue. Token = `{ format, payload, rowsProcessed }`; sources may implement
`CursorProvider` for O(1) seek instead of a linear skip.

### File — `file`
`checkpoint/file/checkpoint.go` — `path` (default temp dir). Atomic
temp-then-rename writes, one JSON file per partition. Single-instance.

### Redis — `redis`
`checkpoint/redis/checkpoint.go` — `addr` (`localhost:6379`), `keyPrefix`
(`flogo:pipeline:ckpt`), `passwordEnv`, `ttlSeconds` (`604800` = 7 days).
Multi-instance safe.

---

## Watermark backends (incremental load)

Persist the high-water mark for incremental (`mode: hwm`) pipelines. Mark =
`{ column, value, valueType, savedAt }`.

### File — `file`
`watermark/file/watermark.go` — `path` (temp dir). Atomic writes, one file per
pipeline. Single-instance.

### Redis — `redis`
`watermark/redis/watermark.go` — `addr`, `keyPrefix` (`flogo:pipeline:hwm`),
`passwordEnv`. Multi-instance safe; no TTL (manual reset).

### PostgreSQL — `postgres`
`watermark/postgres/watermark.go` — `dsn`. Auto-creates
`flogo_pipeline_watermarks`, upserts via `ON CONFLICT`. Multi-instance safe,
auditable (`saved_at`).

---

## Metrics backends

A process-wide recorder; when none is configured a `NoopRecorder` keeps overhead
at zero. Both backends expose the same logical metrics.

### Prometheus — `prometheus`
`metrics/prometheus/prometheus.go` — `listenAddr` (`:9090`), `path` (`/metrics`).

Emitted series:

| Metric | Type | Labels |
|---|---|---|
| `flogo_pipeline_rows_read_total` | counter | `pipeline`, `source_type` |
| `flogo_pipeline_rows_transformed_total` | counter | `pipeline` |
| `flogo_pipeline_rows_written_total` | counter | `pipeline`, `sink_type` |
| `flogo_pipeline_rows_failed_total` | counter | `pipeline` |
| `flogo_pipeline_batch_duration_seconds` | histogram | `pipeline`, `stage` |
| `flogo_pipeline_runs_total` | counter | `pipeline`, `status` |
| `flogo_pipeline_stalls_total` | counter | `pipeline`, `stage` |
| `flogo_pipeline_checkpoints_total` | counter | `pipeline` |
| `flogo_pipeline_throughput_gauge` | gauge | `pipeline` |

Components may emit custom metrics via the `Counter`/`Gauge` escape hatch.

### OpenTelemetry — `otel`
`metrics/otel/otel.go` — `endpoint` (`localhost:4317`), `insecure` (`true`),
`serviceName` (`flogo-datapipeline`). Same logical metrics exported over OTLP/gRPC
(durations in ms). Call `Shutdown` before process exit.

---

## Scaling & coordination

`scaling/coordinator.go` + `partition/workqueue`. `ResolveFromEnv()` reads
`INSTANCE_INDEX` / `INSTANCE_COUNT` (Kubernetes StatefulSet friendly).

| Mode | Behavior |
|---|---|
| `single` (default) | one instance processes everything |
| `sharded` | static partitioning: instance *i* claims partitions where `index % count == i` |
| `distributed` | dynamic work-queue: leader enqueues partitions, workers claim/heartbeat/complete |

`ScalingConfig`: `mode`, `instanceIndex`, `instanceCount`, `partitionStrategy`
(`rowgroup` | `file` | `byterange`), `partitionsPerClaim`; distributed adds
`coordinatorType` (`redis` | `memory` | 🚧 `postgres`), `coordinatorDSN`/`Addr`/
`KeyPrefix`, `leaseSeconds`, `heartbeatSeconds`, `maxRetries`.

Work-queue implementations: **MemoryQueue** (in-process dev/test), **RedisQueue**
(multi-instance; items HASH + pending LIST + inprogress ZSET, Lua CAS claim),
**🚧 PostgresQueue** (`FOR UPDATE SKIP LOCKED`, design-stage).

---

## Pipeline definition reference

`action/pipeline/definition.go`. Top-level `Definition` sections:

| Section | Purpose |
|---|---|
| `source` | source `type` + options (+ optional `errorSink`) |
| `transforms[]` | ordered transform stages |
| `sink` | sink `type` + options |
| `checkpoint` | `type` (`file`/`redis`/`none`), `intervalRows` (`10000`), `intervalSeconds` (`30`), `deleteOnSuccess` |
| `incremental` | `mode` (`hwm`/`none`), `column`, `valueType`, `initialValue`, `storeType`, `storeOptions` |
| `performance` | `batchSize` (`5000`), `transformWorkers` (`1`), `sourceBufferBatches` (`4`), `sinkBufferBatches` (`4`), `maxMemoryMB` (`512`), `rowGroupParallelism` (`4`) |
| `scaling` | see [Scaling](#scaling--coordination) |
| `observability` | `metrics` (`prometheus`/`otel`), `options`, `stallSeconds`, `lineage` (OpenLineage run events) |

**Run output** (`action/pipeline/action.go`): `status` (`completed` | `partial` |
`failed`), `rowsProcessed`, `rowsWritten`, `rowsFailed`, `rowsDropped`,
`durationMs`, `error`, `metadata` (includes `dqReport`). `RunDirect` provides
synchronous execution for the REST API and SDK.

---

## Go SDK (pipeline-as-code)

`sdk/pipeline.go` — a fluent builder that produces the same `Definition` and can
run inline or serialize to Flogo resource JSON.

```go
result, err := sdk.NewPipeline("daily-load").
    FromCSV("/data/orders.csv").
    Filter("amount > 0").
    Map(map[string]string{"order_id": "=$id", "total": "=$amount"}).
    ToJDBC("host=localhost dbname=orders", "orders_enriched").
    Run(ctx)
```

- **Sources:** `FromCSV`, `FromJDBC`, `FromKafka`, `FromParquet`, `FromXML`,
  `FromJSONLines`, `FromFixedWidth`, `FromObjectStore`.
- **Transforms:** `Filter`, `Map`, `Lookup`/`Enrich`, `Validate`, `DQ`,
  `SchemaDrift`, `Parse`.
- **Sinks:** `ToJDBC`, `ToKafka`, `ToParquet`, `ToCSV`, `ToJSONLines`,
  `ToObjectStore`, `ToTemplate`, `ToNull`.
- **Terminal:** `Build()`, `MarshalJSON()`, `Run(ctx)`.
