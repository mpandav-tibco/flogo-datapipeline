# Flogo Data Pipeline — Feature Reference

> A code-level reference for every capability registered in the engine. Each
> component lists its **registered type name(s)** (the value you put in a
> `.datapipeline` file), the file that implements it, its configuration keys, and
> notable behaviors. Compiled by reviewing the `init()`/`Register(...)` calls and
> config structs in the source tree, so it reflects what is actually wired in —
> not aspirational scope.

**Legend:** 🚧 = work-in-progress / not production-ready.

**At a glance:** 10 source families · 9 sink families (incl. DLQ) · 8 transforms ·
2 checkpoint backends · 3 watermark backends · 2 metrics backends · 3 scaling modes.

---

## Table of contents

- [Pipeline model](#pipeline-model)
- [Sources](#sources)
- [Sinks](#sinks)
- [Transforms](#transforms)
- [Checkpoint backends](#checkpoint-backends)
- [Watermark backends (incremental load)](#watermark-backends-incremental-load)
- [Metrics backends](#metrics-backends)
- [Scaling & coordination](#scaling--coordination)
- [Pipeline definition reference](#pipeline-definition-reference)
- [Go SDK (pipeline-as-code)](#go-sdk-pipeline-as-code)

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

Registered via `source.Register(name, factory)`. Sources marked **Partitionable**
implement `PartitionableSource` and can be split across scaling instances.

### CSV / TSV — `csv`, `tsv`
`source/csv/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `delimiter` | `,` | any single char (`tsv` presets `\t`) |
| `hasHeader` | `true` | first row supplies column names |
| `columns` | inferred | explicit schema; required if no header |

Type coercion (int/float/bool/date via per-column `format`), lazy-quote tolerance,
sequential skip, approximate byte-offset checkpoint tokens.

### Parquet — `parquet` · **Partitionable**
`source/parquet/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `columns` | all | column pruning (only listed columns read from disk) |
| `rowGroupParallelism` | `4` | concurrent row-group readers |

Row-group predicate/column pushdown, row-group partition enumeration, sequential
reader caching, partial row-group resume.

### JDBC — `jdbc` · **Partitionable**
`source/jdbc/source.go` · drivers: PostgreSQL, MySQL, SQLite (Oracle via ODBC)

| Key | Default | Notes |
|---|---|---|
| `driver` | `postgres` | SQL driver name |
| `dsn` | — | required connection string |
| `table` / `query` | — | table mode or raw SELECT |
| `columns` | all | column subset |
| `where` | — | extra predicate |
| `orderKey` | — | enables keyset pagination |
| `batchSize` | `5000` | rows per fetch |

Keyset pagination (`WHERE orderKey > $last LIMIT N`, safe under concurrent
inserts) with LIMIT/OFFSET fallback; HWM injection in table mode; connection
pooling; O(1) cursor-based checkpoint resume.

### Kafka — `kafka`
`source/kafka/source.go` · library: IBM sarama (pure Go)

| Key | Default | Notes |
|---|---|---|
| `brokers` | — | required list |
| `topic` | — | required |
| `groupId` | — | required consumer group |
| `startOffset` | `latest` | `earliest` \| `latest` \| int64 |
| `mode` | `streaming` | `streaming` \| `batch` |
| `stopOffset` / `stopTimestamp` / `stopAtCurrentEnd` | — | batch-mode stop conditions |
| `deserializer` | `json` | `json` \| `avro` \| `raw` |
| `schemaRegistryURL` | — | Confluent registry for avro |
| `pollMs` | `500` | poll window |
| `saslMechanism` / `saslUser` / `saslPasswordEnv` | — | `PLAIN`/`SCRAM-SHA-256`/`SCRAM-SHA-512` |
| `tlsCertFile` / `tlsKeyFile` / `tlsCACertFile` | — | TLS client certs |

Consumer-group with auto-commit, buffered channel decoupled from rebalances,
schema inference from first message, bounded batch mode for one-shot loads.

### Fixed-Width / Flat File — `fixedwidth`, `flatfile`
`source/fixedwidth/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `encoding` | `utf-8` | `ebcdic-037`, `ebcdic-500`, `latin-1`, … |
| `skipLines` | `0` | header/trailer lines to skip |
| `recordLength` | `0` | fixed byte length (0 = newline-delimited) |
| `columns[]` | — | `name`, `type`, `start` (1-based), `length`, `format`, `trim`, `impliedDecimal` |

Mainframe positional extraction, EBCDIC/ASCII decoding, implied-decimal scaling
(banking/insurance).

### XML — `xml`
`source/xml/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `recordElement` | — | element that delimits one record |
| `fields[]` | — | `name`, `xpath` (element path / `@attr`), `type` |

SAX-style streaming (no full-document load); simple XPath (element paths,
attributes, text) — no predicates or axes.

### JSON Lines / NDJSON — `jsonlines`, `ndjson`
`source/jsonlines/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `fields` | inferred | column order; inferred from first record if omitted |

Schema-on-read, missing keys → `nil`, 4 MB line buffer.

### Object Store — `s3`, `gcs`, `adls` · **Partitionable**
`source/objstore/source.go`

| Key | Default | Notes |
|---|---|---|
| `prefix` | — | object glob, e.g. `data/2024/*.parquet` |
| `format` | — | inner source type (`csv`, `parquet`, `jsonlines`, …) |
| `bucket` (S3/GCS) / `container` (ADLS) | — | storage location |
| `download` | format-dependent | spool to temp file (always on for parquet) |

Each matched object becomes one partition; parsing delegated to the inner source;
streaming (csv/jsonlines) or temp-file (parquet) paths.

### Sectioned (header/body) — `sectioned`
`source/sectioned/source.go`

| Key | Default | Notes |
|---|---|---|
| `path` | — | required |
| `headerKVRegex` | `^(\S[\S ]*?)\s{2,}(.*?)\s*$` | key-value header lines |
| `bodyColumnMarker` | — | regex identifying the column-name line |
| `bodyDelimiter` | `\s+` | body row splitter |
| `broadcastHeader` | `true` | stamp header KVs on every body row |

For report-style files (semiconductor, mainframe reports) that carry file-level
context followed by a tabular body.

### 🚧 Kubernetes Pod Log — `podlog`
`source/podlog/source.go`

Tails CRI/containerd logs under `/var/log/pods` with multiline (stack-trace)
reassembly, namespace filters, and a crash-safe position registry. Config:
`root`, `node`, `positionFile`, `fromBeginning`, `pollMs`, `bounded`,
`includeNamespaces`/`excludeNamespaces`, and a `multiline` object
(`startPattern`, `flushIdleMs`, `maxLines`). Registered and runnable but tracked
as WIP (see `examples/20-podlog`).

---

## Sinks

Registered via `sink.Register(name, factory)`.

### CSV / TSV — `csv`, `tsv`
`sink/csv/sink.go` — `path`, `delimiter` (`,`), `writeHeader` (`true`). Creates
missing directories; header from first batch.

### JDBC — `jdbc`
`sink/jdbc/sink.go` — drivers: PostgreSQL, MySQL, SQLite (Oracle via ODBC)

| Key | Default | Notes |
|---|---|---|
| `dsn` | — | required |
| `table` | — | required |
| `batchSize` | `1000` | rows per INSERT batch |
| `onConflict` | `insert` | `insert` \| `upsert` \| `ignore` |
| `conflictKey` | — | columns forming the upsert key |
| `autoMigrate` | `false` | `ALTER TABLE` to add new columns |

Dialect-aware upsert (`ON CONFLICT DO UPDATE` / `ON DUPLICATE KEY UPDATE`) and
ignore (`DO NOTHING` / `INSERT IGNORE`); sink-owned buffering for idempotency;
connection pooling.

### Parquet — `parquet`
`sink/parquet/sink.go` — `path`, `compression` (`snappy` | `gzip` | `zstd` |
`uncompressed`), `rowGroupSize` (`100000`). Dynamic schema from first batch,
typed nodes, row-group-aligned flush.

### JSON Lines / NDJSON — `jsonlines`, `ndjson`
`sink/jsonlines/sink.go` — `path`. One JSON object per line, buffered.

### Kafka — `kafka`
`sink/kafka/sink.go` — sarama `SyncProducer`

| Key | Default | Notes |
|---|---|---|
| `brokers` / `topic` | — | required |
| `ackMode` | `all` | `none` \| `leader` \| `all` |
| `compressionCodec` | `none` | `snappy` \| `lz4` \| `zstd` |
| `maxMessageBytes` | `1000000` | max message size |
| `partitionKey` | — | column for hash partitioning |
| `serializer` | `json` | `json` \| `avro` (+ `schemaRegistryURL`) |
| SASL/TLS | — | same keys as the Kafka source |

Blocking producer with broker acks; idempotent producer when broker-enabled.

### Object Store — `s3`, `gcs`, `adls` (+ `-sink` aliases)
`sink/objstore/sink.go`

| Key | Default | Notes |
|---|---|---|
| `key` | — | destination key template with macros |
| `format` | — | inner sink type |
| `bucket` / `container` | — | storage location |

Key macros: `{YYYY} {MM} {DD} {HH} {mm} {ss}` (UTC), `{ts}` (unix seconds),
`{uuid}`. Streaming (csv/jsonlines) or temp-file-then-upload (parquet).

### Dead-Letter Queue — `dlq`
`sink/dlq/sink.go`

| Key | Default | Notes |
|---|---|---|
| `innerType` | — | wrapped sink type |
| `innerOptions` | — | inner sink config |
| `envelopeColumn` | `_dlq_error` | error-JSON column name |
| `maxRowsPerBatch` | `0` | 0 = unlimited |
| `onFull` | `block` | `block` \| `drop` |

Lazily initializes the inner sink (zero cost when there are no failures). Each
row is wrapped in an error envelope (`pipeline`, `runId`, `batchSeq`, `rowIndex`,
`errorMessage`, `originalRow`, `routedAt`).

### Template — `template`
`sink/template/sink.go` — `path`, `template` (Go `text/template` body) or
`templatePath`. Renders the whole batch at close with context `.Rows`, `.Count`,
`.Columns`, `.Attr` and helpers `rpad/lpad/padc`, `sci`, `upper/lower/trim`,
`add`, `seq`. Use for fixed-width and legacy layouts.

### Null / Discard — `null`, `discard`
`sink/null/sink.go` — no config; no-op for benchmarking and DLQ routing.

---

## Transforms

Registered via `transform.Register(name, factory)`. Executed in declared order.

### Filter — `filter`
`transform/filter/transform.go` — `expr`. SQL-like predicate with `> >= < <= = !=`,
`IS [NOT] NULL`, `AND`/`OR`, string/number/bool/null literals. Three-valued logic.
Example: `amount > 0 AND status != 'VOID'`.

### Map / Mapper — `map`, `mapper`
`transform/mapper/transform.go` — `fields` (out-name → expression), optional
`fieldOrder`. Expressions: static literal, `=$field` reference, or
`=func(args…)`. Built-ins include string (`concat/upper/lower/trim/substr/replace`),
math (`add/subtract/multiply/divide/round`), hash/encode
(`md5/sha1/sha256/base64_encode/base64_decode`), date (`now/format_date/parse_date`),
utility (`coalesce/if/uuid/random`). Deterministic column ordering; pass-through
when nothing changes.

### Lookup / Enrich — `lookup`, `enrich`
`transform/lookup/transform.go`

| Key | Default | Notes |
|---|---|---|
| `keyField` | — | column to look up |
| `url` | — | endpoint template with `{keyField}` macro |
| `method` | `GET` | HTTP method |
| `outputFields` | — | columns to extract from JSON response |
| `concurrency` | `10` | parallel HTTP workers |
| `cacheSize` | `1000` | LRU cache entries |
| `timeoutMs` | `5000` | per-request timeout |
| `onError` | `null` | `null` \| `skip` \| `fail` |

LRU cache + singleflight dedup, worker pool, URL templating.

### High-Water Mark — `hwm`
`transform/hwm/transform.go` — `column`, `valueType` (`datetime` | `int` |
`string`). Pass-through observer; exposes `MaxSeen()` for the incremental-load
watermark. Type-safe max tracking, thread-safe.

### Schema Drift — `schemadrift`
`transform/schemadrift/transform.go` — `policy` (`project` default | `additive` |
`strict` | `drop`), `onDrift` (`log` | `alert` | `silent`). Baseline from first
batch; `additive` grows the schema, `strict` aborts, `drop` skips drifted
batches, `project` keeps only baseline columns.

### Data Quality — `dq`
`transform/dq/transform.go`

| Key | Default | Notes |
|---|---|---|
| `expectations[]` | — | see below |
| `onFail` | `route` | `route` \| `fail` \| `warn` \| `drop` |
| `qualityThreshold` | `0.95` | gate threshold |
| `scoreColumn` | — | append per-row score |
| `errorSinkType` / `errorSinkConfig` | — | DLQ for failing rows |
| `reportSinkType` / `reportSinkConfig` | — | DQ report sink (e.g. JDBC) |

Expectation kinds: `expectColumnNotNull`, `expectColumnValuesBetween`,
`expectColumnValuesInSet`, `expectColumnValuesMatchRegex`,
`expectTableRowCountBetween`, `expectTableRowCountNotZero`. Emits a `DQReport`
(counts, per-expectation results, `overallScore`, `passedGate`) surfaced in the
run output metadata.

### Validate — `validate`
`transform/validate/transform.go` — `rules[]` (`field`, `notNull`, `min`, `max`,
`minLen`, `maxLen`, `pattern`, `in`), `onViolation` (`route` | `fail` | `drop`),
optional `errorSinkType`/`errorSinkConfig`. Multiple rules per column; type-safe
numeric comparison.

### Parse — `parse`
`transform/parse/transform.go` — `source`, `mode` (`regex` | `json`), `pattern`
(named groups `(?P<name>…)` for regex), `fields`/`prefix` (json), `onError`
(`keep` | `drop` | `route`), `keepSource` (`true`). Extracts columns from a text
or JSON field.

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
| `observability` | `metrics` (`prometheus`/`otel`), `options`, `stallSeconds` |

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
