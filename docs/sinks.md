# Sinks

A **sink** writes the transformed `RecordBatch`es to a destination. Sinks are
registered with `sink.Register(name, …)` ([`sink/sink.go`](../sink/sink.go)); the
`type` in a pipeline's `sink` block selects one. Any sink can be wrapped by the
**Dead-Letter Queue** to capture rows that fail to write.

> Each sink below links to its own detailed page under [`docs/sinks/`](sinks/) —
> full configuration keys, behavior, and runnable examples.

## At a glance

| Type(s) | Writes |
|---|---|
| [`csv`, `tsv`](sinks/csv.md) | delimited text, optional header |
| [`jdbc`](sinks/jdbc.md) | bulk insert / upsert into PostgreSQL / MySQL / SQL Server / Oracle |
| [`parquet`](sinks/parquet.md) | row-group-aligned Parquet, snappy/gzip/zstd |
| [`avro`](sinks/avro.md) | row-oriented Avro OCF, snappy/deflate/zstandard/null |
| [`jsonlines`, `ndjson`](sinks/jsonlines.md) | one JSON object per line |
| [`kafka`](sinks/kafka.md) | producer with configurable acks & serialization |
| [`s3`, `gcs`, `adls`](sinks/objstore.md) (+ `-sink`) | cloud object storage with key macros |
| [`template`](sinks/template.md) | Go `text/template` rendering (fixed-width / legacy) |
| [`null`, `discard`](sinks/null.md) | drops all rows (benchmarking / dry-run) |
| [`dlq`](sinks/dlq.md) | wraps any sink; captures rejected rows |

## File sinks

**CSV / TSV** — `path`, `delimiter` (`,`), `writeHeader` (`true`). Creates missing
directories; the header comes from the first batch.

**JSON Lines / NDJSON** — `path`; one buffered JSON object per line.

**Parquet** — `path`, `compression` (`snappy`/`gzip`/`zstd`/`uncompressed`),
`rowGroupSize` (`100000`). Schema is inferred from the first batch into typed nodes;
output is flushed row-group-aligned for good downstream query performance.

**Template** — `path` plus an inline `template` (or `templatePath`) rendered with Go
`text/template` at close. Context: `.Rows`, `.Count`, `.Columns`, `.Attr`; helpers
`rpad`/`lpad`/`padc`, `sci`, `upper`/`lower`/`trim`, `add`, `seq`. Use for
fixed-width and legacy output layouts.

## Relational

**JDBC** — `dsn`, `table`, `batchSize` (`1000`), `onConflict`
(`insert`/`upsert`/`ignore`), `conflictKey` (columns forming the upsert key), and
`autoMigrate` (`ALTER TABLE` to add new columns). Dialect-aware upsert
(`ON CONFLICT DO UPDATE` / `ON DUPLICATE KEY UPDATE`) and ignore
(`DO NOTHING` / `INSERT IGNORE`), sink-owned buffering for idempotency, and
connection pooling.

## Streaming & cloud

**Kafka** — `brokers`, `topic`, `ackMode` (`none`/`leader`/`all`),
`compressionCodec` (`none`/`snappy`/`lz4`/`zstd`), `maxMessageBytes`, `partitionKey`
(column for hash partitioning), `serializer` (`json`/`avro` + `schemaRegistryURL`),
and the same SASL/TLS keys as the source. A blocking `SyncProducer` waits for broker
acks; idempotent when the broker enables it.

**Object Store** (`s3`, `gcs`, `adls`, plus `-sink` aliases) — `key` (destination
template), `format` (inner sink type), and `bucket`/`container`. Key macros:
`{YYYY} {MM} {DD} {HH} {mm} {ss}` (UTC), `{ts}` (unix seconds), `{uuid}`. Streaming
(csv/jsonlines) or temp-file-then-upload (parquet).

## Dead-Letter Queue (`dlq`)

Wraps any inner sink so rejected rows go somewhere durable instead of being lost:
`innerType`, `innerOptions`, `envelopeColumn` (`_dlq_error`), `maxRowsPerBatch`
(`0` = unlimited), `onFull` (`block`/`drop`). The inner sink is created lazily — zero
cost when there are no failures. Each captured row is wrapped in an error envelope
(`pipeline`, `runId`, `batchSeq`, `rowIndex`, `errorMessage`, `originalRow`,
`routedAt`). See [Data quality](data-quality.md) for the routing patterns.

## Null / discard

`null` (alias `discard`) takes no config and drops every row — used for benchmarking
and for DLQ routing during dry runs.

## See also

- [Sources](sources.md) · [Transforms](transforms.md)
- [Data quality](data-quality.md) — validate / DQ / DLQ routing
- Per-sink detail: [`docs/sinks/`](sinks/)
