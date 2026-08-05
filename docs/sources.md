# Sources

A **source** reads records from a system or file and emits them as `RecordBatch`es
into the transform chain. Sources are registered with `source.Register(name, …)`
([`source/source.go`](../source/source.go)); the `type` you put in a pipeline's
`source` block selects one. Sources marked **Partitionable** implement
`PartitionableSource` and can be split across [scaling](scaling.md) instances.

> Each source below links to its own detailed page under [`docs/sources/`](sources/) —
> full configuration keys, behavior, and runnable examples.

## At a glance

| Type(s) | Reads | Partitionable |
|---|---|:--:|
| [`csv`, `tsv`](sources/csv.md) | delimited text, optional header | — |
| [`parquet`](sources/parquet.md) | columnar Parquet, row-group parallel | ✅ |
| [`avro`](sources/avro.md) | row-oriented Avro OCF (schema-on-read) | — |
| [`jdbc`](sources/jdbc.md) | SQL query over PostgreSQL / MySQL / SQL Server / Oracle | ✅ |
| [`kafka`](sources/kafka.md) | consumer-group topic consumption | — |
| [`fixedwidth`, `flatfile`](sources/fixedwidth.md) | positional / EBCDIC mainframe files | — |
| [`xml`](sources/xml.md) | streaming element-per-record XML | — |
| [`jsonlines`, `ndjson`](sources/jsonlines.md) | newline-delimited JSON | — |
| [`s3`, `gcs`, `adls`](sources/objstore.md) | files from cloud object storage | ✅ |
| [`sectioned`](sources/sectioned.md) | header block + delimited body table | — |
| [`podlog`](sources/podlog.md) | Kubernetes / CRI container logs | — |
| [`rest`](sources/rest.md) | paginated JSON from an HTTP(S) API | — |

## File sources

**CSV / TSV** — `path`, `delimiter` (`,`; `tsv` presets tab), `hasHeader` (`true`).
Per-column type coercion (int/float/bool/date via `format`), lazy-quote tolerance,
and approximate byte-offset checkpoint tokens. Give explicit `columns` when there is
no header.

**JSON Lines / NDJSON** — `path`; one JSON object per line, schema-on-read (missing
keys become `nil`), 4 MB line buffer. `fields` fixes column order.

**XML** — `path`, `recordElement` (the element that delimits one record), and
`fields[]` with simple XPath (`element/path` or `@attr`). SAX-style streaming — no
full-document load, no predicates or axes.

**Fixed-Width / Flat File** (`fixedwidth`, `flatfile`) — positional extraction for
mainframe files: `encoding` (`ebcdic-037`, `ebcdic-500`, `latin-1`, …), `recordLength`
(0 = newline-delimited), `skipLines`, and `columns[]` with `start`/`length`/`type`/
`format`/`trim`/`impliedDecimal`. Handles EBCDIC decoding and implied-decimal scaling
(banking / insurance).

**Sectioned** (`sectioned`) — report-style files that carry a key/value header block
then a delimited body table: `headerKVRegex`, `bodyColumnMarker`, `bodyDelimiter`,
`broadcastHeader` (stamp header values on every body row).

## Columnar & relational

**Parquet** (`parquet`, *partitionable*) — `path`, optional `columns` (column
pruning — only listed columns are read from disk), `rowGroupParallelism` (`4`).
Row-group predicate/column pushdown, per-row-group partition enumeration, and partial
row-group resume.

**JDBC** (`jdbc`, *partitionable*) — `driver` (`postgres` default, `mysql`,
`sqlserver`, `oracle`), `dsn`, and either `table` (+ optional `columns`/`where`/`orderKey`) or a
raw `query`. Keyset pagination (`WHERE orderKey > $last LIMIT N`, safe under
concurrent inserts) with LIMIT/OFFSET fallback, high-water-mark predicate injection
for [incremental](reliability.md) loads, connection pooling, and O(1) cursor-based
checkpoint resume.

## Streaming & cloud

**Kafka** (`kafka`) — `brokers`, `topic`, `groupId`, `startOffset`
(`earliest`/`latest`/int64). `mode` is `streaming` (run continuously) or `batch`
(one-shot with a `stopOffset` / `stopTimestamp` / `stopAtCurrentEnd` stop condition).
`deserializer` `json`/`avro`/`raw` (+ `schemaRegistryURL` for Avro), consumer-group
auto-commit, and SASL (`PLAIN`/`SCRAM-*`) + TLS. Uses the pure-Go IBM sarama client.

**Object Store** (`s3`, `gcs`, `adls`, *partitionable*) — `prefix` (object glob),
`format` (the inner source type — `csv`/`parquet`/`jsonlines`/…), and `bucket`
(S3/GCS) or `container` (ADLS). Each matched object becomes one partition; parsing is
delegated to the inner source. Credentials come from a bound
[connection](connections.md) or the provider's standard env chain.

## 🚧 Kubernetes pod logs

**podlog** — tails CRI/containerd logs under `/var/log/pods` with multiline
stack-trace reassembly, namespace filters, and a crash-safe position registry.
Registered and runnable but tracked as work-in-progress (see
[`examples/20-podlog`](../examples/20-podlog)).

## Generic HTTP

**REST/API** (`rest`) — pulls a JSON array of records from an HTTP(S) endpoint:
`url`, `authType` (`bearer`/`basic`/`apikey`, credentials from an env var),
`dataPath` (locate the array inside a nested response), and a `pagination` block
(`page`/`offset`/`cursor`, the last via a response-provided cursor value or a full
next-page link). Retries 429/5xx with backoff. The first generic connector for
**any** paginated JSON API, not tied to a specific vendor (see
[`examples/27-rest-api-source`](../examples/27-rest-api-source)).

## See also

- [Sinks](sinks.md) · [Transforms](transforms.md)
- [Connections](connections.md) — bind DSNs / buckets / brokers by name
- [Reliability](reliability.md) — checkpoint resume and incremental (HWM) reads
- Per-source detail: [`docs/sources/`](sources/)
