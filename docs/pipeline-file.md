# The `.datapipeline` file

A pipeline is an ordinary **Flogo app JSON** with a `.datapipeline` extension. The
app has one resource whose `data` is the pipeline **Definition** — the source,
transform chain, sink, and operational settings. Because it is a Flogo app, it
composes with Flogo triggers and runs on the stock engine.

## File anatomy

```jsonc
{
  "name": "orders-etl",
  "type": "flogo:app",
  "appModel": "1.1.1",
  "triggers": [ /* optional: timer / rest / kafka / startup hook */ ],
  "resources": [
    {
      "id": "pipeline:main",
      "data": {
        "source":     { "type": "csv",  "options": { /* … */ } },
        "transforms": [ { "type": "filter", "options": { /* … */ } } ],
        "sink":       { "type": "jdbc", "options": { /* … */ } },
        "graph":         { /* optional: DAG nodes + edges (non-linear topologies) */ },
        "checkpoint":    { /* … */ },
        "incremental":   { /* … */ },
        "performance":   { /* … */ },
        "scaling":       { /* … */ },
        "observability": { /* … */ }
      }
    }
  ],
  "actions": [
    { "ref": "…/action/pipeline", "settings": { "pipelineURI": "res://pipeline:main" } }
  ]
}
```

In the Studio you never edit this by hand — the canvas and Inspector read and write
it for you. The reference below is the source of truth
([`action/pipeline/definition.go`](../action/pipeline/definition.go)).

## `source`

The single input. `type` is a registered source name; `options` is that source's
config.

```json
"source": { "type": "csv", "options": { "path": "data/orders.csv", "hasHeader": true } }
```

A source may also carry an `errorSink` for rows that fail to parse. Full per-source
keys: [Sources](sources.md).

## `transforms[]`

An ordered list; each row flows through them in declared order. Omit or leave empty
for an EL (extract-load) pipeline.

```json
"transforms": [
  { "type": "filter",   "options": { "expr": "amount > 0 AND status != 'VOID'" } },
  { "type": "validate", "options": { "rules": [ /* … */ ], "onViolation": "route" } },
  { "type": "map",      "options": { "fields": { "order_id": "=$id" } } }
]
```

Full per-transform keys and the expression language: [Transforms](transforms.md).

## `sink`

The single output. A `dlq` sink (or a transform's `errorSink`) captures rejected
rows.

```json
"sink": {
  "type": "jdbc",
  "options": { "dsn": "…", "table": "orders", "onConflict": "upsert", "conflictKey": ["order_id"] }
}
```

Full per-sink keys: [Sinks](sinks.md).

## `graph` (optional)

Beyond the single-source → chain → single-sink shape, a pipeline can declare an
explicit **graph** of nodes + edges for non-linear topologies — multiple sources,
multiple sinks, or conditional routing. The `source`, `transforms[]`, and `sink`
above still supply the standard nodes (`source`, `t{i}`, `sink`); any extra node
carries its own `config`. When the graph is **non-reducible** (a typed edge, or
fan-out / fan-in ≥ 2) a streaming graph runner executes it directly; a purely
linear graph is ignored and the linear path runs.

```json
"graph": {
  "nodes": [
    { "id": "source", "kind": "source", "type": "csv",      "ports": { "out": ["rows"] } },
    { "id": "t0",     "kind": "transform", "type": "validate", "ports": { "in": ["in"], "out": ["out"] } },
    { "id": "sink",   "kind": "sink", "type": "parquet",    "ports": { "in": ["in"] } },
    { "id": "reject", "kind": "sink", "type": "parquet",    "ports": { "in": ["in"] },
      "config": { "path": "./out/reject.parquet" } }
  ],
  "edges": [
    ["source:rows", "t0:in"],
    ["t0:out", "sink:in"],
    { "from": "t0:out", "to": "reject:in", "type": "error" }
  ]
}
```

Edge `type`: `success` (the default / array short form) · `expression` (+ a `value`
predicate) · `otherwise` · `error`. Full reference:
[Graph (DAG) execution](graph.md).

## `checkpoint`

Resume an interrupted run from the last saved source position.

| Key | Default | Notes |
|---|---|---|
| `type` | `none` | `file` \| `redis` \| `none` |
| `intervalRows` | `10000` | checkpoint every N rows |
| `intervalSeconds` | `30` | …or every N seconds |
| `deleteOnSuccess` | — | remove the checkpoint after a clean finish |
| `onSaveError` | `continue` | `continue` logs a failed checkpoint save and keeps going (the previous checkpoint and uncommitted source offsets stay in force); `fail` stops the run |
| `key` | per-run id | stable key for single-instance streaming resume |
| `path` | temp dir | **file** backend: directory for checkpoint files |
| `addr` | `localhost:6379` | **redis** backend: host:port |
| `passwordEnv` | — | **redis** backend: env var holding the password |
| `keyPrefix` | `flogo:pipeline:ckpt` | **redis** backend: key namespace |
| `ttlSeconds` | `604800` | **redis** backend: token expiry in seconds (7 days) |

See [Reliability](reliability.md).

## `incremental`

Read only rows newer than the last run (high-water mark).

| Key | Notes |
|---|---|
| `mode` | `hwm` \| `none` |
| `column` | the monotonic column (e.g. `updated_at`) |
| `valueType` | `datetime` \| `int` \| `string` \| `decimal` |
| `initialValue` | starting mark for the first run |
| `storeType` | `file` \| `redis` \| `postgres` |
| `storeOptions` | backend connection config |

See [Reliability](reliability.md).

## `performance`

Throughput and memory knobs.

| Key | Default | Notes |
|---|---|---|
| `batchSize` | `5000` | rows per `RecordBatch` |
| `transformWorkers` | `1` | parallel transform goroutines |
| `sourceBufferBatches` | `4` | read-ahead depth |
| `sinkBufferBatches` | `4` | write buffer depth |
| `maxMemoryMB` | `512` | in-flight memory ceiling (backpressure) |
| `rowGroupParallelism` | `4` | concurrent Parquet row-group readers |

## `scaling`

`single` / `sharded` / `distributed` plus coordinator config. See [Scaling](scaling.md).

## `observability`

`metrics` (`prometheus` \| `otel`), backend `options`, and a `stallSeconds`
threshold. See [Observability](observability.md).

## `contract` (optional)

A portable **data contract** for the pipeline's output — schema + data-quality
+ SLA + independent source/sink reconciliation bundled as one versioned
artifact, checked at both design time (`-validate`) and run time. See
[Data contracts](data-contracts.md).

## Run output

Every run returns a summary ([`action/pipeline/action.go`](../action/pipeline/action.go)):

| Field | Meaning |
|---|---|
| `status` | `completed` \| `partial` \| `failed` — `partial` also covers a completed run that breached a [contract](data-contracts.md)'s `maxDurationMs` SLA or `reconciliation` check |
| `rowsProcessed` | rows read from the source |
| `rowsWritten` | rows written to the sink |
| `rowsFailed` | rows routed to the DLQ / errored |
| `rowsDropped` | rows dropped by a filter / policy |
| `rowsDeleted` | rows removed via a delete-capable sink (e.g. jdbc `opColumn`); `0` when not applicable |
| `durationMs` | wall-clock run time |
| `error` | failure message, if any |
| `metadata` | extra detail, including the `dqReport` when a DQ transform ran |

`RunDirect()` executes synchronously and returns this summary — used by the
[REST API](rest-api.md) and the [Go SDK](sdk.md).
