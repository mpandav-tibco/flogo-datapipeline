# Architecture — a data pipeline as a first-class Flogo action

**Owner:** mpandav · **Status:** Active development · **Updated:** 2026-07-16

## Thesis

A data pipeline is a **computation paradigm** — extract → transform → load — so it
is modeled as a first-class Flogo **action** (`datapipeline:`), a peer of
`flow` / `rules` / `stream`, running on the **stock Flogo runtime** rather than a
hand-wired mesh of activities. One `.datapipeline` file *is* a Flogo app; any Flogo
trigger (timer, REST, Kafka, startup hook) can invoke it; the whole thing compiles
to a single static binary.

## The three artifacts

| Artifact | Path | Role |
|---|---|---|
| **Engine binary** | [`cmd/flogo-datapipeline`](../cmd/flogo-datapipeline) | all-in-one Flogo engine: every source/sink/transform/backend registered via blank imports. Runs any pipeline. |
| **Lean builder** | [`cmd/dp-build`](../cmd/dp-build) | derives the exact import set a given `.datapipeline` needs ([`build/imports.go`](../build/imports.go)) and compiles a minimal per-app binary — the analogue of `flogo build` |
| **Studio** | [`studio/vscode`](../studio/vscode) | VS Code extension: visual canvas, connections, run/preview, build & deploy. Talks to the engine's REST API. |

## Registries — the contribution surface

Every component type is a small registry populated by `init()` at import time. To
add a capability you `Register()` a factory; nothing else in the engine changes.

| Registry | Register call | Implementations |
|---|---|---|
| Source | `source.Register` | `csv` `tsv` `parquet` `jdbc` `kafka` `fixedwidth` `xml` `jsonlines` `s3`/`gcs`/`adls` `sectioned` `podlog` |
| Sink | `sink.Register` | `csv` `jdbc` `parquet` `jsonlines` `kafka` `s3`/`gcs`/`adls` `template` `null` `dlq` |
| Transform | `transform.Register` | `filter` `map` `validate` `lookup` `parse` `dq` `schemadrift` `hwm` |
| Checkpoint | per backend | `file` `redis` |
| Watermark | per backend | `file` `redis` `postgres` |
| Metrics | per backend | `prometheus` `otel` |
| ObjStore provider | `objstore` | `s3` `gcs` `adls` |

Sources, sinks, and transforms implement small interfaces
([`source/source.go`](../source/source.go), [`sink/sink.go`](../sink/sink.go),
[`transform/transform.go`](../transform/transform.go)). A source that also
implements `PartitionableSource` can be split across scaling instances.

## Execution model

```
Source  →  [ Transform₁ → Transform₂ → … ]  →  Sink
                                                 ↓ rejected / failed rows
                                                DLQ Sink
```

The `datapipeline:` action ([`action/pipeline/action.go`](../action/pipeline/action.go)):

1. Instantiates the source, transform chain, and sink from the **Definition**.
2. Streams the source in `RecordBatch`es of `performance.batchSize` rows.
3. Buffers batches between stages (`sourceBufferBatches` / `sinkBufferBatches`) so a
   slow stage exerts **backpressure** instead of unbounded memory growth (capped by
   `maxMemoryMB`).
4. Runs the transform chain (optionally across `transformWorkers` goroutines).
5. Writes to the sink; rows a transform rejects (validate / dq) or the sink fails to
   write route to the **DLQ** branch.
6. Saves checkpoints and advances the watermark as configured.
7. Emits metrics throughout and returns a run summary.

### ETL / ELT / EL is emergent

There is no mode switch in the engine — the shape follows from which transforms you
declare:

| Mode | Shape | Use |
|---|---|---|
| **ETL** | source → transforms → sink | clean / enrich before load |
| **ELT** | source → minimal transform → sink; logic in the target DB | load raw, transform in-database |
| **EL** | source → sink (no transforms) | bulk copy / format conversion |

The Studio surfaces this as an *Execution mode* selector for clarity, but it simply
reflects the transform chain.

## State & durability

- **Checkpoint** ([`checkpoint/`](../checkpoint)) — periodically persists the source
  position (`{format, payload, rowsProcessed}`); an interrupted run resumes from the
  last checkpoint. Sources that implement `CursorProvider` seek in O(1); others
  replay-and-skip. Backends: `file` (single instance), `redis` (multi-instance).
- **Watermark** ([`watermark/`](../watermark)) — for incremental (`mode: hwm`)
  pipelines, persists the maximum value of a column so the next run reads only new
  rows. Backends: `file`, `redis`, `postgres`.

See [Reliability & incremental load](reliability.md).

## Observability

A process-wide metrics recorder ([`metrics/`](../metrics)) is a no-op by default
(zero overhead) and swaps to `prometheus` (scrape endpoint) or `otel` (OTLP/gRPC)
when configured. Both emit the same logical series (rows read / transformed /
written / failed, batch durations, runs, stalls, checkpoints, throughput). A stall
detector flags stages that stop making progress. See [Observability](observability.md).

## Scaling

`ResolveFromEnv()` reads `INSTANCE_INDEX` / `INSTANCE_COUNT` (Kubernetes StatefulSet
friendly). Three modes: `single`, `sharded` (static `index % count` partition claim),
and `distributed` (a coordinator — `redis` or `memory` — leases partitions to workers
with heartbeat and retry). Only partitionable sources (`parquet`, `jdbc`, object
store) split. See [Scaling](scaling.md).

## Invocation & the REST API

A pipeline runs when a trigger fires it. The monolith bundles the **timer**, **REST**,
and **Kafka** triggers plus the TIBCO General **startup** hook (fire once on boot —
the right driver for a long-running streaming source such as the podlog tailer) and
**shutdown** hook (final flush). Set `-port` or `-api-addr` to also expose a
management REST API (`/v1/health`, list / run / status / cancel, plus introspection
used by the Studio). See [REST API](rest-api.md) and [Configuration](configuration.md).

## Boundaries preserved by design

- The engine embeds **no UI**; the Studio talks to it over REST — no reverse proxy,
  no CORS.
- Secrets are **never** written to a `.datapipeline`; the Studio resolves bound
  connections into a transient, git-ignored sibling file at run time
  (see [Security](security.md)).
- A lean `dp-build` binary and the all-in-one engine run the same file identically —
  `dp-build` only trims unused imports.
