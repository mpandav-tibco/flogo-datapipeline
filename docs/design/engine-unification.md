# Design: one execution engine

Status: implemented on `perf/core-hot-path-review`.

## Problem

The pipeline action had two engines with diverging feature sets:

- **Linear** (`executePipelineImpl`): one ~880-line function with three drain
  loops (sequential, ordered-parallel, scaled) for `source → transforms → sink`.
- **Graph** (`runGraph` + `graphexec`): goroutine-per-node dataflow for DAGs
  (fan-out/fan-in, conditional/error edges, multiple sources/sinks).

`graphExecutable` sent a pipeline to the graph engine only when its DAG could
not be reduced to a chain, so behaviour depended on topology:

| Feature | Linear | Graph (before) |
|---|---|---|
| DLQ, per-row isolation/bisection, recovery of unflushed rows | yes | no — a sink error failed the run |
| Per-batch metrics and stall detection | yes | no |
| Transform error → count rows failed, continue | yes | no — error edge or abort |
| `maxMemoryMB` byte budget | yes | no |
| `transformWorkers` ordered parallelism | yes | no |
| Time/idle checkpoints, source cursor in token, Kafka `resumeCursor`, watermark at checkpoint | yes | no |
| Window flush runs through the stages after the window | no | yes |
| el/elt modes, schemadrift, contract DQ, reconciliation, CDC stats, stage stats, sink-close errors | yes | partial |
| Fan-out/fan-in, conditional/error edges, multiple sources/sinks | no | yes |

Every fix had to be made twice (several recent ones were), and the two paths
drifted in exactly the places operators care about: DLQ, metrics and checkpoints.

## Decision

`graphexec` is the only engine. Every pipeline compiles to a plan:

- A **linear definition** compiles to `source → chain → sink`. `chain` is one
  fused node wrapping the existing `transform.Pipeline`, so the per-batch cost,
  pooling semantics, per-stage statistics and ordering are unchanged. With
  `mode: el|elt` (or no transforms) the chain node is omitted.
- An **explicit graph** compiles node-per-node exactly as before.

Engine features are provided by operator wrappers in the pipeline package, so
`graphexec` stays a generic dataflow runtime:

| Wrapper | Provides |
|---|---|
| `meteredSource` | `BatchRead` metrics, stall detection, `RecorderAware` injection |
| `chainOp` | transform metrics, dropped/failed accounting, count-and-continue on transform errors, window `Flush`/`Snapshot`/`Restore`, stage stats, context binding |
| `managedSink` | DLQ with per-row isolation (bisection for atomic sinks), recovery of rows a buffering sink failed to flush, `BatchWritten` metrics, stall detection; one shared DLQ serialised across sinks |

`graphexec` gains only generic capabilities, available to every graph:

- **Memory budget** (`maxMemoryMB`): sources wait for room before reading; a
  batch's bytes are released when its first consumer takes it.
- **Time and idle checkpoints** (`intervalSeconds`): the source emits a barrier
  on time as well as on row count, including on idle polls when rows are
  pending or the source reports uncommitted progress.
- **Ordered parallel replicas** (`transformWorkers`): a transform node can run N
  operator instances; results are re-sequenced so downstream order is unchanged.
- **Separate sink buffer depth** (`sinkBufferBatches`).

The barrier protocol (source pauses → every sink flushes → token saved →
source commits → resume) replaces the linear engine's write-stage barrier. A
checkpoint token now always carries the source cursor (`CursorProvider`) and
the snapshot of every stateful node.

## Compatibility

- Checkpoint tokens written by the old linear engine store a single window
  snapshot in `WindowState`; restore recognises that format and hands it to the
  chain node.
- Behaviour that changes for **explicit graphs** (now matching linear): sink row
  failures are isolated and dead-lettered (or counted, without a DLQ) instead of
  aborting the run; per-batch metrics are emitted; `maxMemoryMB`,
  `intervalSeconds` and source cursors are honoured.
- Transform errors on a node **without** an error edge keep their topology-specific
  meaning: inside a linear chain they count rows as failed (as before); on an
  explicit graph node they abort the run (as before), since the author could
  have declared an error edge.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Throughput regression from goroutine/channel hops | The chain is one fused node, so a linear pipeline has the same three stages it had (reader, transformer, writer); verified with the 53M-row benchmark (`examples/23-enterprise-benchmark`) |
| Lost linear behaviour | Every existing linear test (`run_test.go` and others) now runs through the unified engine unchanged, plus the runtime E2E suite (`e2e/`) |
| Concurrency in shared state (DLQ, counters) | Shared DLQ is mutex-serialised; run counters are atomic; whole suite under `-race` |
