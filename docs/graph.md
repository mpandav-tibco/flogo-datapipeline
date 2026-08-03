# Graph (DAG) execution

A pipeline is normally a single **linear** chain — `Source → [transforms] → Sink`
(plus an optional DLQ branch). When you need more than one source, more than one
sink, or **conditional routing** between stages, a `.datapipeline` can carry an
explicit **graph** (a directed acyclic graph of nodes + edges). The engine runs
that graph with a dedicated **streaming graph runner**
([`action/pipeline/graphexec`](../action/pipeline/graphexec)) — one goroutine per
node, one bounded channel per consumer — so the whole DAG streams concurrently
with backpressure, exactly like the linear path.

## When the graph runner activates

Activation is **automatic and narrowly scoped**. The runner takes over only when
the graph is **non-reducible** — i.e. it has structure the single-chain linear
model cannot represent:

- a **typed edge** (`expression`, `otherwise`, or `error`), or
- any node with **fan-out or fan-in ≥ 2** (multiple sinks, multiple sources, or a
  tee).

A graph that is just a linear chain written out as nodes+edges stays on the
**linear** path (so its checkpoint / HWM / DLQ / parallel-worker behaviour is
untouched). If a graph is non-reducible but references a node that cannot be
resolved, the engine also falls back to linear rather than guess. All of this is
decided by `graphExecutable()` in
[`action/pipeline/graph_run.go`](../action/pipeline/graph_run.go); when it fires
you will see a log line like:

```
executing via graph runner — non-reducible DAG (7 nodes, 6 edges)
```

## The `graph` field

`graph` is an **optional, additive** block alongside the linear `source`,
`transforms[]`, and `sink`. It is validated whenever present (unique node ids,
edges reference declared nodes, acyclic).

```json
"graph": {
  "nodes": [
    { "id": "source", "kind": "source", "type": "csv",    "ports": { "out": ["rows"] } },
    { "id": "t0",     "kind": "transform", "type": "filter", "ports": { "in": ["in"], "out": ["out"] } },
    { "id": "sink",   "kind": "sink", "type": "parquet", "ports": { "in": ["in"] } },
    { "id": "xsink-audit", "kind": "sink", "type": "parquet",
      "ports": { "in": ["in"] },
      "config": { "path": "./out/audit.parquet", "compression": "snappy" } }
  ],
  "edges": [
    ["source:rows", "t0:in"],
    ["t0:out", "sink:in"],
    { "from": "t0:out", "to": "xsink-audit:in", "type": "expression", "value": "unit_price > 90000" }
  ]
}
```

### Nodes

| Field | Description |
|---|---|
| `id` | Unique node id. The **standard** ids resolve to the linear model: `source` → the `source` block, `t{i}` → `transforms[i]`, `sink` → the `sink` block. |
| `kind` | `source` \| `transform` \| `sink`. |
| `type` | The registered component type (e.g. `csv`, `filter`, `parquet`). |
| `ports` | Declared `in` / `out` port names; edge endpoints are written `node:port` (the `:port` suffix is optional and defaults to the node's default port). |
| `config` | **Self-contained** config for *extra* nodes — any source/transform/sink beyond the standard three carries its own full config here (this is how a graph declares additional sinks or feeds). |

### Edges

An edge is either the **array short form** `["from:port", "to:port"]` (a success
edge) or an **object** with an explicit `type`:

| `type` | Carries | Notes |
|---|---|---|
| `success` (also `""` / `default`, or the array form) | **every** successfully-processed row | the main continuation; multiple success edges from one port = a **tee**. |
| `expression` | rows for which `value` evaluates **true** | additive — a matching row still takes any `success` edge too. Uses the [filter expression language](transforms/expression-language.md) (note: equality is a single `=`). |
| `otherwise` | rows that matched **none** of the node's `expression` edges | the default branch of a conditional split. |
| `error` | rows/batches that **failed** at the node (or a splitter's rejects, e.g. `validate`) | the graph-native error/DLQ branch. |

## Every transform is supported

The graph runner runs **all** transform types with the same guarantees as the
linear engine (`unsupportedGraphTransforms` is empty):

- **`hwm`** — the runner injects the prior watermark into the primary source and,
  after a clean run, harvests the observed maximum and persists it to the
  watermark store (incremental load works identically to the linear path).
- **`dq`** — the gate report is surfaced in the run's `Metadata["dqReport"]`.
- **`join` / `union`** — these self-load their bounded secondary side, so they run
  as ordinary operators. (For *streaming, unbounded* multi-source, prefer native
  **fan-in** — several `source` nodes merging into one downstream node — over the
  `union` transform.)
- **`window`** — still-open aggregates are flushed at end-of-input.

The run's row counts (`processed` / `written`) use the same accounting as the
linear path, and per-node counters are surfaced for the Studio run overlay.
**Checkpoint/resume** works for stateless graphs (drain-barrier based); a *windowed*
(stateful) graph currently runs **without** checkpointing — the barrier does not yet
snapshot window state — and logs a warning. Rejected/failed rows ride the **`error`
edges** above (the graph's DLQ equivalent), not the linear `dlq` sink wrapper.

## Common shapes

| Pattern | Topology |
|---|---|
| **Conditional routing / regional fan-out** | `source → filter →` several `expression` edges (one per key) `+ otherwise` → N sinks |
| **Valid / reject split (DLQ)** | `source → validate →` `success`/`expression` to the lake `+ error` edge → a quarantine sink |
| **Tee + audit** | one `success` edge (every row) `+` an `expression` edge (a selective subset) from the same port |
| **Multi-source consolidation** | several `source` nodes **fan in** to one transform chain, then fan out to many sinks |

Worked, benchmarked examples of all of these live under
[`examples/23-enterprise-benchmark`](../examples/23-enterprise-benchmark) (see its
`reports/graph.md`).

## See also

- [Pipeline file](pipeline-file.md) — the full `.datapipeline` schema
- [Architecture](ARCHITECTURE.md) — the execution model and registries
- [`router` / `split`](transforms/router.md) — single-node N-way routing within the linear chain
- [Data quality](data-quality.md) — `validate` / `dq` and error routing
