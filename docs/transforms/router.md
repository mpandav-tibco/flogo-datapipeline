# Router transform — `router` (alias `split`)

N-way **conditional split**: routes each row to the first named sink whose
predicate matches, generalizing the two-way [DLQ](../sinks/dlq.md) branch into an
arbitrary fan-out. A **map-side, per-row** transform — no shuffle, no state.
Implemented in [`transform/router/transform.go`](../../transform/router/transform.go);
registered as `router` (and `split`).

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `routes` | `[]object` | — | ✅ | Ordered list of routes (see below). Non-empty. |
| `defaultSinkType` | string | — | — | Sink type for rows matching no route. When set, unmatched rows go here (not downstream). |
| `defaultSinkConfig` | object | `{}` | — | Config for the default sink. |
| `onUnmatched` | string | `pass` | — | Only when no default sink is set: `pass` forwards unmatched rows downstream to the pipeline's main sink; `drop` discards them. |

Each **route** object:

| Key | Type | Required | Description |
|---|---|:--:|---|
| `when` | string | ✅ | Predicate in the [`filter`](filter.md) expression grammar (e.g. `amount > 1000 AND region = 'EU'`). |
| `sinkType` | string | ✅ | Any registered sink type (`jsonlines`, `csv`, `parquet`, `jdbc`, `kafka`, `s3`, …). |
| `sinkConfig` | object | — | Config passed to that sink (e.g. `{ "path": "./high.jsonl" }`). |
| `name` | string | — | Label for logs / metrics (defaults to `route<N>`). |

## Behavior

- Each row is tested against the routes **in order**; it is written to the
  **first** route whose `when` matches, then removed from the stream (a row is
  never sent to more than one route).
- A row matching **no** route goes to the **default sink** when configured;
  otherwise `onUnmatched` decides: `pass` (default) forwards it downstream,
  `drop` discards it.
- The batch schema is preserved unchanged for every destination.
- The router **owns external sinks**, so a pipeline containing it always runs
  single-threaded (`transformWorkers` is ignored for that pipeline) — the
  parallel path would otherwise open duplicate writers per worker. This is
  enforced via `transform.SinkOwner`.
- Route sinks are flushed and closed when the pipeline ends. Like the
  [`validate`](validate.md) error sink, they are not flushed at every checkpoint
  interval, so routed data has the same at-least-once durability as any sink
  between its own internal flushes.

## Example

Split orders three ways — high-value to a dedicated file, EU orders to another,
everything else onward to the pipeline's main sink:

```json
{
  "type": "router",
  "routes": [
    { "name": "high", "when": "amount > 1000",   "sinkType": "jsonlines", "sinkConfig": { "path": "./out/high.jsonl" } },
    { "name": "eu",   "when": "region = 'EU'",    "sinkType": "csv",       "sinkConfig": { "path": "./out/eu.csv" } }
  ],
  "onUnmatched": "pass"
}
```

With a catch-all default sink instead of passing unmatched rows downstream:

```json
{
  "type": "router",
  "routes": [
    { "name": "errors", "when": "status = 'ERROR'", "sinkType": "jsonlines", "sinkConfig": { "path": "./out/errors.jsonl" } }
  ],
  "defaultSinkType": "parquet",
  "defaultSinkConfig": { "path": "./out/ok.parquet" }
}
```

## See also

- [Transforms index](../transforms.md)
- [`filter`](filter.md) — the predicate grammar used by `when`
- [DLQ sink](../sinks/dlq.md) — the two-way branch the router generalizes
