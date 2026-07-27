# Window transform — `window`

Stateful **event-time windowed aggregation**: buffers keyed window state and emits
one output row per (group, closed window). Unlike the stateless row transforms,
this stage fires windows as the event-time watermark advances (streaming sources)
and flushes remaining open windows at end-of-input (bounded sources). Implemented
in [`transform/window/`](../../transform/window); registered as `window`.

See [Stream processing](../stream-processing.md) for the full design and semantics.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `window` | object | — | ✅ | Window kind + size (see below). |
| `aggregations` | `[]object` | — | ✅ | Aggregations to compute per window (see below). |
| `eventTime` | object | processing time | — | `{ "column", "format" }` — event-time column + layout (`RFC3339`, `epochMillis`, …). Omit to window on processing time. |
| `watermark` | object | — | — | `{ "strategy": "boundedOutOfOrderness", "maxLatenessMs": 5000 }` — how long to wait for out-of-order events. |
| `groupBy` | `[]string` | — | — | Columns to partition windows by (like SQL `GROUP BY`). |
| `emit` | object | — | — | `{ "windowStartAs", "windowEndAs" }` — output column names for the window bounds. |
| `onLate` | string | `drop` | — | `drop` (default) discards records that arrive after their window closed; `route` sends them to an error sink (`errorSinkType` / `errorSinkConfig`, as in [`validate`](validate.md)). |

**Window kinds** (`window.kind`):

| Kind | Extra keys | Meaning |
|---|---|---|
| `tumbling` | `sizeMs` | fixed, non-overlapping windows |
| `sliding` | `sizeMs`, `slideMs` | overlapping windows advancing by `slideMs` |
| `session` | `gapMs` | windows that close after `gapMs` of inactivity per key |
| `global` | — | one unbounded window (fires on flush) |

**Aggregation functions** (`aggregations[].fn`): `sum`, `avg`, `min`, `max`,
`count`, `first`, `last` — each with an `as` (output column) and a `column` (input;
omit for `count`).

## Behavior

- One output row is emitted per (group, window) when the window closes (the
  watermark passes its end), plus a final flush of still-open windows at
  end-of-input.
- Decimal aggregations (`sum` / `avg` / `min` / `max`) are computed **exactly**.
- Window state is **bounded** and spillable to disk for high-cardinality keys, and
  is snapshotted into the checkpoint so a streaming run resumes without double- or
  under-counting — see [reliability](../reliability.md).
- **Scaling guardrail:** a windowed pipeline with a non-empty `groupBy` may not run
  in `sharded` / `distributed` scaling (positional splits would scatter a key's
  rows across instances); use Kafka consumer-group parallelism instead. A global
  window (no `groupBy`) is allowed.

## Example

Per-sensor one-minute average temperature over an event-time tumbling window:

```json
{
  "type": "window",
  "eventTime": { "column": "ts", "format": "RFC3339" },
  "watermark": { "strategy": "boundedOutOfOrderness", "maxLatenessMs": 5000 },
  "window": { "kind": "tumbling", "sizeMs": 60000 },
  "groupBy": ["sensor_id"],
  "aggregations": [
    { "as": "avg_temp", "fn": "avg", "column": "temperature" },
    { "as": "n", "fn": "count" }
  ],
  "emit": { "windowStartAs": "window_start", "windowEndAs": "window_end" }
}
```

## See also

- [Stream processing](../stream-processing.md) — full design + semantics
- [`join`](join.md) — compose before `window` for a windowed keyed join
- [Transforms index](../transforms.md)
