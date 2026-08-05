# Broadcast (fan-out / "tee") transform — `broadcast` (alias `tee`)

Unconditional **fan-out**: writes every row to N additional named sinks, then
continues the SAME batch downstream unchanged — the "read once, stream
anywhere" pattern (capture the data once, deliver it to every destination that
needs it) common to CDC/replication tools. A **map-side, per-row** transform —
no shuffle, no state. Implemented in
[`transform/broadcast/transform.go`](../../transform/broadcast/transform.go);
registered as `broadcast` (and `tee`).

## How it differs from `router`

[`router`](router.md) is a **conditional, exclusive** split: each row goes to
the **first matching** route and is then removed from the stream. `broadcast`
is **unconditional and non-exclusive**: **every** row goes to **every**
configured sink, and the original batch still continues downstream afterwards
(to the next transform, or the pipeline's own main sink) — nothing is removed.

Use `router` to partition rows across destinations by content; use `broadcast`
to duplicate a stream to multiple destinations that each need the full data
(e.g. archive to S3 *and* notify a Kafka topic *and* write to the primary
sink, all from one read of the source).

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `sinks` | `[]object` | — | ✅ | Ordered list of additional sinks (see below). Non-empty. |
| `passthrough` | bool | `true` | — | Whether the original batch continues downstream after being broadcast. Set `false` to make broadcast terminal. |

Each **sink** object:

| Key | Type | Required | Description |
|---|---|:--:|---|
| `sinkType` | string | ✅ | Any registered sink type (`jsonlines`, `csv`, `parquet`, `jdbc`, `kafka`, `s3`, …). |
| `sinkConfig` | object | — | Config passed to that sink. |
| `name` | string | — | Label for logs / metrics (defaults to `sink<N>`). |

## Behavior

- Every row in the batch is written to **every** configured sink, unchanged —
  a full duplicate, not a filtered subset (contrast with `router`, which sends
  each row to at most one destination).
- Sink writes happen in the order configured. If any sink write fails, `Apply`
  fails and the run reports the error — broadcast does not silently skip an
  unreachable destination, since the entire point of fan-out is that *every*
  destination receives *every* row; a silently-skipped sink would be a silent
  replication gap.
- With `passthrough: true` (default), the original batch is returned unchanged
  and continues to the next transform / the pipeline's main sink — broadcast
  sinks are *additional* destinations, not a replacement for the main one.
  With `passthrough: false`, broadcast is terminal: pair it with a `"null"`
  main sink if the broadcast sinks are the pipeline's only real writes.
- Broadcast sinks are flushed and closed when the pipeline ends, same as
  `router`'s route sinks.
- Like `router`, broadcast **owns external sinks** (`transform.SinkOwner`), so
  a pipeline containing it always runs single-threaded — the parallel
  transform-worker path would otherwise open duplicate writers per worker.

## Example

Archive every row to S3 as JSON *and* publish a change notification to Kafka,
while the pipeline's normal JDBC sink still receives every row too:

```json
{
  "type": "broadcast",
  "sinks": [
    { "name": "archive", "sinkType": "s3", "sinkConfig": { "bucket": "raw-archive", "key": "orders/{date}.jsonl", "format": "json" } },
    { "name": "notify",  "sinkType": "kafka", "sinkConfig": { "brokers": "localhost:9092", "topic": "orders-changed" } }
  ]
}
```

## See also

- [Router / split](router.md) — the exclusive counterpart (each row to one destination)
- [Transforms index](../transforms.md)
