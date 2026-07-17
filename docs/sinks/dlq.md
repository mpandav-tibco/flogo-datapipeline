# Dead-Letter Queue sink — `dlq`

Wraps **any** inner sink and captures rejected/failed rows together with an error
envelope. Rows arrive here when a [validate](../transforms/validate.md) /
[dq](../transforms/dq.md) transform routes them, or when a row-level sink write fails.
Implemented in [`sink/dlq/sink.go`](../../sink/dlq/sink.go); registered as `dlq`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `innerType` | string | — | ✅ | The wrapped sink type (`jsonlines`, `jdbc`, `s3`, …). |
| `innerOptions` | object | `{}` | — | Configuration for the inner sink. |
| `envelopeColumn` | string | `_dlq_error` | — | Column that carries the error JSON. |
| `maxRowsPerBatch` | int | `0` (unlimited) | — | Cap rows per batch. |
| `onFull` | string | `block` | — | `block` (accept all) or `drop` (discard overflow). |

## Error envelope

Each routed row gains an `envelopeColumn` string containing:

```json
{
  "pipeline": "…", "runId": "…", "batchSeq": 12, "rowIndex": 3,
  "errorMessage": "amount failed rule min=0", "originalRow": { "…": "…" },
  "routedAt": "2026-07-17T10:00:00.000Z"
}
```

## Behavior

- The inner sink is created lazily on the first routed row — zero cost when nothing fails.
- Configure it as the pipeline's top-level `dlq` block, or as a transform's
  `errorSinkType: "dlq"` / `errorSinkConfig`.

## Example

```json
{
  "type": "dlq",
  "innerType": "s3",
  "innerOptions": {
    "bucket": "adidas-logs",
    "key": "dlq/{YYYY}/{MM}/{DD}/{uuid}.ndjson",
    "format": "jsonlines",
    "endpoint": "http://minio:9000",
    "forcePathStyle": true
  },
  "envelopeColumn": "_dlq_error"
}
```

## See also

- [Sinks index](../sinks.md) · [Validate](../transforms/validate.md) · [Data quality](../data-quality.md) · [Object Store sink](objstore.md)
- Demo: [`examples/03-validate-and-dlq`](../../examples/03-validate-and-dlq)
