# JSON Lines / NDJSON sink — `jsonlines`, `ndjson`

Writes one JSON object per line (NDJSON framing). Implemented in
[`sink/jsonlines/sink.go`](../../sink/jsonlines/sink.go); registered as `jsonlines` and
`ndjson`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Output file (parent directories are created). |

## Behavior

- Each row is encoded as a JSON object (column name → value) followed by a newline, through
  a buffered writer.

## Example

```json
{ "type": "jsonlines", "path": "./out/records.ndjson" }
```

## See also

- [Sinks index](../sinks.md) · [JSON Lines source](../sources/jsonlines.md) · [Object Store sink](objstore.md) (for NDJSON to S3/GCS/ADLS)
