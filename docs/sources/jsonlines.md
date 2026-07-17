# JSON Lines / NDJSON source — `jsonlines`, `ndjson`

Reads newline-delimited JSON: one JSON object per physical line, one record per object.
Implemented in [`source/jsonlines/source.go`](../../source/jsonlines/source.go);
registered as `jsonlines` and `ndjson`.

- **Reads:** newline-delimited JSON (`{…}\n{…}\n…`)
- **Partitionable:** no
- **Streaming:** yes — line-by-line, memory is `O(batchSize)`
- **Checkpoint / resume:** row-count token (sequential skip on resume)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | File to read. |
| `fields` | `[]string` | inferred from the first record | — | Fixes the output column order. When omitted, keys from the first non-blank record are used (sorted for determinism). |

## Behavior

- **Schema-on-read:** values keep their JSON type as `any`; downstream transforms coerce
  as needed. Missing keys produce `nil`; extra keys are ignored once the column set is
  fixed.
- The line buffer is **4 MB**, so large single-line objects are supported.
- Blank lines and lines that fail to parse as JSON are **silently skipped**.

## Example

```json
{ "type": "jsonlines", "path": "/data/events.ndjson" }
```

With a pinned column order:

```json
{ "type": "jsonlines", "path": "/data/events.ndjson", "fields": ["id", "event", "ts", "amount"] }
```

## See also

- [Sources index](../sources.md) · [JSON Lines sink](../sinks/jsonlines.md)
- Demo: [`examples/07-jsonlines-source`](../../examples/07-jsonlines-source)
