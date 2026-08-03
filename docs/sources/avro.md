# Avro source — `avro`

Reads Apache Avro Object Container Files (OCF). The file's embedded writer schema drives
the columns (schema-on-read). Pure Go via the standard
[`hamba/avro`](https://github.com/hamba/avro) OCF reader — no CGO. Implemented in
[`source/avro/source.go`](../../source/avro/source.go); registered as `avro`.

- **Reads:** row-oriented Avro OCF (`.avro`)
- **Partitionable:** — (single sequential reader)
- **Streaming:** bounded — decoded block-by-block, batched
- **Checkpoint / resume:** row-count token

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Local Avro OCF file. |

## Behavior

- **Schema-on-read:** the column set and types come from the OCF header's writer schema.
  A nullable union `["null", T]` is unwrapped to `T`; logical types map as below.
- **Type mapping:** `string`→string, `int`/`long`→int, `float`/`double`→float,
  `boolean`→bool, `date`/`timestamp*`/`time*` logical→date, `decimal` logical→decimal.
  Nested records/arrays/maps and `bytes`/`fixed`/`enum` fall back to `any` (bytes are
  rendered as strings).
- **Compression** is read from the OCF header automatically (`null`, `deflate`, `snappy`,
  `zstandard`).

## Example

```json
{ "type": "avro", "path": "/data/events.avro" }
```

## See also

- [Sources index](../sources.md) · [Avro sink](../sinks/avro.md)
