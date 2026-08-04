# Avro sink — `avro`

Writes Apache Avro Object Container Files (OCF) with an embedded schema derived from the
batch columns. Pure Go via the standard [`hamba/avro`](https://github.com/hamba/avro) OCF
writer — no CGO. Implemented in [`sink/avro/sink.go`](../../sink/avro/sink.go); registered
as `avro`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Output file (parent directories are created). |
| `compression` | string | `snappy` | — | `snappy`, `deflate`, `zstandard`, or `null`/`none`. |
| `recordName` | string | `Row` | — | Avro record name written into the schema. |

## Behavior

- The schema is derived from the first batch. Every field is a **nullable union**
  `["null", T]` (default `null`), so any cell may be null.
- **Type mapping:** int→`long`, float→`double`, bool→`boolean`; string, date, decimal and
  `any` are written as `string` (dates/decimals in their textual form) so consumers need no
  logical-type support. Values are coerced to the exact branch type; a value that cannot be
  coerced is written as `null`.
- **Field names** must be valid Avro identifiers (`[A-Za-z_][A-Za-z0-9_]*`); any other
  character in a column name is replaced with `_`.

## Example

```json
{ "type": "avro", "path": "./out/data.avro", "compression": "snappy", "recordName": "Order" }
```

## See also

- [Sinks index](../sinks.md) · [Avro source](../sources/avro.md)
