# Parquet sink — `parquet`

Writes Apache Parquet with typed columns and row-group-aligned flushing. Implemented in
[`sink/parquet/sink.go`](../../sink/parquet/sink.go); registered as `parquet`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Output file (parent directories are created). |
| `compression` | string | `snappy` | — | `snappy`, `gzip`, `zstd`, or `uncompressed`/`none`. |
| `rowGroupSize` | int | `100000` | — | Rows per row group. |

## Behavior

- The schema is inferred from the first batch into typed Parquet nodes (Int64, Double,
  Boolean, String) — values are **not** stringified.
- Each `Flush` closes the current row group, so the file stays valid and queryable even if
  the run is interrupted.

## Example

```json
{ "type": "parquet", "path": "./out/data.parquet", "compression": "snappy", "rowGroupSize": 100000 }
```

## See also

- [Sinks index](../sinks.md) · [Parquet source](../sources/parquet.md) · [Object Store sink](objstore.md)
- Demo: [`examples/08-parquet-columnar`](../../examples/08-parquet-columnar)
