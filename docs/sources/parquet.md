# Parquet source — `parquet` · *partitionable*

Reads Apache Parquet files with column pruning and row-group parallelism. Pure Go (no
CGO). Implemented in [`source/parquet/source.go`](../../source/parquet/source.go);
registered as `parquet`.

- **Reads:** columnar Parquet
- **Partitionable:** ✅ — one partition per row group (see [Scaling](../scaling.md))
- **Streaming:** bounded — consumes the whole file, batched
- **Checkpoint / resume:** row-count token; partial row-group resume

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Local Parquet file. |
| `columns` | `[]string` | all columns | — | **Column pruning** — only the listed columns are read from disk. |
| `rowGroupParallelism` | int | `4` | — | Concurrent row-group readers (only used when the file has multiple row groups). |

## Behavior

- **Column pushdown:** unlisted columns are never read from storage.
- **Row-group parallelism:** when the file has more than one row group and parallelism > 1,
  row groups are decoded concurrently; a size-bounded overflow buffer keeps batches within
  `batchSize`.
- In single-instance mode a persistent sequential reader is kept across `ReadBatch` calls;
  in partitioned mode each partition opens an independent reader.

## Example

```json
{
  "type": "parquet",
  "path": "/data/transactions.parquet",
  "columns": ["id", "amount", "ts"],
  "rowGroupParallelism": 4
}
```

## See also

- [Sources index](../sources.md) · [Parquet sink](../sinks/parquet.md) · [Scaling](../scaling.md)
- Demo: [`examples/08-parquet-columnar`](../../examples/08-parquet-columnar)
