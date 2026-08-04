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
| `columns` | `[]string` | all columns | — | **Column projection** — only the listed columns' chunks are read from disk. |
| `filter` | string | — | — | **Predicate pushdown** — a single comparison `column OP value` (`=` `!=` `>` `>=` `<` `<=`); the value is a number or a `'quoted string'`. |
| `rowGroupParallelism` | int | `4` | — | Concurrent row-group readers (only used when the file has multiple row groups). |

## Behavior

- **Column pushdown:** only the selected columns' chunks are read from storage — unlisted
  columns' pages are never touched (a genuine storage-level projection, not a post-decode
  filter).
- **Predicate pushdown:** with a `filter`, a row group whose column min/max statistics prove
  no row can match is skipped without decoding; surviving row groups are then filtered
  row-by-row so the result is exact. The filter column is read even when it is not part of
  `columns`. Decimal columns are filtered exactly but not statistics-pruned. Only a single
  comparison is supported — use a [`filter` transform](../transforms/filter.md) for compound
  predicates (`AND`/`OR`).
- **Row-group parallelism:** when the file has more than one row group and parallelism > 1,
  row groups are decoded concurrently into a size-bounded buffer that keeps emitted batches
  within `batchSize` (so a slow sink applies backpressure).
- **Checkpoint resume:** `Skip(n)` advances past `n` emitted (post-filter) rows; with no
  filter, whole row groups are skipped by their row count without decoding.

## Example

```json
{
  "type": "parquet",
  "path": "/data/transactions.parquet",
  "columns": ["id", "amount", "ts"],
  "filter": "amount > 0",
  "rowGroupParallelism": 4
}
```

## See also

- [Sources index](../sources.md) · [Parquet sink](../sinks/parquet.md) · [Scaling](../scaling.md)
- Demo: [`examples/08-parquet-columnar`](../../examples/08-parquet-columnar)
