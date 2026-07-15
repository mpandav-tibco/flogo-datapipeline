# Enterprise Data Pipeline Benchmark - Results

Single-binary Flogo data pipeline moving a **53,000,000-row** order dataset from
every supported source to every supported sink, exercising all seven
transform activities, plus an in-database ELT round and two reliability
showcases.

## Headline

| Metric | Value |
|--------|-------|
| Dataset | **53,000,000 rows** (CSV 5.2G / JSONL 11.7G / Parquet 1.7G) |
| Matrix cases passed | **30 / 30** |
| Total rows moved (matrix) | **1,590,000,000** |
| Peak throughput | **1,569,487/s** (`el05_csv_null`) |
| Sources | csv, jsonl, parquet, postgres, kafka, s3 |
| Sinks | parquet, postgres, kafka, s3, null |
| Transforms | filter, map, validate, schemadrift, dq, hwm, lookup |

See [reports/matrix.md](reports/matrix.md), [reports/elt.md](reports/elt.md)
and [reports/enterprise.md](reports/enterprise.md) for the full tables.

## ELT round

3 in-database transforms over the full table. See [reports/elt.md](reports/elt.md).

## Reliability

- **DLQ isolation:** 50,589,628 good + 2,410,372 rejected = 53,000,000 processed, no rows lost.
- **Checkpoint/resume:** interrupted after 10,000,000 rows, resumed and finished (completed).

