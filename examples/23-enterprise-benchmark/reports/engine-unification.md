# Engine Unification - Before / After

The linear and graph execution engines were merged into one streaming executor
(`action/pipeline/graphexec`): a linear pipeline now compiles to a three-node plan
(source → fused transform chain → sink) and an explicit DAG to one node per graph
node. See [docs/design/engine-unification.md](../../../docs/design/engine-unification.md).
The full 53,000,000-row benchmark was re-run on the unified engine (same host,
same data, lean per-pipe binaries); results are in [matrix.md](matrix.md),
[enterprise.md](enterprise.md), [graph.md](graph.md) and [elt.md](elt.md).

**Every case produced exactly the same row counts as before** (processed / written /
rejected), so correctness is at parity. The tables below compare the run against the
previous published baseline.

## Throughput vs previous baseline

| Case | Pipeline | Before | After | Change | RSS before → after |
|------|----------|-------:|------:|-------:|-------------------:|
| el02 | csv → postgres | 61,183/s | 355,064/s | **5.8×** | 62 → 50 MB |
| el06 | parquet → postgres | 65,824/s | 426,192/s | **6.5×** | 234 → 205 MB |
| el10 | jsonl → postgres | 62,725/s | 271,831/s | **4.3×** | 73 → 59 MB |
| el15 | kafka → postgres | 63,997/s | 225,894/s | **3.5×** | 86 → 70 MB |
| el17 | s3 → postgres | 62,093/s | 354,214/s | **5.7×** | 230 → 846 MB ¹ |
| etl01 | csv → filter → parquet | 585,487/s | 788,808/s | +35% | 157 → 105 MB |
| etl02 | csv → filter/map → parquet | 456,523/s | 784,511/s | +72% | 144 → 116 MB |
| etl03 | csv → validate/map → postgres | 32,681/s | 445,094/s | **13.6×** | 61 → 144 MB |
| etl04 | csv → filter/validate/map → s3 | 113,153/s | 716,633/s | **6.3×** | 181 → 139 MB |
| etl05 | postgres → map → kafka | 139,897/s | 189,307/s | +35% | 64 → 94 MB |
| etl07 | parquet → filter/validate/map → postgres | 31,022/s | 425,235/s | **13.7×** | 220 → 288 MB |
| etl09 | csv → dq → parquet | 300,988/s | 542,933/s | +80% | 146 → 120 MB |
| etl11 | csv → lookup/map → parquet | 290,803/s | 578,659/s | **2.0×** | 153 → 124 MB |
| ent01 | DLQ isolation (53M, 2.4M rejects) | 110,911/s | 589,151/s | **5.3×** | 150 → 169 MB |
| graph01 | 5-way region router → parquet | 674,395/s | 1,486,009/s | **2.2×** | 216 → 172 MB |
| graph04 | 5-way region router → s3 | 651,923/s | 1,238,549/s | **1.9×** | 276 → 224 MB |
| graph05 | 3 sources → chain → 6 sinks (159M rows) | 540,010/s | 777,582/s | +44% | 486 → 516 MB |

Pure extract-load cases without a database sink (csv/parquet/jsonl/kafka → parquet,
kafka, s3, null) are unchanged within ±3%, while peak RSS dropped 20–45% (e.g.
csv → parquet 148 → 96 MB, jsonl → parquet 146 → 80 MB, kafka → parquet 169 → 98 MB).

Where the gains come from: Postgres sinks default to `COPY` (with per-row INSERT
fallback and DLQ isolation), the transform chain runs fused in its own stage
overlapped with source reads and sink writes, the lookup cache adds negative
caching, and validate/filter/map bind columns by position instead of building a map
per row.

¹ See the S3-Parquet note below — not caused by the engine.

## Same-day A/B against `master`

Cases whose delta against the months-old baseline looked negative were re-run
back-to-back against binaries built from `master` (old engines) on the same infra
and data, two rounds each:

| Case | `master` | unified | RSS `master` → unified |
|------|---------:|--------:|-----------------------:|
| el05 csv → null | 1,535,119 / 1,526,014/s | 1,548,938 / 1,530,951/s | 39 → 26 MB |
| el11 postgres → parquet | 424,418 / 426,817/s | 427,144 / 427,844/s | 165 → 99–109 MB |
| el13 postgres → s3 | 404,518 / 407,360/s | 404,806 / 415,100/s | 210 → 140–149 MB |
| el16 s3 → parquet | 429,411 / 431,902/s | 441,001 / 438,764/s | 828–852 → 830–839 MB |
| el17 s3 → postgres | 60,136 / 59,825/s | 402,134 / 403,247/s | 806–832 → 803–816 MB |

The unified engine matches or beats the old engines in every case; the apparent
regressions against the old baseline were environmental.

## Environment notes

- **Keyset index:** the jdbc-source cases page with
  `WHERE order_id > $last ORDER BY order_id LIMIT n`. `sql/schema.sql` did not create
  an index on `orders_raw.order_id`, so every page was a full scan + sort (the first
  `el11` attempt ran at 43k/s until the index was created mid-run). The index is now
  part of the schema, and the `el11` row in the matrix is the clean A/B run.
- **S3 Parquet RSS:** reading the 1.6 GiB `lake/orders.parquet` object through
  range-GETs peaks at ~800 MB on **both** engines (A/B above); the previous baseline
  read a differently-seeded object. This is a property of the S3 Parquet read path,
  tracked separately from the engine work.
- The ELT round runs entirely inside PostgreSQL, so its timings reflect the database,
  not the engine.
