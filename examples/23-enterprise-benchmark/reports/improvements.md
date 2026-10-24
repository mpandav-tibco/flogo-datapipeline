# Benchmark Improvements — POC baseline vs current engine

Same host, same 53,000,000-row dataset, same pipelines, same lean per-pipe
binaries (`dp-build`). **Before** is the original POC run (the published
baseline in `harness/baseline_poc/`); **after** is the current `master`
(unified execution engine + the performance/correctness work of PR #62).

Columns: duration and throughput are engine-measured; peak RSS and CPU are
sampled for the single engine process. *CPU time* is total CPU-seconds the
process consumed; *rows per CPU-s* = rows processed ÷ CPU-seconds — the cost
efficiency (higher is cheaper). Bold marks a material improvement.

## Headline

| Metric | POC baseline | Current | Change |
|--------|-------------:|--------:|-------:|
| Dataset | 53,000,000 rows | 53,000,000 rows | same |
| Matrix cases passed | 30 / 30 | 30 / 30 | same |
| Output row counts | — | identical in every case | correctness parity |
| Peak throughput | 1,580,061/s (`el05_csv_null`) | 1,522,770/s (`el05_csv_null`) | -4% |
| Total matrix wall time (30 cases) | 195 min | 70 min | **2.8x faster** |
| Total CPU-seconds (cases with CPU data) | 10,042 | 7,309 | **-27%** |
| Throughput, geometric mean across 30 cases | — | — | **1.89x** |
| Cases faster (≥5%) / same / slower (≥5%) | — | — | 16 / 10 / 4 |
| Cases with lower peak memory | — | — | 23 of 30 |
| Peak memory (RSS) across matrix | 35 – 346 MB | 26 – 828 MB | |
| Peak memory (RSS), median of 30 cases | 152 MB | 119 MB | **-21%** |
| CPU used (avg cores) across matrix | 0.3 – 2.6 of 10 | 0.7 – 3.2 of 10 | |
| Host | Apple M1 Pro · 10 cores · 32 GB | same | |

## By category

| Category | Cases | Throughput (geo-mean speed-up) | Best case | Peak RSS (median before → after) |
|----------|------:|-------------------------------:|-----------|----------------------------------:|
| Extract-Load | 17 | 1.55x | `el06_parquet_postgres` 6.6x | 154 MB → 106 MB |
| Extract-Transform-Load | 11 | 2.45x | `etl07_parquet_full_postgres` 18.1x | 152 MB → 121 MB |
| Enterprise (DLQ, resume) | 2 | 2.46x | `ent01_dlq_isolation` 5.6x | 150 MB → 173 MB |
| Graph / DAG fan-out | 5 | 1.51x | `graph01_route_region_parquet` 2.2x | 216 MB → 160 MB |

## Extract - Load (raw, no transform)

| Case | Pipeline | Rows out | Duration | Throughput (rows/s) | Speed-up | Peak RSS | CPU cores | CPU time | Rows per CPU-s |
|------|----------|---------:|---------:|--------------------:|---------:|---------:|----------:|---------:|---------------:|
| el01_csv_parquet | `csv -> parquet` | 53,000,000 | 89.0s → 89.0s | 595,613 → **595,198** | -0% | 148 MB → 97 MB (**-34%**) | 2.0 → 1.9 | 178s → 172s (-3%) | 298,088 → 308,678 (+4%) |
| el02_csv_postgres | `csv -> jdbc` | 53,000,000 | 866.3s → 144.2s | 61,183 → **367,471** | **6.0x** | 62 MB → 51 MB (**-17%**) | 0.3 → 0.8 | 263s → 120s (**-54%**) | 201,521 → 440,932 (**2.2x**) |
| el03_csv_kafka | `csv -> kafka` | 53,000,000 | 281.8s → 286.9s | 188,047 → **184,739** | -2% | 66 MB → 62 MB (**-5%**) | 1.4 → 1.4 | 392s → 393s (+0%) | 135,032 → 134,894 (-0%) |
| el04_csv_s3 | `csv -> s3` | 53,000,000 | 92.0s → 92.3s | 576,338 → **574,339** | -0% | 204 MB → 133 MB (**-35%**) | 1.9 → 1.9 | 179s → 175s (-2%) | 296,089 → 303,551 (+3%) |
| el05_csv_null | `csv -> null` | 53,000,000 | 33.5s → 34.8s | 1,580,061 → **1,522,770** | -4% | 39 MB → 26 MB (**-33%**) | 1.4 → 1.2 | 49s → 44s (**-10%**) | 1,090,535 → 1,218,391 (+12%) |
| el06_parquet_postgres | `parquet -> jdbc` | 53,000,000 | 805.2s → 121.5s | 65,824 → **436,085** | **6.6x** | 234 MB → 208 MB (**-11%**) | 0.3 → 0.9 | 250s → 113s (**-55%**) | 211,661 → 467,785 (**2.2x**) |
| el07_parquet_kafka | `parquet -> kafka` | 53,000,000 | 246.4s → 266.6s | 215,074 → **198,816** | -8% | 245 MB → 207 MB (**-15%**) | 1.5 → 1.4 | 370s → 378s (+2%) | 143,437 → 140,397 (-2%) |
| el08_parquet_s3 | `parquet -> s3` | 53,000,000 | 91.5s → 91.5s | 579,507 → **579,273** | -0% | 347 MB → 278 MB (**-20%**) | 2.1 → 2.0 | 192s → 183s (-5%) | 276,618 → 290,252 (+5%) |
| el09_jsonl_parquet | `jsonlines -> parquet` | 53,000,000 | 180.7s → 185.3s | 293,244 → **286,089** | -2% | 146 MB → 90 MB (**-38%**) | 1.9 → 1.7 | 338s → 317s (**-6%**) | 156,573 → 167,087 (+7%) |
| el10_jsonl_postgres | `jsonlines -> jdbc` | 53,000,000 | 845.0s → 196.8s | 62,725 → **269,260** | **4.3x** | 73 MB → 60 MB (**-18%**) | 0.5 → 1.4 | 388s → 271s (**-30%**) | 136,563 → 195,716 (+43%) |
| el11_postgres_parquet | `jdbc -> parquet` | 53,000,000 | 108.1s → 126.1s | 490,405 → **420,248** | -14% | 154 MB → 114 MB (**-26%**) | 2.1 → 2.0 | 222s → 258s (+16%) | 238,309 → 205,108 (-14%) |
| el12_postgres_kafka | `jdbc -> kafka` | 53,000,000 | 293.7s → 299.0s | 180,485 → **177,276** | -2% | 66 MB → 66 MB (-0%) | 1.5 → 1.6 | 451s → 486s (+8%) | 117,621 → 109,166 (-7%) |
| el13_postgres_s3 | `jdbc -> s3` | 53,000,000 | 113.5s → 130.7s | 466,825 → **405,527** | -13% | 204 MB → 135 MB (**-34%**) | 2.0 → 2.0 | 224s → 258s (+15%) | 236,607 → 205,267 (-13%) |
| el14_kafka_parquet | `kafka -> parquet` | 53,000,000 | 217.3s → 212.2s | 243,851 → **249,787** | +2% | 169 MB → 106 MB (**-37%**) | 2.3 → 2.2 | 505s → 473s (**-6%**) | 104,867 → 112,098 (+7%) |
| el15_kafka_postgres | `kafka -> jdbc` | 53,000,000 | 828.2s → 234.7s | 63,997 → **225,802** | **3.5x** | 86 MB → 70 MB (**-18%**) | 0.7 → 2.0 | 571s → 466s (**-18%**) | 92,885 → 113,832 (+23%) |
| el16_s3_parquet | `s3 -> parquet` | 53,000,000 | 117.3s → 124.7s | 451,902 → **425,136** | -6% | 314 MB → 819 MB (+161%) | 1.6 → 1.8 | 192s → 218s (+13%) | 275,468 → 242,785 (-12%) |
| el17_s3_postgres | `s3 -> jdbc` | 53,000,000 | 853.6s → 188.6s | 62,093 → **280,947** | **4.5x** | 230 MB → 828 MB (+260%) | - → 0.7 | - → 140s (-) | - → 378,842 (-) |

## Extract - Transform - Load (in-flight transforms)

| Case | Pipeline | Rows out | Duration | Throughput (rows/s) | Speed-up | Peak RSS | CPU cores | CPU time | Rows per CPU-s |
|------|----------|---------:|---------:|--------------------:|---------:|---------:|----------:|---------:|---------------:|
| etl01_csv_filter_parquet | `csv -> filter -> parquet` | 38,677,277 | 90.5s → 66.6s | 585,487 → **796,142** | +36% | 157 MB → 104 MB (**-34%**) | 2.0 → 2.2 | 184s → 150s (**-19%**) | 287,575 → 354,515 (+23%) |
| etl02_csv_filtermap_parquet | `csv -> filter/map -> parquet` | 38,677,277 | 116.1s → 68.3s | 456,523 → **776,261** | **1.7x** | 144 MB → 121 MB (**-16%**) | 2.0 → 2.5 | 228s → 168s (**-26%**) | 232,252 → 315,476 (+36%) |
| etl03_csv_clean_postgres | `csv -> validate/map -> jdbc` | 50,589,628 | 1621.7s → 112.6s | 32,681 → **470,546** | **14.4x** | 61 MB → 138 MB (+127%) | 0.4 → 1.6 | 613s → 182s (**-70%**) | 86,460 → 291,529 (**3.4x**) |
| etl04_csv_full_s3 | `csv -> filter/validate/map -> s3` | 37,942,161 | 468.4s → 77.3s | 113,153 → **685,809** | **6.1x** | 181 MB → 138 MB (**-23%**) | 1.4 → 2.5 | 648s → 191s (**-71%**) | 81,727 → 277,778 (**3.4x**) |
| etl05_postgres_map_kafka | `jdbc -> map -> kafka` | 53,000,000 | 378.8s → 273.5s | 139,897 → **193,758** | +39% | 64 MB → 93 MB (+46%) | 1.5 → 2.0 | 559s → 538s (-4%) | 94,795 → 98,586 (+4%) |
| etl06_kafka_filter_parquet | `kafka -> filter -> parquet` | 38,677,277 | 223.5s → 210.0s | 237,130 → **252,374** | +6% | 153 MB → 86 MB (**-44%**) | 2.3 → 2.1 | 517s → 433s (**-16%**) | 102,554 → 122,430 (+19%) |
| etl07_parquet_full_postgres | `parquet -> filter/validate/map -> jdbc` | 37,942,161 | 1708.5s → 94.6s | 31,022 → **560,058** | **18.1x** | 220 MB → 281 MB (+28%) | 0.4 → 1.6 | 710s → 150s (**-79%**) | 74,700 → 354,041 (**4.7x**) |
| etl08_csv_schemadrift_parquet | `csv -> schemadrift -> parquet` | 53,000,000 | 87.9s → 88.3s | 603,253 → **600,097** | -1% | 152 MB → 118 MB (**-22%**) | 2.0 → 2.0 | 178s → 180s (+1%) | 298,423 → 294,118 (-1%) |
| etl09_csv_dq_parquet | `csv -> dq -> parquet` | 53,000,000 | 176.1s → 99.0s | 300,988 → **535,289** | **1.8x** | 146 MB → 125 MB (**-14%**) | 1.7 → 3.2 | 298s → 316s (+6%) | 178,151 → 167,881 (-6%) |
| etl10_csv_hwm_parquet | `csv -> hwm -> parquet` | 53,000,000 | 90.7s → 88.0s | 584,666 → **602,006** | +3% | 152 MB → 121 MB (**-20%**) | 2.0 → 2.1 | 181s → 183s (+1%) | 292,494 → 290,252 (-1%) |
| etl11_csv_lookup_parquet | `csv -> lookup/map -> parquet` | 53,000,000 | 182.3s → 91.3s | 290,803 → **580,421** | **2.0x** | 153 MB → 119 MB (**-22%**) | 2.6 → 2.5 | 475s → 226s (**-52%**) | 111,509 → 234,513 (**2.1x**) |

## Enterprise reliability cases (matrix)

| Case | Pipeline | Rows out | Duration | Throughput (rows/s) | Speed-up | Peak RSS | CPU cores | CPU time | Rows per CPU-s |
|------|----------|---------:|---------:|--------------------:|---------:|---------:|----------:|---------:|---------------:|
| ent01_dlq_isolation | `csv -> validate -> parquet` | 50,589,628 | 478.9s → 86.1s | 110,661 → **615,249** | **5.6x** | 150 MB → 173 MB (+15%) | 1.2 → 2.6 | 587s → 225s (**-62%**) | 90,290 → 235,556 (**2.6x**) |
| ent02_checkpoint_resume | `csv -> filter -> null` | 38,677,277 | 38.9s → 35.8s | 1,362,118 → **1,481,730** | +9% | 35 MB → 26 MB (**-26%**) | 2.6 → 1.3 | 100s → 46s (**-53%**) | 532,663 → 1,139,785 (**2.1x**) |

## Graph (DAG) fan-out / fan-in

One source read routed to N sinks in a single pass (graph05: 3 sources → 6 sinks).

| Case | Pipeline | Rows out | Duration | Throughput (rows/s) | Speed-up | Peak RSS | CPU cores | CPU time | Rows per CPU-s |
|------|----------|---------:|---------:|--------------------:|---------:|---------:|----------:|---------:|---------------:|
| graph01_route_region_parquet | `regional fan-out router (5-way)` | 39,750,329 | 78.6s → 35.4s | 674,395 → **1,496,879** | **2.2x** | 216 MB → 160 MB (**-26%**) | 3.2 → 3.6 | 249s → 128s (**-48%**) | 212,851 → 412,773 (**1.9x**) |
| graph02_valreject_parquet | `valid / reject split (DLQ)` | 39,750,329 | 72.9s → 64.5s | 727,442 → **821,807** | +13% | 158 MB → 118 MB (**-25%**) | 3.3 → 2.3 | 238s → 148s (**-38%**) | 222,596 → 358,835 (**1.6x**) |
| graph03_tee_audit_parquet | `tee + high-value audit` | 43,617,427 | 76.7s → 69.1s | 691,256 → **767,393** | +11% | 179 MB → 118 MB (**-34%**) | 3.2 → 2.3 | 249s → 161s (**-35%**) | 213,108 → 330,012 (**1.5x**) |
| graph04_route_region_s3 | `regional fan-out router (5-way) -> S3` | 39,750,329 | 81.3s → 40.8s | 651,923 → **1,298,320** | **2.0x** | 276 MB → 213 MB (**-23%**) | 3.1 → 3.2 | 250s → 132s (**-47%**) | 211,577 → 402,736 (**1.9x**) |
| graph05_multi_consolidate | `multi-source consolidation (3 feeds -> filter/map/validate -> 6 sinks)` | 119,250,987 | 294.4s → 205.5s | 540,010 → **773,704** | +43% | 486 MB → 538 MB (+11%) | 4.5 → 3.7 | 1333s → 755s (**-43%**) | 119,244 → 210,512 (**1.8x**) |

## Reliability round

| Scenario | POC baseline | Current |
|----------|-------------:|--------:|
| DLQ isolation: good + rejected = processed | 50,589,628 + 2,410,372 = 53,000,000 (conserved) | 50,589,628 + 2,410,372 = 53,000,000 (conserved) |
| DLQ isolation duration / throughput | 477.9s / 110,911/s | 88.3s / 600,403/s (**5.4x**) |
| Checkpoint/resume: rows committed before interruption | 3,000,000 | 12,000,000 |
| Checkpoint/resume: resumed run | completed, 53,000,000 processed in 102.6s | completed, 53,000,000 processed in 35.4s |

## Same-day A/B validation (cases at or below the POC numbers)

Cases that did not improve against the months-old POC run were re-run
back-to-back against binaries built from the pre-change engine, on the same
day, infra and data (two rounds per engine; pre-change = master before PR #62 (old linear/graph engines)). This separates environment drift from engine
behaviour: where both engines land at the same number, the gap to the POC
baseline is environmental.

| Case | Pre-change engine (rows/s) | Current engine (rows/s) | Change | Peak RSS pre → current |
|------|---------------------------:|------------------------:|-------:|-----------------------:|
| el03_csv_kafka | 236,918 / 224,992 | 234,300 / 226,207 | -0% | 67 MB → 63 MB |
| el05_csv_null | 1,535,119 / 1,526,014 | 1,548,938 / 1,530,951 | +1% | 39 MB → 26 MB |
| el07_parquet_kafka | 229,439 / 224,245 | 232,707 / 217,631 | -1% | 225 MB → 215 MB |
| el11_postgres_parquet | 424,418 / 426,817 | 427,144 / 427,844 | +0% | 165 MB → 109 MB |
| el13_postgres_s3 | 404,518 / 407,360 | 404,806 / 415,100 | +1% | 212 MB → 149 MB |
| el16_s3_parquet | 429,411 / 431,902 | 441,001 / 438,764 | +2% | 852 MB → 839 MB |
| el17_s3_postgres | 60,136 / 59,825 | 402,134 / 403,247 | **6.7x** | 832 MB → 816 MB |

## ELT round (runs inside PostgreSQL)

Pushed-down SQL; timings reflect the database, not the engine.

| Case | What | Before | After |
|------|------|-------:|------:|
| elt01_curate | Filter + normalise + derive net_amount in-database | 79.2s | 51.5s |
| elt02_rollup | GROUP BY region x category revenue aggregate | 10.5s | 8.9s |
| elt03_dedup | Window de-duplication (latest row per order_id) | 167.6s | 81.3s |

## Deployed binary size (lean per-app `dp-build`)

| Deployed pipeline | POC baseline | Current |
|-------------------|-------------:|--------:|
| `csv -> null` | 7.6 MB | 8.8 MB |
| `csv -> postgres (+lib/pq)` | 8.2 MB | 9.4 MB |
| `csv -> s3 (+AWS SDK +parquet)` | 14.0 MB | 14.5 MB |
| `kafka -> parquet (+sarama)` | 13.6 MB | 14.1 MB |
| `csv -> filter/validate/map -> s3` | 14.2 MB | 15.3 MB |
| `all-in-one binary (every source x sink)` | 57.9 MB | 57.8 MB |

## Where the gains come from

- **Postgres sinks** default to `COPY` (with automatic INSERT fallback and per-row DLQ
  isolation), instead of batched multi-row INSERTs — the 3.5–18x jdbc-sink speed-ups.
- **One streaming executor**: the transform chain runs as its own fused stage, overlapped
  with source reads and sink writes (bounded channels, back-pressure), for linear pipelines
  and DAGs alike.
- **Positional column binding** in filter / router / validate / map / dq / join — no per-row
  map allocation — lower CPU per row and lower memory.
- **Lookup** negative caching (not-found keys are not re-fetched) and a bounded LRU.
- **DQ** gate evaluates expectations on bound columns.
- **Graph/DAG** nodes buffer sink writes and share the memory budget.

## Notes

- The jdbc-source cases page with keyset pagination on `orders_raw.order_id`; the index is
  part of `sql/schema.sql` (both runs had it).
- Reading the 1.6 GiB Parquet object from S3 (`el16`, `el17`) peaks at ~800 MB RSS on both
  the old and the new engine in a same-day A/B (see [engine-unification.md](engine-unification.md));
  the POC baseline read a differently-seeded object, so its lower RSS is not comparable.
- The checkpoint/resume interruption is time-based, so the faster engine has committed
  more rows (and resumes from further along) when it is interrupted.
- Single-run numbers on a laptop: expect ±3–5% run-to-run noise; treat changes inside that
  band as parity. Kafka-sink and postgres-source throughput also drift with broker/table
  state between days — the same-day A/B above shows both engines at the same number, and
  `el17` (s3 -> postgres) measured 281k/s in the matrix and 313k/s on an immediate re-run.
- Lean binaries are ~1 MB larger than in the POC, almost entirely from the newer Go
  toolchain and dependency updates: the pre-change engine built today with the same
  toolchain is 8.73 MB for `csv -> null` vs 8.84 MB now (+0.1 MB for this work). All
  sizes are stripped builds (`-s -w`).
