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
| Peak throughput | **1,580,061/s** (`el05_csv_null`) |
| Sources | csv, jsonl, parquet, postgres, kafka, s3 |
| Sinks | parquet, postgres, kafka, s3, null |
| Transforms | filter, map, validate, schemadrift, dq, hwm, lookup |

See [reports/matrix.md](reports/matrix.md), [reports/elt.md](reports/elt.md),
[reports/enterprise.md](reports/enterprise.md) and [reports/graph.md](reports/graph.md)
for the full tables.

## Footprint & TCO

Every case above is a **single process** (`scaling.mode=single`) — one
statically-linked binary, no cluster, no JVM, no external runtime.

| Metric | Value |
|--------|-------|
| Instances | **1** (single-instance; sharded/distributed modes exist but were not needed) |
| Deployed binary size | **7.6 to 14.2 MB per app (all-in-one harness 57.9 MB)** |
| Host | Apple M1 Pro · 10 cores · 32 GB RAM |
| Peak memory (RSS) across matrix | **35–347 MB** |
| CPU used (avg cores) across matrix | **0.3–2.6 of 10 cores** |

53,000,000 rows were moved by a single process peaking at a few hundred MB of RAM and a
fraction of the host's cores — no cluster to provision, license, or scale.

### Per-app binary size (import-driven `dp-build`)

A deployed pipeline is compiled with `dp-build`, which links ONLY the
components the app references — the datapipeline analogue of `flogo build`
(see `build/imports.go`). Each app ships far smaller than the all-in-one
engine, and CGO-free unless it uses the sqlite driver.

| Deployed pipeline | Binary |
|-------------------|-------:|
| `csv -> null` | **7.6 MB** |
| `csv -> postgres (+lib/pq)` | **8.2 MB** |
| `csv -> s3 (+AWS SDK +parquet)` | **14.0 MB** |
| `kafka -> parquet (+sarama)` | **13.6 MB** |
| `csv -> filter/validate/map -> s3` | **14.2 MB** |
| _all-in-one benchmark binary (every source x sink)_ | 57.9 MB |

## ELT round

3 in-database transforms over the full table. See [reports/elt.md](reports/elt.md).

## Reliability

- **DLQ isolation:** 50,589,628 good + 2,410,372 rejected = 53,000,000 processed, no rows lost.
- **Checkpoint/resume:** interrupted after 3,000,000 rows, resumed and finished (completed).

## Graph (DAG) fan-out

The streaming graph runner routes **one source read to N sinks in a
single pass** - the linear engine would need one full pass per sink. 5 non-reducible DAGs ran at up to **727,442/s**, matching the linear engine while doing N-way fan-out; a 5-way regional router finishes ~5.7x sooner than five linear passes and reads the source once.

The **multi-source consolidation** DAG (`graph05_multi_consolidate`) fans **three feeds in** (CSV + JSON Lines + Parquet, 159,000,000 rows) through a filter -> map -> validate chain and **six sinks out** in one 486 MB pass at 540,010/s - a shape the single-source/single-sink linear engine cannot express at all.

See [reports/graph.md](reports/graph.md).

