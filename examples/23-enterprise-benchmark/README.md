# Enterprise Data Pipeline Benchmark

A single, self-contained proof that the Flogo data pipeline engine handles a
**real enterprise workload at scale**: one 53-million-row order dataset
(~5 GB CSV / 12 GB JSONL / 1.7 GB Parquet) moved from **every supported source
to every supported sink**, exercising **all seven transform activities**, plus
an in-database ELT round and two production-reliability showcases.

## The scenario

A retailer consolidates its **order history** from four systems of record into
an analytics warehouse and a data lake:

| System of record | Format | Role in the benchmark |
|------------------|--------|-----------------------|
| Legacy export drops | CSV / JSON Lines | file sources |
| Operational database | PostgreSQL | relational source & warehouse sink |
| Order event bus | Kafka | streaming source & sink |
| Data lake | S3 (MinIO) / Parquet | columnar source & sink |

The pipeline must land raw feeds fast, clean and validate messy rows, enrich by
region, quarantine bad records for audit, and resume cleanly after an
interruption — the everyday demands of a production data platform.

## What it proves

- **Every source x every sink** — 17 extract-load pipelines cover csv, jsonl,
  parquet, postgres, kafka and s3 as both source and destination.
- **All seven transforms** — filter, map, validate, schemadrift, dq
  (data-quality gate), hwm (incremental high-water-mark) and lookup
  (HTTP enrichment), across 11 ETL pipelines.
- **In-database ELT** — curate, roll-up and de-duplicate the full 53 M-row
  table server-side (transform pushed into PostgreSQL).
- **Enterprise reliability** — dead-letter isolation (no row lost) and
  checkpoint/resume (interrupt-safe loads).

Every pipeline is one Flogo app (a timer trigger fires it once) executed by a
single statically-linked Go binary — no external runtime, no cluster.

## Results

See **[SUMMARY.md](SUMMARY.md)** for the headline numbers and
**[reports/](reports/)** for the full tables:
[matrix.md](reports/matrix.md) · [elt.md](reports/elt.md) ·
[enterprise.md](reports/enterprise.md).

The `pipes/` directory holds all 30 pipeline definitions — they are the
showcase artifacts and are meant to be read.

## Run it yourself

Prerequisites: Docker, Go (CGO enabled), Python 3.

```bash
# one command, end to end (infra -> build -> data -> seed -> run -> reports)
./scripts/run-benchmark.sh                 # full 53,000,000-row run (~an hour)
./scripts/run-benchmark.sh 1000000         # 1M-row quick pass (~minutes)
```

Or drive the stages individually:

```bash
./scripts/setup-infra.sh                   # postgres:5433, kafka:9092, minio:9000
./scripts/build.sh                         # -> bin/dp (single binary)
./scripts/gen-data.sh 53000000             # -> data/orders.{csv,jsonl,parquet}
python3 harness/gen_pipes.py               # -> pipes/*.datapipeline + cases.json
python3 harness/seed.py pg kafka s3        # load the backends

python3 harness/lookup_mock.py 9099 &      # enrichment endpoint for the lookup case
python3 harness/run_matrix.py              # 17 EL + 11 ETL + 2 reliability cases
python3 harness/run_elt.py                 # 3 in-database transforms
python3 harness/run_enterprise.py          # DLQ + checkpoint/resume
python3 harness/gen_reports.py             # -> SUMMARY.md, reports/*.md

./scripts/teardown-infra.sh                # tear the containers down
```

Run a single case while iterating: `python3 harness/run_matrix.py el14_kafka_parquet`.

## Layout

```
pipes/       30 pipeline definitions (the showcase) + 4 _prep loaders
harness/     gen_pipes, seed, run_matrix, run_elt, run_enterprise, gen_reports, lib
scripts/     setup-infra, build, gen-data, run-benchmark
sql/         warehouse schema (orders_raw / _sink / _clean / _rejects)
reports/     generated result tables      SUMMARY.md  headline
data/        generated dataset (git-ignored; regenerate with gen-data.sh)
```

The dataset is deterministic (seed 42) and regenerates byte-for-byte, so results
are reproducible on any machine.
