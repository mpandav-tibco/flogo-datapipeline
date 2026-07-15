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

## Lean per-app binaries (import-driven build)

The `bin/dp` used above is the **all-in-one** engine — it links every source,
sink, transform and driver so one binary can run all 30 cases. That is a test
harness, not a deployment artifact.

A *deployed* pipeline ships as its own **lean binary** containing only the
components it references — the same import-driven model as Flogo flow and the
Flogo agents. The core derivation lives in [build/imports.go](../../build/imports.go)
and is driven by [cmd/dp-build](../../cmd/dp-build):

```bash
./scripts/build-lean.sh                     # build lean binaries for sample pipes
./bin/dp-build -n -f pipes/el04_csv_s3.datapipeline   # show the derived imports
./bin/dp-build -f pipes/el04_csv_s3.datapipeline -o bin/orders   # build one
```

Each app links only what it uses, is CGO-free unless it needs sqlite, and ships
as one small static file (see the *Footprint & TCO* table in
[SUMMARY.md](SUMMARY.md)):

| Deployed pipeline | Binary |
|-------------------|-------:|
| csv → null | ~7.5 MB |
| csv → postgres | ~8 MB |
| csv → s3 | ~14 MB |
| all-in-one benchmark engine | ~58 MB |

## Layout

```
pipes/       30 pipeline definitions (the showcase) + 4 _prep loaders
harness/     gen_pipes, seed, run_matrix, run_elt, run_enterprise, gen_reports, lib
scripts/     setup-infra, build, build-lean, gen-data, run-benchmark, teardown-infra
sql/         warehouse schema (orders_raw / _sink / _clean / _rejects)
reports/     generated result tables      SUMMARY.md  headline
data/        generated dataset (git-ignored; regenerate with gen-data.sh)
```

The import-driven lean builder is part of the repo core, not this example:
[build/imports.go](../../build/imports.go) (derivation) and
[cmd/dp-build](../../cmd/dp-build) (the `flogo build`-style command).

The dataset is deterministic (seed 42) and regenerates byte-for-byte, so results
are reproducible on any machine.
