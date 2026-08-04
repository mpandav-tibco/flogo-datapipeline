# Flogo Data Pipeline Examples

These examples demonstrate each feature of flogo-datapipeline independently. Start with 01 and work forward — each builds on the previous.

## Examples

| # | Example | Feature | Effort |
|---|---------|---------|--------|
| 01 | [csv-to-null](01-csv-to-null) | Read CSV, discard output — baseline pipeline skeleton | zero setup |
| 02 | [filter-and-map](02-filter-and-map) | Row filtering and field mapping/transformation | zero setup |
| 03 | [validate-and-dlq](03-validate-and-dlq) | Schema validation with dead-letter queue for bad records | zero setup |
| 04 | [http-lookup-enrich](04-http-lookup-enrich) | HTTP lookup to enrich records at runtime | zero setup |
| 05 | [fixed-width-ebcdic](05-fixed-width-ebcdic) | Fixed-width / EBCDIC mainframe file ingestion | zero setup |
| 06 | [xml-source](06-xml-source) | XML file as pipeline source | zero setup |
| 07 | [jsonlines-source](07-jsonlines-source) | JSON Lines (NDJSON) file as pipeline source | zero setup |
| 08 | [parquet-columnar](08-parquet-columnar) | Parquet columnar file ingestion | zero setup* |
| 11 | [schema-drift](11-schema-drift) | Detect and handle schema drift in incoming data | zero setup |
| 12 | [incremental-hwm](12-incremental-hwm) | Incremental loading with high-water mark tracking | Docker |
| 13 | [data-quality](13-data-quality) | Data quality rules and scoring | zero setup |
| 14 | [prometheus-metrics](14-prometheus-metrics) | Expose pipeline metrics via Prometheus endpoint | zero setup |
| 15 | [checkpoint-resume](15-checkpoint-resume) | Checkpoint state and resume after interruption | zero setup |
| 16 | [scaling-sharded](16-scaling-sharded) | Horizontal scaling with sharded partitioning | zero setup |
| 17 | [kafka-streaming](17-kafka-streaming) | Kafka as streaming source and sink | Docker |
| 19 | [full-production](19-full-production) | Full production pipeline combining all major features | Docker |
| 20 | [podlog](20-podlog) | 🚧 Kubernetes pod-log collector (design / WIP — not yet runnable) | — |
| 21 | [otel-metrics](21-otel-metrics) | Export pipeline metrics via OpenTelemetry (OTLP/gRPC) | OTel collector |
| 22 | [format-conversion](22-format-conversion) | Convert between data formats (CSV / JSON / Parquet) | zero setup |

\* Example 08 requires Python/DuckDB to generate the test Parquet data file — see its README.

## Prerequisites

- **Go 1.22+** — <https://go.dev/dl/>
- **flogo-datapipeline binary** — built from this repository (see below)
- **Docker** — required for examples 12 (Postgres), 17 (Kafka + Postgres), and 19 (full production); available from <https://docs.docker.com/get-docker/>
- **Python 3 + DuckDB** — required to generate test data for example 08; install with `pip install duckdb`

## Build the Binary

From the repository root:

```bash
go build -o flogo-datapipeline ./cmd/flogo-datapipeline
```

On Windows:

```bash
go build -o flogo-datapipeline.exe ./cmd/flogo-datapipeline
```

## Quick Start

Open any `.datapipeline` file in VS Code with **Flogo Data Pipeline Studio** installed to get the visual canvas. The extension provides drag-and-drop pipeline editing, field mapping, and one-click run.

## Feature Coverage Matrix

The table below shows which feature areas are exercised by each example. Numbers refer to the example index (01-19).

| Feature | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 19 |
|---------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-----|
| filter | | ✓ | ✓ | | | | | | | | ✓ | | | ✓ | ✓ | ✓ |
| map | | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | | | ✓ | | | | ✓ | ✓ |
| validate | | | ✓ | | | | | | | | ✓ | | | | | ✓ |
| dlq | | | ✓ | | | | | | | | ✓ | | | | | ✓ |
| lookup | | | | ✓ | | | | | | | | | | | | ✓ |
| hwm | | | | | | | | | | ✓ | | | ✓ | | | ✓ |
| schemadrift | | | | | | | | | ✓ | | | | | | | ✓ |
| dq | | | | | | | | | | | ✓ | | | | | ✓ |
| metrics | | | | | | | | | | | | ✓ | | | ✓ | ✓ |
| checkpoint | | | | | | | | | | | | | ✓ | | | ✓ |
| scaling | | | | | | | | | | | | | | ✓ | | ✓ |
| kafka | | | | | | | | | | | | | | | ✓ | ✓ |
| objstore | | | | | | | | | | | | | | | | ✓ |
