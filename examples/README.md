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
| 09 | [jdbc-source-sqlite](09-jdbc-source-sqlite) | JDBC source using SQLite | SQLite |
| 10 | [csv-to-sqlite](10-csv-to-sqlite) | Write pipeline output to a SQLite database | SQLite |
| 11 | [schema-drift](11-schema-drift) | Detect and handle schema drift in incoming data | zero setup |
| 12 | [incremental-hwm](12-incremental-hwm) | Incremental loading with high-water mark tracking | zero setup |
| 13 | [data-quality](13-data-quality) | Data quality rules and scoring | zero setup |
| 14 | [prometheus-metrics](14-prometheus-metrics) | Expose pipeline metrics via Prometheus endpoint | zero setup |
| 15 | [checkpoint-resume](15-checkpoint-resume) | Checkpoint state and resume after interruption | zero setup |
| 16 | [scaling-sharded](16-scaling-sharded) | Horizontal scaling with sharded partitioning | zero setup |
| 17 | [kafka-streaming](17-kafka-streaming) | Kafka as streaming source and sink | Docker |
| 18 | [elt-mode](18-elt-mode) | ELT mode — load raw then transform in-database | SQLite |
| 19 | [full-production](19-full-production) | Full production pipeline combining all major features | Docker |

\* Example 08 requires Python/DuckDB to generate the test Parquet data file — see its README.

## Prerequisites

- **Go 1.22+** — <https://go.dev/dl/>
- **flogo-datapipeline binary** — built from this repository (see below)
- **sqlite3 CLI** — required for examples 09, 10, and 18; available from <https://www.sqlite.org/download.html>
- **Docker** — required for example 17 (Kafka) and example 19 (full production); available from <https://docs.docker.com/get-docker/>
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

| Feature | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 |
|---------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-----|
| filter | | ✓ | ✓ | | | | | | | | | | ✓ | | | ✓ | ✓ | | ✓ |
| map | | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | | | ✓ | | | | ✓ | ✓ | ✓ |
| validate | | | ✓ | | | | | | | | | | ✓ | | | | | | ✓ |
| dlq | | | ✓ | | | | | | | | | | ✓ | | | | | | ✓ |
| lookup | | | | ✓ | | | | | ✓ | | | | | | | | | | ✓ |
| hwm | | | | | | | | | | | | ✓ | | | ✓ | | | ✓ | ✓ |
| schemadrift | | | | | | | | | | | ✓ | | | | | | | | ✓ |
| dq | | | | | | | | | | | | | ✓ | | | | | | ✓ |
| metrics | | | | | | | | | | | | | | ✓ | | | ✓ | | ✓ |
| checkpoint | | | | | | | | | | | | | | | ✓ | | | | ✓ |
| scaling | | | | | | | | | | | | | | | | ✓ | | | ✓ |
| kafka | | | | | | | | | | | | | | | | | ✓ | | ✓ |
| objstore | | | | | | | | | | | | | | | | | | | ✓ |
