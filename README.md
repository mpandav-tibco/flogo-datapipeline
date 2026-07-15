# Flogo Data Pipeline

A lightweight, embeddable data pipeline engine built on the [Project Flogo](https://github.com/project-flogo/core) runtime, with a visual authoring experience inside VS Code.

---

## Why we are building this

Enterprise integration teams spend a disproportionate amount of time on data movement: extracting records from one system, applying business rules, and loading them into another. Most tools in this space are either too heavy (full ETL platforms requiring dedicated infrastructure), too raw (hand-written scripts with no operational visibility), or too locked-in (cloud-vendor-specific services).

We are building Flogo Data Pipeline to fill the gap for teams that need:

- **A single deployable binary** — no JVM, no Docker required to get started, no cluster to manage for small-to-medium workloads.
- **A Flogo-native programming model** — pipelines are Flogo app JSON, so they compose naturally with existing Flogo triggers, activities, and the broader Flogo ecosystem.
- **A visual editor that lives in the developer's IDE** — the VS Code extension brings a canvas directly into the tool engineers already have open, with no browser, no SaaS account, no account required.
- **Honest operational primitives** — checkpointing, watermarking, schema drift detection, data quality scoring, and Prometheus metrics are first-class concepts, not add-ons.

This is an active project at an early stage. The sections below document what is **working today** and what is **planned next**.

---

## What it is

### The engine (Go binary)

A single Go binary (`flogo-datapipeline`) that reads a `.datapipeline` file, instantiates a pipeline from the Flogo resource descriptor, and runs it. The pipeline executes as:

```
Source  →  [Transform₁ → Transform₂ → … ]  →  Sink
                                               ↓ (bad rows)
                                              DLQ Sink
```

The binary exposes an optional REST API (`/v1/…`) for health checks, triggering runs, and reading run history. It can be embedded in a larger Flogo application or run standalone.

### The VS Code extension

A custom editor for `.datapipeline` files that gives you:

- A visual canvas to compose source → transform chain → sink (plus a DLQ branch)
- A properties panel to configure each stage — no raw JSON editing required
- Plain-language structured editors for columns, field mappings, validation
  rules, data-quality expectations, filters and the JDBC conflict key
- **Reusable connections** — define a database / stream / object-store / HTTP
  endpoint once as a `*.fgconn` file and bind it by name on any source or sink.
  Secrets are kept in VS Code Secret Storage, never written to a file
- **Schema inference** — one click samples a CSV/TSV/JSON-lines file to fill in
  column names and types
- File/folder browse buttons for path inputs
- One-click run and stop with streamed status, plus a side-effect-free dry-run
  preview into the native Data Preview panel
- Pipeline templates to bootstrap new pipelines in seconds
- An organized project layout (`pipelines/`, `connections/`, `data/`) and
  sidebar views for pipelines and connections

See [`studio/vscode/README.md`](studio/vscode/README.md) for full details.

---

## What is available today

### Sources (10)

| Source | What it reads |
|---|---|
| CSV | Delimited text files with optional header row; configurable separator |
| TSV | Tab-separated variant of CSV |
| Fixed-Width (EBCDIC) | Positional flat files from mainframe systems; EBCDIC-037/500 encoding, implied decimal support |
| Parquet | Columnar Parquet files with row-group parallelism and column/predicate pushdown |
| XML | Streaming SAX-style XML; one record per named element |
| JSON Lines / NDJSON | Newline-delimited JSON, one object per row |
| JDBC | SQL query over PostgreSQL, MySQL, or SQLite with keyset-pagination chunking and high-water-mark predicate injection |
| Kafka | Consumer group-based consumption with offset checkpointing |
| Cloud Object Store | Files from S3, Google Cloud Storage, or Azure ADLS Gen2 |
| Text Document (sectioned) | Flat files with a key/value header block plus a delimited body table |

### Sinks (5 + DLQ)

| Sink | What it writes |
|---|---|
| JDBC | Bulk insert / upsert into PostgreSQL, MySQL, or SQLite; configurable conflict strategy |
| Parquet | Row-group-aligned Parquet output |
| Kafka | Producer with exactly-once semantics via transactions |
| Cloud Object Store | S3, GCS, or Azure Blob; output key supports date macros |
| Null | Discards all rows (benchmarking and dry-run) |
| **Dead Letter Queue (DLQ)** | Captures rows rejected by the validate transform; can be any supported sink type |

### Transforms (7)

| Transform | What it does |
|---|---|
| Filter | Row-level boolean expression; non-matching rows are dropped |
| Map | Field projection, rename, type coercion, and expression-derived columns |
| Validate | Per-field rules (not-null, min, max, pattern, enum); violations route to DLQ or fail-fast |
| Lookup | HTTP enrichment with LRU cache; appends columns from an external API call |
| High-Water Mark (HWM) | Tracks the maximum value of a column per batch for incremental load patterns |
| Schema Drift | Detects column additions, removals, and type changes at runtime; policies: `additive`, `project`, `strict`, `drop` |
| Data Quality (DQ) | Evaluates per-column expectations; produces a DQ report with pass/fail scores |

### Operational primitives

| Feature | Backends available |
|---|---|
| Execution mode | `etl` (transform in-flight), `el` (extract-load, no transforms), `elt` (load raw, transform in-target) |
| Checkpointing (resume after interruption) | Local file, Redis |
| Watermarking (incremental high-water marks) | Local file, Redis, PostgreSQL |
| Metrics | Prometheus endpoint (`/metrics`) + OpenTelemetry OTLP gRPC export |
| Distributed scaling | Sharded (static partition assignment) or distributed (PostgreSQL work-queue coordinator with lease-based claim, heartbeat, and retry) |

### Pipeline templates (6)

Ready-to-run templates in the VS Code extension:

- CSV → PostgreSQL (ETL with filter + map)
- Parquet on S3 → JDBC (ETL with validate + map, Redis checkpoint, scaling config)
- Database → Database incremental sync (HWM-driven)
- Kafka → JDBC streaming ETL
- Mainframe EBCDIC → PostgreSQL
- Extract & Load only (EL mode, no transforms)

### Examples (19)

Working examples covering every source type, every transform type, incremental loading, schema drift, data quality, Prometheus metrics, checkpoint/resume, horizontal scaling, Kafka streaming, and a full production pipeline composition.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────┐
│  VS Code Extension  (TypeScript + WebView canvas)       │
│  Visual editor → serialises to .datapipeline JSON        │
└───────────────────────┬─────────────────────────────────┘
                        │  file on disk
┌───────────────────────▼─────────────────────────────────┐
│  flogo-datapipeline binary  (Go, single static binary)  │
│                                                         │
│  Flogo engine ──► action/pipeline                       │
│       │                                                 │
│  Source ──► [Transform chain] ──► Sink                  │
│       │                              │                  │
│  Checkpoint (file / Redis)      DLQ Sink                │
│  Watermark  (file / Redis / PG) Metrics (Prom / OTLP)   │
└─────────────────────────────────────────────────────────┘
                        │  REST API (optional)
              /v1/health, /v1/pipelines, /v1/pipelines/{ref}/run
```

---

## Getting started

### Prerequisites

- Go 1.21+ (to build from source)
- VS Code 1.83+

### Build the binary

```bash
git clone https://github.com/mpandav-tibco/flogo-datapipeline
cd flogo-datapipeline
go build -o flogo-datapipeline ./cmd/flogo-datapipeline/
```

### Install the VS Code extension

1. Open VS Code
2. Go to Extensions → `…` → **Install from VSIX…**
3. Select the packaged `studio/vscode/flogo-datapipeline-studio-*.vsix`
   (build it with `cd studio/vscode && npm install && npm run package && npx vsce package`)
4. Set the binary path in settings: `flogoDataPipelineStudio.binaryPath`

### Run an example

```bash
./flogo-datapipeline -f examples/02-filter-and-map/filter-and-map.datapipeline
```

Or open the file in VS Code and click **Run** in the canvas toolbar.

---

## Enterprise use cases we are targeting

These are the specific enterprise problems we are designing for:

### 1. Mainframe / legacy data offload

Large enterprises run critical record systems on mainframes producing EBCDIC-encoded fixed-width files. Getting that data into modern stores (PostgreSQL, S3, Parquet) typically requires specialised ETL tooling. The fixed-width source with EBCDIC/037/500 encoding and implied-decimal support is designed specifically for this pattern.

**Target:** On-prem to cloud migration; batch offload from mainframe batch jobs.

### 2. Incremental database synchronisation

Extracting full tables on a schedule is expensive and slow at scale. The HWM (high-water mark) pattern paired with the JDBC source and a watermark backend allows pipelines to resume from the last processed row across restarts.

**Target:** Database-to-database sync, operational data store refresh, CDC-light patterns.

### 3. Streaming Kafka ingestion into JDBC / data lake

Teams building event-driven architectures need a lightweight path from Kafka topics to relational databases or cloud data lakes without a full Kafka Streams or Flink deployment. The Kafka source + JDBC/Parquet/S3 sink path covers this.

**Target:** Event log archiving, operational reporting from event streams, streaming ETL into Snowflake-ready Parquet.

### 4. Data quality enforcement at ingestion time

Enterprises often discover data quality problems after they have propagated through multiple systems. The validate transform enforces per-field rules at ingestion time; violations are routed to a DLQ sink so bad records are captured, not silently dropped.

**Target:** Financial data ingestion, regulatory reporting, MDM feed processing.

### 5. Cloud data lake ingestion (Parquet / object store)

Teams loading data into data lakes need to land files in Parquet format on S3, GCS, or Azure Blob with correct row-group sizing for downstream query performance. The Parquet sink with row-group configuration and the object store sinks cover this.

**Target:** Data lake / lakehouse ingestion layer, Spark / Athena / BigQuery pre-processing.

### 6. Schema drift monitoring in live feeds

Production data feeds change over time without notice. The schema drift transform detects column additions, removals, and type changes at runtime and applies a configurable policy so pipelines do not silently break or silently ingest structurally invalid data.

**Target:** Third-party data feed ingestion, API-to-database pipelines, partner data exchange.

---

## What is planned next

These are honest work items — not committed roadmap, but the next things we intend to build:

### Engine

- [ ] **Parquet source column pushdown to remote stores** — currently reads all columns when source is S3/GCS-backed Parquet; predicate and column pruning before download
- [ ] **REST-API `/v1/pipelines/{ref}` PUT and definition GET** — currently stubbed as 501; needed for runtime pipeline update without restart
- [ ] **MSSQL and Oracle JDBC drivers** — PostgreSQL, MySQL, SQLite are current; these are the next priority enterprise databases
- [ ] **SFTP and local directory watch sources** — file-arrival triggered ingestion without cron
- [ ] **Avro source and sink** — Avro is common in Kafka-based enterprise architectures
- [ ] **Redis-backed distributed work queue** — distributed scaling currently uses the PostgreSQL work-queue coordinator; a Redis-backed coordinator is planned as a lighter-weight alternative

### VS Code extension

- [ ] **Pipeline run history view** — visualise past runs, row counts, durations, and error rates in the sidebar
- [ ] **Inline validation error highlighting** — surface validate-transform failures as canvas annotations during test runs
- [ ] **Multi-pipeline workspace support** — run and monitor multiple pipelines from a single VS Code window
- [ ] **Export to Kubernetes Job manifest** — generate a ready-to-apply K8s Job spec from the current pipeline

### Longer-term (not yet scheduled)

- A **lightweight pipeline server** — a small daemon that manages pipeline lifecycle (schedule, retry, run history) without needing a full Flogo server
- **Visual DQ report viewer** — a canvas panel showing per-field quality scores from the DQ transform output
- **Pipeline lineage metadata** — expose column-level lineage (what came from where) as a queryable artifact

---

## Contributing

This is an internal prototype at the moment. If you are on the team, open a PR and tag a reviewer. External contributions are not yet accepted while the core API is still stabilising.

---

## License

Private — not open source at this time.
