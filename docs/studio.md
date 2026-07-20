# Studio (VS Code)

**Flogo Data Pipeline Studio** is a VS Code extension that turns a `.datapipeline`
file into a visual **ETL / ELT / EL** canvas — author, run, preview, and deploy without
hand-editing JSON. It talks directly to the engine's [REST API](rest-api.md).

> This is the orientation guide. The full reference lives in
> [`studio/vscode/README.md`](../studio/vscode/README.md) and the settings behaviour in
> [`studio/vscode/PIPELINE_SETTINGS.md`](../studio/vscode/PIPELINE_SETTINGS.md).

## The canvas

A left-to-right flow of stage cards — **Source** (blue) → **Transforms** (purple) →
**Sink** (green), plus a dashed **DLQ** branch — connected by directional links. Click a
card to configure it in the **Inspector**; the header shows a colour-coded
Source / Sink / Transform / DLQ badge. The **＋** pills add stages; the inline **＋** on a
link inserts a transform at that position. After a run the connectors animate to show
data moved through the pipeline.

## Plain-language editors

Instead of raw JSON, structured widgets handle columns, field mappings, validation
rules, DQ expectations, filters, and the JDBC conflict key. The source Columns editor
has **✨ Infer columns from file** — it samples a CSV/TSV/JSON-lines header and fills in
names and types.

## Pipeline Settings

The **⚙ Pipeline Settings** panel holds everything that isn't a single stage, grouped
and colour-coded: **Execution mode**, **Checkpoint & resume**, **Incremental load
(HWM)**, **Performance**, **Scaling**, and **Observability**. Every setting is written
into the file and echoed in the run log — what you configure is exactly what the engine
runs.

## Run, preview, report

- **Run** executes against the real engine with streamed status in the
  **Flogo Data Pipeline Run** output channel; validation appears in **Problems**.
- **Preview** is a side-effect-free dry run into the native **Data Preview** panel — no
  writes to the sink.
- **Run Report** shows KPI cards (processed / written / duration / status), a row funnel
  (read → dropped → failed → written), any DQ report, and an *Effective configuration*
  grid mirroring the engine's run-config log line.

## Connections

Define a database / stream / object-store / HTTP endpoint once as a `*.fgconn` file and
bind it by name on any stage; secrets go to VS Code Secret Storage. See
[Connections](connections.md).

## Templates

**Data Pipeline: New Pipeline from Template…** scaffolds a working pipeline: CSV →
PostgreSQL, Parquet on S3 → JDBC, DB → DB incremental sync, Kafka → JDBC, Mainframe
EBCDIC → PostgreSQL, or Extract & Load only.

## Build & deploy

**Build** produces a native binary, a Dockerfile, a container image, or a TIBCO
Platform bundle from the current pipeline. See [Deployment](deployment.md).

## Settings

| Setting | Default | Description |
|---|---|---|
| `flogoDataPipelineStudio.binaryPath` | bundled `bin/flogo-datapipeline` | path to the engine binary |
| `flogoDataPipelineStudio.apiPort` | `9080` | starting REST API port (auto-increments if busy) |
| `flogoDataPipelineStudio.sourcePath` | — | Go module root; when set, **Run** rebuilds from source first |

## See also

- [Getting started](getting-started.md) · [Connections](connections.md) · [Deployment](deployment.md)
- [`studio/vscode/README.md`](../studio/vscode/README.md) — full command & UI reference
