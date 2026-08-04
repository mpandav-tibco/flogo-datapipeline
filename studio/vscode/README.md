# Flogo Data Pipeline Studio (VS Code)

Visual **ETL / ELT / EL** designer for [`flogo-datapipeline`](../../). Author, run, and
debug `*.datapipeline` flows from a drag-and-connect canvas — with reusable
connections, live run metrics, dry-run preview, and native VS Code integration
(Problems, Output, Data Preview panels).

---

## Highlights

- **Visual canvas** — source → transforms → sink, plus a Dead-Letter Queue (DLQ)
  branch. Click a node to configure it in the Inspector.
- **Plain-language editors** — structured widgets for columns, field mappings,
  validation rules, data-quality expectations, filters and the JDBC conflict
  key, instead of hand-written JSON.
- **Reusable connections** — define a database / stream / object-store / HTTP
  endpoint once as a `*.fgconn` file and bind it by name on any source or sink.
  **Secrets go to VS Code Secret Storage — never written to a file.**
- **Schema inference** — one click samples a CSV/TSV/JSON-lines file and fills in
  the column names and types.
- **Run & preview** — run against the real engine with streamed status, or do a
  side-effect-free **dry-run preview** into the native Data Preview panel.
- **Build & deploy** — export a binary, a Dockerfile, or a TIBCO Platform bundle
  (Dockerfile + sharded StatefulSet + README).

---

## The canvas

Each pipeline renders as a left-to-right flow of stage cards connected by
directional links:

- **Stage cards** are colour-accented by role — **Source** (blue), **Transform**
  (purple), **Sink** (green), **DLQ** (dashed/amber) — and carry a small kind icon
  so a `jdbc` source is instantly distinguishable from a `jdbc` sink. The card
  title is the stage's custom name (or a friendly type label such as
  `Null (discard)`), with connection/table/path detail on the meta line.
- **Connectors** show flow direction with an arrowhead. After a run they light up
  blue and animate a travelling streak so you can see data moved through the
  pipeline; while a run is in progress the streak animates faster.
- **Inspector** (right panel) opens when you select a stage. Its header shows a
  colour-coded **Source / Sink / Transform / DLQ** badge next to the type, so the
  config you are editing is never ambiguous. Drag the panel's left edge to
  resize it.
- The **＋** pills (bottom-left) add a Source, Transform, Sink, or DLQ, and the
  in-line **＋** on a connector inserts a transform at that position.

## Pipeline Settings panel

The **⚙ Pipeline Settings** button (top toolbar) opens a dedicated panel for the
pipeline-level settings that don't belong to any single stage. Sections are
collapsible and colour-coded by category:

| Section | Controls |
|---|---|
| **Execution mode** | `etl` (transform in-flight) · `el` (extract-load, no transforms) · `elt` (load raw, transform in-target) |
| **Reliability · Checkpoint & resume** | store (none / file / redis), directory or address, checkpoint every N rows / N seconds, delete-on-success |
| **Reliability · Incremental load (HWM)** | watermark column, value type, initial value, and the store backend (file / redis / postgres) with its connection options |
| **Load strategy · Performance** | batch size, read-ahead buffer depth, max in-flight memory (backpressure), transform workers, Parquet row-group parallelism |
| **Scaling** | `single` · `sharded` (instance index / count) · `distributed` (PostgreSQL coordinator DSN, lease, heartbeat, retries) |
| **Observability · Metrics** | `prometheus` (listen address / path) or `otel` (OTLP endpoint), plus a stall-detection threshold |

Every setting is written into the `.datapipeline` file and echoed back in the
run log, so what you configure here is exactly what the engine runs. See
[PIPELINE_SETTINGS.md](PIPELINE_SETTINGS.md) for the full behaviour reference.

## Run Report

After a run, the **Run Report** tab shows colour-accented KPI cards (processed,
written, duration, status), a row-funnel (read → dropped → failed → written), any
data-quality report, and an **Effective configuration** grid that mirrors the
engine's run-config log line — the active mode, batch size, scaling, checkpoint,
incremental, and metrics settings, each colour-coded to its Settings section.
Click **⚙ Edit settings** on that grid to jump straight back to the panel.

---

## Project structure

The extension encourages a tidy layout. Run **Data Pipeline: Initialize Project
Structure** (or just create your first pipeline) to scaffold:

```
<your-project>/
├── pipelines/       # *.datapipeline flows  (new pipelines default here)
├── connections/     # *.fgconn reusable connections (no secrets in these files)
├── data/            # local input / output sample data
└── README.md        # describes the layout
```

- **`pipelines/*.datapipeline`** — a Flogo app JSON whose `resources[0].data`
  holds the pipeline (`source`, `transforms[]`, `sink`, optional `dlq`,
  `performance`, `scaling`, `checkpoint`).
- **`connections/*.fgconn`** — one JSON file per connection. Holds only
  non-secret fields plus `secretKeys` (which keys live in Secret Storage).

---

## Connections

Connections let you avoid typing DSNs and secrets into pipeline files.

### Create

- Click **＋** on the **Connections** view (activity bar *or* Explorer), or run
  **Data Pipeline: New Connection…**.
- Pick a type, name it, and fill in the fields. Secret fields (passwords, keys,
  tokens) are entered as passwords and stored in **VS Code Secret Storage**.

Supported types: **PostgreSQL, MySQL, SQL Server, Oracle** (`db`), **Apache Kafka**
(`stream`), **Amazon S3, Google Cloud Storage, Azure ADLS Gen2** (`obj`), and
**HTTP** (`http`).

### Edit

Click a connection (in the tree or the `connections/` folder) to open the
**visual connection editor** — a form with Save / Test / View JSON / Delete.
Existing secrets show as `•••••••• (unchanged)` and are only rewritten if you
type a new value.

### Bind

In a source/sink inspector whose type matches a connection family (e.g. a JDBC
sink), a **Connection** dropdown appears at the top. Selecting a connection:

- stores `connection: "<name>"` on the stage,
- hides the DSN/secret fields (the connection owns them).

At **run/preview** time the host resolves the bound connection into concrete
engine config (`driver`, `dsn`, `bucket`, `brokers`, …) in a transient
`.<name>.resolved.datapipeline` sibling file that the engine reads — so the
on-disk pipeline never contains secrets. The transient file is removed on stop
and is git-ignored.

### Test

**Test** does a best-effort TCP reachability probe to the endpoint
(host:port). It verifies the endpoint is reachable, not credentials.

---

## Working with pipelines

- **New:** **Data Pipeline: New Pipeline from Template…** — pick a starting point
  (CSV→PostgreSQL, Parquet on S3→JDBC, DB→DB incremental sync, Kafka→JDBC,
  Mainframe EBCDIC→PostgreSQL, Extract & Load). Saves into `pipelines/`.
- **Configure a stage:** click a node → edit fields in the Inspector. Source/sink
  types and transform stages are managed with type-aware widgets.
- **Columns:** the source Columns editor has **✨ Infer columns from file** —
  samples the header + rows of the referenced CSV/TSV/JSON-lines file and fills
  in names and types.
- **JDBC sink:** choose `insert` / `upsert` / `ignore`. `upsert` reveals a
  **Conflict key** column picker. `Auto-migrate` creates the table and adds new
  columns automatically.
- **DLQ:** right-click the sink → **Add DLQ** to route write-time failures to a
  separate destination without affecting the main sink.
- **Run:** the **Run** button (or status bar) starts the engine and streams
  status. Output goes to the native **Flogo Data Pipeline Run** Output channel.
- **Dry-run preview:** samples rows through the transform chain into the native
  **Data Preview** panel — no writes to the sink.

---

## Commands

All under the **Data Pipeline** category:

| Command | Description |
|---|---|
| New Pipeline from Template… | Create a pipeline from a starter template |
| New Data Pipeline File… | Create an empty pipeline |
| Initialize Project Structure | Scaffold `pipelines/`, `connections/`, `data/` |
| Run in Data Pipeline Studio | Run the active pipeline |
| Stop Pipeline | Stop the engine |
| Configure and Run | Choose build target + port, then run |
| Build | Binary / Dockerfile / image / TIBCO Platform bundle |
| New Connection… | Create a connection |
| Refresh Connections | Reload the connections view |
| Where are Connections stored? | Show the `connections/` path |

Connection **Test / Edit / Delete** are available from the right-click menu on a
connection in the tree, and from inside the visual connection editor.

---

## Settings

| Setting | Default | Description |
|---|---|---|
| `flogoDataPipelineStudio.binaryPath` | `""` | Path to the `flogo-datapipeline` binary. Defaults to `bin/flogo-datapipeline[.exe]` in the extension. |
| `flogoDataPipelineStudio.apiPort` | `9080` | Starting port for the engine REST API. Auto-increments if busy. |
| `flogoDataPipelineStudio.sourcePath` | `""` | Go module root (dir with `go.mod`). When set, **Run** rebuilds the binary from source first. |

---

## Development

```bash
cd studio/vscode
npm install
npm run compile      # build both bundles (extension + webview)
npm run watch        # rebuild on change
npm run typecheck    # tsc --noEmit
npm run lint         # eslint
```

Then press **F5** (or launch an Extension Development Host pointed at this
folder) to try it.

### Architecture

| Path | Role |
|---|---|
| `src/extension.ts` | Activation; wires providers, views, commands, status bar |
| `src/editor/pipelineEditorProvider.ts` | Custom editor for `*.datapipeline`; run/preview/validate/export; connection injection |
| `src/connections/store.ts` | `*.fgconn` file store + Secret Storage; `resolveStageConfig` |
| `src/connections/editor.ts` | Visual custom editor for `*.fgconn` |
| `src/connections/sidebar.ts` | Connections tree view |
| `src/connections/commands.ts` | Create / edit / test / delete flows |
| `src/schema/infer.ts` | CSV/TSV/JSON-lines schema inference |
| `src/sidebar.ts` | Pipelines tree view |
| `src/panel/dataPreviewView.ts` | Native Data Preview panel |
| `src/binaryManager.js` | Engine process lifecycle + REST readiness |
| `src/model/pipeline.ts` | Parse / merge / validate pipeline docs |
| `src/protocol.ts` | Host ⇄ webview message contract |
| `webview/main.ts` | Canvas + Inspector + editors (webview app) |

The engine binary is expected at
`bin/flogo-datapipeline-<platform>-<arch>[.exe]` (see `binaryManager.js`), or
set `flogoDataPipelineStudio.binaryPath`.

---

## Security notes

- Secrets are stored in VS Code **Secret Storage** and referenced by
  `secretKeys` in `.fgconn` files — never written to disk in plain text.
- The transient `.*.resolved.datapipeline` run file (which does contain resolved
  values) is created next to the pipeline, deleted on stop, and git-ignored.
  Keep it out of commits.
