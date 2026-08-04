# Getting started

Build the engine, run a bundled example, then author your own pipeline in the
VS Code Studio.

## Prerequisites

| Tool | Version | For |
|---|---|---|
| Go | 1.22+ | building the engine from source |
| VS Code | 1.83+ | the visual Studio extension |
| Docker | any | Postgres / Kafka / full-production examples (optional) |

## 1. Build the engine

```bash
git clone https://github.com/mpandav-tibco/flogo-datapipeline
cd flogo-datapipeline
go build -o flogo-datapipeline ./cmd/flogo-datapipeline
```

This produces the **all-in-one** engine binary (~58 MB) with every source, sink,
and transform compiled in — ideal for development and for running any example. For
a lean, per-app production binary, see [Deployment](deployment.md).

> On Windows, use `-o flogo-datapipeline.exe`.

## 2. Run an example

Every capability has a runnable demo under [`examples/`](../examples/). Start with
the zero-setup filter/map pipeline:

```bash
./flogo-datapipeline -f examples/02-filter-and-map/filter-and-map.datapipeline
```

The engine loads the file, runs the pipeline to completion, logs a run summary
(rows read / written / failed, duration), and exits. Browse the full catalog in
[`examples/README.md`](../examples/README.md).

## 3. Install the Studio (VS Code)

1. Build the VSIX (or use a provided one):
   ```bash
   cd studio/vscode && npm install && npm run package && npx vsce package
   ```
2. In VS Code: **Extensions → ⋯ → Install from VSIX…** and pick
   `studio/vscode/flogo-datapipeline-studio-*.vsix`.
3. Point the extension at your engine binary — set
   `flogoDataPipelineStudio.binaryPath`, or drop the binary at
   `bin/flogo-datapipeline` inside the extension.

See [Studio](studio.md) for the full extension guide.

## 4. Author and run a pipeline

1. Command Palette → **Data Pipeline: New Pipeline from Template…** and pick a
   starting point (e.g. *CSV → PostgreSQL*).
2. Click a stage on the canvas to configure it in the Inspector; use
   **✨ Infer columns from file** to sample a CSV/JSON header.
3. Click **Run** to execute against the engine with streamed status, or
   **Preview** for a side-effect-free dry run into the native Data Preview panel.

## Next steps

- Understand the model → [Architecture](ARCHITECTURE.md)
- Learn the file format → [Pipeline file](pipeline-file.md)
- Pick components → [Sources](sources.md) · [Sinks](sinks.md) · [Transforms](transforms.md)
- Make runs durable → [Reliability & incremental load](reliability.md)
- Ship it → [Deployment](deployment.md)
