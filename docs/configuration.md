# Configuration

How to run and tune the engine. Secrets are always passed as
**environment-variable names** (or via [Connections](connections.md) Secret
Storage) — never written into a `.datapipeline` file.

## CLI flags

```bash
flogo-datapipeline -f my-pipeline.datapipeline -port 9080
```

| Flag | Default | Purpose |
|---|---|---|
| `-f` | `$PIPELINE_FILE` | path to the `.datapipeline` app (required) |
| `-port` | `0` (off) | REST management API port (e.g. `9080`); wins over `-api-addr` |
| `-api-addr` | `$PIPELINE_API_ADDR` | REST API TCP address (e.g. `:8888`) |

Without `-port` / `-api-addr` the engine runs the pipeline (driven by its triggers)
and exits when done; the REST API is not started.

## Environment variables

| Var | Purpose |
|---|---|
| `PIPELINE_FILE` | default for `-f` |
| `PIPELINE_API_ADDR` | default for `-api-addr` (e.g. `:8888`) |
| `INSTANCE_INDEX` | this instance's index for sharded / distributed [scaling](scaling.md) |
| `INSTANCE_COUNT` | total instance count |
| `FLOGO_APP_PROPS_OVERRIDE`, `FLOGO_APP_PROP_*` | standard Flogo app-property overrides resolved at startup |

Secret env vars referenced by config (`passwordEnv`, `saslPasswordEnv`, `apiKeyEnv`,
and the like) are read **by name** at run time — the value never appears in the file.

## Enabling the REST API

Set a port to start the management + introspection API alongside the engine:

```bash
PIPELINE_API_ADDR=:8888 ./flogo-datapipeline -f my.datapipeline
# or
./flogo-datapipeline -f my.datapipeline -port 9080
```

Endpoints and payloads: [REST API](rest-api.md).

## Execution mode (ETL / ELT / EL)

The mode is **emergent** from the transform chain — there is no engine switch:

| Mode | Declare |
|---|---|
| **ETL** | source → transforms → sink |
| **ELT** | source → minimal transform → sink; heavy logic in the target DB |
| **EL** | source → sink, no transforms |

The Studio's *Execution mode* selector simply reflects this. See
[Architecture](ARCHITECTURE.md).

## Performance tuning

Set under `performance` in the file (see [Pipeline file](pipeline-file.md)):

- **`batchSize`** — larger batches amortise per-batch overhead but raise memory;
  `5000` is a good default. Bulk JDBC / Parquet loads benefit from 10k–50k.
- **`transformWorkers`** — raise for CPU-heavy transforms (`lookup`, `dq`); keep at
  `1` when row order must be preserved.
- **`sourceBufferBatches` / `sinkBufferBatches`** — read-ahead / write buffering to
  smooth bursty stages.
- **`maxMemoryMB`** — hard ceiling; the source blocks (backpressure) rather than
  exceed it.
- **`rowGroupParallelism`** — concurrent Parquet row-group readers (also the Parquet
  scaling partition unit).

## Reliability & scale

- Durable resume and incremental reads → [Reliability & incremental load](reliability.md)
- Horizontal scale → [Scaling](scaling.md)
- Metrics endpoints → [Observability](observability.md)

## Studio settings

When you run from VS Code, the extension manages the binary and API port for you:

| Setting | Default |
|---|---|
| `flogoDataPipelineStudio.binaryPath` | bundled `bin/flogo-datapipeline` |
| `flogoDataPipelineStudio.apiPort` | `9080` (auto-increments if busy) |
| `flogoDataPipelineStudio.sourcePath` | — (rebuild from source before Run when set) |

See [Studio](studio.md).
