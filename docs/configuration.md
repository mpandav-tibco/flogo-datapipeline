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
| `-validate` | `false` | validate the pipeline definition(s) and exit — no engine boot, no triggers, no source/sink/DB/Kafka connections |

Without `-port` / `-api-addr` the engine runs the pipeline (driven by its triggers)
and exits when done; the REST API is not started.

## Validating in CI (`-validate`)

`-validate` checks every `pipeline:` resource's definition — required fields and
cross-field rules (`Definition.Defaults()` + `Validate()`, the same check the REST
API's `PUT /v1/pipelines/{ref}` uses) — and exits **without** starting the engine,
any trigger, or any source/sink connection. Safe to run against every
`.datapipeline` in a repo as a CI gate, with no test infrastructure required:

```bash
flogo-datapipeline -f my-pipeline.datapipeline -validate
# OK       pipeline:my-pipeline (source=csv sink=jdbc)
# 1 pipeline definition(s) valid
echo $?  # 0

flogo-datapipeline -f broken.datapipeline -validate
# INVALID  pipeline:broken: source.type is required
# 1/1 pipeline definition(s) invalid
echo $?  # 1
```

This is config-shape validation only (it does not open a database/Kafka
connection or confirm a table/topic exists) — pair it with the Studio's dry-run
preview or a real run against test infrastructure for end-to-end confidence.

## Environment variables

| Var | Purpose |
|---|---|
| `PIPELINE_FILE` | default for `-f` |
| `PIPELINE_API_ADDR` | default for `-api-addr` (e.g. `:8888`) |
| `PIPELINE_SHUTDOWN_TIMEOUT` | how long SIGTERM/SIGINT waits for in-flight runs before exiting (Go duration, default `30s`). Stateless runs first drain (stop reading, write what was read, final checkpoint); whatever has not finished after ¾ of it is cancelled. Keep it below the pod's `terminationGracePeriodSeconds`. |
| `INSTANCE_INDEX` | this instance's index for sharded / distributed [scaling](scaling.md) |
| `INSTANCE_COUNT` | total instance count |
| `FLOGO_LOG_FORMAT` | `JSON` for structured (JSON Lines) logs; unset/anything else = human-readable console (see [Logging](#logging)) |

Secret env vars referenced by config (`passwordEnv`, `saslPasswordEnv`, `apiKeyEnv`,
`accessKeyIdEnv`, and the like) are read **by name** at run time — the value never
appears in the file.

## Logging

datapipeline uses the same logger as the underlying `project-flogo/core` runtime, so
the engine's own log-format switch works unmodified — no datapipeline-specific flag or
dependency is involved:

```bash
FLOGO_LOG_FORMAT=JSON ./flogo-datapipeline -f my.datapipeline
```

Every log line (engine lifecycle, triggers, and datapipeline's own pipeline-run
messages) is emitted as one JSON object per line, e.g.:

```json
{"level":"info","timestamp":"2026-08-05T20:53:07.778+0200","logger":"flogo.pipeline","msg":"pipeline run started — source=csv sink=null","pipeline.name":"employees-to-null","pipeline.runId":"859ddd83bdc6723f"}
```

The default (unset) format is the human-readable console form used elsewhere in this
guide's examples. The `-validate` flag's own OK/INVALID summary line is always plain
text regardless of `FLOGO_LOG_FORMAT` — it's a CLI result, not a log event.

**Field names.** The envelope keys (`level`, `timestamp`, `logger`, `msg`) are zap's
own production defaults, set inside `project-flogo/core` — datapipeline has no hook to
rename them without forking that dependency. datapipeline's own structured fields
(`pipeline.name`, `pipeline.runId`, `app.name`, `app.version`,
`deployment.environment`) already use dotted namespacing, which lines up with common
log-schema conventions (ECS, OpenTelemetry resource attributes) even though the
envelope keys don't match ECS's exact names (`@timestamp`, `log.level`, `message`).

If your log pipeline expects strict [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
field names, remap at ingestion — the standard place this is done for any non-ECS-native
producer (Filebeat/Logstash "rename" processor, Vector VRL, Fluentd
`record_transformer`), not something datapipeline hand-rolls at the source:

| zap field | ECS field |
|---|---|
| `timestamp` | `@timestamp` |
| `level` | `log.level` |
| `msg` | `message` |
| `logger` | `log.logger` |

A minimal [Vector](https://vector.dev/) remap for this log shape:

```toml
[transforms.ecs_rename]
type = "remap"
inputs = ["datapipeline_logs"]
source = '''
.@timestamp = del(.timestamp)
.log.level = del(.level)
.message = del(.msg)
.log.logger = del(.logger)
'''
```

## App properties

A pipeline can declare typed `properties[]` at the app top level and reference them
anywhere with `$property["name"]` (or the legacy `$property[name]`). This matches
TIBCO Flogo — datapipeline embeds the same `project-flogo/core` runtime — so a
pipeline externalises configuration exactly the way a flow does.

Values resolve in this order (highest first). As in Flogo core, **environment
variables are opt-in**: nothing is read from the environment unless
`FLOGO_APP_PROPS_ENV` is set.

| Priority | Source | Notes |
|---|---|---|
| 1 | `FLOGO_APP_PROPS_OVERRIDE=name=val,n2=v2` | explicit, always-on escape hatch (datapipeline/agents extension, not in core) |
| 2 | `FLOGO_APP_PROPS_ENV` | **opt-in.** `auto` tries `name`, then `name` with dots→underscores, then `UPPERCASE`; or `{"mappings":{"prop":"ENV_VAR"}}` |
| 3 | `FLOGO_APP_PROPS_JSON=a.json,b.json` | flat JSON overlays; later files win |
| 4 | `properties[].value` | default declared in the file |

> **Static resolution.** Datapipeline resolves `$property[...]` **once, at load**, then
> strips `properties[]` before the engine starts. This keeps typed values (numbers,
> booleans, JSON) correctly typed in config fields, but means there is **no runtime
> hot-reconfigure** (`FLOGO_APP_PROP_SNAPSHOTS` / `_RECONFIGURE`) — the one behavioural
> difference from a long-running Flogo flow. To override per environment in Kubernetes,
> set `FLOGO_APP_PROPS_ENV=auto` and supply the values as container env (e.g. from a
> ConfigMap/Secret).

## Enabling the REST API

Set a port to start the management + introspection API alongside the engine:

```bash
PIPELINE_API_ADDR=:8888 ./flogo-datapipeline -f my.datapipeline
# or
./flogo-datapipeline -f my.datapipeline -port 9080
```

Endpoints and payloads: [REST API](rest-api.md).

## Execution mode (ETL / ELT / EL)

A top-level `mode` field (`etl` | `el` | `elt`, default `etl`) selects the
execution strategy:

| Mode | Declare | Behaviour |
|---|---|---|
| **etl** (default) | source → transforms → sink | transforms run in-flight |
| **el** | source → sink | in-flight transforms are bypassed; declared transforms are ignored (warning logged) |
| **elt** | source → sink; heavy logic in the target DB | raw load like `el`; transforms are expected to run in-target, not in-flight (notice logged) |

The Studio's *Execution mode* selector writes this `mode` field. See
[Architecture](ARCHITECTURE.md).

## Performance tuning

Set under `performance` in the file (see [Pipeline file](pipeline-file.md)):

- **`batchSize`** — larger batches amortise per-batch overhead but raise memory;
  `5000` is a good default. Bulk JDBC / Parquet loads benefit from 10k–50k.
- **`transformWorkers`** — raise for CPU-heavy transforms (`lookup`, `dq`); output
  order is preserved either way (the engine reassembles worker results in read
  order), but it's only actually engaged when the pipeline has no HWM/DQ/window/
  schemadrift accumulator and no checkpointing (all of which need an ordered,
  single-threaded cursor) — otherwise the engine logs why it fell back to
  single-threaded transform and keeps running correctly. Safe with delete-capable
  sinks (e.g. jdbc's `opColumn` mode): the sink write itself is never
  parallelized, only the transform stage is.
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
