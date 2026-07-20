# REST API

The engine can run a small **management + introspection** HTTP API alongside the
Flogo engine. It is **optional** — started only when you set `-port` / `-api-addr` /
`PIPELINE_API_ADDR` (see [Configuration](configuration.md)). It shares the engine's
action cache, so a run goes through the same code path as a triggered run.

```bash
./flogo-datapipeline -f my.datapipeline -port 9080
```

Base path `/v1`. Timeouts: read 30s, write unbounded (a synchronous run may take
arbitrarily long), idle 120s. The last **100** run records per pipeline are kept in an
in-memory ring buffer.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/health` | liveness probe |
| `GET` | `/v1/pipelines` | list registered pipelines + last-run summary |
| `POST` | `/v1/pipelines/{ref}/run` | run a pipeline (synchronous) |
| `GET` | `/v1/pipelines/{ref}/status` | last N run records (ring buffer) |
| `DELETE` | `/v1/pipelines/{ref}/run/{runId}` | cancel an in-flight run |

`{ref}` is the Flogo resource reference, e.g. `res://pipeline:daily-tx` (URL-encode
the `:` and `/`).

## Health

```bash
curl localhost:9080/v1/health
```
```json
{ "status": "ok", "pipelines": 1 }
```

## List pipelines

```bash
curl localhost:9080/v1/pipelines
```
```json
{ "pipelines": [
  { "ref": "res://pipeline:daily-tx", "sourceType": "csv", "sinkType": "jdbc",
    "status": "completed", "lastRunAt": "2026-07-16T09:00:00Z",
    "lastRunDurationMs": 1840, "lastRunStatus": "completed" }
] }
```

## Run a pipeline

`POST /v1/pipelines/{ref}/run` — synchronous; returns the run summary when finished.
Request body (all fields optional):

| Field | Purpose |
|---|---|
| `pipelineRef` | overrides the `{ref}` path param (for proxy callers) |
| `runId` | caller-supplied idempotency token (else the server generates one) |
| `params` | key/values injected into the source path template (`${date}` → value) |
| `inputPath` | override the source path for this run only (ad-hoc file processing) |

```bash
curl -X POST 'localhost:9080/v1/pipelines/res%3A%2F%2Fpipeline%3Adaily-tx/run' \
  -H 'content-type: application/json' \
  -d '{ "params": { "date": "2026-07-16" } }'
```
```json
{
  "runId": "9f3c…", "status": "completed",
  "rowsProcessed": 100000, "rowsWritten": 99850, "rowsFailed": 150, "rowsDropped": 0,
  "durationMs": 1840,
  "startedAt": "2026-07-16T09:00:00Z", "finishedAt": "2026-07-16T09:00:01Z",
  "qualityReport": { "passed": 6, "failed": 0 }
}
```

`status` is one of `running` · `completed` · `partial` · `failed` · `cancelled`.

## Run history

```bash
curl 'localhost:9080/v1/pipelines/res%3A%2F%2Fpipeline%3Adaily-tx/status'
```
Returns the live `PipelineInfo` plus a `runs[]` array of the most recent runs.

## Cancel a run

```bash
curl -X DELETE 'localhost:9080/v1/pipelines/res%3A%2F%2Fpipeline%3Adaily-tx/run/9f3c…'
```
Cancels the in-flight run with that `runId`; it finishes with `status: cancelled`.

## Introspection (Studio)

These endpoints back the Studio's schema-inference and pickers. They connect to a
target and return metadata — used at design time, not on the hot path.

| Path | Returns |
|---|---|
| `GET /v1/introspect/tables` | tables for a JDBC connection |
| `GET /v1/introspect/columns` | columns for a table |
| `GET /v1/introspect/source-schema` | inferred schema for a file source |
| `GET /v1/introspect/objects` | objects under an object-store prefix |
| `GET /v1/introspect/kafka-topics` | topics on a Kafka cluster |
| `GET /v1/introspect/schema-registry` | subjects in a schema registry |
| `GET /v1/introspect/xml` | element/attribute structure of an XML file |

## Errors

All 4xx/5xx responses use `{ "code": "…", "message": "…" }` (e.g. `not_found`,
`bad_request`).

## See also

- [Configuration](configuration.md) — enabling the API and the port model
- [Studio](studio.md) — how the extension drives these endpoints
