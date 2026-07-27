# Observability

The engine exposes the same logical metrics through either **Prometheus** (pull) or
**OpenTelemetry** (push). A process-wide recorder ([`metrics/`](../metrics)) is a
no-op until you configure one, so there is zero overhead by default. Configure it in
the pipeline's `observability` block.

```json
"observability": {
  "metrics": "prometheus",
  "options": { "listenAddr": ":9090", "path": "/metrics" },
  "stallSeconds": 60
}
```

## Prometheus

`metrics: prometheus`, `options.listenAddr` (`:9090`), `options.path` (`/metrics`).
Scrape the endpoint:

```bash
curl localhost:9090/metrics
```

## OpenTelemetry

`metrics: otel`, `options.endpoint` (`localhost:4317`), `options.insecure` (`true`),
`options.serviceName` (`flogo-datapipeline`). Metrics export over OTLP/gRPC (durations
in milliseconds) to any OTel collector.

## Metric catalog

Both backends emit the same series:

| Metric | Type | Labels |
|---|---|---|
| `flogo_pipeline_rows_read_total` | counter | `pipeline`, `source_type` |
| `flogo_pipeline_rows_transformed_total` | counter | `pipeline` |
| `flogo_pipeline_rows_written_total` | counter | `pipeline`, `sink_type` |
| `flogo_pipeline_rows_failed_total` | counter | `pipeline` |
| `flogo_pipeline_batch_duration_seconds` | histogram | `pipeline`, `stage` |
| `flogo_pipeline_runs_total` | counter | `pipeline`, `status` |
| `flogo_pipeline_stalls_total` | counter | `pipeline`, `stage` |
| `flogo_pipeline_checkpoints_total` | counter | `pipeline` |
| `flogo_pipeline_throughput_gauge` | gauge | `pipeline` |

## Stall detection

`stallSeconds` sets the threshold after which a stage that has made no progress is
flagged (incrementing `flogo_pipeline_stalls_total`). Useful for catching a wedged
source or a blocked sink in a long streaming run.

## Audit log

An optional, append-only **governance audit trail**. When enabled, each pipeline
run emits structured JSON records (one object per line — JSONL) for its lifecycle:

| Event | Emitted | Key fields |
|---|---|---|
| `pipeline.run.started` | at run start | `runId`, `pipeline`, `source`, `sink`, `transforms[]`, `scaling`, `checkpoint`, `incremental` |
| `pipeline.run.finished` | at run end | `runId`, `pipeline`, `status`, `rowsProcessed`, `rowsWritten`, `rowsFailed`, `rowsDropped`, `durationMs`, `error` |

Every record also carries an `event` name and a UTC `ts` timestamp.

Auditing is **off by default**. Enable it per pipeline:

```json
"observability": {
  "audit": { "enabled": true, "file": "/var/log/flogo/audit.jsonl" }
}
```

or globally with environment variables (which override the config):

| Variable | Effect |
|---|---|
| `DP_AUDIT=on` \| `1` \| `true` | enable auditing |
| `DP_AUDIT_FILE=/path` | write to this append-only file (else stdout) |

With no `file` set, records go to **stdout**. The file is opened in append mode,
so re-runs accumulate one durable trail.

**No secrets are ever written.** Records carry only component *types* and run
counters — never option values, DSNs, credentials, paths, or row data — so the
trail can be shipped to a central log store safely.

## Run summary vs metrics

Metrics are the *time-series* view. Each run also returns a one-shot **summary**
(`status`, `rowsProcessed`, `rowsWritten`, `rowsFailed`, `rowsDropped`, `durationMs`,
`metadata.dqReport`) via the [REST API](rest-api.md) and the Studio Run Report — see
[Pipeline file → Run output](pipeline-file.md#run-output).

## See also

- [`examples/14-prometheus-metrics`](../examples/14-prometheus-metrics) ·
  [`examples/21-otel-metrics`](../examples/21-otel-metrics)
- [Feature reference](FEATURES.md#metrics-backends)
