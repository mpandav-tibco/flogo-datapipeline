# Example 21 — OpenTelemetry (OTLP) metrics export

**Backend:** `observability.metrics = "otel"`

Companion to [14-prometheus-metrics](../14-prometheus-metrics). Same pipeline
shape (CSV → filter → map → null sink), but instead of exposing a Prometheus
scrape endpoint, it **pushes** pipeline metrics to an OpenTelemetry collector over
OTLP/gRPC.

## When to use OTel vs Prometheus

| | Prometheus | OpenTelemetry (this example) |
|---|---|---|
| Model | Pull (collector scrapes `/metrics`) | Push (pipeline exports to a collector) |
| Config | `options.listenAddr`, `options.path` | `options.endpoint` (OTLP gRPC) |
| Best for | Long-lived services with a stable scrape target | Short-lived / batch runs, or an existing OTel pipeline |

Because data pipelines are often **batch** (start → process → exit), the push
model fits well: metrics are flushed to the collector on shutdown, so you don't
miss the final batch of a short run.

## Configuration

```json
"observability": {
  "metrics": "otel",
  "options": {
    "endpoint": "localhost:4317",
    "serviceName": "flogo-datapipeline-otel-demo"
  },
  "stallSeconds": 30
}
```

- `endpoint` — the OTLP/gRPC address of your collector (default `localhost:4317`).
- `serviceName` — the `service.name` resource attribute attached to every metric.
- `stallSeconds` — emit a stall warning + `flogo_pipeline_stalls_total` when a
  single batch stage exceeds this many seconds.

## Metrics exported

Same meter set as the Prometheus backend, exported as OTLP instruments:

- `flogo.pipeline.rows.read` / `.transformed` / `.written` / `.failed`
- `flogo.pipeline.batch.duration` (histogram, seconds)
- `flogo.pipeline.runs` (by status)
- `flogo.pipeline.throughput` (rows/second)

## Running it

You need an OTLP collector listening on `:4317`. A minimal one with a debug
exporter (prints received metrics to its logs):

```yaml
# otel-collector.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug]
```

```bash
# 1. Start a collector (Docker)
docker run -d --name otel-collector -p 4317:4317 \
  -v "$PWD/otel-collector.yaml:/etc/otelcol-contrib/config.yaml" \
  otel/opentelemetry-collector-contrib:latest

# 2. Run the pipeline
../../flogo-datapipeline -f otel-metrics.datapipeline

# 3. Watch the collector receive metrics
docker logs -f otel-collector | grep flogo.pipeline
```

You should see `flogo.pipeline.runs` and `flogo.pipeline.batch.duration` arrive at
the collector after the run completes.
