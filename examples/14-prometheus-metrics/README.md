# Example 14 — Prometheus Metrics & Observability

**Mode:** ETL (Extract, Transform, Load)
**Source:** CSV file (500 rows)
**Transforms:** `filter` (drop non-positive values) + `map` (rename + uppercase)
**Sink:** Null (discard — focus is on metrics, not persistence)
**Metrics backend:** Prometheus HTTP scrape endpoint on `:2112/metrics`

This example shows how to enable the built-in **Prometheus metrics backend** to
expose per-batch throughput, error rates, and latency histograms from a running
pipeline. You scrape the endpoint with `curl` (or any Prometheus-compatible
scraper) and optionally feed the data into Grafana for dashboards.

---

## Directory Layout

```
14-prometheus-metrics/
├── prometheus-metrics.datapipeline   # Pipeline definition with metrics block
├── data/
│   └── large_dataset.csv             # 500-row sample dataset (~20% filtered)
├── run.sh                            # Demo run script with live scraping
└── README.md                         # This file
```

---

## What is the Recorder Interface?

The Flogo Data Pipeline engine exposes a `Recorder` interface that every
internal subsystem calls to emit observations. The interface is backend-agnostic:

```go
type Recorder interface {
    // Increment a counter by delta.
    Count(name string, delta int64, tags map[string]string)

    // Set a gauge to the current value (e.g., rows/sec).
    Gauge(name string, value float64, tags map[string]string)

    // Record a single observation into a histogram bucket.
    Histogram(name string, value float64, tags map[string]string)
}
```

The pipeline wires a concrete backend to the `Recorder` at startup based on the
`"metrics"` block in the pipeline file. If no `"metrics"` block is present, a
no-op recorder is used and zero overhead is added.

Current backends:

| Backend | Config `type` | Transport |
|---------|---------------|-----------|
| Prometheus | `"prometheus"` | HTTP pull (scrape) |
| OpenTelemetry | `"otel"` | gRPC/HTTP push (OTLP) |
| StatsD | `"statsd"` | UDP push |
| None (default) | omit block | No-op; zero overhead |

---

## The Prometheus Backend

### How it works

When `"type": "prometheus"` is configured, the engine:

1. Registers all pipeline metric families with the default Prometheus registry.
2. Starts an HTTP server on `addr` (default `:2112`).
3. Exposes the standard `/metrics` path (or whatever `path` is set to).
4. The server runs for the lifetime of the pipeline process — scrape at any time
   during or after the run (until the process exits).

Prometheus uses a **pull model**: it scrapes your endpoint on a schedule you
define in `prometheus.yml`. The pipeline does not push data anywhere.

### Metrics Block Configuration

```json
"metrics": {
  "type":   "prometheus",
  "addr":   ":2112",
  "path":   "/metrics"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | `"prometheus"` selects the Prometheus backend |
| `addr` | string | `":2112"` | `host:port` to bind the HTTP server. Use `"0.0.0.0:2112"` to listen on all interfaces, or `"127.0.0.1:2112"` to restrict to localhost. |
| `path` | string | `"/metrics"` | URL path for the scrape endpoint. Use the default unless your scraper requires a different path. |

Port `2112` is the community convention for Prometheus Go application metrics
(chosen to avoid clashing with the standard `9090` Prometheus server port).

---

## Metric Catalog

All metrics are prefixed `flogo_pipeline_`. Tags (labels in Prometheus
terminology) are automatically attached where relevant.

### Counters

Counters only ever increase. They reset to zero when the process restarts.

| Metric | Labels | Description |
|--------|--------|-------------|
| `flogo_pipeline_rows_read_total` | `pipeline`, `source_type` | Cumulative rows read from the source after header parsing. |
| `flogo_pipeline_rows_written_total` | `pipeline`, `sink_type` | Cumulative rows successfully delivered to the sink. |
| `flogo_pipeline_rows_filtered_total` | `pipeline` | Rows dropped by `filter` transforms (did not match the predicate). |
| `flogo_pipeline_rows_failed_total` | `pipeline`, `stage` | Rows that caused a processing error (type cast failure, validation violation that propagates, etc.). |
| `flogo_pipeline_batches_total` | `pipeline` | Number of batches processed end-to-end. |
| `flogo_pipeline_dlq_rows_total` | `pipeline` | Rows routed to the dead-letter queue (only non-zero when a `validate` transform with `onViolation: "route"` is configured). |

### Gauges

Gauges can go up or down. They reflect the current state at scrape time.

| Metric | Labels | Description |
|--------|--------|-------------|
| `flogo_pipeline_rows_per_second` | `pipeline` | Rolling throughput: rows read in the last completed batch divided by that batch's wall-clock duration. |
| `flogo_pipeline_active_batches` | `pipeline` | Number of batches currently in flight (useful with parallel partitions). |
| `flogo_pipeline_memory_mb` | `pipeline` | Approximate heap allocation in MiB (sampled at batch boundaries). |

### Histograms

Histograms track distributions across pre-defined buckets and expose `_bucket`,
`_sum`, and `_count` series.

| Metric | Labels | Buckets (ms) | Description |
|--------|--------|-------------|-------------|
| `flogo_pipeline_batch_duration_ms` | `pipeline`, `stage` | 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 | End-to-end wall time per batch, broken down by stage: `read`, `transform`, `write`. |
| `flogo_pipeline_batch_size_rows` | `pipeline` | 1, 10, 50, 100, 500, 1000, 5000 | Actual row count per batch (may be smaller than `batchSize` for the last batch). |

---

## The Pipeline Explained

```json
{
  "name": "prometheus-metrics",
  "mode": "etl",
  "source": {
    "type": "csv",
    "path": "./data/large_dataset.csv",
    "hasHeader": true,
    "columns": [
      { "name": "id",       "type": "int"    },
      { "name": "value",    "type": "float"  },
      { "name": "label",    "type": "string" },
      { "name": "category", "type": "string" },
      { "name": "ts",       "type": "string" }
    ]
  },
  "transforms": [
    { "type": "filter", "expr": "value > 0" },
    {
      "type": "map",
      "fields": {
        "id":    "=$id",
        "val":   "=$value",
        "label": "=upper($.label)"
      }
    }
  ],
  "sink": { "type": "null" },
  "metrics": {
    "type": "prometheus",
    "addr": ":2112",
    "path": "/metrics"
  },
  "performance": { "batchSize": 50, "maxMemoryMB": 256 }
}
```

The `batchSize` is intentionally set to `50` (rather than the production
default of 5000) so the 500-row file produces **10 batches**. This creates
visible histogram distributions — with a batch size of 5000 you would get only
one batch and the histograms would be uninteresting.

### What each transform does

| Transform | Effect on metrics |
|-----------|-------------------|
| `filter: value > 0` | Increments `flogo_pipeline_rows_filtered_total` for each dropped row. Of 500 rows, ~96 have `value <= 0` and are filtered. |
| `map` | Renames `value` to `val`, uppercases `label`. Contributes to `batch_duration_ms{stage="transform"}`. |

---

## Sample Data

`data/large_dataset.csv` contains 500 rows generated with a fixed random seed:

```
id,value,label,category,ts
1,1395.39,premium_unit_1,clothing,2024-01-01T03:43:00Z
2,297.98,standard_set_2,health,2024-01-02T06:14:00Z
7,-38.86,maxi_pack_7,electronics,2024-01-07T02:35:00Z   ← filtered (value < 0)
41,0.0,premium_device_41,health,2024-02-11T18:30:00Z    ← filtered (value = 0)
...
500,9977.86,ultra_set_500,toys,2024-05-15T06:46:00Z
```

Distribution:
- **~404 rows** pass the `value > 0` filter and reach the map transform and sink.
- **~96 rows** (zero or negative `value`) are dropped and counted in
  `flogo_pipeline_rows_filtered_total`.

---

## How to Run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`.
- `curl` available (standard on macOS/Linux; use Git Bash on Windows).
- Run from the `14-prometheus-metrics/` directory so relative paths resolve.

### Option A — automated demo script

```bash
chmod +x run.sh
./run.sh
```

The script starts the pipeline in the background, waits one second, scrapes
the metrics endpoint, then waits for the pipeline to finish and scrapes again.

### Option B — two-terminal live observation

Open two terminals, both in the `14-prometheus-metrics/` directory.

**Terminal 1 — start the pipeline:**

```bash
flogo-datapipeline -f prometheus-metrics.datapipeline
```

**Terminal 2 — watch metrics update in real time:**

```bash
# Scrape once
curl http://localhost:2112/metrics | grep flogo_pipeline

# Or watch repeatedly (Unix/macOS)
watch -n1 'curl -s http://localhost:2112/metrics | grep flogo_pipeline'
```

### Option C — Prometheus server scrape

Add the following job to your `prometheus.yml` and restart Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: flogo_pipeline
    scrape_interval: 5s
    static_configs:
      - targets:
          - localhost:2112
    metrics_path: /metrics
```

Then open the Prometheus UI at `http://localhost:9090` and query:

```promql
rate(flogo_pipeline_rows_read_total[1m])
```

---

## Expected Output

### Pipeline log (stdout)

```
INFO  pipeline starting            name=prometheus-metrics mode=etl
INFO  prometheus metrics server    addr=:2112 path=/metrics
INFO  csv source opened            path=./data/large_dataset.csv rows=500
INFO  batch processed              batch=1 rows_read=50 rows_passed=40 rows_filtered=10
INFO  batch processed              batch=2 rows_read=50 rows_passed=42 rows_filtered=8
...
INFO  batch processed              batch=10 rows_read=50 rows_passed=41 rows_filtered=9
INFO  pipeline complete            rows_read=500 rows_written=0 rows_filtered=96 duration=47ms
```

### curl scrape output (during run)

```
# HELP flogo_pipeline_rows_read_total Cumulative rows read from the source.
# TYPE flogo_pipeline_rows_read_total counter
flogo_pipeline_rows_read_total{pipeline="prometheus-metrics",source_type="csv"} 300

# HELP flogo_pipeline_rows_filtered_total Rows dropped by filter transforms.
# TYPE flogo_pipeline_rows_filtered_total counter
flogo_pipeline_rows_filtered_total{pipeline="prometheus-metrics"} 58

# HELP flogo_pipeline_rows_written_total Cumulative rows delivered to the sink.
# TYPE flogo_pipeline_rows_written_total counter
flogo_pipeline_rows_written_total{pipeline="prometheus-metrics",sink_type="null"} 0

# HELP flogo_pipeline_rows_per_second Current throughput gauge.
# TYPE flogo_pipeline_rows_per_second gauge
flogo_pipeline_rows_per_second{pipeline="prometheus-metrics"} 4250.3

# HELP flogo_pipeline_batch_duration_ms Batch processing time by stage.
# TYPE flogo_pipeline_batch_duration_ms histogram
flogo_pipeline_batch_duration_ms_bucket{pipeline="prometheus-metrics",stage="read",le="5"} 6
flogo_pipeline_batch_duration_ms_bucket{pipeline="prometheus-metrics",stage="read",le="10"} 6
flogo_pipeline_batch_duration_ms_bucket{pipeline="prometheus-metrics",stage="read",le="25"} 6
...
flogo_pipeline_batch_duration_ms_sum{pipeline="prometheus-metrics",stage="read"} 18.4
flogo_pipeline_batch_duration_ms_count{pipeline="prometheus-metrics",stage="read"} 6
```

---

## How to Verify

After the run completes:

1. **Exit code:** `echo $?` should print `0`.
2. **rows_read_total = 500** — all rows were read from the CSV.
3. **rows_written_total = 0** — null sink consumed but did not persist rows.
4. **rows_filtered_total ~ 96** — rows where `value <= 0` were dropped.
5. **batches_total = 10** — 500 rows / batchSize 50 = 10 batches.
6. **batch_duration_ms histogram** — each stage (`read`, `transform`, `write`)
   has a non-zero `_count` equal to the number of batches processed.

---

## Grafana Dashboard Wiring

### Import the dashboard

The standard approach for a Flogo pipeline dashboard:

1. Install Grafana (or use Grafana Cloud).
2. Add your Prometheus instance as a data source.
3. Create a new dashboard with the panels below.

### Recommended panels

| Panel title | Visualization | PromQL |
|-------------|---------------|--------|
| Throughput (rows/sec) | Time series | `flogo_pipeline_rows_per_second{pipeline="prometheus-metrics"}` |
| Read rate (rows/min) | Time series | `rate(flogo_pipeline_rows_read_total[1m]) * 60` |
| Filter rate (rows/min) | Time series | `rate(flogo_pipeline_rows_filtered_total[1m]) * 60` |
| Error rate | Time series | `rate(flogo_pipeline_rows_failed_total[1m])` |
| Batch read latency P50 | Stat | `histogram_quantile(0.50, rate(flogo_pipeline_batch_duration_ms_bucket{stage="read"}[1m]))` |
| Batch read latency P99 | Stat | `histogram_quantile(0.99, rate(flogo_pipeline_batch_duration_ms_bucket{stage="read"}[1m]))` |
| Cumulative rows read | Stat | `flogo_pipeline_rows_read_total` |
| Cumulative rows filtered | Stat | `flogo_pipeline_rows_filtered_total` |
| Memory usage (MiB) | Time series | `flogo_pipeline_memory_mb` |

### Sample dashboard JSON snippet

```json
{
  "title": "Flogo Pipeline — Prometheus Metrics",
  "panels": [
    {
      "type": "timeseries",
      "title": "Rows per Second",
      "targets": [
        {
          "expr": "flogo_pipeline_rows_per_second",
          "legendFormat": "{{pipeline}}"
        }
      ]
    },
    {
      "type": "timeseries",
      "title": "Read Rate (rows/min)",
      "targets": [
        {
          "expr": "rate(flogo_pipeline_rows_read_total[1m]) * 60",
          "legendFormat": "{{pipeline}} / {{source_type}}"
        }
      ]
    }
  ]
}
```

---

## OpenTelemetry Alternative

If you prefer push-based metrics compatible with OTLP collectors (Jaeger, Tempo,
Grafana Agent, the OpenTelemetry Collector), use the `otel` backend instead:

```json
"metrics": {
  "type":     "otel",
  "endpoint": "http://localhost:4317",
  "protocol": "grpc",
  "interval": 10
}
```

| Field | Description |
|-------|-------------|
| `endpoint` | OTLP collector address (gRPC default port 4317, HTTP 4318). |
| `protocol` | `"grpc"` or `"http/protobuf"`. |
| `interval` | Push interval in seconds (default `10`). |

The same metric names (`flogo_pipeline_*`) and label keys are used regardless
of backend, so you can switch from Prometheus to OTEL without changing dashboards
(as long as your visualization tool queries both sources).

---

## Performance Tuning Guidance

### What the metrics tell you

| Observation | Likely cause | Action |
|-------------|--------------|--------|
| `rows_per_second` low, `batch_duration_ms{stage="read"}` high | I/O bound — slow disk or network source | Increase `batchSize`; add source partitions |
| `rows_per_second` low, `batch_duration_ms{stage="transform"}` high | CPU bound — complex expression evaluation | Simplify expressions; enable parallel transforms |
| `rows_per_second` low, `batch_duration_ms{stage="write"}` high | Sink bottleneck — slow DB writes or network | Tune sink `batchSize`; use upsert mode; check DB indexes |
| `rows_filtered_total` unexpectedly high | Filter predicate too strict | Review filter logic against actual data distribution |
| `rows_failed_total` > 0 | Type cast failures or schema mismatches | Check source data quality; add a `validate` transform upstream |
| `memory_mb` growing monotonically | Memory leak or oversized batch buffer | Reduce `batchSize`; reduce `maxMemoryMB` to trigger backpressure earlier |

### batchSize interaction with metrics

Metrics are emitted **per batch**. A larger `batchSize` produces fewer, coarser
data points in histograms. A smaller `batchSize` gives more granular histograms
but increases per-row overhead. For observability experiments, use a `batchSize`
between 50 and 500. For production throughput, use 1000–10000 depending on row
width.

---

## Configuration Reference

### metrics block

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `type` | string | Yes | — | `"prometheus"`, `"otel"`, `"statsd"`, or omit block for no-op |
| `addr` | string | No | `":2112"` | Prometheus only. `host:port` to bind the HTTP server. |
| `path` | string | No | `"/metrics"` | Prometheus only. URL path for the scrape endpoint. |
| `endpoint` | string | No | — | OTEL only. OTLP collector address. |
| `protocol` | string | No | `"grpc"` | OTEL only. `"grpc"` or `"http/protobuf"`. |
| `interval` | int | No | `10` | OTEL/StatsD only. Push interval in seconds. |

### performance block

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows per internal batch. Lower values give finer metric resolution. |
| `maxMemoryMB` | int | `512` | Soft heap limit in MiB. |

---

## Prometheus Scrape Config Reference

Minimal `prometheus.yml` to scrape this pipeline:

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: flogo_pipeline_example14
    static_configs:
      - targets:
          - localhost:2112
    metrics_path: /metrics
    # Optional: add a constant label to all metrics from this target
    relabel_configs:
      - target_label: environment
        replacement: dev
```

For production, use service discovery instead of `static_configs`:

```yaml
scrape_configs:
  - job_name: flogo_pipelines
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        target_label: __address__
        regex: (.+)
        replacement: ${1}
```

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [11-schema-drift](../11-schema-drift/) | Detect column additions/removals at runtime |
| [12-incremental-hwm](../12-incremental-hwm/) | Resume from last processed row using a high-water mark |
| [03-validate-and-dlq](../03-validate-and-dlq/) | Route invalid rows to a dead-letter queue (adds DLQ metrics) |
