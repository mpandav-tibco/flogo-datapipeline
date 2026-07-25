# Example 24 — Windowed Aggregation (event-time)

Per-sensor **1-minute event-time tumbling rollup** (avg / max / count) of
temperature readings — the first example of the `window` transform (stream
processing). See [`docs/stream-processing.md`](../../docs/stream-processing.md).

```
CSV (sensor, ts, temperature)
        │
        ▼
  window  ── tumbling 60s, event-time = ts (RFC3339)
          groupBy sensor · avg_temp / max_temp / n
        │
        ▼
  JSON-lines sink  → out/rollup.jsonl
```

## Run it

```bash
mkdir -p out
../../flogo-datapipeline -f sensor-rollup.datapipeline
# Ctrl-C after "pipeline run completed" (the timer trigger fires once)
cat out/rollup.jsonl
```

Expected: **9 rows in → 8 window rows out** (one per sensor per minute). The
`10:02` windows are still open when the input ends, so they are emitted by the
end-of-input **Flush**. Example output:

```json
{"sensor":"S1","window_start":1784887200000,"window_end":1784887260000,"avg_temp":20.75,"max_temp":21,"n":2}
{"sensor":"S2","window_start":1784887200000,"window_end":1784887260000,"avg_temp":18,"max_temp":18,"n":1}
...
```

`window_start` / `window_end` are epoch millis (UTC).

## Build it visually

Open `sensor-rollup.datapipeline` in the Data Pipeline Studio. The **Window**
node exposes: window type (tumbling / sliding / session / global) + size,
event-time column + format, group-by keys, an aggregations builder
(sum/avg/min/max/count/first/last), allowed lateness, and state TTL. A following
map/sink sees the aggregated columns (`sensor`, `window_start`, `window_end`,
`avg_temp`, …).

## Config reference (`window` transform)

| Field | Meaning |
|---|---|
| `window.kind` | `tumbling` \| `sliding` \| `session` \| `global` |
| `window.sizeMs` / `slideMs` / `gapMs` | window width / slide step / session gap |
| `eventTime.column` / `format` | timestamp column + format (`RFC3339`, `DateTime`, `epochMillis`, …); omit → processing time |
| `watermark.maxLatenessMs` | out-of-order tolerance before a window fires |
| `groupBy` | aggregation key columns |
| `aggregations[]` | `{ as, fn, column }` (fn ∈ sum/avg/min/max/count/first/last) |
| `stateTtlMs` | drop abandoned window state older than this (bounds memory) |
| `onLate` | `drop` (default) — records after their window closed |

## Make it a live stream

Swap the source to Kafka (`"type":"kafka"`, `"mode":"streaming"`) and drive it
with the App-Startup trigger — the *same* window config then runs continuously
over live events (see `examples/17-kafka-streaming` for the Kafka source setup).
