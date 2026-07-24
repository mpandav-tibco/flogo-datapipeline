# Example 25 — Kafka Windowed Stream (live streaming use case)

The **continuous streaming** counterpart to example 24: consume sensor JSON from a
**Kafka** topic and emit a per-sensor **1-minute event-time windowed rollup**
(avg / max / count) as the event-time watermark advances. This is the flagship
stream-processing use case from [`docs/stream-processing.md`](../../docs/stream-processing.md) §5.

```
Kafka topic "sensor-events" (JSON: sensor, ts, temperature)
        │  (streaming consumer group, poll loop — runs continuously)
        ▼
  filter  temperature > 0
        ▼
  window  tumbling 60s · event-time = ts · groupBy sensor · avg/max/count
        │  (each window fires when the watermark passes its end)
        ▼
  JSON-lines sink  → out/windowed.jsonl   (checkpoint-flushed every ~2s)
```

## Run it (verified live)

Uses a local Kafka broker on `localhost:9092` (any KRaft/Confluent broker works).

```bash
mkdir -p out

# 1. Publish the sample events (10 messages spanning 10:00–10:03).
../../flogo-datapipeline -f producer.datapipeline        # runs once, then Ctrl-C

# 2. Run the streaming windowed consumer (continuous).
../../flogo-datapipeline -f kafka-windowed.datapipeline  # Ctrl-C to stop

# 3. Inspect the fired windows.
cat out/windowed.jsonl
```

**Observed output** (windows 10:00–10:02 fire; 10:03 stays open until a later
event advances the watermark):

```
S1 [10:00] avg=20.75 max=21  n=2      S1 [10:01] avg=22.5 n=1     S1 [10:02] avg=23   n=1
S2 [10:00] avg=18    max=18  n=1      S2 [10:01] avg=19.5 n=1     S2 [10:02] avg=20   n=1
                                      S3 [10:01] avg=25   n=1     S3 [10:02] avg=26.5 n=1
```

## Why the last window stays open

A tumbling window fires when the **watermark** (max event time − allowed lateness)
reaches its end. The final `10:03` reading has no later event to push the
watermark past `10:04`, so its window is still open — correct streaming behaviour.
It fires as soon as newer data arrives (or on a graceful drain).

## Key config

| Piece | Setting |
|---|---|
| Kafka source | `type:kafka`, `mode` default (streaming poll loop), `deserializer:json`, `groupId`, `startOffset:earliest` |
| Window | `kind:tumbling`, `sizeMs:60000`, `eventTime.column:ts` (RFC3339), `groupBy:[sensor]` |
| Sink flush | `checkpoint.intervalSeconds:2` so results reach disk continuously (streaming has no EOF flush) |
| Durable state | `checkpoint.key:"kafka-windowed-stream"` — a **stable** key so open windows resume across restarts |

## Durable window state — survives a restart (verified live)

Open windows are **checkpointed and restored**, so killing the consumer mid‑stream
does not lose in‑flight aggregations. The window state is written **inside the
checkpoint token** (`checkpoints/kafka-windowed-stream.ckpt.json` → `windowState`)
at the same instant as the Kafka position, and replayed on the next start.

```bash
# 1. Publish only the first two 10:00 readings (window stays open, nothing fires).
#    Then run the consumer briefly and stop it — the open window is checkpointed.
../../flogo-datapipeline -f kafka-windowed.datapipeline   # Ctrl-C after ~3s

cat checkpoints/kafka-windowed-stream.ckpt.json           # → windowState with the open window

# 2. Restart the consumer — it logs "restored window state (N bytes)" and resumes.
../../flogo-datapipeline -f kafka-windowed.datapipeline
#   → later events advance the watermark and the restored window fires with the
#     exact pre-restart aggregate (no double-count, no loss).
```

Guarantee: **at‑least‑once** (Kafka offset via consumer‑group commit + window state
via checkpoint). Combine with an **idempotent/upsert sink** keyed by
`(sensor, window_start)` for **exactly‑once *results***. See
[`docs/stream-processing.md`](../../docs/stream-processing.md) §13.

## Turn it into a service

Swap the timer trigger for the **App-Startup** hook so the pipeline starts with
the engine and runs until shutdown; point the sink at Kafka/JDBC for downstream
consumers. Add `trigger.earlyEveryMs` + `accumulation:"accumulating"` on the
window to emit **running totals** before each window closes (upsert-friendly).
