# Example 17 — Kafka Source → JDBC Sink (Streaming ETL)

This example demonstrates how Flogo Data Pipeline consumes a continuous stream of JSON messages from a Kafka topic, applies two transform stages, and writes surviving rows in micro-batches to a PostgreSQL database — all without an intermediate file.

The focus is on the **Kafka source**: consumer groups, timed polling, the difference between streaming and batch behaviour, and the SASL/SCRAM-SHA-512 configuration needed for production brokers.

---

## What This Example Demonstrates

| Feature | Detail |
|---|---|
| Kafka source | `"type": "kafka"` — polls a topic using a long-lived consumer group |
| Consumer group | `groupId: "pipeline-consumer-1"` — offset is committed per group |
| Timed poll | `pollTimeoutMs: 5000` — pipeline collects messages for 5 s then flushes to transforms |
| Streaming ETL | No intermediate file; Kafka offsets drive backpressure |
| Filter transform | Drops rows where `temperature <= 0` (sensor offline / calibration zero) |
| Validate transform (drop) | Rejects rows with null `sensor_id` or temperature outside `[-100, 200]` |
| JDBC sink (PostgreSQL) | `driver: postgres`, `onConflict: insert`, `batchSize: 100` |
| Local Kafka + PostgreSQL via Docker | `docker-compose.yml` with bitnami/kafka 3.6, bitnami/zookeeper 3.9, postgres 16 |
| SASL/SCRAM config | Documented for production; not enabled in the demo |

---

## Directory Layout

```
17-kafka-streaming/
├── kafka-streaming.datapipeline   # Pipeline definition
├── docker-compose.yml             # Local Kafka + Zookeeper + PostgreSQL
├── produce.sh                     # Publish 20 test messages to the topic
├── setup.sh                       # Create the PostgreSQL target table
├── verify.sh                      # Query PostgreSQL and confirm row counts
└── README.md
```

---

## How Kafka Polling Works (Streaming vs Batch)

### Batch sources

CSV, JDBC, and Parquet sources all have a **finite record set** known at startup. The engine reads every row, applies transforms, and then the pipeline exits cleanly.

### Kafka source (streaming)

The Kafka source does **not** know how many messages are waiting. Instead it enters a poll loop:

```
while running:
    batch = consumer.Poll(pollTimeoutMs)   // block for up to 5 000 ms
    if len(batch) > 0:
        forward batch to transform chain
        commit offsets
    elif shutdown signal received:
        break
```

- **pollTimeoutMs** (default `5000`) is the maximum time the consumer blocks waiting for messages. A low value (e.g. 100 ms) increases CPU usage; a high value (e.g. 30 000 ms) increases latency.
- Each poll returns **up to** `performance.batchSize` (500 in this example) messages. If the topic has more messages than `batchSize`, multiple polls are issued in rapid succession until the consumer catches up.
- The pipeline runs **until terminated** (SIGTERM / Ctrl-C). There is no "end of stream" for a Kafka topic.
- **Offset commit** happens after each poll batch is successfully flushed to the sink. If the pipeline crashes mid-batch, uncommitted offsets are reprocessed on restart — resulting in at-least-once delivery. Combine with `onConflict: upsert` in the sink to make this idempotent.

### Timing of the 20-message test

```
produce.sh publishes 20 messages instantly
                    ↓
pipeline polls Kafka (up to 5 000 ms)
                    ↓
first poll returns all 20 messages in one batch
                    ↓
filter drops 2 (temperature = 0)
validate drops 2 (temperature 250, empty sensor_id)
                    ↓
16 rows written to PostgreSQL in 1 JDBC batch (batchSize: 100)
                    ↓
second poll waits 5 000 ms, returns empty → pipeline idles
(terminate with Ctrl-C or SIGTERM)
```

---

## Sample Data

`produce.sh` publishes 20 sensor JSON messages. Each message represents a temperature/humidity reading from a building sensor:

```json
{"sensor_id":"S001","location":"floor-1","temperature":23.4,"humidity":55.2,"unit":"C","ts":"2026-07-08T08:00:00Z"}
```

### Message breakdown

| # | sensor_id | temperature | Outcome | Reason |
|---|---|---|---|---|
| 01–08 | S001–S008 | 13.9 – 52.0 | Written | Valid |
| 09–12 | S001–S004 | 20.1 – 32.0 | Written | Valid (second reading cycle) |
| 13–16 | S009–S012 | 17.8 – 22.0 | Written | Valid (server-room + offices) |
| 17 | S013 | 0 | **Dropped** | Filter: `temperature > 0` is false |
| 18 | S014 | 0 | **Dropped** | Filter: `temperature > 0` is false |
| 19 | S015 | 250 | **Dropped** | Validate: `temperature` max is 200 |
| 20 | _(empty)_ | 22.0 | **Dropped** | Validate: `sensor_id` notNull fails |

Expected rows in PostgreSQL after the pipeline run: **16**

---

## Pipeline Walkthrough

### 1. Source — Kafka

```json
"source": {
  "type": "kafka",
  "brokers": "localhost:9092",
  "topic": "sensor-data",
  "groupId": "pipeline-consumer-1",
  "sasl": false,
  "pollTimeoutMs": 5000
}
```

The consumer joins the group `pipeline-consumer-1`. Kafka assigns the topic partition(s) to this consumer. All unread messages from the last committed offset are delivered in poll batches. The `groupId` is critical: if you run the pipeline a second time with the **same** groupId and the topic already has no new messages, no rows will be written (the consumer resumes from where it left off). Change the `groupId` or reset the offset to reprocess.

### 2. Transform 1 — Filter

```json
{ "type": "filter", "expr": "temperature > 0" }
```

Sensor readings with `temperature = 0` indicate a powered-off or calibration-reset device. These are excluded before validation to avoid polluting the DQ stats. Two messages (S013, S014) are dropped here.

### 3. Transform 2 — Validate (drop)

```json
{
  "type": "validate",
  "rules": [
    { "field": "sensor_id",   "notNull": true },
    { "field": "temperature", "min": -100, "max": 200 }
  ],
  "onViolation": "drop"
}
```

Two rules guard the physical plausibility of each reading:

| Rule | Field | Constraint | Row dropped |
|---|---|---|---|
| Not null | `sensor_id` | Must be present and non-empty | msg 20 (empty string) |
| Range | `temperature` | Must be in `[-100, 200]` °C | msg 19 (250 °C) |

`onViolation: "drop"` silently discards invalid rows. Switch to `"route"` and add a `dlqSink` if you need to inspect rejected readings (see Common Modifications below).

### 4. Sink — JDBC (PostgreSQL)

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "postgres://postgres:postgres@localhost:5432/pipelinedemo?sslmode=disable",
  "table": "sensor_readings",
  "batchSize": 100,
  "onConflict": "insert"
}
```

`onConflict: "insert"` means plain `INSERT` statements — no duplicate checking. For this demo this is intentional: every Kafka message produces exactly one row. In production, prefer `onConflict: "upsert"` with a `conflictKey` so pipeline restarts after a crash do not produce duplicate rows.

The JDBC sink collects rows in memory until `batchSize` (100) is reached or the poll batch is exhausted, then issues one `INSERT` statement per batch. For this 16-row demo a single INSERT batch is issued.

---

## How to Run

### Prerequisites

| Tool | Purpose |
|---|---|
| Docker + docker compose | Run local Kafka broker + PostgreSQL target |
| flogo-datapipeline CLI | Execute the pipeline |
| kcat or kafka-console-producer.sh | Produce test messages (one or the other) |

### Step 1 — Start Kafka

```bash
docker compose up -d
```

Wait approximately 15–20 seconds for the broker to become healthy:

```bash
docker compose ps
# NAME                IMAGE                   STATUS
# dp-ex17-postgres    postgres:16-alpine      Up (healthy)
# kafka               bitnami/kafka:3.6       Up (healthy)
# zookeeper           bitnami/zookeeper:3.9   Up (healthy)
```

### Step 2 — Create the PostgreSQL target table

```bash
chmod +x setup.sh
./setup.sh
```

`setup.sh` starts the stack (if it is not already running) and creates the `sensor_readings` table in PostgreSQL with a composite index on `(sensor_id, ts)`.

### Step 3 — Publish test messages

```bash
chmod +x produce.sh
./produce.sh
```

Publishes 20 JSON messages to the `sensor-data` topic. The script auto-detects `kcat`, `kafkacat`, or `kafka-console-producer.sh`. If none are available it falls back to `docker exec` into the running broker container.

### Step 4 — Run the pipeline

```bash
flogo-datapipeline run kafka-streaming.datapipeline
```

Expected console output:

```
[source]    kafka  localhost:9092  topic=sensor-data  group=pipeline-consumer-1
[poll]      batch_1  messages_received=20  poll_ms=5000
[filter]    dropped=2  passed=18
[validate]  dropped=2  passed=16
[sink]      jdbc postgres localhost:5432/pipelinedemo  rows_written=16  batches=1
[poll]      batch_2  messages_received=0   poll_ms=5000  (idle — waiting for more messages)
^C
[shutdown]  consumer offsets committed  group=pipeline-consumer-1
```

Terminate the pipeline with Ctrl-C when the second poll is idle.

### Step 5 — Verify

```bash
chmod +x verify.sh
./verify.sh
```

---

## Expected verify.sh Output

### Total rows

```
total_rows
----------
16
```

### Rows by location

```
location      row_count
-----------   ---------
floor-1       4
floor-2       4
server-room   2
office-a      1
office-b      1
roof          2
basement      2
```

### Temperature stats per sensor

```
sensor_id   location      readings   min_temp   max_temp   avg_temp
---------   -----------   --------   --------   --------   --------
S001        floor-1       2          23.4       23.9       23.65
S002        floor-1       2          24.1       24.5       24.3
S003        floor-2       2          19.8       20.1       19.95
S004        floor-2       2          31.5       32.0       31.75
S005        roof          1          47.2       47.2       47.2
S006        roof          1          52.0       52.0       52.0
S007        basement      1          14.3       14.3       14.3
S008        basement      1          13.9       13.9       13.9
S009        server-room   1          18.5       18.5       18.5
S010        server-room   1          17.8       17.8       17.8
S011        office-a      1          22.0       22.0       22.0
S012        office-b      1          21.5       21.5       21.5
```

---

## Reprocessing (Consumer Group Reset)

The Kafka source tracks progress via the consumer group offset stored in the broker. After a successful run, the group `pipeline-consumer-1` has committed offsets pointing to the end of the partition.

To reprocess the same messages:

**Option A — Change the groupId in the .datapipeline file:**

```json
"source": {
  "groupId": "pipeline-consumer-2"
}
```

**Option B — Reset the offset for the existing group:**

```bash
# With kafka-console-consumer.sh (Apache Kafka):
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group pipeline-consumer-1 \
  --topic sensor-data \
  --reset-offsets --to-earliest --execute

# With kcat:
# kcat does not support offset reset; use Option A or the kafka-consumer-groups tool.
```

**Option C — Produce more messages:**

```bash
./produce.sh   # publishes another 20 messages to the topic
flogo-datapipeline run kafka-streaming.datapipeline
```

The broker appends new messages; the consumer picks up from where it left off. You will see another 16 rows inserted (32 total in PostgreSQL).

---

## Production SASL/SCRAM Configuration

The demo uses `sasl: false` (plaintext). For brokers secured with SASL/SCRAM enable authentication in the source block:

```json
"source": {
  "type": "kafka",
  "brokers": "kafka.internal.example.com:9092",
  "topic": "sensor-data",
  "groupId": "pipeline-consumer-1",
  "sasl": true,
  "scramAlgorithm": "SHA-512",
  "usernameEnv": "KAFKA_USERNAME",
  "passwordEnv": "KAFKA_PASSWORD",
  "pollTimeoutMs": 5000
}
```

| Field | Type | Values | Description |
|---|---|---|---|
| `sasl` | bool | `true` / `false` | Enable SASL authentication |
| `scramAlgorithm` | string | `"SHA-256"` / `"SHA-512"` | SCRAM hash algorithm; SHA-512 is recommended |
| `usernameEnv` | string | env var name | Name of the env var holding the SASL username |
| `passwordEnv` | string | env var name | Name of the env var holding the SASL password |

Set the credentials before running:

```bash
export KAFKA_USERNAME=pipeline-user
export KAFKA_PASSWORD=supersecretpassword
flogo-datapipeline run kafka-streaming.datapipeline
```

Never put credentials in the `.datapipeline` file. The `usernameEnv` / `passwordEnv` pattern keeps secrets out of source control and works with any secrets manager (Vault, AWS Secrets Manager, Kubernetes Secrets) that can inject environment variables.

### TLS (Encrypted Transport)

TLS is required for most production Kafka clusters. Configure it alongside SASL:

```json
"source": {
  "type": "kafka",
  "brokers": "kafka.internal.example.com:9093",
  "topic": "sensor-data",
  "groupId": "pipeline-consumer-1",
  "sasl": true,
  "scramAlgorithm": "SHA-512",
  "usernameEnv": "KAFKA_USERNAME",
  "passwordEnv": "KAFKA_PASSWORD",
  "tls": true,
  "tlsCaCertFile": "/etc/ssl/certs/kafka-ca.pem",
  "pollTimeoutMs": 5000
}
```

| Field | Type | Description |
|---|---|---|
| `tls` | bool | Enable TLS transport encryption |
| `tlsCaCertFile` | string | Path to the CA certificate that signed the broker's TLS certificate |
| `tlsClientCertFile` | string | (Optional) Path to client certificate for mTLS |
| `tlsClientKeyFile` | string | (Optional) Path to client private key for mTLS |

### Adding SASL to the docker-compose broker (for testing)

Replace the `kafka` service environment block in `docker-compose.yml` with:

```yaml
environment:
  KAFKA_CFG_ZOOKEEPER_CONNECT: "zookeeper:2181"
  KAFKA_CFG_LISTENERS: "SASL_PLAINTEXT://0.0.0.0:9092"
  KAFKA_CFG_ADVERTISED_LISTENERS: "SASL_PLAINTEXT://localhost:9092"
  KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "SASL_PLAINTEXT:SASL_PLAINTEXT"
  KAFKA_CFG_SASL_ENABLED_MECHANISMS: "SCRAM-SHA-512"
  KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL: "SCRAM-SHA-512"
  KAFKA_CFG_INTER_BROKER_LISTENER_NAME: "SASL_PLAINTEXT"
  KAFKA_CLIENT_USERS: "pipeline-user"
  KAFKA_CLIENT_PASSWORDS: "supersecretpassword"
  ALLOW_PLAINTEXT_LISTENER: "no"
```

Then set `sasl: true`, `scramAlgorithm: "SHA-512"`, `usernameEnv: "KAFKA_USERNAME"`, `passwordEnv: "KAFKA_PASSWORD"` in the `.datapipeline` source block.

---

## Comparison to Kafka Streams / Apache Flink

| Aspect | flogo-datapipeline | Kafka Streams | Apache Flink |
|---|---|---|---|
| Deployment | Single binary; no JVM | JVM library (embedded in app) | JVM cluster (JobManager + TaskManagers) |
| State | Stateless transforms (no windowing) | Rich stateful (KTable, windowed aggregations) | Rich stateful (keyed state, event time) |
| Sink targets | JDBC, Parquet, object store, Kafka | Kafka topics only | Kafka, JDBC, Parquet, many connectors |
| Configuration | Declarative JSON | Java/Scala DSL or Kafka Streams DSL | Java/Python DSL or SQL |
| Best for | Streaming ETL — ingest Kafka → normalise → write to DB | Stream-to-stream joins, real-time aggregations | Complex event processing, exactly-once semantics at scale |
| Throughput | Moderate (single node) | High (partitioned, parallelised) | Very high (distributed cluster) |

Use flogo-datapipeline when you need a **lightweight streaming ETL** that can run as a single binary without a JVM or a cluster. Use Kafka Streams or Flink when you need stateful windowed aggregations or event-time joins across multiple topics.

---

## Key Configuration Reference

### kafka source

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `brokers` | string | yes | — | Comma-separated `host:port` list of Kafka bootstrap brokers |
| `topic` | string | yes | — | Topic name to consume from |
| `groupId` | string | yes | — | Consumer group ID; offsets are committed per group |
| `sasl` | bool | no | `false` | Enable SASL authentication |
| `scramAlgorithm` | string | no | — | `"SHA-256"` or `"SHA-512"`; required when `sasl: true` |
| `usernameEnv` | string | no | — | Env var name containing the SASL username |
| `passwordEnv` | string | no | — | Env var name containing the SASL password |
| `pollTimeoutMs` | int | no | `5000` | How long each poll blocks waiting for messages (ms) |
| `tls` | bool | no | `false` | Enable TLS transport |
| `tlsCaCertFile` | string | no | — | Path to CA certificate file for TLS verification |

### jdbc sink (streaming-specific notes)

| Field | Value in this example | Streaming recommendation |
|---|---|---|
| `batchSize` | `100` | Match to `performance.batchSize`; lower = more frequent flushes, lower latency |
| `onConflict` | `insert` | Use `upsert` + `conflictKey` for at-least-once idempotency after restarts |
| `driver` | `postgres` | Production-ready; also supports `mysql` |

### performance block

| Field | Value | Effect |
|---|---|---|
| `batchSize` | `500` | Max rows the engine holds in memory between Kafka poll and JDBC flush |
| `maxMemoryMB` | `256` | Memory budget; engine applies backpressure if exceeded |

---

## Common Modifications

### Route invalid rows to a dead-letter queue instead of dropping them

```json
{
  "type": "validate",
  "rules": [
    { "field": "sensor_id",   "notNull": true },
    { "field": "temperature", "min": -100, "max": 200 }
  ],
  "onViolation": "route",
  "errorSinkType": "csv",
  "errorSinkConfig": { "path": "./rejected_readings.csv" }
}
```

Add a top-level `"dlqSink"` matching the `errorSinkConfig` shape to capture the DLQ output.

### Add a map transform to rename / cast fields before writing

```json
{
  "type": "map",
  "fields": {
    "sensor_id":   "=$sensor_id",
    "location":    "=upper($.location)",
    "temp_c":      "=$temperature",
    "humidity_pct":"=$humidity",
    "recorded_at": "=$ts"
  }
}
```

Insert this transform between the validate step and the sink. Update the `sensor_readings` table DDL in `setup.sh` to use the new column names.

### Consume multiple partitions (scale out)

Set `scaling.mode` to `"sharded"` and run one process per partition. Each instance should use the **same** `groupId` — Kafka's group coordinator will rebalance partition assignments automatically:

```json
"scaling": { "mode": "sharded", "instanceIndex": 0, "instanceCount": 3 }
```

### Use upsert for idempotent loads (production hardening)

```json
"sink": {
  "type": "jdbc",
  "driver": "postgres",
  "dsn": "host=localhost port=5432 user=etl password=secret dbname=iot sslmode=require",
  "table": "sensor_readings",
  "batchSize": 500,
  "onConflict": "upsert",
  "conflictKey": ["sensor_id", "ts"]
}
```

---

## Tear Down

```bash
docker compose down          # stop and remove containers
docker compose down -v       # also remove named volumes (kafka_data, zookeeper_data, postgres_data)
```
