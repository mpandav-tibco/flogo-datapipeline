# Kafka source — `kafka`

Consumes a Kafka topic as a consumer group, in continuous **streaming** mode or one-shot
**batch** mode. Pure Go (IBM Sarama, no CGO). Implemented in
[`source/kafka/source.go`](../../source/kafka/source.go); registered as `kafka`.

- **Reads:** consumer-group topic consumption
- **Partitionable:** no (the consumer group assigns partitions)
- **Streaming:** yes (streaming mode) or bounded (batch mode)
- **Checkpoint / resume:** consumer-group offsets committed to the broker

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `brokers` | string or `[]string` | — | ✅ | Broker `host:port` list (CSV or array). |
| `topic` | string | — | ✅ | Topic to consume. |
| `groupId` | string | — | ✅ | Consumer group ID. |
| `startOffset` | string or int64 | `latest` | — | `earliest`, `latest`, or an explicit offset. |
| `mode` | string | `streaming` | — | `streaming` (run forever) or `batch` (stop on a condition). |
| `stopOffset` | int64 | — | — | **Batch:** stop when any partition reaches this offset. |
| `stopTimestamp` | string (RFC3339) | — | — | **Batch:** stop when a message timestamp ≥ this. |
| `stopAtCurrentEnd` | bool | `false` | — | **Batch:** drain to each partition's end-of-log snapshotted at open. |
| `deserializer` | string | `json` | — | `json`, `avro`, or `raw`. |
| `schemaRegistryURL` | string | — | — | Confluent Schema Registry (required for `avro`). |
| `pollMs` | int | `500` | — | Poll window in milliseconds. |
| `batchSize` | int | `1000` | — | Messages per batch (internal buffer = `batchSize × 2`). |
| `saslMechanism` | string | — | — | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `saslUser` | string | — | — | SASL username. |
| `saslPasswordEnv` | string | — | — | **Env var name** holding the SASL password. |
| `tlsCertFile` / `tlsKeyFile` / `tlsCACertFile` | string | — | — | TLS client-cert paths. |

## Behavior

- **Deserializers:** `json` → object; `raw` → `{key, value}` strings; `avro` → Confluent
  wire format (magic byte + schema ID + Avro binary) resolved against the registry.
- **Batch mode** requires at least one of `stopOffset`, `stopTimestamp`, or
  `stopAtCurrentEnd`; the first condition met wins. Use it for one-shot backfills.
- Offsets auto-commit (~1 s); `Flush` waits to guarantee the final commit.
- Deserialization errors are logged (WARN) and skipped.

## Example — streaming JSON

```json
{
  "type": "kafka",
  "brokers": ["kafka-0:9092", "kafka-1:9092"],
  "topic": "events",
  "groupId": "my-pipeline",
  "startOffset": "latest",
  "mode": "streaming",
  "deserializer": "json"
}
```

## Example — batch drain with SASL/TLS

```json
{
  "type": "kafka",
  "brokers": "kafka-prod:9092",
  "topic": "transactions",
  "groupId": "batch-loader",
  "startOffset": "earliest",
  "mode": "batch",
  "stopAtCurrentEnd": true,
  "saslMechanism": "SCRAM-SHA-256",
  "saslUser": "app-user",
  "saslPasswordEnv": "KAFKA_PASSWORD",
  "tlsCACertFile": "/etc/kafka/ca.crt"
}
```

## See also

- [Sources index](../sources.md) · [Kafka sink](../sinks/kafka.md)
- Demo: [`examples/17-kafka-streaming`](../../examples/17-kafka-streaming)
