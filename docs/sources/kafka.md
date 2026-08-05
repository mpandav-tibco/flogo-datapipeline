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
| `deserializer` | string | `json` | — | `json`, `avro`, `raw`, or `debezium`. |
| `schemaRegistryURL` | string | — | — | Confluent Schema Registry (required for `avro`). |
| `opColumnName` | string | `__op` | — | `debezium` only: metadata column carrying the change type (`c`/`u`/`d`/`r`). |
| `pollMs` | int | `500` | — | Poll window in milliseconds. |
| `batchSize` | int | `1000` | — | Messages per batch (internal buffer = `batchSize × 2`). |
| `saslMechanism` | string | — | — | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. |
| `saslUser` | string | — | — | SASL username. |
| `saslPasswordEnv` | string | — | — | **Env var name** holding the SASL password. |
| `tlsCertFile` / `tlsKeyFile` / `tlsCACertFile` | string | — | — | TLS client-cert paths. |

## Behavior

- **Deserializers:** `json` → object; `raw` → `{key, value}` strings; `avro` → Confluent
  wire format (magic byte + schema ID + Avro binary) resolved against the registry;
  `debezium` → a Debezium / Kafka Connect change-event envelope (see below).
- **Batch mode** requires at least one of `stopOffset`, `stopTimestamp`, or
  `stopAtCurrentEnd`; the first condition met wins. Use it for one-shot backfills.
- Offsets auto-commit (~1 s); `Flush` waits to guarantee the final commit.
- Deserialization errors are logged (WARN) and skipped.

## Consuming CDC change streams (`deserializer: "debezium"`)

Rather than this engine implementing CDC capture itself, the `debezium`
deserializer lets it **consume** a real change stream captured by
[Debezium](https://debezium.io) (or anything emitting the same Kafka Connect
envelope): compose with the ecosystem's CDC engine instead of duplicating it.

For each message it accepts either the "unwrapped" envelope
(`connect.json.converter.schemas.enable=false`) or the default "wrapped" form
(nested under a top-level `payload` key alongside `schema`), and flattens it to
a row: the changed record's columns (`after` for create/update/snapshot,
`before` for a delete — `after` is null on a delete, so `before` is the only
place the deleted row's key survives) plus a metadata column (`opColumnName`,
default `__op`) carrying the raw op code `c`/`u`/`d`/`r`.

Pair it with the [JDBC sink](../sinks/jdbc.md)'s `opColumn` delete-capable mode
(same default column name and the same `"d"`/`"delete"` vocabulary) to mirror
upstream inserts, updates, **and deletes** into a target table — a working
CDC-to-target pipeline without this engine ever performing log-based capture.
A raw Kafka tombstone (the log-compaction delete marker: an empty value with no
envelope) is skipped with a warning — the preceding `op:"d"` envelope message
already carried the full delete.

```json
{
  "type": "kafka",
  "brokers": "localhost:9092",
  "topic": "dbserver1.inventory.customers",
  "groupId": "dp-cdc-consumer",
  "deserializer": "debezium"
}
```

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
