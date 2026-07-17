# Kafka sink — `kafka`

Publishes each row as a message to a Kafka topic using a synchronous producer (durable
before checkpoint). Pure Go (IBM Sarama). Implemented in
[`sink/kafka/sink.go`](../../sink/kafka/sink.go); registered as `kafka`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `brokers` | string or `[]string` | — | ✅ | Broker `host:port` list. |
| `topic` | string | — | ✅ | Target topic. |
| `ackMode` | string | `all` | — | `none`, `leader`, or `all`. |
| `compressionCodec` | string | `none` | — | `none`, `snappy`, `lz4`, `zstd`. |
| `maxMessageBytes` | int | `1000000` | — | Max single-message size. |
| `partitionKey` | string | — | — | Column used as the message key (hash partitioning; random when unset). |
| `serializer` | string | `json` | — | `json` (Avro is a stub that falls back to JSON). |
| `saslMechanism` / `saslUser` / `saslPasswordEnv` | string | — | — | SASL auth (`saslPasswordEnv` is the **env var name**). |
| `tlsCertFile` / `tlsKeyFile` / `tlsCACertFile` | string | — | — | TLS client-cert paths. |

## Behavior

- `WriteBatch` blocks until the broker acknowledges per `ackMode`, so a checkpoint is only
  taken after messages are durable.
- Rows are serialized to JSON; the whole batch is sent in one `SendMessages` call.

## Example

```json
{
  "type": "kafka",
  "brokers": ["kafka-0:9092", "kafka-1:9092"],
  "topic": "orders",
  "ackMode": "all",
  "compressionCodec": "snappy",
  "partitionKey": "customer_id"
}
```

## See also

- [Sinks index](../sinks.md) · [Kafka source](../sources/kafka.md)
- Demo: [`examples/17-kafka-streaming`](../../examples/17-kafka-streaming)
