// Package kafka registers a Kafka producer sink with the sink registry.
// It uses the sarama library (github.com/IBM/sarama) SyncProducer so that
// WriteBatch blocks until the broker acknowledges each batch, making it
// compatible with the pipeline's checkpoint-then-flush contract.
//
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/kafka"
//
// # Configuration keys
//
//	brokers           []string  Kafka broker addresses (required)
//	topic             string    Kafka topic to produce to (required)
//	ackMode           string    "none" | "leader" | "all" (default "all")
//	compressionCodec  string    "none" | "snappy" | "lz4" | "zstd" (default "none")
//	maxMessageBytes   int       Max message size in bytes (default 1000000)
//	partitionKey      string    Column name to use as the Kafka message key for
//	                            hash-partitioning; absent = random partitioning
//	serializer        string    "json" | "avro" (default "json")
//	schemaRegistryURL string    Confluent Schema Registry URL (avro only)
//	flushTimeoutMs    int       Max wait for a flush call (default 1000 — no-op for SyncProducer)
//	saslMechanism     string    "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
//	saslUser          string    SASL username
//	saslPasswordEnv   string    Env var name holding the SASL password
//	tlsCertFile       string    Path to TLS client certificate file
//	tlsKeyFile        string    Path to TLS client key file
//	tlsCACertFile     string    Path to TLS CA certificate file
package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/kafkautil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

var klog = corelog.ChildLogger(corelog.RootLogger(), "kafka-sink")

func init() {
	sink.Register("kafka", newKafkaSink)
}

// kafkaSink implements sink.Sink using a sarama SyncProducer.
//
// WriteBatch serialises each row to JSON (or Avro when schemaRegistryURL is set)
// and calls producer.SendMessages in a single batch call that blocks until the
// broker acks. This is compatible with the pipeline's checkpoint-then-Flush
// ordering contract: after WriteBatch returns, the data is durably committed
// before the engine writes the checkpoint token.
type kafkaSink struct {
	producer    sarama.SyncProducer
	topic       string
	partitionKey string
	serializer  string
	rowsWritten int64
}

// kafkaSinkConfig holds parsed configuration.
type kafkaSinkConfig struct {
	brokers          []string
	topic            string
	ackMode          string // "none" | "leader" | "all"
	compressionCodec string // "none" | "snappy" | "lz4" | "zstd"
	maxMessageBytes  int
	partitionKey     string
	serializer       string
}

func newKafkaSink(cfg map[string]interface{}) (sink.Sink, error) {
	kc, err := parseSinkConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: %w", err)
	}

	sc, err := kafkautil.BuildSaramaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: %w", err)
	}

	// Ack mode — maps to sarama RequiredAcks.
	switch kc.ackMode {
	case "none":
		sc.Producer.RequiredAcks = sarama.NoResponse
	case "leader":
		sc.Producer.RequiredAcks = sarama.WaitForLocal
	default: // "all"
		sc.Producer.RequiredAcks = sarama.WaitForAll
	}

	// Compression.
	switch kc.compressionCodec {
	case "snappy":
		sc.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		sc.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		sc.Producer.Compression = sarama.CompressionZSTD
	default: // "none"
		sc.Producer.Compression = sarama.CompressionNone
	}

	if kc.maxMessageBytes > 0 {
		sc.Producer.MaxMessageBytes = kc.maxMessageBytes
	}

	// SyncProducer requires both Return channels to be true.
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true

	// Partition strategy.
	if kc.partitionKey != "" {
		sc.Producer.Partitioner = sarama.NewHashPartitioner
	} else {
		sc.Producer.Partitioner = sarama.NewRandomPartitioner
	}

	producer, err := sarama.NewSyncProducer(kc.brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: new sync producer: %w", err)
	}

	klog.Infof("kafka sink: connected to brokers %v, topic=%q ackMode=%s compression=%s",
		kc.brokers, kc.topic, kc.ackMode, kc.compressionCodec)

	return &kafkaSink{
		producer:     producer,
		topic:        kc.topic,
		partitionKey: kc.partitionKey,
		serializer:   kc.serializer,
	}, nil
}

// WriteBatch serialises each row and sends all messages to Kafka in a single
// SendMessages call. Blocks until the broker acknowledges.
//
// Idempotency note: retrying the same batch at the same pipeline offset
// re-sends the same messages. When ackMode="all", enabling Kafka's idempotent
// producer (enable.idempotence=true, configurable via the broker) makes this
// exactly-once. Without idempotence, at-least-once delivery is guaranteed.
func (s *kafkaSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil || len(batch.Rows) == 0 {
		return nil
	}

	msgs := make([]*sarama.ProducerMessage, 0, len(batch.Rows))

	for _, row := range batch.Rows {
		// Build a column-name → value map for serialisation.
		rowMap := make(map[string]interface{}, len(batch.Columns))
		for j, col := range batch.Columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
		}

		value, err := s.serializeRow(rowMap)
		if err != nil {
			return fmt.Errorf("kafka sink: serialize row: %w", err)
		}

		msg := &sarama.ProducerMessage{
			Topic: s.topic,
			Value: sarama.ByteEncoder(value),
		}

		// Set the partition key from the configured column.
		if s.partitionKey != "" {
			if keyVal, ok := rowMap[s.partitionKey]; ok && keyVal != nil {
				msg.Key = sarama.StringEncoder(fmt.Sprintf("%v", keyVal))
			}
		}

		msgs = append(msgs, msg)
	}

	if err := s.producer.SendMessages(msgs); err != nil {
		return fmt.Errorf("kafka sink: SendMessages topic=%q: %w", s.topic, err)
	}

	s.rowsWritten += int64(len(msgs))
	return nil
}

// Flush is a no-op for SyncProducer: SendMessages already blocks until the
// broker acknowledges. Implements the sink.Sink Flush contract.
func (s *kafkaSink) Flush() error {
	return nil
}

// Close shuts down the SyncProducer and logs the total rows written.
func (s *kafkaSink) Close() error {
	if err := s.producer.Close(); err != nil {
		return fmt.Errorf("kafka sink: close producer: %w", err)
	}
	klog.Infof("kafka sink: closed topic=%q totalRowsWritten=%d", s.topic, s.rowsWritten)
	return nil
}

// serializeRow converts a row map to the configured wire format.
// JSON is the default and always-available serializer.
// Avro serialization (schemaRegistryURL) logs a warning and falls back to JSON
// until the Avro schema-registration+encoding path is implemented.
func (s *kafkaSink) serializeRow(row map[string]interface{}) ([]byte, error) {
	if s.serializer == "avro" {
		klog.Warnf("kafka sink: avro serializer not yet implemented — serializing as JSON for topic %q", s.topic)
	}
	return json.Marshal(row)
}

// --- Config parser ---

func parseSinkConfig(cfg map[string]interface{}) (*kafkaSinkConfig, error) {
	kc := &kafkaSinkConfig{
		ackMode:          "all",
		compressionCodec: "none",
		maxMessageBytes:  1000000,
		serializer:       "json",
	}

	switch v := cfg["brokers"].(type) {
	case []string:
		kc.brokers = v
	case []interface{}:
		for _, b := range v {
			if s, ok := b.(string); ok {
				kc.brokers = append(kc.brokers, s)
			}
		}
	case string:
		kc.brokers = []string{v}
	}
	if len(kc.brokers) == 0 {
		return nil, fmt.Errorf("'brokers' is required")
	}

	kc.topic, _ = cfg["topic"].(string)
	if kc.topic == "" {
		return nil, fmt.Errorf("'topic' is required")
	}

	if am, ok := cfg["ackMode"].(string); ok && am != "" {
		switch am {
		case "none", "leader", "all":
			kc.ackMode = am
		default:
			return nil, fmt.Errorf("ackMode %q must be 'none', 'leader', or 'all'", am)
		}
	}

	if cc, ok := cfg["compressionCodec"].(string); ok && cc != "" {
		switch cc {
		case "none", "snappy", "lz4", "zstd":
			kc.compressionCodec = cc
		default:
			return nil, fmt.Errorf("compressionCodec %q must be 'none', 'snappy', 'lz4', or 'zstd'", cc)
		}
	}

	if mb, ok := cfg["maxMessageBytes"].(int); ok && mb > 0 {
		kc.maxMessageBytes = mb
	} else if mb, ok := cfg["maxMessageBytes"].(float64); ok && mb > 0 {
		kc.maxMessageBytes = int(mb)
	}

	kc.partitionKey, _ = cfg["partitionKey"].(string)

	if ser, ok := cfg["serializer"].(string); ok && ser != "" {
		switch ser {
		case "json", "avro":
			kc.serializer = ser
		default:
			return nil, fmt.Errorf("serializer %q must be 'json' or 'avro'", ser)
		}
	}

	return kc, nil
}
