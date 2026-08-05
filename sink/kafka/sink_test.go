package kafka

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/IBM/sarama"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// mockSyncProducer implements sarama.SyncProducer by embedding the interface
// (unused methods are present but panic if called) and overriding only the two
// methods the sink actually uses.
type mockSyncProducer struct {
	sarama.SyncProducer
	sent    []*sarama.ProducerMessage
	sendErr error
	closed  bool
}

func (m *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, msgs...)
	return nil
}

func (m *mockSyncProducer) Close() error { m.closed = true; return nil }

func TestParseSinkConfigDefaults(t *testing.T) {
	kc, err := parseSinkConfig(map[string]interface{}{
		"brokers": []interface{}{"b1:9092", "b2:9092"},
		"topic":   "events",
	})
	if err != nil {
		t.Fatalf("parseSinkConfig: %v", err)
	}
	if len(kc.brokers) != 2 || kc.topic != "events" {
		t.Fatalf("brokers/topic = %v / %q", kc.brokers, kc.topic)
	}
	if kc.ackMode != "all" || kc.compressionCodec != "none" || kc.serializer != "json" || kc.maxMessageBytes != 1000000 {
		t.Errorf("defaults wrong: %+v", kc)
	}
}

func TestParseSinkConfigBrokerForms(t *testing.T) {
	t.Run("[]string", func(t *testing.T) {
		kc, err := parseSinkConfig(map[string]interface{}{"brokers": []string{"a:1"}, "topic": "t"})
		if err != nil || len(kc.brokers) != 1 {
			t.Fatalf("kc=%+v err=%v", kc, err)
		}
	})
	t.Run("single string", func(t *testing.T) {
		kc, err := parseSinkConfig(map[string]interface{}{"brokers": "a:1", "topic": "t"})
		if err != nil || len(kc.brokers) != 1 || kc.brokers[0] != "a:1" {
			t.Fatalf("kc=%+v err=%v", kc, err)
		}
	})
}

func TestParseSinkConfigOptions(t *testing.T) {
	kc, err := parseSinkConfig(map[string]interface{}{
		"brokers":          []interface{}{"a:1"},
		"topic":            "t",
		"ackMode":          "leader",
		"compressionCodec": "zstd",
		"maxMessageBytes":  float64(2048), // JSON numbers decode to float64
		"partitionKey":     "id",
		"serializer":       "avro",
	})
	if err != nil {
		t.Fatalf("parseSinkConfig: %v", err)
	}
	if kc.ackMode != "leader" || kc.compressionCodec != "zstd" || kc.maxMessageBytes != 2048 ||
		kc.partitionKey != "id" || kc.serializer != "avro" {
		t.Errorf("options parsed wrong: %+v", kc)
	}

	// int (non-JSON) maxMessageBytes form
	kc2, _ := parseSinkConfig(map[string]interface{}{"brokers": "a:1", "topic": "t", "maxMessageBytes": 4096})
	if kc2.maxMessageBytes != 4096 {
		t.Errorf("maxMessageBytes int = %d, want 4096", kc2.maxMessageBytes)
	}
}

func TestParseSinkConfigErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
	}{
		{"missing brokers", map[string]interface{}{"topic": "t"}},
		{"missing topic", map[string]interface{}{"brokers": "a:1"}},
		{"bad ackMode", map[string]interface{}{"brokers": "a:1", "topic": "t", "ackMode": "quorum"}},
		{"bad compression", map[string]interface{}{"brokers": "a:1", "topic": "t", "compressionCodec": "brotli"}},
		{"bad serializer", map[string]interface{}{"brokers": "a:1", "topic": "t", "serializer": "protobuf"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parseSinkConfig(c.cfg); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestNewKafkaSinkParseError(t *testing.T) {
	// Missing brokers → newKafkaSink fails at parse before any broker dial.
	if _, err := newKafkaSink(map[string]interface{}{"topic": "t"}); err == nil {
		t.Fatal("expected error for missing brokers")
	}
}

func TestSerializeRow(t *testing.T) {
	s := &kafkaSink{serializer: "json"}
	b, err := s.serializeRow(map[string]interface{}{"a": float64(1)})
	if err != nil {
		t.Fatalf("serializeRow json: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil || m["a"] != float64(1) {
		t.Errorf("json round-trip = %v (err %v)", m, err)
	}

	// avro is not implemented — it warns and falls back to JSON.
	s.serializer = "avro"
	b2, err := s.serializeRow(map[string]interface{}{"a": "x"})
	if err != nil {
		t.Fatalf("serializeRow avro-fallback: %v", err)
	}
	if err := json.Unmarshal(b2, &m); err != nil {
		t.Errorf("avro fallback did not produce JSON: %v", err)
	}
}

func TestKafkaSinkWriteBatchAndClose(t *testing.T) {
	m := &mockSyncProducer{}
	s := &kafkaSink{producer: m, topic: "t", partitionKey: "id", serializer: "json"}
	batch := &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "v", Type: record.TypeString}},
		Rows: [][]interface{}{
			{int64(1), "a"}, // key present
			{int64(2), "b"}, // key present
			{nil, "c"},      // key column nil → no message key
		},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(m.sent) != 3 {
		t.Fatalf("sent %d messages, want 3", len(m.sent))
	}
	if m.sent[0].Key == nil {
		t.Error("row0 should carry a partition key")
	}
	if m.sent[2].Key != nil {
		t.Error("row2 (nil id) should have no key")
	}

	// nil and empty batches are no-ops.
	if err := s.WriteBatch(nil); err != nil {
		t.Errorf("WriteBatch(nil): %v", err)
	}
	if err := s.WriteBatch(&record.RecordBatch{}); err != nil {
		t.Errorf("WriteBatch(empty): %v", err)
	}

	if err := s.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !m.closed {
		t.Error("producer was not closed")
	}
}

func TestKafkaSinkWriteBatchSendError(t *testing.T) {
	m := &mockSyncProducer{sendErr: errors.New("broker down")}
	s := &kafkaSink{producer: m, topic: "t", serializer: "json"}
	batch := &record.RecordBatch{
		Columns: []record.Column{{Name: "v", Type: record.TypeString}},
		Rows:    [][]interface{}{{"x"}},
	}
	if err := s.WriteBatch(batch); err == nil {
		t.Fatal("expected WriteBatch error when SendMessages fails")
	}
}
