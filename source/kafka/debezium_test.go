package kafka

import "testing"

// TestDebeziumDeserialize_Update covers the common "unwrapped" envelope
// (schemas.enable=false): op=u uses "after" and carries the op column.
func TestDebeziumDeserialize_Update(t *testing.T) {
	envelope := `{"before":{"id":1,"status":"open"},"after":{"id":1,"status":"closed"},"op":"u","ts_ms":123}`
	obj, err := debeziumDeserialize("__op", nil, []byte(envelope))
	if err != nil {
		t.Fatalf("debeziumDeserialize: %v", err)
	}
	if obj["status"] != "closed" {
		t.Errorf("status = %v, want closed (must use 'after' for op=u)", obj["status"])
	}
	if obj["__op"] != "u" {
		t.Errorf("__op = %v, want u", obj["__op"])
	}
}

// TestDebeziumDeserialize_Delete covers a delete: "after" is null, so the row
// must come from "before" (Debezium's own delete semantics) — a JDBC sink's
// opColumn mode needs the deleted row's KEY, which only "before" carries.
func TestDebeziumDeserialize_Delete(t *testing.T) {
	envelope := `{"before":{"id":7,"status":"open"},"after":null,"op":"d","ts_ms":456}`
	obj, err := debeziumDeserialize("__op", nil, []byte(envelope))
	if err != nil {
		t.Fatalf("debeziumDeserialize: %v", err)
	}
	if obj["id"] != float64(7) {
		t.Errorf("id = %v, want 7 (must use 'before' for op=d)", obj["id"])
	}
	if obj["__op"] != "d" {
		t.Errorf("__op = %v, want d", obj["__op"])
	}
}

// TestDebeziumDeserialize_WrappedPayload covers Kafka Connect's default
// schemas.enable=true form, where the envelope is nested under "payload"
// alongside a "schema" key.
func TestDebeziumDeserialize_WrappedPayload(t *testing.T) {
	msg := `{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":3,"name":"x"},"op":"c"}}`
	obj, err := debeziumDeserialize("__op", nil, []byte(msg))
	if err != nil {
		t.Fatalf("debeziumDeserialize: %v", err)
	}
	if obj["name"] != "x" || obj["__op"] != "c" {
		t.Errorf("obj = %v, want name=x __op=c", obj)
	}
}

// TestDebeziumDeserialize_CustomOpColumn proves the metadata column name is
// configurable (so it can be aligned with a jdbc sink's own opColumn setting
// if a pipeline author picks a non-default name).
func TestDebeziumDeserialize_CustomOpColumn(t *testing.T) {
	envelope := `{"after":{"id":1},"op":"c"}`
	obj, err := debeziumDeserialize("_change_type", nil, []byte(envelope))
	if err != nil {
		t.Fatalf("debeziumDeserialize: %v", err)
	}
	if obj["_change_type"] != "c" {
		t.Errorf("obj = %v, want _change_type=c", obj)
	}
	if _, ok := obj["__op"]; ok {
		t.Error("default __op key should not be present when a custom opColumn is used")
	}
}

func TestDebeziumDeserialize_Errors(t *testing.T) {
	if _, err := debeziumDeserialize("__op", nil, nil); err == nil {
		t.Error("empty value (raw tombstone) expected an error")
	}
	if _, err := debeziumDeserialize("__op", nil, []byte(`{bad`)); err == nil {
		t.Error("malformed JSON expected an error")
	}
	// op=u but "after" is missing entirely — no data to extract.
	if _, err := debeziumDeserialize("__op", nil, []byte(`{"op":"u"}`)); err == nil {
		t.Error("missing 'after' field expected an error")
	}
}

func TestNewKafkaSource_DebeziumDeserializerConfig(t *testing.T) {
	cfg, err := parseSourceConfig(map[string]interface{}{
		"brokers": []interface{}{"localhost:9092"}, "topic": "t", "groupId": "g",
		"deserializer": "debezium", "opColumnName": "_op",
	})
	if err != nil {
		t.Fatalf("parseSourceConfig: %v", err)
	}
	if cfg.deserializer != "debezium" {
		t.Errorf("deserializer = %q, want debezium", cfg.deserializer)
	}
	if cfg.opColumnName != "_op" {
		t.Errorf("opColumnName = %q, want _op", cfg.opColumnName)
	}
}

func TestNewKafkaSource_DebeziumDeserializerDefaultOpColumn(t *testing.T) {
	cfg, err := parseSourceConfig(map[string]interface{}{
		"brokers": []interface{}{"localhost:9092"}, "topic": "t", "groupId": "g",
		"deserializer": "debezium",
	})
	if err != nil {
		t.Fatalf("parseSourceConfig: %v", err)
	}
	if cfg.opColumnName != "__op" {
		t.Errorf("opColumnName = %q, want default __op", cfg.opColumnName)
	}
}

func TestNewKafkaSource_UnknownDeserializerRejected(t *testing.T) {
	if _, err := parseSourceConfig(map[string]interface{}{
		"brokers": []interface{}{"localhost:9092"}, "topic": "t", "groupId": "g",
		"deserializer": "xml",
	}); err == nil {
		t.Error("unknown deserializer expected an error")
	}
}
