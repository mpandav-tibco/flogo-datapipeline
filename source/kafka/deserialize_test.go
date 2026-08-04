package kafka

import (
	"testing"
	"time"
)

func TestJSONDeserialize(t *testing.T) {
	obj, err := jsonDeserialize(nil, []byte(`{"a":1,"b":"x"}`))
	if err != nil {
		t.Fatalf("jsonDeserialize: %v", err)
	}
	if obj["b"] != "x" {
		t.Errorf("b = %v, want x", obj["b"])
	}
	if _, err := jsonDeserialize(nil, []byte(`{bad`)); err == nil {
		t.Error("malformed JSON expected an error")
	}
}

func TestRawDeserialize(t *testing.T) {
	obj, err := rawDeserialize([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("rawDeserialize: %v", err)
	}
	if obj["key"] != "k" || obj["value"] != "v" {
		t.Errorf("raw = %v, want key=k value=v", obj)
	}
}

func TestAvroDeserializeErrors(t *testing.T) {
	// Too short (needs at least the 5-byte Confluent header).
	if _, err := avroDeserialize(nil, nil, []byte{0x00, 0x01}); err == nil {
		t.Error("too-short message expected an error")
	}
	// Wrong magic byte.
	if _, err := avroDeserialize(nil, nil, []byte{0x01, 0, 0, 0, 1, 2}); err == nil {
		t.Error("bad magic byte expected an error")
	}
}

func TestHWMCoerceInt64(t *testing.T) {
	cases := []struct {
		in   interface{}
		want int64
	}{
		{int64(5), 5}, {int(7), 7}, {float64(9), 9}, {"11", 11}, {"not-int", 0}, {true, 0},
	}
	for _, c := range cases {
		if got := hwmCoerceInt64(c.in); got != c.want {
			t.Errorf("hwmCoerceInt64(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestHWMCoerceDatetime(t *testing.T) {
	if v := hwmCoerceDatetime("2024-01-02"); v == nil {
		t.Error("date string should parse")
	}
	if v := hwmCoerceDatetime(time.Now()); v == nil {
		t.Error("time.Time should pass through")
	}
	if v := hwmCoerceDatetime("garbage"); v != nil {
		t.Errorf("unparseable string = %v, want nil", v)
	}
	if v := hwmCoerceDatetime(42); v != nil {
		t.Errorf("int = %v, want nil", v)
	}
}

func TestPassesHWMFilter(t *testing.T) {
	ksInt := &kafkaSource{hwmColumn: "seq", hwmValueType: "int", hwmValue: "10"}
	if !ksInt.passesHWMFilter(map[string]interface{}{"seq": int64(11)}) {
		t.Error("seq 11 > 10 should pass")
	}
	if ksInt.passesHWMFilter(map[string]interface{}{"seq": int64(10)}) {
		t.Error("seq 10 is not > 10, should not pass")
	}
	if !ksInt.passesHWMFilter(map[string]interface{}{"other": 1}) {
		t.Error("absent hwm column should pass through")
	}

	ksDate := &kafkaSource{hwmColumn: "ts", hwmValueType: "datetime", hwmValue: "2024-01-01"}
	if !ksDate.passesHWMFilter(map[string]interface{}{"ts": "2024-06-01"}) {
		t.Error("later date should pass")
	}
	if ksDate.passesHWMFilter(map[string]interface{}{"ts": "2023-01-01"}) {
		t.Error("earlier date should not pass")
	}

	ksStr := &kafkaSource{hwmColumn: "id", hwmValueType: "string", hwmValue: "m"}
	if !ksStr.passesHWMFilter(map[string]interface{}{"id": "z"}) {
		t.Error("z > m should pass")
	}
}

func TestInferSchemaAndExtractRow(t *testing.T) {
	ks := &kafkaSource{}
	ks.inferSchema(map[string]interface{}{"b": 2, "a": 1, "c": 3})
	sch := ks.Schema()
	if len(sch) != 3 || sch[0].Name != "a" || sch[1].Name != "b" || sch[2].Name != "c" {
		t.Fatalf("schema = %v, want deterministic a,b,c order", sch)
	}
	row := ks.extractRow(map[string]interface{}{"a": 1, "b": 2, "c": 3})
	if len(row) != 3 || row[0] != 1 || row[2] != 3 {
		t.Errorf("row = %v, want [1 2 3]", row)
	}
}

func TestChooseDeserializer(t *testing.T) {
	for _, d := range []string{"json", "raw", "avro", ""} {
		ks := &kafkaSource{cfg: &kafkaSourceConfig{deserializer: d}}
		if fn := ks.chooseDeserializer(); fn == nil {
			t.Errorf("chooseDeserializer(%q) returned nil", d)
		}
	}
}
