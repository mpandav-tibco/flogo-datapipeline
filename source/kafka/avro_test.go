package kafka

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"
)

const avroTestSchema = `{"type":"record","name":"R","fields":[
  {"name":"id","type":"int"},
  {"name":"name","type":["null","string"]},
  {"name":"amount","type":"double"},
  {"name":"active","type":"boolean"},
  {"name":"count","type":"long"},
  {"name":"ratio","type":"float"},
  {"name":"blob","type":"bytes"}
]}`

// mockRegistry serves the given schema JSON at /schemas/ids/7 (Confluent shape),
// 404 for any other id.
func mockRegistry(t *testing.T, schemaJSON string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/7" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schemaJSON})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// confluentMsg builds a Confluent wire-format message: magic 0x00 + 4-byte
// big-endian schema id + hamba-encoded Avro binary body.
func confluentMsg(t *testing.T, schemaJSON string, schemaID int, rec map[string]interface{}) []byte {
	t.Helper()
	sch := avro.MustParse(schemaJSON)
	body, err := avro.Marshal(sch, rec)
	if err != nil {
		t.Fatalf("avro.Marshal: %v", err)
	}
	hdr := []byte{0x00, byte(schemaID >> 24), byte(schemaID >> 16), byte(schemaID >> 8), byte(schemaID)}
	return append(hdr, body...)
}

func TestAvroDeserialize_SchemaRegistry(t *testing.T) {
	srv := mockRegistry(t, avroTestSchema)
	sr := newSchemaRegistryClient(srv.URL)

	msg := confluentMsg(t, avroTestSchema, 7, map[string]interface{}{
		"id": 42, "name": "alice", "amount": 9.5, "active": true,
		"count": int64(100), "ratio": float32(1.5), "blob": []byte("xy"),
	})
	obj, err := avroDeserialize(sr, nil, msg)
	if err != nil {
		t.Fatalf("avroDeserialize: %v", err)
	}
	if obj["id"] != int64(42) {
		t.Errorf("id = %v (%T), want int64(42)", obj["id"], obj["id"])
	}
	if obj["name"] != "alice" {
		t.Errorf("name = %v, want alice", obj["name"])
	}
	if obj["amount"] != 9.5 {
		t.Errorf("amount = %v, want 9.5", obj["amount"])
	}
	if obj["active"] != true {
		t.Errorf("active = %v, want true", obj["active"])
	}
	if obj["count"] != int64(100) {
		t.Errorf("count = %v, want int64(100)", obj["count"])
	}
	if obj["ratio"] != float32(1.5) {
		t.Errorf("ratio = %v, want float32(1.5)", obj["ratio"])
	}
	if b, ok := obj["blob"].([]byte); !ok || string(b) != "xy" {
		t.Errorf("blob = %v, want xy", obj["blob"])
	}

	// A second decode with the same schema id exercises the GetSchema cache and
	// the null union branch (name = null).
	msg2 := confluentMsg(t, avroTestSchema, 7, map[string]interface{}{
		"id": 1, "name": nil, "amount": 0.0, "active": false,
		"count": int64(0), "ratio": float32(0), "blob": []byte(""),
	})
	obj2, err := avroDeserialize(sr, nil, msg2)
	if err != nil {
		t.Fatalf("avroDeserialize 2: %v", err)
	}
	if obj2["name"] != nil {
		t.Errorf("name = %v, want nil (null union branch)", obj2["name"])
	}
}

func TestAvroDeserialize_Errors(t *testing.T) {
	// Message shorter than the 5-byte Confluent header.
	if _, err := avroDeserialize(nil, nil, []byte{0x00, 0x01}); err == nil {
		t.Error("too-short message expected an error")
	}
	// Wrong magic byte.
	if _, err := avroDeserialize(nil, nil, []byte{0x01, 0, 0, 0, 7, 0}); err == nil {
		t.Error("bad magic byte expected an error")
	}

	srv := mockRegistry(t, avroTestSchema)
	sr := newSchemaRegistryClient(srv.URL)
	// Schema id 9 is not served → registry 404 → error.
	badID := confluentMsg(t, avroTestSchema, 9, map[string]interface{}{
		"id": 1, "name": nil, "amount": 0.0, "active": false, "count": int64(0), "ratio": float32(0), "blob": []byte(""),
	})
	if _, err := avroDeserialize(sr, nil, badID); err == nil {
		t.Error("unknown schema id expected an error")
	}

	// Unreachable registry.
	srBad := newSchemaRegistryClient("http://127.0.0.1:0")
	msg := confluentMsg(t, avroTestSchema, 7, map[string]interface{}{
		"id": 1, "name": nil, "amount": 0.0, "active": false, "count": int64(0), "ratio": float32(0), "blob": []byte(""),
	})
	if _, err := avroDeserialize(srBad, nil, msg); err == nil {
		t.Error("unreachable registry expected an error")
	}
}

func TestDecodeAvroRecord_Errors(t *testing.T) {
	if _, err := decodeAvroRecord(`{bad`, nil); err == nil {
		t.Error("bad schema JSON expected an error")
	}
	if _, err := decodeAvroRecord(`{"type":"string"}`, nil); err == nil {
		t.Error("non-record schema expected an error")
	}
}

func TestAvroUnwrapUnion(t *testing.T) {
	if nu, tn := avroUnwrapUnion("int"); nu || tn != "int" {
		t.Errorf("plain string type = (%v,%q), want (false,int)", nu, tn)
	}
	if nu, tn := avroUnwrapUnion([]interface{}{"null", "string"}); !nu || tn != "string" {
		t.Errorf("nullable union = (%v,%q), want (true,string)", nu, tn)
	}
	if _, tn := avroUnwrapUnion([]interface{}{}); tn != "string" {
		t.Errorf("empty union type = %q, want default string", tn)
	}
}
