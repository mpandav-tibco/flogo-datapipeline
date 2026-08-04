package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestIntrospectXML(t *testing.T) {
	s := newTestServerWithPipeline(t)
	xmlPath := filepath.Join(t.TempDir(), "products.xml")
	if err := os.WriteFile(xmlPath, []byte(`<?xml version="1.0"?>
<products>
  <product><id>1</id><name>Widget</name><price>9.99</price></product>
  <product><id>2</id><name>Gadget</name><price>19.99</price></product>
  <product><id>3</id><name>Gizmo</name><price>29.99</price></product>
</products>`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Happy path: detects the repeating record element and its fields.
	w := do(t, s, http.MethodPost, "/v1/introspect/xml", `{"path":"`+xmlPath+`"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("xml introspect = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var res XMLIntrospectResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.RecordElement != "product" || len(res.Fields) < 3 {
		t.Errorf("xml result = %+v, want recordElement=product with 3 fields", res)
	}

	// Error paths.
	if w := do(t, s, http.MethodGet, "/v1/introspect/xml", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET xml = %d, want 405", w.Code)
	}
	if w := do(t, s, http.MethodPost, "/v1/introspect/xml", `{bad`); w.Code != http.StatusBadRequest {
		t.Errorf("bad json = %d, want 400", w.Code)
	}
	if w := do(t, s, http.MethodPost, "/v1/introspect/xml", `{}`); w.Code != http.StatusBadRequest {
		t.Errorf("missing path = %d, want 400", w.Code)
	}
	if w := do(t, s, http.MethodPost, "/v1/introspect/xml", `{"path":"/no/such/file.xml"}`); w.Code != http.StatusBadGateway {
		t.Errorf("nonexistent file = %d, want 502", w.Code)
	}
}

func TestIntrospectSourceSchema(t *testing.T) {
	s := newTestServerWithPipeline(t)
	csvPath := filepath.Join(t.TempDir(), "in.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	body := `{"type":"csv","config":{"path":"` + csvPath + `","hasHeader":true,` +
		`"columns":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}}`
	w := do(t, s, http.MethodPost, "/v1/introspect/source-schema", body)
	if w.Code != http.StatusOK {
		t.Fatalf("source-schema = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var res struct {
		Columns []ColumnInfo `json:"columns"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Columns) != 2 {
		t.Errorf("source-schema columns = %+v, want 2", res.Columns)
	}
}

// TestIntrospectValidation covers the no-infra validation/error paths of every
// introspection endpoint (wrong method, malformed body, missing required
// fields, and unknown provider/source type).
func TestIntrospectValidation(t *testing.T) {
	s := newTestServerWithPipeline(t)

	cases := []struct {
		path string
		body string
		want int
	}{
		{"/v1/introspect/tables", `{bad`, http.StatusBadRequest},
		{"/v1/introspect/tables", `{}`, http.StatusBadRequest},                               // missing driver/dsn
		{"/v1/introspect/columns", `{"driver":"postgres","dsn":"x"}`, http.StatusBadRequest}, // missing table
		{"/v1/introspect/objects", `{}`, http.StatusBadRequest},                              // missing provider
		{"/v1/introspect/objects", `{"provider":"nope-xyz","config":{}}`, http.StatusBadGateway},
		{"/v1/introspect/kafka-topics", `{}`, http.StatusBadRequest},  // missing brokers
		{"/v1/introspect/source-schema", `{}`, http.StatusBadRequest}, // missing type
		{"/v1/introspect/source-schema", `{"type":"nope-xyz"}`, http.StatusBadGateway},
	}
	for _, c := range cases {
		if w := do(t, s, http.MethodPost, c.path, c.body); w.Code != c.want {
			t.Errorf("POST %s %q = %d, want %d (body=%s)", c.path, c.body, w.Code, c.want, w.Body.String())
		}
	}

	// Every introspect endpoint rejects a non-POST method with 405.
	for _, p := range []string{
		"/v1/introspect/tables", "/v1/introspect/columns", "/v1/introspect/objects",
		"/v1/introspect/kafka-topics", "/v1/introspect/schema-registry",
		"/v1/introspect/source-schema",
	} {
		if w := do(t, s, http.MethodGet, p, ""); w.Code != http.StatusMethodNotAllowed {
			t.Errorf("GET %s = %d, want 405", p, w.Code)
		}
	}
}
