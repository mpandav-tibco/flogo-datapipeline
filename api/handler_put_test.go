package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
)

func mustDef(t *testing.T, jsonStr string) *pipeline.Definition {
	t.Helper()
	def := &pipeline.Definition{}
	if err := json.Unmarshal([]byte(jsonStr), def); err != nil {
		t.Fatalf("unmarshal def: %v", err)
	}
	def.Defaults()
	if err := def.Validate(); err != nil {
		t.Fatalf("validate def: %v", err)
	}
	return def
}

func newTestServerWithPipeline(t *testing.T) *Server {
	t.Helper()
	f := &pipeline.Factory{}
	def := mustDef(t, `{"source":{"type":"csv","options":{"path":"/tmp/a.csv"}},"sink":{"type":"null"}}`)
	if _, err := f.Replace("res://pipeline:demo", def); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}
	return NewServer(f, ":0")
}

func doPUT(t *testing.T, s *Server, ref, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/v1/pipelines/"+ref, bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	return w
}

// TestPutPipeline_ReplaceExisting swaps an existing pipeline's definition and
// asserts subsequent lookups see the new source/sink and the response echoes it.
func TestPutPipeline_ReplaceExisting(t *testing.T) {
	s := newTestServerWithPipeline(t)
	body := `{"source":{"type":"jsonlines","options":{"path":"/tmp/b.ndjson"}},"sink":{"type":"jdbc"}}`
	w := doPUT(t, s, "demo", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	act := s.findAction("demo")
	if act == nil {
		t.Fatal("pipeline demo disappeared after PUT")
	}
	if act.SourceType() != "jsonlines" || act.SinkType() != "jdbc" {
		t.Errorf("after PUT source/sink = %s/%s, want jsonlines/jdbc", act.SourceType(), act.SinkType())
	}

	var summary map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	src, _ := summary["source"].(map[string]interface{})
	if src["type"] != "jsonlines" {
		t.Errorf("summary source.type = %v, want jsonlines", src["type"])
	}
}

// TestPutPipeline_CreateNew creates a not-yet-registered pipeline via PUT and
// expects 201 Created.
func TestPutPipeline_CreateNew(t *testing.T) {
	s := newTestServerWithPipeline(t)
	body := `{"source":{"type":"csv","options":{"path":"/tmp/c.csv"}},"sink":{"type":"null"}}`
	w := doPUT(t, s, "fresh", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body.String())
	}
	if s.findAction("fresh") == nil {
		t.Error("new pipeline not registered after PUT")
	}
}

// TestPutPipeline_InvalidJSON rejects a malformed body and leaves the existing
// pipeline untouched.
func TestPutPipeline_InvalidJSON(t *testing.T) {
	s := newTestServerWithPipeline(t)
	w := doPUT(t, s, "demo", `{not-json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if act := s.findAction("demo"); act == nil || act.SourceType() != "csv" {
		t.Error("existing pipeline mutated by an invalid PUT")
	}
}

// TestPutPipeline_InvalidDefinition rejects a well-formed but invalid definition
// (missing sink.type) and leaves the existing pipeline untouched.
func TestPutPipeline_InvalidDefinition(t *testing.T) {
	s := newTestServerWithPipeline(t)
	w := doPUT(t, s, "demo", `{"source":{"type":"csv"}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if act := s.findAction("demo"); act == nil || act.SinkType() != "null" {
		t.Error("existing pipeline mutated by an invalid PUT")
	}
}

// TestPutPipeline_EmptyBody rejects a request with no body.
func TestPutPipeline_EmptyBody(t *testing.T) {
	s := newTestServerWithPipeline(t)
	req := httptest.NewRequest(http.MethodPut, "/v1/pipelines/demo", nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}
