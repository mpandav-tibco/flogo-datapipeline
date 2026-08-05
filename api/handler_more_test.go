package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	// Register the source/sink used by the runnable test pipeline so RunDirect,
	// Preview and status paths execute a real (tiny) pipeline end to end.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/null"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
)

// newRunnableServer seeds a real csv→null pipeline the run/preview/status
// handlers can execute.
func newRunnableServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "in.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n2,bob\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := &pipeline.Factory{}
	defJSON := fmt.Sprintf(`{
		"source":{"type":"csv","path":%q,
			"columns":[{"name":"id","type":"int"},{"name":"name","type":"string"}],
			"options":{"hasHeader":true}},
		"sink":{"type":"null"}}`, csvPath)
	def := mustDef(t, defJSON)
	if _, err := f.Replace("res://pipeline:run-demo", def); err != nil {
		t.Fatalf("seed runnable: %v", err)
	}
	return NewServer(f, ":0"), "run-demo"
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	return w
}

func TestHealth(t *testing.T) {
	s := newTestServerWithPipeline(t)
	w := do(t, s, http.MethodGet, "/v1/health", "")
	if w.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", w.Code)
	}
	var hr HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &hr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hr.Status != "ok" || hr.Pipelines != 1 {
		t.Errorf("health = %+v, want ok/1", hr)
	}

	// Wrong method → 405.
	if w := do(t, s, http.MethodPost, "/v1/health", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /v1/health = %d, want 405", w.Code)
	}
}

func TestListPipelines(t *testing.T) {
	s := newTestServerWithPipeline(t)
	w := do(t, s, http.MethodGet, "/v1/pipelines", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var lr PipelineListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &lr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(lr.Pipelines) != 1 {
		t.Errorf("list returned %d pipelines, want 1", len(lr.Pipelines))
	}

	if w := do(t, s, http.MethodDelete, "/v1/pipelines", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /v1/pipelines = %d, want 405", w.Code)
	}
}

func TestRunPipelineSync(t *testing.T) {
	s, ref := newRunnableServer(t)
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/run", `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("run status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "completed" {
		t.Errorf("run status field = %v, want completed", resp["status"])
	}

	// Status endpoint now reflects the run in its ring buffer.
	sw := do(t, s, http.MethodGet, "/v1/pipelines/"+ref+"/status", "")
	if sw.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", sw.Code)
	}
	var st PipelineStatusResponse
	if err := json.Unmarshal(sw.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(st.Runs) != 1 {
		t.Errorf("status runs = %d, want 1", len(st.Runs))
	}
}

func TestRunPipelineAsync(t *testing.T) {
	s, ref := newRunnableServer(t)
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/run?async=true", `{"runId":"async-1"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("async run status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	// Wait for the background goroutine to finish and record its result.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if last, ok := s.ringFor(ref).latest(); ok && last.Status != "running" {
			if last.Status != "completed" {
				t.Errorf("async run final status = %s, want completed", last.Status)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("async run did not complete within timeout")
}

func TestRunPipelineErrors(t *testing.T) {
	s, ref := newRunnableServer(t)
	// Unknown ref → 404.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/nope/run", `{}`); w.Code != http.StatusNotFound {
		t.Errorf("run unknown ref = %d, want 404", w.Code)
	}
	// Malformed JSON body → 400.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/run", `{bad`); w.Code != http.StatusBadRequest {
		t.Errorf("run bad body = %d, want 400", w.Code)
	}
}

func TestPipelineDefinitionAndStatusNotFound(t *testing.T) {
	s := newTestServerWithPipeline(t)
	// definition of the seeded pipeline → 200.
	w := do(t, s, http.MethodGet, "/v1/pipelines/demo/definition", "")
	if w.Code != http.StatusOK {
		t.Fatalf("definition status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// unknown pipeline → 404 for both status and definition.
	if w := do(t, s, http.MethodGet, "/v1/pipelines/nope/definition", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown definition = %d, want 404", w.Code)
	}
	if w := do(t, s, http.MethodGet, "/v1/pipelines/nope/status", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown status = %d, want 404", w.Code)
	}
}

func TestCancelRunNotFound(t *testing.T) {
	s := newTestServerWithPipeline(t)
	// Unknown runId → 404.
	if w := do(t, s, http.MethodDelete, "/v1/pipelines/demo/run/no-such-run", ""); w.Code != http.StatusNotFound {
		t.Errorf("cancel unknown run = %d, want 404", w.Code)
	}
}

func TestRoutingEdgeCases(t *testing.T) {
	s := newTestServerWithPipeline(t)
	// No ref in path → 404.
	if w := do(t, s, http.MethodGet, "/v1/pipelines/", ""); w.Code != http.StatusNotFound {
		t.Errorf("no-ref = %d, want 404", w.Code)
	}
	// Unknown sub-route → 404.
	if w := do(t, s, http.MethodGet, "/v1/pipelines/demo/bogus", ""); w.Code != http.StatusNotFound {
		t.Errorf("bogus route = %d, want 404", w.Code)
	}
	// Wrong method on /run (GET instead of POST) → 404 (no matching case).
	if w := do(t, s, http.MethodGet, "/v1/pipelines/demo/run", ""); w.Code != http.StatusNotFound {
		t.Errorf("GET /run = %d, want 404", w.Code)
	}
}

func TestSampleInputAndTestStageBadBody(t *testing.T) {
	s := newTestServerWithPipeline(t)
	// sampleinput with invalid JSON → 400.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/demo/sampleinput", `{bad`); w.Code != http.StatusBadRequest {
		t.Errorf("sampleinput bad body = %d, want 400", w.Code)
	}
	// teststage with invalid JSON → 400.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/demo/teststage", `{bad`); w.Code != http.StatusBadRequest {
		t.Errorf("teststage bad body = %d, want 400", w.Code)
	}
	// teststage with no rows → 400.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/demo/teststage", `{"stage":0,"rows":[]}`); w.Code != http.StatusBadRequest {
		t.Errorf("teststage no rows = %d, want 400", w.Code)
	}
	// unknown ref sampleinput → 404.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/nope/sampleinput", `{}`); w.Code != http.StatusNotFound {
		t.Errorf("sampleinput unknown ref = %d, want 404", w.Code)
	}
}

func TestPreviewRunnable(t *testing.T) {
	s, ref := newRunnableServer(t)
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/preview?limit=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("preview status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}
