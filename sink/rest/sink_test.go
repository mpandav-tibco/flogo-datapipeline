package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

var testCols = []record.Column{
	{Name: "id", Type: record.TypeInt},
	{Name: "name", Type: record.TypeString},
}

func batch(rows ...[]interface{}) *record.RecordBatch {
	return &record.RecordBatch{Columns: testCols, Rows: rows}
}

// capturedRequest records one inbound HTTP request's method, path, and
// decoded JSON body for assertions.
type capturedRequest struct {
	method string
	path   string
	body   interface{}
}

func newCaptureServer(t *testing.T, status int) (*httptest.Server, *[]capturedRequest, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var reqs []capturedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body interface{}
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		mu.Lock()
		reqs = append(reqs, capturedRequest{method: r.Method, path: r.URL.Path, body: body})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	return srv, &reqs, &mu
}

func TestRestSink_PerRow_OneRequestPerRow(t *testing.T) {
	srv, reqs, mu := newCaptureServer(t, http.StatusCreated)
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{"url": srv.URL + "/customers", "method": "POST"})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch(
		[]interface{}{int64(1), "alice"},
		[]interface{}{int64(2), "bob"},
	)); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 2 {
		t.Fatalf("got %d requests, want 2 (one per row)", len(*reqs))
	}
	for _, r := range *reqs {
		if r.method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.method)
		}
	}
	body0 := (*reqs)[0].body.(map[string]interface{})
	if body0["name"] != "alice" {
		t.Errorf("request 0 body = %+v, want name=alice", body0)
	}
}

func TestRestSink_PerRow_URLFieldMacro(t *testing.T) {
	srv, reqs, mu := newCaptureServer(t, http.StatusOK)
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{
		"url": srv.URL + "/customers/{id}", "method": "PUT",
	})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch([]interface{}{int64(42), "carol"})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 1 {
		t.Fatalf("got %d requests, want 1", len(*reqs))
	}
	if (*reqs)[0].path != "/customers/42" {
		t.Errorf("path = %q, want /customers/42 ({id} substituted)", (*reqs)[0].path)
	}
}

func TestRestSink_BatchMode_OneRequestForWholeBatch(t *testing.T) {
	srv, reqs, mu := newCaptureServer(t, http.StatusOK)
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{
		"url": srv.URL + "/bulk", "mode": "batch",
	})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch(
		[]interface{}{int64(1), "alice"},
		[]interface{}{int64(2), "bob"},
		[]interface{}{int64(3), "carol"},
	)); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 1 {
		t.Fatalf("got %d requests, want 1 (whole batch in one request)", len(*reqs))
	}
	arr, ok := (*reqs)[0].body.([]interface{})
	if !ok || len(arr) != 3 {
		t.Fatalf("batch body = %+v, want a JSON array of 3 objects", (*reqs)[0].body)
	}
}

func TestRestSink_RetriesOn500ThenSucceeds(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{
		"url": srv.URL, "maxRetries": float64(2), "retryBackoffMs": float64(1),
	})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch([]interface{}{int64(1), "alice"})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (1 failure + 1 success)", calls)
	}
}

func TestRestSink_FirstRowFailureReturnsErrorWithoutSendingLaterRows(t *testing.T) {
	var paths []string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusBadRequest) // non-retriable, non-2xx
	}))
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{"url": srv.URL + "/x", "maxRetries": float64(0)})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	err = s.WriteBatch(batch(
		[]interface{}{int64(1), "alice"},
		[]interface{}{int64(2), "bob"},
	))
	if err == nil {
		t.Fatal("expected an error from a 400 response")
	}
	if !strings.Contains(err.Error(), "row 0") {
		t.Errorf("error = %v, want it to name row 0", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Fatalf("got %d requests, want exactly 1 (stops at the first failure within this call)", len(paths))
	}
}

func TestRestSink_AuthBearerFromEnv(t *testing.T) {
	t.Setenv("REST_SINK_TEST_TOKEN", "s3cr3t")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{
		"url": srv.URL, "authType": "bearer", "bearerTokenEnv": "REST_SINK_TEST_TOKEN",
	})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch([]interface{}{int64(1), "alice"})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if gotAuth != "Bearer s3cr3t" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer s3cr3t")
	}
}

func TestRestSink_MissingBearerEnvFailsAtConstruction(t *testing.T) {
	_ = os.Unsetenv("REST_SINK_TEST_MISSING_TOKEN")
	if _, err := sink.New("rest", map[string]interface{}{
		"url": "http://example.invalid", "authType": "bearer", "bearerTokenEnv": "REST_SINK_TEST_MISSING_TOKEN",
	}); err == nil {
		t.Fatal("expected an error when the bearer token env var is unset")
	}
}

func TestRestSink_RequiresURL(t *testing.T) {
	if _, err := sink.New("rest", map[string]interface{}{}); err == nil {
		t.Fatal("expected an error when 'url' is missing")
	}
}

func TestRestSink_RejectsUnknownMode(t *testing.T) {
	if _, err := sink.New("rest", map[string]interface{}{"url": "http://example.invalid", "mode": "bogus"}); err == nil {
		t.Fatal("expected an error for an unknown 'mode'")
	}
}

func TestRestSink_SuccessStatusCodesOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // 200 — NOT in the configured success set below
	}))
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{
		"url": srv.URL, "successStatusCodes": []interface{}{float64(201)}, "maxRetries": float64(0),
	})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(batch([]interface{}{int64(1), "alice"})); err == nil {
		t.Fatal("expected an error: 200 is not in the configured successStatusCodes ([201])")
	}
}

func TestRestSink_EmptyOrNilBatchIsANoOp(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s, err := sink.New("rest", map[string]interface{}{"url": srv.URL})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	defer s.Close()

	if err := s.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	if err := s.WriteBatch(&record.RecordBatch{Columns: testCols}); err != nil {
		t.Fatalf("WriteBatch(empty): %v", err)
	}
	if called {
		t.Error("no HTTP request should have been made for an empty/nil batch")
	}
}
