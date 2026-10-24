package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRestSource_MaxResponseBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `[{"a":"%s"}]`, strings.Repeat("x", 2048))
	}))
	defer srv.Close()
	src, err := newRestSource(map[string]interface{}{"url": srv.URL, "maxResponseBytes": 1024.0})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.ReadBatch(10); err == nil || !strings.Contains(err.Error(), "maxResponseBytes") {
		t.Fatalf("want maxResponseBytes error, got %v", err)
	}
}

func TestRestSource_CloseInterruptsRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	src, err := newRestSource(map[string]interface{}{"url": srv.URL, "maxRetries": 3.0})
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, func() { _ = src.Close() })
	start := time.Now()
	if _, err := src.ReadBatch(10); err == nil {
		t.Fatal("want error after Close")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Close did not interrupt the retry wait (%v)", d)
	}
}
