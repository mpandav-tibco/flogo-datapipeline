package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRestSink_HonoursRetryAfter(t *testing.T) {
	var calls int32
	var first time.Time
	var gap time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		gap = time.Since(first)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	snk, err := newRestSink(map[string]interface{}{"url": srv.URL, "retryBackoffMs": 1, "maxRetries": 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := snk.WriteBatch(batch([]interface{}{int64(1), "a"})); err != nil {
		t.Fatal(err)
	}
	if gap < 900*time.Millisecond {
		t.Fatalf("retried after %v; Retry-After asked for 1s", gap)
	}
}

func TestRestSink_CloseInterruptsRetryWait(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	snk, err := newRestSink(map[string]interface{}{"url": srv.URL, "maxRetries": 3})
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, func() { _ = snk.Close() })
	start := time.Now()
	err = snk.WriteBatch(batch([]interface{}{int64(1), "a"}))
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("want closed error, got %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Close did not interrupt the retry wait (%v)", d)
	}
}

func TestRestSink_ConcurrentPerRow(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]bool{}
	var inFlight, peak int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		mu.Lock()
		seen[r.URL.Path] = true
		mu.Unlock()
		if r.URL.Path == "/x/7" || r.URL.Path == "/x/3" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	snk, err := newRestSink(map[string]interface{}{"url": srv.URL + "/x/{id}", "concurrency": 4.0, "maxRetries": 0})
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]interface{}
	for i := 0; i < 3; i++ {
		rows = append(rows, []interface{}{int64(i), "ok"})
	}
	if err := snk.WriteBatch(batch(rows...)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !seen["/x/"+string(rune('0'+i))] {
			t.Fatalf("row %d not delivered: %v", i, seen)
		}
	}

	// Rows 3 and 7 fail: the lowest failing index must be reported.
	rows = nil
	for i := 0; i < 12; i++ {
		rows = append(rows, []interface{}{int64(i), "x"})
	}
	err = snk.WriteBatch(batch(rows...))
	if err == nil || !strings.Contains(err.Error(), "row 3") {
		t.Fatalf("want error for row 3, got %v", err)
	}
	if p := atomic.LoadInt32(&peak); p > 4 || p < 2 {
		t.Fatalf("peak in-flight=%d; want within (1,4]", p)
	}
}
