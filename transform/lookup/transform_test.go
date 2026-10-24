package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func mkBatch(keyField string, keys ...interface{}) *record.RecordBatch {
	rows := make([][]interface{}, len(keys))
	for i, k := range keys {
		rows[i] = []interface{}{k}
	}
	return &record.RecordBatch{Columns: []record.Column{{Name: keyField, Type: record.TypeString}}, Rows: rows}
}

func newLookup(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := newLookupTransform(cfg)
	if err != nil {
		t.Fatalf("newLookupTransform: %v", err)
	}
	return tr
}

func TestLookup_ConfigValidation(t *testing.T) {
	if _, err := newLookupTransform(map[string]interface{}{"url": "http://x"}); err == nil {
		t.Error("expected error when keyField missing")
	}
	if _, err := newLookupTransform(map[string]interface{}{"keyField": "id"}); err == nil {
		t.Error("expected error when url missing")
	}
	tr := newLookup(t, map[string]interface{}{"keyField": "id", "url": "http://x/{id}"})
	if tr.Name() != "lookup" {
		t.Errorf("Name = %q", tr.Name())
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestLookup_EnrichSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"name":"Ada","tier":"gold"}`)
	}))
	defer srv.Close()

	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/c/{id}",
		"outputFields": []interface{}{"name", "tier"},
	})
	out, err := tr.Apply(mkBatch("id", "42"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Value(0, "name") != "Ada" || out.Value(0, "tier") != "gold" {
		t.Errorf("enrichment wrong: %v", out.Rows[0])
	}
}

func TestLookup_404IsNull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
	})
	out, err := tr.Apply(mkBatch("id", "x"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 || out.Value(0, "name") != nil {
		t.Errorf("404 should yield nil enrichment, got %v", out.Rows)
	}
}

func TestLookup_OnErrorNull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "onError": "null",
	})
	out, err := tr.Apply(mkBatch("id", "x"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Errorf("onError=null should keep the row, got %d", out.Len())
	}
}

func TestLookup_OnErrorSkip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "onError": "skip",
	})
	out, err := tr.Apply(mkBatch("id", "x"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("onError=skip should drop the row, got %d rows", out.Len())
	}
}

func TestLookup_OnErrorFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "onError": "fail",
	})
	if _, err := tr.Apply(mkBatch("id", "x")); err == nil {
		t.Error("onError=fail should return an error")
	}
}

func TestLookup_Caches(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		fmt.Fprintf(w, `{"name":"same"}`)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
		"concurrency": 1,
	})
	// Same key three times across two batches — server should be hit once.
	_, _ = tr.Apply(mkBatch("id", "k", "k"))
	_, _ = tr.Apply(mkBatch("id", "k"))
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Errorf("server hits = %d, want 1 (cached)", got)
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	c := newCache(2)
	c.Add("a", cacheEntry{data: map[string]interface{}{"v": 1}})
	c.Add("b", cacheEntry{data: map[string]interface{}{"v": 2}})
	_, _ = c.Get("a")                                            // a becomes MRU
	c.Add("c", cacheEntry{data: map[string]interface{}{"v": 3}}) // evicts b (LRU)
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be present")
	}
	c.Purge()
	if _, ok := c.Get("a"); ok {
		t.Error("Purge should empty the cache")
	}
}

func TestLookup_CachesNotFoundByDefault(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		http.NotFound(w, r)
	}))
	defer srv.Close()
	for _, tc := range []struct {
		cfg  map[string]interface{}
		want int64
	}{
		{map[string]interface{}{}, 1},
		{map[string]interface{}{"cacheNotFound": false}, 3},
	} {
		atomic.StoreInt64(&hits, 0)
		cfg := map[string]interface{}{"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "concurrency": 1}
		for k, v := range tc.cfg {
			cfg[k] = v
		}
		tr := newLookup(t, cfg)
		_, _ = tr.Apply(mkBatch("id", "gone", "gone"))
		_, _ = tr.Apply(mkBatch("id", "gone"))
		if got := atomic.LoadInt64(&hits); got != tc.want {
			t.Errorf("cfg %v: server hits = %d, want %d", tc.cfg, got, tc.want)
		}
	}
}

func TestLookup_JSONConfigNumbersApply(t *testing.T) {
	var cfg map[string]interface{}
	if err := json.Unmarshal([]byte(`{"keyField":"id","url":"http://x/{id}","concurrency":3,"cacheSize":7,"timeoutMs":1234}`), &cfg); err != nil {
		t.Fatal(err)
	}
	l := newLookup(t, cfg).(*lookupTransform)
	if l.concurrency != 3 || l.client.Timeout != 1234*time.Millisecond {
		t.Fatalf("JSON numbers ignored: concurrency=%d timeout=%v", l.concurrency, l.client.Timeout)
	}
}

func TestLookup_MaxResponseBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"name":"%s"}`, strings.Repeat("x", 100))
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
		"maxResponseBytes": 50, "onError": "fail",
	})
	if _, err := tr.Apply(mkBatch("id", "k")); err == nil || !strings.Contains(err.Error(), "maxResponseBytes") {
		t.Fatalf("want maxResponseBytes error, got %v", err)
	}
}

func TestLookup_BoundContextCancelsInFlight(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
		"timeoutMs": 30000, "onError": "fail",
	})
	ctx, cancel := context.WithCancel(context.Background())
	tr.(transform.ContextBinder).BindContext(ctx)
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := tr.Apply(mkBatch("id", "k"))
	if err == nil {
		t.Fatal("want error after cancellation")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("cancellation took %v; request was not bound to the run context", d)
	}
}

// With the default onError "null", a cancelled run must fail the batch rather
// than emit rows whose enrichment was aborted (null fields).
func TestLookup_CancelledRunFailsBatchEvenWithOnErrorNull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	tr.(transform.ContextBinder).BindContext(ctx)
	time.AfterFunc(50*time.Millisecond, cancel)
	out, err := tr.Apply(mkBatch("id", "a", "b"))
	if err == nil || out != nil {
		t.Fatalf("want batch failure on cancellation, got out=%v err=%v", out, err)
	}
}

// Not-found answers expire after notFoundTtlMs, so a key that appears
// upstream is picked up again; found answers expire only with cacheTtlMs.
func TestLookup_NotFoundTTL(t *testing.T) {
	var hits int64
	var exists atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		if !exists.Load() {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"name":"now-here"}`)
	}))
	defer srv.Close()
	tr := newLookup(t, map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
		"notFoundTtlMs": 100.0,
	})
	out, _ := tr.Apply(mkBatch("id", "k", "k"))
	if out.Rows[0][1] != nil || atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("first lookup: row=%v hits=%d", out.Rows[0], hits)
	}
	exists.Store(true)
	out, _ = tr.Apply(mkBatch("id", "k")) // still within TTL: cached miss
	if out.Rows[0][1] != nil || atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("within TTL: row=%v hits=%d", out.Rows[0], hits)
	}
	time.Sleep(150 * time.Millisecond)
	out, _ = tr.Apply(mkBatch("id", "k", "k"))
	if out.Rows[0][1] != "now-here" || atomic.LoadInt64(&hits) != 2 {
		t.Fatalf("after TTL: row=%v hits=%d; want refreshed value, 2 hits", out.Rows[0], hits)
	}
}
