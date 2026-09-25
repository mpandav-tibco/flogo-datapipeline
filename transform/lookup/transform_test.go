package lookup

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

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
	c.Add("a", map[string]interface{}{"v": 1})
	c.Add("b", map[string]interface{}{"v": 2})
	_, _ = c.Get("a")                          // a becomes MRU
	c.Add("c", map[string]interface{}{"v": 3}) // evicts b (LRU)
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
