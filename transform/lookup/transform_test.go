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

func inBatch(keyField string, keys ...interface{}) *record.RecordBatch {
	rows := make([][]interface{}, len(keys))
	for i, k := range keys {
		rows[i] = []interface{}{k}
	}
	return &record.RecordBatch{
		Columns: []record.Column{{Name: keyField, Type: record.TypeString}},
		Rows:    rows,
	}
}

func TestLookup_ConfigValidation(t *testing.T) {
	if _, err := transform.New("lookup", map[string]interface{}{"url": "http://x"}); err == nil {
		t.Error("expected error when keyField missing")
	}
	if _, err := transform.New("lookup", map[string]interface{}{"keyField": "id"}); err == nil {
		t.Error("expected error when url missing")
	}
}

func TestLookup_EnrichAndCache(t *testing.T) {
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		fmt.Fprint(w, `{"name":"Alice","tier":"gold"}`)
	}))
	defer srv.Close()

	tr, err := transform.New("lookup", map[string]interface{}{
		"keyField":     "id",
		"url":          srv.URL + "/c/{id}",
		"outputFields": []interface{}{"name", "tier"},
		"concurrency":  4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close()

	// Two distinct keys + a repeat of the first (should hit cache).
	out, err := tr.Apply(inBatch("id", "1", "2", "1"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out.Columns) != 3 {
		t.Fatalf("columns = %+v", out.Columns)
	}
	if out.Value(0, "name") != "Alice" || out.Value(0, "tier") != "gold" {
		t.Errorf("row0 not enriched: %v", out.Rows[0])
	}
	// Only 2 unique keys => at most 2 server calls (cache/singleflight dedup).
	if c := atomic.LoadInt64(&calls); c > 2 {
		t.Errorf("server called %d times, want <=2 (cache miss)", c)
	}
}

func TestLookup_404FillsNull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	tr, _ := transform.New("lookup", map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"},
	})
	defer tr.Close()
	out, err := tr.Apply(inBatch("id", "missing"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Value(0, "name") != nil {
		t.Errorf("404 should yield nil enrichment, got %v", out.Value(0, "name"))
	}
}

func TestLookup_OnErrorFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	tr, _ := transform.New("lookup", map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "onError": "fail",
	})
	defer tr.Close()
	if _, err := tr.Apply(inBatch("id", "boom")); err == nil {
		t.Error("onError=fail should return an error on HTTP 500")
	}
}

func TestLookup_OnErrorSkip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			fmt.Fprint(w, `{"name":"Bob"}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	tr, _ := transform.New("lookup", map[string]interface{}{
		"keyField": "id", "url": srv.URL + "/{id}", "outputFields": []interface{}{"name"}, "onError": "skip",
	})
	defer tr.Close()
	out, err := tr.Apply(inBatch("id", "ok", "bad"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 1 {
		t.Errorf("skip should drop the failing row, got %d rows", out.Len())
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	c := newLRUCache(2)
	c.Put("a", map[string]interface{}{"v": 1})
	c.Put("b", map[string]interface{}{"v": 2})
	_, _ = c.Get("a") // touch a → b is now LRU
	c.Put("c", map[string]interface{}{"v": 3})
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("a should still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("c should be present")
	}
	c.Clear()
	if _, ok := c.Get("a"); ok {
		t.Error("cache should be empty after Clear")
	}
}
