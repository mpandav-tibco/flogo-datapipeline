package router

import (
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// Compile-time checks that the router satisfies both contracts.
var (
	_ transform.Transform = (*routerTransform)(nil)
	_ transform.SinkOwner = (*routerTransform)(nil)
)

// captureSink is an in-memory test sink that records the rows written to it,
// keyed by an "id" in its config so each route/test can inspect its own stream.
type captureSink struct{ id string }

var (
	capMu     sync.Mutex
	capDests  = map[string]*[][]interface{}{}
	capClosed = map[string]bool{}
)

func init() {
	sink.Register("captureTest", func(cfg map[string]interface{}) (sink.Sink, error) {
		id, _ := cfg["id"].(string)
		capMu.Lock()
		if _, ok := capDests[id]; !ok {
			capDests[id] = &[][]interface{}{}
		}
		capMu.Unlock()
		return &captureSink{id: id}, nil
	})
}

func (c *captureSink) WriteBatch(b *record.RecordBatch) error {
	capMu.Lock()
	defer capMu.Unlock()
	d := capDests[c.id]
	for _, row := range b.Rows {
		cp := make([]interface{}, len(row)) // copy: the batch is pooled/recycled after WriteBatch
		copy(cp, row)
		*d = append(*d, cp)
	}
	return nil
}
func (c *captureSink) Flush() error { return nil }
func (c *captureSink) Close() error {
	capMu.Lock()
	capClosed[c.id] = true
	capMu.Unlock()
	return nil
}

func capturedRows(id string) [][]interface{} {
	capMu.Lock()
	defer capMu.Unlock()
	if d, ok := capDests[id]; ok {
		return *d
	}
	return nil
}

func capRoute(name, when, id string) map[string]interface{} {
	return map[string]interface{}{
		"name": name, "when": when,
		"sinkType": "captureTest", "sinkConfig": map[string]interface{}{"id": id},
	}
}

var testCols = []record.Column{
	{Name: "amount", Type: record.TypeInt},
	{Name: "region", Type: record.TypeString},
}

func batch(rows ...[]interface{}) *record.RecordBatch {
	return &record.RecordBatch{Columns: testCols, Rows: rows}
}

// TestRouterFirstMatchAndPassthrough verifies ordered first-match routing and
// that rows matching no route pass downstream (onUnmatched defaults to "pass").
func TestRouterFirstMatchAndPassthrough(t *testing.T) {
	tf, err := newRouter(map[string]interface{}{
		"routes": []interface{}{
			capRoute("high", "amount > 1000", "t1-high"),
			capRoute("eu", "region = 'EU'", "t1-eu"),
		},
	})
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	defer tf.Close()

	// {2000,EU} matches BOTH route0 (amount>1000) and route1 (region=EU) — first
	// match (high) must win. {500,EU} → eu. {100,US} → passthrough. {5000,US} → high.
	out, err := tf.Apply(batch(
		[]interface{}{int64(2000), "EU"},
		[]interface{}{int64(500), "EU"},
		[]interface{}{int64(100), "US"},
		[]interface{}{int64(5000), "US"},
	))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := len(capturedRows("t1-high")); got != 2 {
		t.Fatalf("high route got %d rows, want 2 (first-match precedence)", got)
	}
	if got := len(capturedRows("t1-eu")); got != 1 {
		t.Fatalf("eu route got %d rows, want 1", got)
	}
	if out == nil || len(out.Rows) != 1 {
		t.Fatalf("passthrough got %v, want 1 row", out)
	}
	if amt, _ := out.Rows[0][0].(int64); amt != 100 {
		t.Fatalf("passthrough row = %v, want amount=100", out.Rows[0])
	}
	// first-match: the {2000,EU} row went high, not eu.
	if r := capturedRows("t1-eu"); len(r) == 1 {
		if amt, _ := r[0][0].(int64); amt != 500 {
			t.Fatalf("eu route row = %v, want amount=500 (2000/EU should have gone high)", r[0])
		}
	}
}

// TestRouterDefaultSink verifies unmatched rows go to the default sink and are
// NOT passed downstream when a default is configured.
func TestRouterDefaultSink(t *testing.T) {
	tf, err := newRouter(map[string]interface{}{
		"routes": []interface{}{
			capRoute("high", "amount > 1000", "t2-high"),
		},
		"defaultSinkType":   "captureTest",
		"defaultSinkConfig": map[string]interface{}{"id": "t2-default"},
	})
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	defer tf.Close()

	out, err := tf.Apply(batch(
		[]interface{}{int64(2000), "US"},
		[]interface{}{int64(100), "US"},
	))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Fatalf("passthrough = %v, want nil (default sink consumes unmatched)", out)
	}
	if got := len(capturedRows("t2-high")); got != 1 {
		t.Fatalf("high got %d rows, want 1", got)
	}
	if got := len(capturedRows("t2-default")); got != 1 {
		t.Fatalf("default got %d rows, want 1", got)
	}
}

// TestRouterOnUnmatchedDrop verifies unmatched rows are dropped (no passthrough)
// when onUnmatched is "drop" and no default sink is set.
func TestRouterOnUnmatchedDrop(t *testing.T) {
	tf, err := newRouter(map[string]interface{}{
		"routes":      []interface{}{capRoute("high", "amount > 1000", "t3-high")},
		"onUnmatched": "drop",
	})
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	defer tf.Close()

	out, err := tf.Apply(batch(
		[]interface{}{int64(2000), "US"},
		[]interface{}{int64(100), "US"},
	))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Fatalf("passthrough = %v, want nil (drop)", out)
	}
	if got := len(capturedRows("t3-high")); got != 1 {
		t.Fatalf("high got %d rows, want 1", got)
	}
}

// TestRouterCloseFlushesAndClosesSinks verifies Close closes every route sink
// and the default sink.
func TestRouterCloseFlushesAndClosesSinks(t *testing.T) {
	tf, err := newRouter(map[string]interface{}{
		"routes": []interface{}{
			capRoute("a", "amount > 1000", "t4-a"),
			capRoute("b", "region = 'EU'", "t4-b"),
		},
		"defaultSinkType":   "captureTest",
		"defaultSinkConfig": map[string]interface{}{"id": "t4-default"},
	})
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	if err := tf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, id := range []string{"t4-a", "t4-b", "t4-default"} {
		capMu.Lock()
		closed := capClosed[id]
		capMu.Unlock()
		if !closed {
			t.Fatalf("sink %q was not closed", id)
		}
	}
}

// TestRouterSinkOwner verifies the router advertises itself as sink-owning so the
// engine keeps it on the single-threaded path.
func TestRouterSinkOwner(t *testing.T) {
	tf, err := newRouter(map[string]interface{}{
		"routes": []interface{}{capRoute("a", "amount > 1000", "t5-a")},
	})
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	defer tf.Close()
	so, ok := tf.(transform.SinkOwner)
	if !ok || !so.OwnsExternalSink() {
		t.Fatalf("router must implement transform.SinkOwner and own its sink")
	}
}

// TestRouterConfigErrors covers config validation at construction.
func TestRouterConfigErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
	}{
		{"no routes", map[string]interface{}{}},
		{"empty routes", map[string]interface{}{"routes": []interface{}{}}},
		{"missing when", map[string]interface{}{"routes": []interface{}{
			map[string]interface{}{"sinkType": "captureTest", "sinkConfig": map[string]interface{}{"id": "e1"}}}}},
		{"missing sinkType", map[string]interface{}{"routes": []interface{}{
			map[string]interface{}{"when": "amount > 1"}}}},
		{"bad onUnmatched", map[string]interface{}{
			"routes":      []interface{}{capRoute("a", "amount > 1", "e2")},
			"onUnmatched": "sideways"}},
		{"unknown sink type", map[string]interface{}{"routes": []interface{}{
			map[string]interface{}{"when": "amount > 1", "sinkType": "no-such-sink"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newRouter(tc.cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
