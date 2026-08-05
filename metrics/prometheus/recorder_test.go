package prometheus

import (
	"context"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

// TestPrometheusRecorderLifecycle exercises the full recorder: construction
// (which binds the shared HTTP server on an ephemeral port), every batch/counter
// method, dynamic instruments (including the label-mismatch skip), and Shutdown.
func TestPrometheusRecorderLifecycle(t *testing.T) {
	r, err := newPrometheusRecorder(map[string]interface{}{"listenAddr": ":0", "path": "/metrics"})
	if err != nil {
		t.Fatalf("newPrometheusRecorder: %v", err)
	}

	r.BatchRead("p", "csv", 100, 10*time.Millisecond)
	r.BatchRead("p", "csv", 0, 0) // zero-duration skips the throughput gauge branch
	r.BatchTransformed("p", 100, 90, 5*time.Millisecond)
	r.BatchWritten("p", "jdbc", 90, 8*time.Millisecond)
	r.BatchFailed("p", 10)
	r.CheckpointSaved("p")
	r.PipelineComplete("p", "completed", 100, 90, 10, time.Second)
	r.Stall("p", "write", 40*time.Millisecond)

	// Dynamic instruments: create, reuse (cached), and mismatch-skip branches.
	r.Counter("prom_unit_ctr", "help", map[string]string{"a": "1"}, 3)
	r.Counter("prom_unit_ctr", "help", map[string]string{"a": "1"}, 2)
	r.Counter("prom_unit_ctr", "help", map[string]string{"a": "1", "b": "2"}, 1) // mismatch → skip
	r.Gauge("prom_unit_gauge", "help", map[string]string{"a": "1"}, 1.5)
	r.Gauge("prom_unit_gauge", "help", map[string]string{"a": "1"}, 2.5)
	r.Gauge("prom_unit_gauge", "help", map[string]string{"x": "9"}, 9) // mismatch → skip

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// TestPrometheusFactoryDefaults builds the recorder through the metrics registry
// with default options; the shared server is already initialised so this reuses
// it (the sync.Once path).
func TestPrometheusFactoryDefaults(t *testing.T) {
	r, err := metrics.New("prometheus", map[string]interface{}{})
	if err != nil {
		t.Fatalf("metrics.New(prometheus): %v", err)
	}
	r.BatchFailed("p2", 1)
	_ = r.Shutdown(context.Background())
}
