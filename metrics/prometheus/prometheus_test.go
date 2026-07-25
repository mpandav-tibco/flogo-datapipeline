package prometheus

import (
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
)

// TestDynamicMetricLabelMismatchDoesNotPanic verifies that emitting the same
// dynamic metric name with a different label-key set is skipped rather than
// panicking on a Prometheus cardinality mismatch (a metrics call must never
// crash the pipeline).
func TestDynamicMetricLabelMismatchDoesNotPanic(t *testing.T) {
	// White-box init: use a fresh registry directly, bypassing the HTTP server.
	shared.reg = prom.NewRegistry()
	shared.dynCounters = nil
	shared.dynGauges = nil

	r := &promRecorder{}

	// First calls establish the label schema {pipeline}.
	r.Gauge("test_metric", "help", map[string]string{"pipeline": "p1"}, 1)
	r.Counter("test_ctr", "help", map[string]string{"pipeline": "p1"}, 1)

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("mismatched label keys panicked instead of being skipped: %v", rec)
		}
	}()

	// A later call with an extra label key must be skipped, not panic.
	r.Gauge("test_metric", "help", map[string]string{"pipeline": "p1", "extra": "x"}, 2)
	r.Counter("test_ctr", "help", map[string]string{"pipeline": "p1", "extra": "x"}, 2)

	// A later call with the original schema (different value) still records.
	r.Gauge("test_metric", "help", map[string]string{"pipeline": "p2"}, 3)
	r.Counter("test_ctr", "help", map[string]string{"pipeline": "p2"}, 4)
}

func TestEqualKeys(t *testing.T) {
	if !equalKeys([]string{"a", "b"}, []string{"a", "b"}) {
		t.Error("identical slices should be equal")
	}
	if equalKeys([]string{"a"}, []string{"a", "b"}) {
		t.Error("different lengths should not be equal")
	}
	if equalKeys([]string{"a", "b"}, []string{"a", "c"}) {
		t.Error("different values should not be equal")
	}
	if !equalKeys(nil, nil) {
		t.Error("nil/empty should be equal")
	}
}
