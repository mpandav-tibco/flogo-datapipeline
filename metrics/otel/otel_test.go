package otel

import (
	"context"
	"testing"
	"time"

	met "github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

// shutdownQuietly shuts a recorder down. A pre-cancelled context makes the final
// export return immediately instead of blocking on the (absent) collector; with
// no collector the call may error, which is acceptable — we only exercise the
// Shutdown path and assert it does not hang.
func shutdownQuietly(t *testing.T, r met.Recorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Logf("Shutdown returned (non-fatal, no collector): %v", err)
	}
}

func exerciseRecorder(t *testing.T, r met.Recorder) {
	t.Helper()
	r.BatchRead("p", "csv", 100, 10*time.Millisecond)
	r.BatchRead("p", "csv", 0, 0) // zero-duration skips the throughput branch
	r.BatchTransformed("p", 100, 90, 5*time.Millisecond)
	r.BatchWritten("p", "jdbc", 90, 8*time.Millisecond)
	r.BatchFailed("p", 10)
	r.CheckpointSaved("p")
	r.PipelineComplete("p", "completed", 100, 90, 10, time.Second)
	r.Stall("p", "write", 40*time.Millisecond)
	// Dynamic instruments: first call creates, second hits the cached branch.
	r.Counter("custom.count", "help", map[string]string{"k": "v"}, 3)
	r.Counter("custom.count", "help", map[string]string{"k": "v"}, 2)
	r.Gauge("custom.gauge", "help", map[string]string{"k": "v"}, 1.5)
	r.Gauge("custom.gauge", "help", map[string]string{"k": "v"}, 2.5)
	shutdownQuietly(t, r)
}

func TestNewOtelRecorderDefaults(t *testing.T) {
	r, err := newOtelRecorder(map[string]interface{}{})
	if err != nil {
		t.Fatalf("newOtelRecorder(defaults): %v", err)
	}
	exerciseRecorder(t, r)
}

func TestNewOtelRecorderCustomConfig(t *testing.T) {
	r, err := newOtelRecorder(map[string]interface{}{
		"endpoint":    "localhost:4317",
		"insecure":    false, // exercise the TLS (non-insecure) option branch
		"serviceName": "unit-test-svc",
	})
	if err != nil {
		t.Fatalf("newOtelRecorder(custom): %v", err)
	}
	exerciseRecorder(t, r)
}

func TestOtelRegisteredWithMetricsFactory(t *testing.T) {
	// init() registered the backend under "otel"; the metrics factory must build it.
	r, err := met.New("otel", map[string]interface{}{"insecure": true})
	if err != nil {
		t.Fatalf("metrics.New(otel): %v", err)
	}
	shutdownQuietly(t, r)
}

func TestAttrsOddCountPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on odd key/value count")
		}
	}()
	//lint:ignore SA5012 deliberately odd to exercise the panic-on-malformed-input path
	_ = attrs("only-one-key")
}
