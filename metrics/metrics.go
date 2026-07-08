// Package metrics defines the Recorder interface that the pipeline engine calls
// at each batch boundary to emit operational telemetry.
//
// Two concrete backends are provided as sub-packages:
//   - metrics/prometheus — Prometheus counters/histograms on an HTTP /metrics endpoint
//   - metrics/otel       — OpenTelemetry metrics pushed to any OTEL collector via gRPC
//
// A zero-allocation NoopRecorder is used when no backend is configured, so
// there is zero overhead for users who opt out of observability.
//
// Usage (in a pipeline definition JSON):
//
//	"observability": {
//	  "metrics": "prometheus",
//	  "options": { "listenAddr": ":9090", "path": "/metrics" },
//	  "stallSeconds": 30
//	}
//
// Import the desired backend sub-package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/metrics/prometheus"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/metrics/otel"
package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Recorder is the metrics emission interface called by the pipeline engine.
// All methods must be safe to call concurrently from multiple goroutines.
type Recorder interface {
	// BatchRead is called after each source ReadBatch completes.
	// rows is the number of rows returned; dur is the wall-clock latency of
	// ReadBatch (includes backpressure wait time when a future buffered pipeline
	// is in use — document accordingly).
	BatchRead(pipelineRef, sourceType string, rows int, dur time.Duration)

	// BatchTransformed is called after the transform pipeline completes a batch.
	// rowsIn is the batch size entering the transform chain; rowsOut is the
	// number of rows surviving all transform stages (filter may reduce this).
	BatchTransformed(pipelineRef string, rowsIn, rowsOut int, dur time.Duration)

	// BatchWritten is called after each successful sink WriteBatch.
	BatchWritten(pipelineRef, sinkType string, rows int, dur time.Duration)

	// BatchFailed is called when rows are routed to the dead-letter path
	// (transform error or explicit DLQ routing).
	BatchFailed(pipelineRef string, rows int)

	// CheckpointSaved is called after each checkpoint persist.
	CheckpointSaved(pipelineRef string)

	// PipelineComplete is called at the end of each pipeline run.
	PipelineComplete(pipelineRef, status string, rowsProcessed, rowsWritten, rowsFailed int64, dur time.Duration)

	// Stall is called when a single batch takes longer than the configured
	// stallThreshold to process. stage names the batch stage that stalled
	// ("read", "transform", or "write").
	Stall(pipelineRef, stage string, dur time.Duration)

	// Shutdown flushes any buffered telemetry and releases backend resources.
	// Called from the pipeline action's Close/lifecycle hook on process exit.
	// Must be idempotent and safe to call after the first call returns.
	Shutdown(ctx context.Context) error
}

// NoopRecorder discards all metrics with zero allocations.
// Used when no metrics backend is configured.
type NoopRecorder struct{}

func (NoopRecorder) BatchRead(_, _ string, _ int, _ time.Duration)                          {}
func (NoopRecorder) BatchTransformed(_ string, _, _ int, _ time.Duration)                   {}
func (NoopRecorder) BatchWritten(_, _ string, _ int, _ time.Duration)                       {}
func (NoopRecorder) BatchFailed(_ string, _ int)                                            {}
func (NoopRecorder) CheckpointSaved(_ string)                                               {}
func (NoopRecorder) PipelineComplete(_, _ string, _, _, _ int64, _ time.Duration)           {}
func (NoopRecorder) Stall(_, _ string, _ time.Duration)                                     {}
func (NoopRecorder) Shutdown(_ context.Context) error                                       { return nil }

// Factory constructs a Recorder from a generic config map.
// The map is the "options" block from the pipeline ObservabilityDef.
type Factory func(cfg map[string]interface{}) (Recorder, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a backend name with its Factory.
// Called from each metrics sub-package's init() function.
// Panics on duplicate registration (programming error).
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("metrics: backend %q already registered", name))
	}
	registry[name] = f
}

// New constructs a Recorder for the named backend using the provided config.
// Returns an error if the backend is unknown or the config is invalid.
// Returns NoopRecorder{} when name is "" or "none".
func New(name string, cfg map[string]interface{}) (Recorder, error) {
	if name == "" || name == "none" {
		return NoopRecorder{}, nil
	}
	mu.RLock()
	f, ok := registry[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("metrics: unknown backend %q — import the backend sub-package (e.g. _ \"github.com/mpandav-tibco/flogo-datapipeline/metrics/prometheus\")", name)
	}
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	return f(cfg)
}

// Backends returns the names of all registered metrics backends.
func Backends() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}
