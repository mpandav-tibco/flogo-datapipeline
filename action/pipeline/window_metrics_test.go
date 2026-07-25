package pipeline

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	wintransform "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

// capturingRecorder records Gauge calls; every other Recorder method is a no-op
// inherited from NoopRecorder.
type capturingRecorder struct {
	metrics.NoopRecorder
	gauges map[string]float64
}

func (c *capturingRecorder) Gauge(name, _ string, _ map[string]string, value float64) {
	if c.gauges == nil {
		c.gauges = map[string]float64{}
	}
	c.gauges[name] = value
}

// fakeWindowOp implements wintransform.WindowedOperator with canned stats.
type fakeWindowOp struct{ st wintransform.Stats }

func (f fakeWindowOp) Flush() (*record.RecordBatch, error) { return nil, nil }
func (f fakeWindowOp) LateCount() int64                    { return f.st.LateDropped }
func (f fakeWindowOp) Stats() wintransform.Stats           { return f.st }
func (f fakeWindowOp) Snapshot() ([]byte, error)           { return nil, nil }
func (f fakeWindowOp) Restore([]byte) error                { return nil }

func TestEmitWindowMetrics(t *testing.T) {
	rec := &capturingRecorder{}
	a := &DataPipelineAction{ref: "p1", recorder: rec}
	a.emitWindowMetrics(fakeWindowOp{st: wintransform.Stats{
		WindowsEmitted:  7,
		LateDropped:     3,
		StateEvicted:    2,
		OpenWindows:     5,
		WatermarkMillis: 1784887260000,
		SpillFailures:   1,
	}})

	want := map[string]float64{
		"flogo_pipeline_window_open":                 5,
		"flogo_pipeline_window_emitted_total":        7,
		"flogo_pipeline_window_late_dropped_total":   3,
		"flogo_pipeline_window_state_evicted_total":  2,
		"flogo_pipeline_window_watermark_ms":         1784887260000,
		"flogo_pipeline_window_spill_failures_total": 1,
	}
	for name, v := range want {
		got, ok := rec.gauges[name]
		if !ok {
			t.Errorf("missing gauge %q", name)
			continue
		}
		if got != v {
			t.Errorf("gauge %q = %v, want %v", name, got, v)
		}
	}
	if len(rec.gauges) != len(want) {
		t.Errorf("emitted %d gauges, want %d: %v", len(rec.gauges), len(want), rec.gauges)
	}

	// A nil operator must be a safe no-op (no panic, no extra gauges).
	a.emitWindowMetrics(nil)
}
