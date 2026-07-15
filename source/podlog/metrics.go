package podlog

import (
	"sync/atomic"

	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

// Reconciliation metrics — the customer's core requirement ("prove nothing was
// lost"). These podlog-specific operational counters complement the generic
// pipeline metrics (flogo_pipeline_rows_read/written/failed_total) that already
// express the read == written + dlq invariant. Together they let an operator
// alert on drift per node/namespace instead of discovering loss by accident.
//
// Metrics are emitted through the pipeline's pluggable metrics.Recorder (injected
// by the action via RecorderAware.SetRecorder), so they honour the configured
// backend — prometheus, otel, or none — exactly like the built-in pipeline
// metrics. The source never imports a concrete backend. When no recorder is set
// (e.g. unit tests, or metrics=none), the counters still accumulate in memory
// for the always-on shutdown reconciliation summary.
const (
	metricLinesRead      = "podlog_lines_read_total"
	metricBytesCommitted = "podlog_bytes_committed_total"
	metricFilesOpen      = "podlog_files_open"
	metricRotations      = "podlog_rotations_total"
)

// reconCounters holds process-local atomic totals so the source can log a final
// reconciliation summary even when metrics are disabled — the operator always
// gets an auditable "read=N committedBytes=M rotations=R" line at shutdown. It
// also forwards each change to the active Recorder (nil-safe) so the same
// numbers appear in prometheus/otel when a backend is configured.
type reconCounters struct {
	rec metrics.Recorder // nil until the action injects it; nil = summary-only

	linesRead      atomic.Int64
	bytesCommitted atomic.Int64
	rotations      atomic.Int64
}

func (r *reconCounters) setRecorder(rec metrics.Recorder) { r.rec = rec }

func (r *reconCounters) addLinesRead(node, namespace string, n int) {
	if n <= 0 {
		return
	}
	r.linesRead.Add(int64(n))
	if r.rec != nil {
		r.rec.Counter(metricLinesRead, "Logical log records emitted by the podlog source, by node and namespace.",
			map[string]string{"node": node, "namespace": namespace}, float64(n))
	}
}

func (r *reconCounters) addBytesCommitted(node string, n int64) {
	if n <= 0 {
		return
	}
	r.bytesCommitted.Add(n)
	if r.rec != nil {
		r.rec.Counter(metricBytesCommitted, "File bytes durably delivered (position-registry advance after sink ack), by node.",
			map[string]string{"node": node}, float64(n))
	}
}

func (r *reconCounters) addRotation(node string) {
	r.rotations.Add(1)
	if r.rec != nil {
		r.rec.Counter(metricRotations, "Detected CRI log rotations/truncations, by node.",
			map[string]string{"node": node}, 1)
	}
}

func (r *reconCounters) setFilesOpen(node string, n int) {
	if r.rec != nil {
		r.rec.Gauge(metricFilesOpen, "CRI log files currently being tailed, by node.",
			map[string]string{"node": node}, float64(n))
	}
}
