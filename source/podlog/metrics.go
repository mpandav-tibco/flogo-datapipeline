package podlog

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Reconciliation metrics — the customer's core requirement ("prove nothing was
// lost"). These podlog-specific operational counters complement the generic
// pipeline metrics (flogo_pipeline_rows_read/written/failed_total) that already
// express the read == written + dlq invariant. Together they let an operator
// alert on drift per node/namespace instead of discovering loss by accident.
//
// The collectors are registered on the Prometheus DEFAULT registry via promauto
// at package init. They cost nothing when no metrics backend is configured (the
// counters simply accumulate in memory). When observability.metrics=prometheus
// is enabled, the shared /metrics server gathers the default registry too (see
// metrics/prometheus), so these appear on the same endpoint automatically — the
// source stays decoupled from any specific backend.
var (
	// podlog_lines_read_total{node,namespace} — logical records emitted by the
	// source after CRI reassembly + multiline stitching. This is the left-hand
	// side of the reconciliation invariant read == written + dlq.
	metricLinesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "podlog_lines_read_total",
		Help: "Logical log records emitted by the podlog source (after CRI reassembly and multiline stitching), by node and namespace.",
	}, []string{"node", "namespace"})

	// podlog_bytes_committed_total{node} — bytes durably delivered, advanced only
	// when the position registry is persisted after the sink acknowledges.
	metricBytesCommitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "podlog_bytes_committed_total",
		Help: "File bytes durably delivered (position-registry advance after sink ack), by node.",
	}, []string{"node"})

	// podlog_files_open{node} — number of CRI log files currently being tailed.
	metricFilesOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "podlog_files_open",
		Help: "CRI log files currently being tailed, by node.",
	}, []string{"node"})

	// podlog_rotations_total{node} — detected log rotations/truncations (a fresh
	// file at the same path, or size regression), by node.
	metricRotations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "podlog_rotations_total",
		Help: "Detected CRI log rotations/truncations, by node.",
	}, []string{"node"})
)

// reconCounters holds process-local atomic totals so the source can log a final
// reconciliation summary even when Prometheus is disabled — the operator always
// gets an auditable "read=N committedBytes=M rotations=R" line at shutdown.
type reconCounters struct {
	linesRead      atomic.Int64
	bytesCommitted atomic.Int64
	rotations      atomic.Int64
}

func (r *reconCounters) addLinesRead(node, namespace string, n int) {
	if n <= 0 {
		return
	}
	r.linesRead.Add(int64(n))
	metricLinesRead.WithLabelValues(node, namespace).Add(float64(n))
}

func (r *reconCounters) addBytesCommitted(node string, n int64) {
	if n <= 0 {
		return
	}
	r.bytesCommitted.Add(n)
	metricBytesCommitted.WithLabelValues(node).Add(float64(n))
}

func (r *reconCounters) addRotation(node string) {
	r.rotations.Add(1)
	metricRotations.WithLabelValues(node).Inc()
}

func setFilesOpen(node string, n int) {
	metricFilesOpen.WithLabelValues(node).Set(float64(n))
}
