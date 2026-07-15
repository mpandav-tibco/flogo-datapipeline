// Package prometheus registers a Prometheus metrics backend with the metrics
// registry. Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/metrics/prometheus"
//
// Config options (set in the pipeline definition's observability.options block):
//
//	listenAddr  string  HTTP listen address (default ":9090")
//	path        string  Metrics endpoint path (default "/metrics")
//
// A package-level sync.Once ensures that a single HTTP server and Prometheus
// registry are shared across all DataPipelineAction instances in a process,
// preventing "address already in use" errors when multiple pipeline resources
// share the same Flogo app.
package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

func init() {
	metrics.Register("prometheus", newPrometheusRecorder)
}

// shared holds the single Prometheus registry and HTTP server for the process.
// All promRecorder instances created within a process share the same instruments
// so that a Flogo app with multiple pipeline handlers does not try to bind the
// same port twice and does not create duplicate metric descriptors.
var shared struct {
	once   sync.Once
	reg    *prom.Registry
	server *http.Server

	// instruments — one set per process, keyed by pipeline label at emission time
	rowsRead        *prom.CounterVec
	rowsTransformed *prom.CounterVec
	rowsWritten     *prom.CounterVec
	rowsFailed      *prom.CounterVec
	batchDuration   *prom.HistogramVec
	pipelineRuns    *prom.CounterVec
	stallsTotal     *prom.CounterVec
	throughputGauge *prom.GaugeVec
	checkpoints     *prom.CounterVec

	// Dynamic component instruments (Recorder.Counter/Gauge). Sources and sinks
	// emit their own metrics through these, lazily created on first use and keyed
	// by metric name. The registry is the same isolated one the /metrics server
	// gathers, so component metrics honour the configured backend.
	dynMu       sync.Mutex
	dynCounters map[string]*prom.CounterVec
	dynGauges   map[string]*prom.GaugeVec
}

func initShared(listenAddr, path string) error {
	var initErr error
	shared.once.Do(func() {
		reg := prom.NewRegistry()

		rowsRead := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_rows_read_total",
			Help: "Total rows read from source, partitioned by pipeline and source type.",
		}, []string{"pipeline", "source_type"})

		rowsTransformed := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_rows_transformed_total",
			Help: "Total rows surviving the transform chain, partitioned by pipeline.",
		}, []string{"pipeline"})

		rowsWritten := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_rows_written_total",
			Help: "Total rows successfully written to the sink.",
		}, []string{"pipeline", "sink_type"})

		rowsFailed := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_rows_failed_total",
			Help: "Total rows routed to the dead-letter path (transform errors or explicit DLQ).",
		}, []string{"pipeline"})

		// Histogram bucket boundaries chosen to cover sub-millisecond reads
		// (small CSV batches) up to multi-second writes (large JDBC batches).
		batchDuration := prom.NewHistogramVec(prom.HistogramOpts{
			Name:    "flogo_pipeline_batch_duration_seconds",
			Help:    "Wall-clock latency per batch, by pipeline and stage (read/transform/write). Includes backpressure wait when applicable.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"pipeline", "stage"})

		pipelineRuns := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_runs_total",
			Help: "Total pipeline run completions, partitioned by pipeline and status (completed/partial/failed).",
		}, []string{"pipeline", "status"})

		stallsTotal := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_stalls_total",
			Help: "Total stall events (batches that exceeded the configured stallThreshold), by pipeline and stage.",
		}, []string{"pipeline", "stage"})

		throughputGauge := prom.NewGaugeVec(prom.GaugeOpts{
			Name: "flogo_pipeline_throughput_rows_per_second",
			Help: "Rows-per-second throughput observed in the most recent read batch, by pipeline.",
		}, []string{"pipeline"})

		checkpoints := prom.NewCounterVec(prom.CounterOpts{
			Name: "flogo_pipeline_checkpoints_saved_total",
			Help: "Total checkpoint persist operations, by pipeline.",
		}, []string{"pipeline"})

		for _, c := range []prom.Collector{
			rowsRead, rowsTransformed, rowsWritten, rowsFailed,
			batchDuration, pipelineRuns, stallsTotal, throughputGauge, checkpoints,
		} {
			if err := reg.Register(c); err != nil {
				initErr = fmt.Errorf("prometheus: register collector: %w", err)
				return
			}
		}

		shared.reg = reg
		shared.rowsRead = rowsRead
		shared.rowsTransformed = rowsTransformed
		shared.rowsWritten = rowsWritten
		shared.rowsFailed = rowsFailed
		shared.batchDuration = batchDuration
		shared.pipelineRuns = pipelineRuns
		shared.stallsTotal = stallsTotal
		shared.throughputGauge = throughputGauge
		shared.checkpoints = checkpoints

		mux := http.NewServeMux()
		mux.Handle(path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			EnableOpenMetrics: false,
		}))
		srv := &http.Server{
			Addr:         listenAddr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		shared.server = srv

		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// Log to stderr — we cannot reach the Flogo logger from init.
				fmt.Printf("prometheus metrics server error: %v\n", err)
			}
		}()
	})
	return initErr
}

// promRecorder implements metrics.Recorder using shared Prometheus instruments.
type promRecorder struct{}

func newPrometheusRecorder(cfg map[string]interface{}) (metrics.Recorder, error) {
	listenAddr, _ := cfg["listenAddr"].(string)
	if listenAddr == "" {
		listenAddr = ":9090"
	}
	path, _ := cfg["path"].(string)
	if path == "" {
		path = "/metrics"
	}

	if err := initShared(listenAddr, path); err != nil {
		return nil, err
	}
	return &promRecorder{}, nil
}

func (r *promRecorder) BatchRead(pipeline, srcType string, rows int, dur time.Duration) {
	shared.rowsRead.WithLabelValues(pipeline, srcType).Add(float64(rows))
	shared.batchDuration.WithLabelValues(pipeline, "read").Observe(dur.Seconds())
	if dur.Seconds() > 0 {
		shared.throughputGauge.WithLabelValues(pipeline).Set(float64(rows) / dur.Seconds())
	}
}

func (r *promRecorder) BatchTransformed(pipeline string, rowsIn, rowsOut int, dur time.Duration) {
	shared.rowsTransformed.WithLabelValues(pipeline).Add(float64(rowsOut))
	shared.batchDuration.WithLabelValues(pipeline, "transform").Observe(dur.Seconds())
	_ = rowsIn // rowsIn available for future filter-ratio gauge if needed
}

func (r *promRecorder) BatchWritten(pipeline, sinkType string, rows int, dur time.Duration) {
	shared.rowsWritten.WithLabelValues(pipeline, sinkType).Add(float64(rows))
	shared.batchDuration.WithLabelValues(pipeline, "write").Observe(dur.Seconds())
}

func (r *promRecorder) BatchFailed(pipeline string, rows int) {
	shared.rowsFailed.WithLabelValues(pipeline).Add(float64(rows))
}

func (r *promRecorder) CheckpointSaved(pipeline string) {
	shared.checkpoints.WithLabelValues(pipeline).Inc()
}

func (r *promRecorder) PipelineComplete(pipeline, status string, rowsProcessed, rowsWritten, rowsFailed int64, dur time.Duration) {
	shared.pipelineRuns.WithLabelValues(pipeline, status).Inc()
	// Record end-to-end pipeline duration as a read-stage observation so it
	// appears in the same histogram without adding a dedicated summary.
	shared.batchDuration.WithLabelValues(pipeline, "pipeline").Observe(dur.Seconds())
	_ = rowsProcessed
	_ = rowsWritten
	_ = rowsFailed
}

func (r *promRecorder) Stall(pipeline, stage string, dur time.Duration) {
	shared.stallsTotal.WithLabelValues(pipeline, stage).Inc()
	_ = dur
}

// sortedLabelKeys returns the label names in deterministic order, so a metric's
// label set is stable across calls (Prometheus requires a fixed label schema per
// metric name).
func sortedLabelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *promRecorder) Counter(name, help string, labels map[string]string, delta float64) {
	keys := sortedLabelKeys(labels)
	shared.dynMu.Lock()
	if shared.dynCounters == nil {
		shared.dynCounters = map[string]*prom.CounterVec{}
	}
	cv, ok := shared.dynCounters[name]
	if !ok {
		cv = prom.NewCounterVec(prom.CounterOpts{Name: name, Help: help}, keys)
		if err := shared.reg.Register(cv); err != nil {
			// Already registered by a racing caller: reuse the existing collector.
			if are, isARE := err.(prom.AlreadyRegisteredError); isARE {
				if existing, castOK := are.ExistingCollector.(*prom.CounterVec); castOK {
					cv = existing
				}
			}
		}
		shared.dynCounters[name] = cv
	}
	shared.dynMu.Unlock()
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = labels[k]
	}
	cv.WithLabelValues(vals...).Add(delta)
}

func (r *promRecorder) Gauge(name, help string, labels map[string]string, value float64) {
	keys := sortedLabelKeys(labels)
	shared.dynMu.Lock()
	if shared.dynGauges == nil {
		shared.dynGauges = map[string]*prom.GaugeVec{}
	}
	gv, ok := shared.dynGauges[name]
	if !ok {
		gv = prom.NewGaugeVec(prom.GaugeOpts{Name: name, Help: help}, keys)
		if err := shared.reg.Register(gv); err != nil {
			if are, isARE := err.(prom.AlreadyRegisteredError); isARE {
				if existing, castOK := are.ExistingCollector.(*prom.GaugeVec); castOK {
					gv = existing
				}
			}
		}
		shared.dynGauges[name] = gv
	}
	shared.dynMu.Unlock()
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = labels[k]
	}
	gv.WithLabelValues(vals...).Set(value)
}

// Shutdown gracefully stops the shared HTTP server.
// Because the server is shared, subsequent calls from other recorder instances
// will be no-ops (the server is already shut down).
func (r *promRecorder) Shutdown(ctx context.Context) error {
	if shared.server == nil {
		return nil
	}
	return shared.server.Shutdown(ctx)
}
