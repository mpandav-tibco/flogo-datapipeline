// Package otel registers an OpenTelemetry metrics backend with the metrics
// registry. Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/metrics/otel"
//
// Config options (set in the pipeline definition's observability.options block):
//
//	endpoint    string  OTEL collector gRPC endpoint (default "localhost:4317")
//	insecure    bool    Use plaintext gRPC (default true for local collectors)
//	serviceName string  Value of the service.name resource attribute (default "flogo-datapipeline")
//
// The exporter uses OTLP/gRPC. Swapping to OTLP/HTTP is a one-line change:
// replace otlpmetricgrpc with otlpmetrichttp and update the exporter call.
//
// Metric export is asynchronous; call Recorder.Shutdown(ctx) before process
// exit to ensure the final batch is flushed to the collector.
package otel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	met "github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

func init() {
	met.Register("otel", newOtelRecorder)
}

// otelRecorder implements metrics.Recorder using OpenTelemetry instruments.
type otelRecorder struct {
	provider *sdkmetric.MeterProvider
	meter    metric.Meter

	// Counters
	rowsRead        metric.Int64Counter
	rowsTransformed metric.Int64Counter
	rowsWritten     metric.Int64Counter
	rowsFailed      metric.Int64Counter
	pipelineRuns    metric.Int64Counter
	stallsTotal     metric.Int64Counter
	checkpoints     metric.Int64Counter

	// Histograms (durations recorded as milliseconds for OTEL convention)
	batchDuration metric.Float64Histogram

	// Gauges
	throughput metric.Float64Gauge

	// Dynamic component instruments (Recorder.Counter/Gauge), lazily created on
	// first use and keyed by metric name, so sources/sinks emit their own metrics
	// through the OTEL backend.
	dynMu       sync.Mutex
	dynCounters map[string]metric.Float64Counter
	dynGauges   map[string]metric.Float64Gauge
}

func newOtelRecorder(cfg map[string]interface{}) (met.Recorder, error) {
	endpoint, _ := cfg["endpoint"].(string)
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	useInsecure := true
	if v, ok := cfg["insecure"].(bool); ok {
		useInsecure = v
	}

	serviceName, _ := cfg["serviceName"].(string)
	if serviceName == "" {
		serviceName = "flogo-datapipeline"
	}

	// Build OTLP exporter options.
	exporterOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(endpoint),
	}
	if useInsecure {
		// WithInsecure disables TLS for the gRPC connection. This is appropriate
		// for local collectors (localhost:4317) and internal cluster networks.
		// For production use over public networks, omit WithInsecure and configure
		// TLS via WithTLSCredentials.
		exporterOpts = append(exporterOpts, otlpmetricgrpc.WithInsecure())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exporter, err := otlpmetricgrpc.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: create exporter: %w", err)
	}

	// Resource attributes identify this service in the OTEL backend.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: build resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			// Export every 15 seconds by default; configurable via OTEL_METRIC_EXPORT_INTERVAL.
			sdkmetric.WithInterval(15*time.Second),
		)),
		sdkmetric.WithResource(res),
	)

	meter := provider.Meter("flogo-datapipeline",
		metric.WithInstrumentationVersion("v1"),
	)

	r := &otelRecorder{provider: provider, meter: meter}

	r.rowsRead, err = meter.Int64Counter("flogo.pipeline.rows.read",
		metric.WithDescription("Total rows read from source."),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: rows_read counter: %w", err)
	}

	r.rowsTransformed, err = meter.Int64Counter("flogo.pipeline.rows.transformed",
		metric.WithDescription("Total rows surviving the transform chain."),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: rows_transformed counter: %w", err)
	}

	r.rowsWritten, err = meter.Int64Counter("flogo.pipeline.rows.written",
		metric.WithDescription("Total rows successfully written to the sink."),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: rows_written counter: %w", err)
	}

	r.rowsFailed, err = meter.Int64Counter("flogo.pipeline.rows.failed",
		metric.WithDescription("Total rows routed to the dead-letter path."),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: rows_failed counter: %w", err)
	}

	r.pipelineRuns, err = meter.Int64Counter("flogo.pipeline.runs",
		metric.WithDescription("Total pipeline run completions."),
		metric.WithUnit("{run}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: pipeline_runs counter: %w", err)
	}

	r.stallsTotal, err = meter.Int64Counter("flogo.pipeline.stalls",
		metric.WithDescription("Total stall events (batches that exceeded stallThreshold)."),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: stalls counter: %w", err)
	}

	r.checkpoints, err = meter.Int64Counter("flogo.pipeline.checkpoints.saved",
		metric.WithDescription("Total checkpoint persist operations."),
		metric.WithUnit("{checkpoint}"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: checkpoints counter: %w", err)
	}

	// Histogram boundaries in seconds, matching the Prometheus backend.
	r.batchDuration, err = meter.Float64Histogram("flogo.pipeline.batch.duration",
		metric.WithDescription("Wall-clock latency per batch, by pipeline and stage. Includes backpressure wait."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: batch_duration histogram: %w", err)
	}

	r.throughput, err = meter.Float64Gauge("flogo.pipeline.throughput",
		metric.WithDescription("Rows per second observed in the most recent read batch."),
		metric.WithUnit("{row}/s"),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metrics: throughput gauge: %w", err)
	}

	return r, nil
}

// attrs builds a small attribute set from key/value string pairs.
// OTEL attributes are the equivalent of Prometheus label pairs.
func attrs(kv ...string) metric.MeasurementOption {
	if len(kv)%2 != 0 {
		panic("otel metrics: attrs requires an even number of key/value strings")
	}
	set := make([]attribute.KeyValue, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		set = append(set, attribute.String(kv[i], kv[i+1]))
	}
	return metric.WithAttributeSet(attribute.NewSet(set...))
}

func (r *otelRecorder) BatchRead(pipeline, srcType string, rows int, dur time.Duration) {
	ctx := context.Background()
	r.rowsRead.Add(ctx, int64(rows), attrs("pipeline", pipeline, "source_type", srcType))
	r.batchDuration.Record(ctx, dur.Seconds(), attrs("pipeline", pipeline, "stage", "read"))
	if dur.Seconds() > 0 {
		r.throughput.Record(ctx, float64(rows)/dur.Seconds(), attrs("pipeline", pipeline))
	}
}

func (r *otelRecorder) BatchTransformed(pipeline string, rowsIn, rowsOut int, dur time.Duration) {
	ctx := context.Background()
	r.rowsTransformed.Add(ctx, int64(rowsOut), attrs("pipeline", pipeline))
	r.batchDuration.Record(ctx, dur.Seconds(), attrs("pipeline", pipeline, "stage", "transform"))
	_ = rowsIn
}

func (r *otelRecorder) BatchWritten(pipeline, sinkType string, rows int, dur time.Duration) {
	ctx := context.Background()
	r.rowsWritten.Add(ctx, int64(rows), attrs("pipeline", pipeline, "sink_type", sinkType))
	r.batchDuration.Record(ctx, dur.Seconds(), attrs("pipeline", pipeline, "stage", "write"))
}

func (r *otelRecorder) BatchFailed(pipeline string, rows int) {
	r.rowsFailed.Add(context.Background(), int64(rows), attrs("pipeline", pipeline))
}

func (r *otelRecorder) CheckpointSaved(pipeline string) {
	r.checkpoints.Add(context.Background(), 1, attrs("pipeline", pipeline))
}

func (r *otelRecorder) PipelineComplete(pipeline, status string, rowsProcessed, rowsWritten, rowsFailed int64, dur time.Duration) {
	ctx := context.Background()
	r.pipelineRuns.Add(ctx, 1, attrs("pipeline", pipeline, "status", status))
	r.batchDuration.Record(ctx, dur.Seconds(), attrs("pipeline", pipeline, "stage", "pipeline"))
	_ = rowsProcessed
	_ = rowsWritten
	_ = rowsFailed
}

func (r *otelRecorder) Stall(pipeline, stage string, dur time.Duration) {
	r.stallsTotal.Add(context.Background(), 1, attrs("pipeline", pipeline, "stage", stage))
	_ = dur
}

// labelAttrs builds an OTEL measurement option from a label map.
func labelAttrs(labels map[string]string) metric.MeasurementOption {
	set := make([]attribute.KeyValue, 0, len(labels))
	for k, v := range labels {
		set = append(set, attribute.String(k, v))
	}
	return metric.WithAttributeSet(attribute.NewSet(set...))
}

func (r *otelRecorder) Counter(name, help string, labels map[string]string, delta float64) {
	r.dynMu.Lock()
	if r.dynCounters == nil {
		r.dynCounters = map[string]metric.Float64Counter{}
	}
	c, ok := r.dynCounters[name]
	if !ok {
		var err error
		c, err = r.meter.Float64Counter(name, metric.WithDescription(help))
		if err != nil {
			r.dynMu.Unlock()
			return
		}
		r.dynCounters[name] = c
	}
	r.dynMu.Unlock()
	c.Add(context.Background(), delta, labelAttrs(labels))
}

func (r *otelRecorder) Gauge(name, help string, labels map[string]string, value float64) {
	r.dynMu.Lock()
	if r.dynGauges == nil {
		r.dynGauges = map[string]metric.Float64Gauge{}
	}
	g, ok := r.dynGauges[name]
	if !ok {
		var err error
		g, err = r.meter.Float64Gauge(name, metric.WithDescription(help))
		if err != nil {
			r.dynMu.Unlock()
			return
		}
		r.dynGauges[name] = g
	}
	r.dynMu.Unlock()
	g.Record(context.Background(), value, labelAttrs(labels))
}

// Shutdown flushes the OTEL metric exporter and shuts down the provider.
// Must be called before process exit to ensure the last metric batch is sent.
func (r *otelRecorder) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}
