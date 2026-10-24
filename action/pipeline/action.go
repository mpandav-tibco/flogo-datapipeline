package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"time"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/app/resource"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/engine"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/audit"
	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/lineage"
	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	dqtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	hwmtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
	wintransform "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

func init() {
	_ = resource.RegisterLoader(ResType, defLoader{})
	_ = action.Register(&DataPipelineAction{}, &Factory{})
}

// DataPipelineAction implements action.AsyncAction.
// Each Run() executes one complete pipeline pass:
//
//	source → transform chain → sink
//
// with checkpointing at configurable row intervals.
// Run() returns immediately; all I/O happens in a dedicated goroutine that
// calls handler.HandleResult on completion.
//
// recorder is never nil — it is set to metrics.NoopRecorder{} when no metrics
// backend is configured, so all recording calls are zero-cost no-ops.
type DataPipelineAction struct {
	def      *Definition
	ref      string
	ioMd     *metadata.IOMetadata
	recorder metrics.Recorder // never nil; NoopRecorder when unconfigured
	auditor  audit.Auditor    // never nil; audit.Noop when unconfigured
	lineage  lineage.Emitter  // never nil; lineage.Noop when unconfigured
}

// dlqWriteSink is the interface subset of dlqsink.dlqSink used by the action.
// It extends sink.Sink with the error-per-row write path.
type dlqWriteSink interface {
	sink.Sink
	// WriteBatchErrors writes a batch of failed rows, associating each row with
	// the error message at the corresponding index of the errors slice.
	WriteBatchErrors(batch *record.RecordBatch, errors []string) error
	// InjectContext sets the pipeline reference and run ID for error envelopes.
	InjectContext(pipelineRef, runID string)
}

var _ action.AsyncAction = (*DataPipelineAction)(nil)

func (a *DataPipelineAction) Metadata() *action.Metadata       { return actionMd }
func (a *DataPipelineAction) IOMetadata() *metadata.IOMetadata { return a.ioMd }

// Ref returns the pipeline resource reference (e.g. "res://pipeline:daily-tx").
// Used by the REST API server to populate PipelineInfo fields.
func (a *DataPipelineAction) Ref() string { return a.ref }

// SourceType returns the source format declared in the pipeline definition.
func (a *DataPipelineAction) SourceType() string { return a.def.Source.Type }

// SinkType returns the sink type declared in the pipeline definition.
func (a *DataPipelineAction) SinkType() string { return a.def.Sink.Type }

// shortName strips the "res://pipeline:" prefix from a pipeline ref so the
// structured log field is a clean name (e.g. "filter-and-map", not the full URI).
func shortName(ref string) string {
	return strings.TrimPrefix(ref, "res://pipeline:")
}

// newRunLog builds a per-run logger following the OOTB Flogo Integration flow
// logging convention (see project-flogo/flow instance.TaskInst, which attaches
// activity.name / flow.name / flow.id / app.name / app.version /
// deployment.environment as structured fields).
//
// The pipeline identity (pipeline.name + pipeline.runId — the analogues of the
// flow's named logger + flow.id) is always bound so every run is correlatable.
// The richer app/deployment metadata is attached only when contextual logging
// is enabled (FLOGO_LOG_CTX=true / log.CtxLoggingEnabled()), exactly as a Flogo
// flow only decorates its activity logger with that metadata when ctx logging
// is on — keeping the default console output clean.
func newRunLog(pipelineRef, runID string) corelog.Logger {
	fields := []corelog.Field{
		corelog.FieldString("pipeline.name", shortName(pipelineRef)),
		corelog.FieldString("pipeline.runId", runID),
	}
	if corelog.CtxLoggingEnabled() {
		fields = append(fields,
			corelog.FieldString("app.name", engine.GetAppName()),
			corelog.FieldString("app.version", engine.GetAppVersion()),
		)
		if env := engine.GetEnvName(); env != "" {
			fields = append(fields, corelog.FieldString("deployment.environment", env))
		}
	}
	return corelog.ChildLoggerWithFields(flog, fields...)
}

// finalizeStatus applies the standard runErr/RowsFailed status rules, then
// the contract's optional MaxDurationMs SLA check (see ContractDef). The SLA
// check only reclassifies an otherwise-"completed" run — an already
// failed/partial run isn't further changed. Shared by RunDirect and Run's
// goroutine so this decision (and the SLA check specifically) has exactly one
// implementation instead of being duplicated and drifting.
func (a *DataPipelineAction) finalizeStatus(runLog corelog.Logger, out *Output, runErr error) {
	switch {
	case runErr != nil && errors.Is(runErr, context.Canceled):
		// Stopped on request (shutdown / API cancel): not a failure. No
		// checkpoint was taken, so a restart resumes from the last durable one.
		out.Status = "cancelled"
		out.Error = runErr.Error()
	case runErr != nil:
		out.Status = "failed"
		out.Error = runErr.Error()
	case out.RowsFailed > 0:
		out.Status = "partial"
	default:
		out.Status = "completed"
	}
	if out.Status == "completed" && a.def.Contract != nil && a.def.Contract.MaxDurationMs > 0 && out.DurationMs > a.def.Contract.MaxDurationMs {
		out.Status = "partial"
		runLog.Warnf("contract SLA violation: run took %dms, exceeding maxDurationMs=%d — the run's data is otherwise complete and correct, only marked \"partial\" to surface the SLA breach",
			out.DurationMs, a.def.Contract.MaxDurationMs)
	}
	if out.Status == "completed" && out.Metadata != nil {
		if rec, ok := out.Metadata["reconciliation"].(map[string]interface{}); ok {
			if mismatched, _ := rec["mismatched"].(bool); mismatched {
				if onMismatch, _ := rec["onMismatch"].(string); onMismatch == "fail" {
					out.Status = "partial"
					runLog.Warnf("contract reconciliation mismatch: sourceCount=%v sinkCount=%v driftPct=%v — the run's data is otherwise complete and correct, only marked \"partial\" to surface the mismatch",
						rec["sourceCount"], rec["sinkCount"], rec["driftPct"])
				}
			}
		}
	}
}

// runReconciliation runs a data contract's optional post-run reconciliation
// check (see ContractDef.Reconciliation): independently querying the source's
// and sink's own row counts (via the optional source.Reconciler /
// sink.Reconciler interfaces, reusing their still-open connections) and
// comparing them against each other — entirely independent of the pipeline's
// internal RowsProcessed/RowsWritten counters. Must be called BEFORE the sink
// closes its connection. A source/sink type that doesn't implement the
// interface is skipped with a warning (documented scope limit — only jdbc
// implements this today), never failing the run outright.
func (a *DataPipelineAction) runReconciliation(runLog corelog.Logger, def *Definition, src source.Source, snk sink.Sink, out *Output) {
	if def.Contract == nil || def.Contract.Reconciliation == nil || !def.Contract.Reconciliation.Enabled {
		return
	}
	rc := def.Contract.Reconciliation

	srcRec, ok := src.(source.Reconciler)
	if !ok {
		runLog.Warnf("contract.reconciliation is enabled but source type %q does not support it (only jdbc does today) — skipping", def.Source.Type)
		return
	}
	snkRec, ok := snk.(sink.Reconciler)
	if !ok {
		runLog.Warnf("contract.reconciliation is enabled but sink type %q does not support it (only jdbc does today) — skipping", def.Sink.Type)
		return
	}

	sourceCount, err := srcRec.ReconciliationCount(rc.SourceCountQuery)
	if err != nil {
		runLog.Warnf("contract reconciliation: source count query failed (skipping check): %v", err)
		return
	}
	sinkCount, err := snkRec.ReconciliationCount(rc.SinkCountQuery)
	if err != nil {
		runLog.Warnf("contract reconciliation: sink count query failed (skipping check): %v", err)
		return
	}

	var driftPct float64
	if sourceCount > 0 {
		driftPct = math.Abs(float64(sourceCount-sinkCount)) / float64(sourceCount)
	} else if sinkCount != 0 {
		driftPct = 1.0 // source is empty but sink is not — maximal drift
	}
	mismatched := driftPct > rc.MaxDriftPct

	if out.Metadata == nil {
		out.Metadata = make(map[string]interface{})
	}
	out.Metadata["reconciliation"] = map[string]interface{}{
		"sourceCount": sourceCount,
		"sinkCount":   sinkCount,
		"driftPct":    driftPct,
		"mismatched":  mismatched,
		"onMismatch":  rc.OnMismatch,
	}
	if mismatched {
		runLog.Warnf("contract reconciliation mismatch: sourceCount=%d sinkCount=%d driftPct=%.4f (maxDriftPct=%.4f)",
			sourceCount, sinkCount, driftPct, rc.MaxDriftPct)
	} else {
		runLog.Infof("contract reconciliation OK: sourceCount=%d sinkCount=%d driftPct=%.4f (maxDriftPct=%.4f)",
			sourceCount, sinkCount, driftPct, rc.MaxDriftPct)
	}
}

// RunDirect executes the pipeline synchronously in the calling goroutine and
// returns its Output. It is used by the REST API server for synchronous run
// requests (async=false) and by the SDK inline Run() path.
// ctx should carry a cancel function so the caller can abort the run.
func (a *DataPipelineAction) RunDirect(ctx context.Context, in *Input) (*Output, error) {
	runID := in.RunID
	if runID == "" {
		runID = newRunID()
	}

	ctx, runDone := activeRuns.start(ctx)
	defer runDone()

	runLog := newRunLog(a.ref, runID)
	runLog.Infof("pipeline run started — source=%s sink=%s", a.def.Source.Type, a.def.Sink.Type)
	a.auditRunStart(runID)
	a.lineageStart(runID)

	start := time.Now()
	out, runErr := a.executePipeline(ctx, in, runLog, runID)
	out.DurationMs = time.Since(start).Milliseconds()

	a.finalizeStatus(runLog, out, runErr)
	if runErr != nil && out.Status != "cancelled" {
		runLog.Errorf("pipeline run failed after %dms: %v", out.DurationMs, runErr)
	}
	runLog.Infof("pipeline run %s — processed=%d written=%d failed=%d dropped=%d deleted=%d duration=%dms",
		out.Status, out.RowsProcessed, out.RowsWritten, out.RowsFailed, out.RowsDropped, out.RowsDeleted, out.DurationMs)
	a.auditRunFinish(runID, out)
	a.lineageFinish(runID, out)

	return out, runErr
}

// Run spawns the pipeline goroutine and returns immediately.
func (a *DataPipelineAction) Run(
	ctx context.Context,
	input map[string]interface{},
	handler action.ResultHandler,
) error {
	in := &Input{}
	if err := in.FromMap(input); err != nil {
		return fmt.Errorf("pipeline: bad input: %w", err)
	}

	// Determine runID before spawning so the structured log fields are set from
	// the very first log line — matching the flogo-agent pattern where session ID
	// is bound to the logger before "chat request received" fires.
	runID := in.RunID
	if runID == "" {
		runID = newRunID()
	}

	ctx, runDone := activeRuns.start(ctx)
	go func() {
		defer runDone()
		runLog := newRunLog(a.ref, runID)
		runLog.Infof("pipeline run started — source=%s sink=%s", a.def.Source.Type, a.def.Sink.Type)
		a.auditRunStart(runID)
		a.lineageStart(runID)

		start := time.Now()
		out, runErr := a.runWithRestart(ctx, in, runLog, runID)
		totalDur := time.Since(start)
		out.DurationMs = totalDur.Milliseconds()

		a.finalizeStatus(runLog, out, runErr)
		if runErr != nil && out.Status != "cancelled" {
			runLog.Errorf("pipeline run failed after %dms: %v", out.DurationMs, runErr)
		}
		runLog.Infof("pipeline run %s — processed=%d written=%d failed=%d dropped=%d deleted=%d duration=%dms",
			out.Status, out.RowsProcessed, out.RowsWritten, out.RowsFailed, out.RowsDropped, out.RowsDeleted, out.DurationMs)
		a.auditRunFinish(runID, out)
		a.lineageFinish(runID, out)

		// Emit end-of-run metric. Called regardless of success/failure so the
		// "failed" status label is captured in the pipeline_runs counter.
		a.recorder.PipelineComplete(
			a.ref, out.Status,
			out.RowsProcessed, out.RowsWritten, out.RowsFailed,
			totalDur,
		)

		handler.HandleResult(out.ToMap(), runErr)
		handler.Done()
	}()
	return nil
}

// runWithRestart executes the pipeline and, when restart.onFailure is set,
// executes it again after a backoff whenever it fails — each attempt is a
// fresh plan that resumes from the last checkpoint — until it succeeds or the
// run is stopped. The backoff resets after an attempt that ran for a while.
func (a *DataPipelineAction) runWithRestart(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (*Output, error) {
	policy := a.def.Restart
	attempt := 0
	for {
		t0 := time.Now()
		out, err := a.executePipeline(ctx, in, runLog, runID)
		if err == nil || !policy.OnFailure || ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return out, err
		}
		if time.Since(t0) > 5*time.Minute {
			attempt = 0
		}
		attempt++
		delay := policy.backoff(attempt)
		runLog.Errorf("pipeline run attempt %d failed (processed=%d written=%d): %v — restarting from the last checkpoint in %s (restart.onFailure)",
			attempt, out.RowsProcessed, out.RowsWritten, err, delay)
		a.recorder.PipelineComplete(a.ref, "failed", out.RowsProcessed, out.RowsWritten, out.RowsFailed, time.Since(t0))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return out, err
		case <-drainSignal(ctx):
			return out, err
		}
	}
}

// stallThreshold returns the stall detection duration for this pipeline.
// Returns 0 when stall detection is disabled (StallSeconds <= 0).
func (a *DataPipelineAction) stallThreshold() time.Duration {
	s := a.def.Observability.StallSeconds
	if s <= 0 {
		return 0
	}
	return time.Duration(s) * time.Second
}

// checkStall emits a Stall event and a warning log when dur exceeds the
// configured stallThreshold. Called once per batch stage (read/transform/write)
// using time.Since(t0) — no goroutine or timer is needed; exactly one alert is
// emitted per stalled batch regardless of how long the stall lasts.
func (a *DataPipelineAction) checkStall(runLog corelog.Logger, stage string, dur time.Duration) {
	thresh := a.stallThreshold()
	if thresh > 0 && dur > thresh {
		a.recorder.Stall(a.ref, stage, dur)
		runLog.Warnf("stall detected in stage %q — batch took %v (threshold %v)",
			stage, dur.Round(time.Millisecond), thresh)
	}
}

// Shutdown flushes the metrics backend and releases its resources.
// Call this from a Flogo app lifecycle hook (or os.Signal handler) before
// process exit to ensure the final OTEL metric batch is flushed to the
// collector. For the Prometheus backend, this gracefully stops the HTTP server.
func (a *DataPipelineAction) Shutdown(ctx context.Context) error {
	_ = a.auditor.Close()
	_ = a.lineage.Close()
	return a.recorder.Shutdown(ctx)
}

// transformTypes returns the ordered transform type names for auditing.
func (a *DataPipelineAction) transformTypes() []string {
	types := make([]string, 0, len(a.def.Transforms))
	for _, t := range a.def.Transforms {
		types = append(types, t.Type)
	}
	return types
}

// auditRunStart records a governance audit record at the start of a run. It
// emits only component TYPES and run identity — never option values or secrets.
func (a *DataPipelineAction) auditRunStart(runID string) {
	a.auditor.Log("pipeline.run.started", map[string]interface{}{
		"runId":       runID,
		"pipeline":    a.ref,
		"source":      a.def.Source.Type,
		"sink":        a.def.Sink.Type,
		"transforms":  a.transformTypes(),
		"scaling":     a.def.Scaling.Mode,
		"checkpoint":  a.def.Checkpoint.Type,
		"incremental": a.def.Incremental.Mode,
	})
}

// auditRunFinish records a governance audit record at the end of a run with the
// final status and row counters.
func (a *DataPipelineAction) auditRunFinish(runID string, out *Output) {
	a.auditor.Log("pipeline.run.finished", map[string]interface{}{
		"runId":         runID,
		"pipeline":      a.ref,
		"status":        out.Status,
		"rowsProcessed": out.RowsProcessed,
		"rowsWritten":   out.RowsWritten,
		"rowsFailed":    out.RowsFailed,
		"rowsDropped":   out.RowsDropped,
		"rowsDeleted":   out.RowsDeleted,
		"durationMs":    out.DurationMs,
		"error":         out.Error,
	})
}

// lineageDatasetName picks a non-secret dataset identifier from a component's
// path/options (path, table, topic, bucket, index), falling back to the type.
func lineageDatasetName(typ, path string, opts map[string]interface{}) string {
	if path != "" {
		return path
	}
	for _, k := range []string{"path", "table", "topic", "bucket", "index"} {
		if v, ok := opts[k].(string); ok && v != "" {
			return v
		}
	}
	return typ
}

func colDefsToFields(cols []ColumnDef) []lineage.Field {
	if len(cols) == 0 {
		return nil
	}
	fs := make([]lineage.Field, len(cols))
	for i, c := range cols {
		fs[i] = lineage.Field{Name: c.Name, Type: c.Type}
	}
	return fs
}

func (a *DataPipelineAction) sourceDataset() lineage.Dataset {
	return lineage.Dataset{
		Namespace: a.def.Source.Type,
		Name:      lineageDatasetName(a.def.Source.Type, a.def.Source.Path, a.def.Source.Options),
		Fields:    colDefsToFields(a.def.Source.Columns),
	}
}

func (a *DataPipelineAction) sinkDataset() lineage.Dataset {
	return lineage.Dataset{
		Namespace: a.def.Sink.Type,
		Name:      lineageDatasetName(a.def.Sink.Type, "", a.def.Sink.Options),
	}
}

// lineageStart emits an OpenLineage START event naming the source and sink datasets.
func (a *DataPipelineAction) lineageStart(runID string) {
	a.lineage.Emit(lineage.Event{
		Type:        lineage.Start,
		RunID:       runID,
		Job:         a.ref,
		Inputs:      []lineage.Dataset{a.sourceDataset()},
		Outputs:     []lineage.Dataset{a.sinkDataset()},
		RowCount:    -1,
		RowsDeleted: -1,
	})
}

// lineageFinish emits a COMPLETE (or FAIL) event with the output row count.
func (a *DataPipelineAction) lineageFinish(runID string, out *Output) {
	evType := lineage.Complete
	switch out.Status {
	case "failed":
		evType = lineage.Fail
	case "cancelled":
		evType = lineage.Abort
	}
	// -1 tells the emitter to omit the CDC-stats facet entirely: only a
	// delete-capable sink (signalled by out.Metadata["cdcStats"], set in
	// execute right after the sink reports its stats) populates
	// this, so a plain insert/upsert-only pipeline's lineage output isn't
	// cluttered with an always-zero "rowsDeleted" field.
	rowsDeleted := int64(-1)
	if out.Metadata != nil {
		if _, ok := out.Metadata["cdcStats"]; ok {
			rowsDeleted = out.RowsDeleted
		}
	}
	a.lineage.Emit(lineage.Event{
		Type:        evType,
		RunID:       runID,
		Job:         a.ref,
		Inputs:      []lineage.Dataset{a.sourceDataset()},
		Outputs:     []lineage.Dataset{a.sinkDataset()},
		RowCount:    out.RowsWritten,
		RowsDeleted: rowsDeleted,
	})
}

// executePipeline runs the full source → transform → sink loop.
// Called from the goroutine spawned by Run() and directly from RunDirect().
// runLog carries the per-run structured fields {"pipeline": "...", "runId": "..."}.
//
// A deferred recover converts any panic from a source/transform/sink (e.g. a
// type-mapping panic deep in a columnar writer) into a normal error return, so
// one malformed run fails gracefully with status "failed" instead of crashing
// the whole engine process (which would take down every other pipeline and the
// REST API with it).
func (a *DataPipelineAction) executePipeline(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (out *Output, err error) {
	defer func() {
		if r := recover(); r != nil {
			if out == nil {
				out = &Output{}
			}
			err = fmt.Errorf("pipeline panicked: %v", r)
			runLog.Errorf("recovered from panic in pipeline run: %v\n%s", r, debug.Stack())
		}
	}()
	return a.execute(ctx, in, runLog, runID)
}

// buildSourceConfig merges definition config with runtime overrides.
// a.def.Source.Columns is []ColumnDef; we convert it to a slice of maps that
// carry every column attribute (name/type/format/scale plus the fixed-width
// start/length/impliedDecimal/trim). Emitting maps — rather than
// []record.Column — means every source parser (csv, fixedwidth, jsonlines, …)
// receives the full schema instead of silently losing the fixed-width layout
// fields that record.Column cannot represent.
func (a *DataPipelineAction) buildSourceConfig(in *Input) map[string]interface{} {
	cols := make([]interface{}, len(a.def.Source.Columns))
	for i, cd := range a.def.Source.Columns {
		m := map[string]interface{}{
			"name": cd.Name,
			"type": cd.Type,
		}
		if cd.Format != "" {
			m["format"] = cd.Format
		}
		if cd.Scale != 0 {
			m["scale"] = cd.Scale
		}
		if cd.Start != 0 {
			m["start"] = cd.Start
		}
		if cd.Length != 0 {
			m["length"] = cd.Length
		}
		if cd.ImpliedDecimal != 0 {
			m["impliedDecimal"] = cd.ImpliedDecimal
		}
		if cd.Trim {
			m["trim"] = cd.Trim
		}
		if cd.Nullable {
			m["nullable"] = cd.Nullable
		}
		cols[i] = m
	}
	cfg := map[string]interface{}{
		"type":     a.def.Source.Type,
		"encoding": a.def.Source.Encoding,
		"columns":  cols, // []interface{} of maps — full schema incl. fixed-width layout
	}
	for k, v := range a.def.Source.Options {
		cfg[k] = v
	}
	path := a.def.Source.Path
	if in.InputPath != "" {
		path = in.InputPath
	}
	if in.Params != nil {
		for k, v := range in.Params {
			path = strings.ReplaceAll(path, "${"+k+"}", fmt.Sprintf("%v", v))
		}
	}
	cfg["path"] = path
	return cfg
}

func openCheckpoint(def *Definition) (checkpoint.Checkpoint, error) {
	cd := def.Checkpoint
	if cd.Type == "" || cd.Type == "none" {
		return checkpoint.NoopCheckpoint{}, nil
	}
	cfg := map[string]interface{}{
		"path":        cd.Path,
		"addr":        cd.Addr,
		"passwordEnv": cd.PasswordEnv,
		"keyPrefix":   cd.KeyPrefix,
		"ttlSeconds":  cd.TTLSeconds,
	}
	return checkpoint.New(cd.Type, cfg)
}

// emitWindowMetrics publishes the window operator's counters through the active
// metrics backend (prometheus/otel/none) via the generic Gauge escape hatch, so
// streaming windowed pipelines are observable without any backend-specific code.
// Called at each checkpoint (for continuous streams) and at end-of-run flush.
func (a *DataPipelineAction) emitWindowMetrics(winOp wintransform.WindowedOperator) {
	if winOp == nil {
		return
	}
	st := winOp.Stats()
	labels := map[string]string{"pipeline": a.ref}
	a.recorder.Gauge("flogo_pipeline_window_open", "Currently open (unfired) windows held in operator state.", labels, float64(st.OpenWindows))
	a.recorder.Gauge("flogo_pipeline_window_emitted_total", "Cumulative windows fired to the sink.", labels, float64(st.WindowsEmitted))
	a.recorder.Gauge("flogo_pipeline_window_late_dropped_total", "Cumulative records dropped for arriving beyond the watermark + allowed lateness.", labels, float64(st.LateDropped))
	a.recorder.Gauge("flogo_pipeline_window_state_evicted_total", "Cumulative window state entries evicted by TTL.", labels, float64(st.StateEvicted))
	a.recorder.Gauge("flogo_pipeline_window_watermark_ms", "Current event-time watermark (epoch milliseconds).", labels, float64(st.WatermarkMillis))
	a.recorder.Gauge("flogo_pipeline_window_spill_failures_total", "Cumulative keys dropped because spill-to-disk failed (should stay 0).", labels, float64(st.SpillFailures))
}

// hasUncommitted reports whether src has consumed input it has not yet
// acknowledged (see source.UncommittedReporter).
func hasUncommitted(src source.Source) bool {
	r, ok := src.(source.UncommittedReporter)
	return ok && r.HasUncommitted()
}

// checkpointFailed applies checkpoint.onSaveError to a failed checkpoint: it
// returns the error to abort the run under "fail", otherwise logs it.
func checkpointFailed(runLog corelog.Logger, def *Definition, what string, err error) error {
	if def.Checkpoint.failOnCheckpointError() {
		return fmt.Errorf("%s failed (checkpoint.onSaveError=fail): %w", what, err)
	}
	runLog.Warnf("%s failed (continuing; previous checkpoint retained): %v", what, err)
	return nil
}

func newRunID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// orDefault returns v, or def when v is empty. Used for log echoes of settings
// whose zero value is filled in later by Definition.defaults().
func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// orDefaultInt returns v, or def when v is zero.
func orDefaultInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// openWatermarkStore creates the watermark.Store configured in def.Incremental.
// When incremental mode is "none" (or omitted), a NoopStore is returned so the
// rest of the engine never needs to nil-check wmStore.
func openWatermarkStore(def *Definition) (watermark.Store, error) {
	inc := def.Incremental
	if inc.Mode != "hwm" {
		return watermark.NoopStore{}, nil
	}
	storeType := inc.StoreType
	if storeType == "" {
		storeType = "file"
	}
	cfg := make(map[string]interface{}, len(inc.StoreOptions)+1)
	for k, v := range inc.StoreOptions {
		cfg[k] = v
	}
	return watermark.New(storeType, cfg)
}

// saveWatermark reads the current in-memory maximum from the hwm transform (if
// one was found in the pipeline) and persists it to the watermark store.
// Failures are logged as warnings — a missed watermark save means the next run
// re-processes the last batch, but the jdbc sink's upsert mode makes that safe.
func saveWatermark(
	runLog corelog.Logger,
	store watermark.Store,
	provider hwmtransform.HWMProvider,
	def *Definition,
	ref string,
) {
	if def.Incremental.Mode != "hwm" || provider == nil {
		return
	}
	maxVal := provider.MaxSeen()
	if maxVal == nil {
		return // no rows observed this run — nothing to persist
	}

	// Format the value as a stable string.
	var valStr string
	switch v := maxVal.(type) {
	case time.Time:
		valStr = v.UTC().Format(time.RFC3339Nano)
	default:
		valStr = fmt.Sprintf("%v", v)
	}

	m := &watermark.Mark{
		Column:    provider.WatermarkColumn(),
		Value:     valStr,
		ValueType: provider.WatermarkValueType(),
		SavedAt:   time.Now().UTC(),
	}
	if err := store.Save(ref, m); err != nil {
		runLog.Warnf("watermark save failed (continuing): %v", err)
	} else {
		runLog.Infof("watermark saved — %s = %s", m.Column, m.Value)
	}
}

// findWindowedOperator walks the pipeline stages and returns the first stage that
// implements wintransform.WindowedOperator (i.e. the "window" transform), or nil
// when no windowed aggregation is declared. The engine calls Flush() on it after
// the source is exhausted to emit windows still open.
func findWindowedOperator(p *transform.Pipeline) wintransform.WindowedOperator {
	for _, stage := range p.Stages() {
		if w, ok := stage.(wintransform.WindowedOperator); ok {
			return w
		}
	}
	return nil
}

// findHWMTransform walks the pipeline stages and returns the first stage that
// implements hwmtransform.HWMProvider (i.e. the "hwm" transform).
// Returns nil if no hwm transform is present, which is valid when the pipeline
// runs in non-incremental mode.
func findHWMTransform(p *transform.Pipeline) hwmtransform.HWMProvider {
	for _, stage := range p.Stages() {
		if h, ok := stage.(hwmtransform.HWMProvider); ok {
			return h
		}
	}
	return nil
}

// findDQGateProvider walks the pipeline stages and returns the first stage that
// implements dqtransform.DQGateProvider (i.e. the "dq" transform).
// Returns nil if no dq transform is present; callers must nil-check before use.
func findDQGateProvider(p *transform.Pipeline) dqtransform.DQGateProvider {
	for _, stage := range p.Stages() {
		if d, ok := stage.(dqtransform.DQGateProvider); ok {
			return d
		}
	}
	return nil
}

// pipelineHasSinkOwner reports whether any stage owns external sink(s) (see
// transform.SinkOwner) — e.g. the router. Such pipelines must run
// single-threaded: the parallel path builds one transform pipeline per worker,
// which would open duplicate writers to the same destination.
func pipelineHasSinkOwner(p *transform.Pipeline) bool {
	for _, stage := range p.Stages() {
		if so, ok := stage.(transform.SinkOwner); ok && so.OwnsExternalSink() {
			return true
		}
	}
	return false
}

// openDLQ constructs the DLQ sink wrapper when def.DLQ.Type is non-empty.
// The inner sink connection is opened lazily (on first bad row) inside dlqSink
// itself, so this call is cheap — it only validates config and builds the
// wrapper struct.  Returns nil when no DLQ is configured.
func openDLQ(runLog corelog.Logger, def *Definition, pipelineRef, runID string) (dlqWriteSink, error) {
	if def.DLQ.Type == "" {
		return nil, nil
	}

	// Build the config map that the dlq sink factory expects.
	// innerType / innerOptions name the real destination; the dlq wrapper adds
	// the error-envelope column on top.
	opts := def.DLQ.Options
	if opts == nil {
		opts = map[string]interface{}{}
	}
	cfg := map[string]interface{}{
		"innerType":       def.DLQ.Type,
		"innerOptions":    opts,
		"maxRowsPerBatch": def.DLQ.MaxRowsPerBatch,
		"onFull":          def.DLQ.OnFull,
	}
	// Propagate envelopeColumn if the operator set it inside the options block.
	if ec, ok := opts["envelopeColumn"].(string); ok && ec != "" {
		cfg["envelopeColumn"] = ec
	}

	raw, err := sink.New("dlq", cfg)
	if err != nil {
		return nil, fmt.Errorf("dlq sink: %w", err)
	}

	ds, ok := raw.(dlqWriteSink)
	if !ok {
		// The "dlq" type was registered by a different package that does not
		// implement dlqWriteSink — fall back gracefully so the pipeline still runs
		// but per-row error metadata is not injected.
		runLog.Warnf("dlq sink does not implement dlqWriteSink — error envelopes will be omitted")
		return &plainDLQAdapter{raw}, nil
	}
	ds.InjectContext(pipelineRef, runID)
	return ds, nil
}

// plainDLQAdapter wraps a bare sink.Sink as a dlqWriteSink so that openDLQ
// can always return the interface even when the registered "dlq" type was
// provided by a third-party package that does not embed *dlqsink.dlqSink.
// WriteBatchErrors degrades gracefully: it calls plain WriteBatch, discarding
// the per-row error strings since the inner sink has no envelope column.
type plainDLQAdapter struct{ sink.Sink }

func (p *plainDLQAdapter) WriteBatchErrors(batch *record.RecordBatch, _ []string) error {
	return p.Sink.WriteBatch(batch)
}
func (p *plainDLQAdapter) InjectContext(_, _ string) {}

// rowLevelWrite is the write path with per-row DLQ fallback.
//
// Happy path (no error from snk.WriteBatch): returns (nRows, nil) — zero extra
// allocations, identical behaviour to the old single WriteBatch call.
//
// On batch-level error: falls back to writing each row individually (see
// isolateRows). A row whose single-row write succeeds is already delivered to
// snk and is counted as written — it is NOT written again, which would
// duplicate it in non-idempotent sinks. Rows that fail individually are
// forwarded to the DLQ (when configured) with their per-row error message.
//
// Buffering sinks (e.g. jdbc) can fail a flush that also contained rows
// accepted by EARLIER calls; they hand those back as sink.UnflushedRowsError.
// Those rows were already counted as written, so they are isolated too and
// each one that fails moves from out.RowsWritten to out.RowsFailed instead of
// being lost silently.
//
// rowLevelWrite always returns nil from the error position unless an
// unrecoverable infrastructure failure occurs (e.g. the DLQ sink itself
// returns an error). Individual row failures are recorded in out.RowsFailed
// and the run continues; the caller's out.RowsFailed > 0 check will set the
// run status to "partial".
//
// The caller must NOT call record.Put(batch) before rowLevelWrite returns; it
// is safe to call record.Put immediately after.
func (a *DataPipelineAction) rowLevelWrite(
	runLog corelog.Logger,
	snk sink.Sink,
	dlq dlqWriteSink,
	batch *record.RecordBatch,
	runID string,
	out *Output,
) (written int, err error) {
	// --- happy path ---
	batchErr := snk.WriteBatch(batch)
	if batchErr == nil {
		return batch.Len(), nil
	}
	runLog.Warnf("batch write failed (%d rows), falling back to per-row isolation: %v",
		batch.Len(), batchErr)

	// --- per-row isolation fallback ---
	written, failed, err := a.isolateRows(runLog, snk, dlq, batch.Columns, batch.Rows)
	out.RowsFailed += int64(failed)
	if err != nil {
		return written, err
	}
	var carry *sink.UnflushedRowsError
	if errors.As(batchErr, &carry) {
		if err := a.recoverUnflushed(runLog, snk, dlq, carry, out); err != nil {
			return written, err
		}
	}
	return written, nil
}

// recoverUnflushed re-writes rows a buffering sink failed to persist after
// earlier calls had accepted them (and they were counted as written): each
// row is retried in isolation; failures are dead-lettered and moved from
// RowsWritten to RowsFailed.
func (a *DataPipelineAction) recoverUnflushed(runLog corelog.Logger, snk sink.Sink, dlq dlqWriteSink, carry *sink.UnflushedRowsError, out *Output) error {
	runLog.Warnf("sink flush failed with %d previously accepted rows; isolating them: %v", len(carry.Rows), carry.Err)
	_, failed, err := a.isolateRows(runLog, snk, dlq, carry.Columns, carry.Rows)
	out.RowsWritten -= int64(failed)
	out.RowsFailed += int64(failed)
	return err
}

// flushSink flushes the sink, recovering (isolating + dead-lettering) any
// previously accepted rows a buffering sink reports as unpersisted instead of
// failing the run on one bad row. Other flush errors are returned.
func (a *DataPipelineAction) flushSink(runLog corelog.Logger, snk sink.Sink, dlq dlqWriteSink, out *Output) error {
	err := snk.Flush()
	var carry *sink.UnflushedRowsError
	if err == nil || !errors.As(err, &carry) {
		return err
	}
	return a.recoverUnflushed(runLog, snk, dlq, carry, out)
}

// isolateRows writes rows so that each one is persisted or rejected on its
// own — every attempt is WriteBatch then Flush, so a buffering sink cannot
// merely buffer a row — and dead-letters the ones that fail. Sinks with
// all-or-nothing writes (sink.AtomicWriter) are isolated by bisection: a
// failed chunk is split in halves until the bad rows are single, so a batch
// with a few bad rows costs O(bad·log n) writes instead of n. Other sinks are
// written one row at a time, since a failed multi-row attempt might already
// have delivered part of the chunk.
func (a *DataPipelineAction) isolateRows(runLog corelog.Logger, snk sink.Sink, dlq dlqWriteSink, cols []record.Column, rows [][]interface{}) (written, failed int, err error) {
	// Reusable scratch batch (its Rows slice aliases rows, so never pooled with them).
	chunk := record.Get()
	chunk.Columns = cols
	// Sinks whose Flush is expensive (an object-store rolling sink uploads)
	// expose a local flush that is enough to learn whether a row was accepted.
	flush := snk.Flush
	if lf, ok := snk.(interface{ FlushLocal() error }); ok {
		flush = lf.FlushLocal
	}
	write := func(part [][]interface{}) error {
		chunk.Rows = part
		werr := snk.WriteBatch(chunk)
		if werr == nil {
			werr = flush()
		}
		return werr
	}

	var badRows [][]interface{}
	var badErrs []string
	reject := func(row []interface{}, rowErr error) {
		runLog.Debugf("row failed write (routing to DLQ): %v", rowErr)
		badRows = append(badRows, row)
		badErrs = append(badErrs, rowErr.Error())
		failed++
	}
	var bisect func(part [][]interface{})
	bisect = func(part [][]interface{}) {
		werr := write(part)
		switch {
		case werr == nil:
			written += len(part)
		case len(part) == 1:
			reject(part[0], werr)
		default:
			mid := len(part) / 2
			bisect(part[:mid:mid])
			bisect(part[mid:])
		}
	}

	if aw, ok := snk.(sink.AtomicWriter); ok && aw.AtomicWrites() && len(rows) > 1 {
		mid := len(rows) / 2 // the whole set just failed; start from its halves
		bisect(rows[:mid:mid])
		bisect(rows[mid:])
	} else {
		for i := range rows {
			if werr := write(rows[i : i+1 : i+1]); werr == nil {
				written++
			} else {
				reject(rows[i], werr)
			}
		}
	}

	chunk.Columns = nil
	chunk.Rows = nil
	record.Put(chunk)

	if len(badRows) > 0 && dlq != nil {
		badBatch := record.Get()
		badBatch.Columns = cols
		badBatch.Rows = badRows
		if dlqErr := dlq.WriteBatchErrors(badBatch, badErrs); dlqErr != nil {
			// DLQ write failure is an unrecoverable infrastructure problem —
			// surface it so the caller can abort the pipeline rather than silently
			// losing the error evidence.
			record.Put(badBatch)
			return written, failed, fmt.Errorf("dlq write: %w", dlqErr)
		}
		record.Put(badBatch)
		runLog.Infof("routed %d failed rows to DLQ", len(badRows))
	} else if len(badRows) > 0 {
		runLog.Warnf("%d rows failed write and no DLQ is configured — rows are lost", len(badRows))
	}
	return written, failed, nil
}
