package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime/debug"
	"strings"
	"sync"
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
		out, runErr := a.executePipeline(ctx, in, runLog, runID)
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
	// executePipelineImpl right after the sink reports its stats) populates
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
	return a.executePipelineImpl(ctx, in, runLog, runID)
}

func (a *DataPipelineAction) executePipelineImpl(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (*Output, error) {
	def := a.def
	out := &Output{}

	// Honor a context that is already cancelled before any work begins. Without
	// this the read-ahead goroutine sees the cancelled context, produces nothing,
	// and the drain loop returns nil — reporting a misleading "completed" run with
	// zero rows. A caller that aborts pre-flight must get a deterministic failure
	// so it never mistakes a cancelled run for a successful load.
	if err := ctx.Err(); err != nil {
		return out, err
	}

	// Graph runner: when the pipeline carries a NON-REDUCIBLE DAG — conditional
	// edges or true fan-out/fan-in that the linear source→transforms→sink chain
	// cannot express — execute it with the streaming graph runner. A pure linear
	// chain, even one promoted to an explicit graph block, is NOT diverted here:
	// it stays on the linear path below and keeps checkpoint, HWM, DQ and
	// parallel-worker behaviour untouched (graphExecutable returns false for it).
	if graphExecutable(def) {
		runLog.Infof("executing via graph runner — non-reducible DAG (%d nodes, %d edges)",
			len(def.Graph.Nodes), len(def.Graph.Edges))
		return a.runGraph(ctx, in, runLog, runID)
	}

	// Echo the effective run configuration so operators can confirm from the run
	// log which settings were honored (many settings have no other visible
	// side-effect). Kept to a single line to avoid log noise.
	runLog.Infof("run config — mode=%s batchSize=%d bufferBatches=%d maxMemoryMB=%d transformWorkers=%d scaling=%s checkpoint=%s(rows=%d,secs=%d) incremental=%s metrics=%s",
		orDefault(def.Mode, "etl"), def.Performance.BatchSize, def.Performance.SourceBufferBatches,
		def.Performance.MaxMemoryMB, orDefaultInt(def.Performance.TransformWorkers, 1),
		orDefault(def.Scaling.Mode, "single"), orDefault(def.Checkpoint.Type, "none"),
		def.Checkpoint.IntervalRows, def.Checkpoint.IntervalSeconds,
		orDefault(def.Incremental.Mode, "none"), orDefault(def.Observability.Metrics, "none"))

	// --- checkpoint ---
	ckpt, err := openCheckpoint(def)
	if err != nil {
		return out, fmt.Errorf("checkpoint open: %w", err)
	}
	defer ckpt.Close()

	// --- watermark store ---
	// openWatermarkStore always returns a non-nil store (NoopStore when disabled).
	wmStore, err := openWatermarkStore(def)
	if err != nil {
		return out, fmt.Errorf("watermark store open: %w", err)
	}
	defer wmStore.Close()

	// Load the prior mark and, when present, inject it into the source config
	// so that the jdbc (or other HWM-aware) source can append the WHERE predicate.
	var priorMark *watermark.Mark
	if def.Incremental.Mode == "hwm" {
		priorMark, err = wmStore.Load(a.ref)
		if err != nil {
			return out, fmt.Errorf("watermark load: %w", err)
		}
		// If there is no stored mark but an initialValue is configured, synthesise
		// a mark so the source predicates from the configured start point.
		if priorMark == nil && def.Incremental.InitialValue != "" {
			priorMark = &watermark.Mark{
				Column:    def.Incremental.Column,
				Value:     def.Incremental.InitialValue,
				ValueType: def.Incremental.ValueType,
				SavedAt:   time.Now().UTC(),
			}
		}
		if priorMark != nil {
			runLog.Infof("incremental load — %s > %s (type: %s)",
				priorMark.Column, priorMark.Value, priorMark.ValueType)
		} else {
			runLog.Infof("incremental load — no prior mark, performing full-table load")
		}
	}

	// --- source ---
	srcCfg := a.buildSourceConfig(in)
	// Inject the high-water-mark into the source config so HWM-aware sources
	// (e.g. the jdbc source) can append a parameterised WHERE predicate.
	// Keys are intentionally prefixed with "hwm" to avoid colliding with any
	// source-specific config key.
	if priorMark != nil {
		srcCfg["hwmColumn"] = priorMark.Column
		srcCfg["hwmValue"] = priorMark.Value
		srcCfg["hwmValueType"] = priorMark.ValueType
	}
	// Checkpoint-mode Kafka resume: seek the source to the exact processed
	// offsets recorded in the checkpoint token (captured atomically with the
	// window-state snapshot) so the source position and the restored window
	// state resume from the SAME token — independent of whether the last external
	// offset commit succeeded. This closes the crash-between-token-and-commit
	// double-count window. Single mode only: sharded/distributed disallow
	// windowed Kafka (co-partition guard) and resume per-partition inside drain.
	if def.Source.Type == "kafka" && (def.Scaling.Mode == "" || def.Scaling.Mode == "single") {
		rk := runID
		if def.Checkpoint.Key != "" {
			rk = def.Checkpoint.Key
		}
		if tok, tErr := ckpt.Load(rk); tErr == nil && tok != nil && len(tok.Payload) > 0 {
			srcCfg["resumeCursor"] = tok.Payload
		}
	}
	src, err := source.New(def.Source.Type, srcCfg)
	if err != nil {
		return out, fmt.Errorf("source open: %w", err)
	}
	defer src.Close()
	// Inject the active metrics Recorder into the source when it opts in via the
	// RecorderAware interface, so component-specific metrics (e.g. a tailing
	// source's reconciliation counters) emit through the configured backend
	// (prometheus/otel/none) instead of a hard-wired one — discovered by type
	// assertion, exactly like checkpoint.CursorProvider.
	if ra, ok := src.(metrics.RecorderAware); ok {
		ra.SetRecorder(a.recorder)
	}

	// --- transforms ---
	// Build the user-declared transform configs.
	transformCfgs := make([]map[string]interface{}, 0, len(def.Transforms)+1)

	// Execution mode gates the in-flight transform stage:
	//   etl  → transforms run in-flight (default).
	//   el   → raw extract-load; the transform pipeline is bypassed entirely.
	//   elt  → load raw now, transform in-target later; in-flight transforms are
	//          also bypassed (they are expected to run in the destination system).
	mode := def.Mode
	if mode == "" {
		mode = "etl"
	}
	inFlightTransforms := mode == "etl"
	if !inFlightTransforms && (len(def.Transforms) > 0 || def.SchemaDrift.Policy != "") {
		if mode == "elt" {
			runLog.Infof("mode=elt — %d declared transform(s) are NOT run in-flight; they are expected to execute in the target system", len(def.Transforms))
		} else {
			runLog.Warnf("mode=el — raw extract-load; %d declared transform(s) will be ignored", len(def.Transforms))
		}
	}

	// Auto-prepend the schemadrift transform when a drift policy is configured.
	// The schemadrift package must be imported (blank import) alongside this
	// package in the main program; if it is not imported the Build call below
	// will return an "unknown type" error with a clear message.
	if inFlightTransforms && def.SchemaDrift.Policy != "" {
		driftCfg := map[string]interface{}{
			"type":    "schemadrift",
			"policy":  def.SchemaDrift.Policy,
			"onDrift": def.SchemaDrift.OnDrift,
		}
		transformCfgs = append(transformCfgs, driftCfg)
	}

	if inFlightTransforms {
		for _, td := range def.Transforms {
			transformCfgs = append(transformCfgs, td.Extra)
		}
	}

	// Auto-append a dq transform stage built from the contract's expectations,
	// after any user-declared transforms (validating what's about to reach the
	// sink) — UNLESS the pipeline already has an explicit dq stage, so a
	// pipeline with its own (possibly different) dq gate is never
	// double-gated. See ContractDef.Expectations.
	if inFlightTransforms && def.Contract != nil && len(def.Contract.Expectations) > 0 {
		hasExplicitDQ := false
		for _, td := range def.Transforms {
			if td.Type == "dq" {
				hasExplicitDQ = true
				break
			}
		}
		if !hasExplicitDQ {
			contractDQCfg := map[string]interface{}{
				"type":             "dq",
				"expectations":     def.Contract.Expectations,
				"qualityThreshold": def.Contract.QualityThreshold,
				"onFail":           def.Contract.OnViolation,
			}
			if def.Contract.ErrorSinkType != "" {
				contractDQCfg["errorSinkType"] = def.Contract.ErrorSinkType
				contractDQCfg["errorSinkConfig"] = def.Contract.ErrorSinkConfig
			}
			transformCfgs = append(transformCfgs, contractDQCfg)
		} else {
			runLog.Infof("contract.expectations declared but the pipeline already has an explicit dq transform — not auto-appending a second gate")
		}
	}

	txPipeline, err := transform.Build(transformCfgs)
	if err != nil {
		return out, fmt.Errorf("transforms build: %w", err)
	}
	defer txPipeline.Close()
	txPipeline.BindContext(ctx)

	// Locate the hwm transform (if present) so the engine can read MaxSeen()
	// after each checkpoint interval. A nil hwmProvider means no hwm transform
	// was declared; watermark persistence is skipped silently.
	hwmProvider := findHWMTransform(txPipeline)

	// Locate the dq transform (if present) so the engine can include the last
	// DQ gate report in Output.Metadata after the pipeline completes.
	// A nil dqProvider means no dq transform was declared; metadata is skipped.
	dqProvider := findDQGateProvider(txPipeline)

	// Locate the window transform (if present). The engine flushes its remaining
	// open windows after the source EOF, and excludes windowed pipelines from the
	// parallel transform path (windowed aggregation is stateful and order-sensitive).
	winOp := findWindowedOperator(txPipeline)

	// --- sink ---
	snk, err := sink.New(def.Sink.Type, def.Sink.Options)
	if err != nil {
		return out, fmt.Errorf("sink open: %w", err)
	}
	// Guard against double-close: the happy path closes the sink explicitly (to
	// surface upload/commit errors that only occur at Close); this deferred close
	// then only runs on an early error return.
	sinkClosed := false
	defer func() {
		if !sinkClosed {
			_ = snk.Close()
		}
	}()

	// --- DLQ sink (lazy: inner connection opened only on first bad row) ---
	// dlq is nil when no DLQ is configured; rowLevelWrite handles nil safely.
	dlq, err := openDLQ(runLog, def, a.ref, runID)
	if err != nil {
		return out, fmt.Errorf("dlq open: %w", err)
	}
	if dlq != nil {
		defer dlq.Close()
	}

	// --- read → transform → write loop ---
	batchSize := def.Performance.BatchSize
	ckptIntervalRows := int64(def.Checkpoint.IntervalRows)
	ckptIntervalSecs := def.Checkpoint.IntervalSeconds

	// maxMemoryMB backpressure budget for the read-ahead buffer. The reader
	// goroutine reads ahead up to sourceBufferBatches batches, but never lets the
	// total in-flight (un-consumed) bytes exceed this limit — protecting memory
	// when a downstream stage (transform/sink) is slower than the source.
	maxInFlightBytes := int64(def.Performance.MaxMemoryMB) * 1024 * 1024
	bufDepth := def.Performance.SourceBufferBatches
	if bufDepth < 1 {
		bufDepth = 1
	}
	// Transformed batches that may queue for the sink writer (see writeStage).
	sinkDepth := def.Performance.SinkBufferBatches
	if sinkDepth < 1 {
		sinkDepth = 1
	}

	// transformWorkers > 1 requests multi-core transform parallelism. It is only
	// safe when transforms are STATELESS and there is no ordered-cursor state to
	// protect, i.e. no HWM/DQ/schemadrift accumulator and no checkpointing. When
	// eligible we run an ordered-parallel path: N worker goroutines each own a
	// private transform pipeline, and a single collector reassembles results in
	// the original batch order before writing — so the sink write and all
	// counters stay single-threaded (no data race) while transforms use N cores.
	// When the constraints don't hold we keep transform single-threaded; reads
	// are still overlapped with transform+write by the read-ahead goroutine.
	//
	// Note this exclusion list deliberately does NOT test for a delete-capable/
	// CDC-target sink (e.g. jdbc's opColumn mode, sink.StatsReporter). That's
	// safe by construction, not by omission: only transform.Apply runs on the
	// worker goroutines above — the sink write itself (rowLevelWrite, called
	// from process() below) always runs on the single collector goroutine that
	// drains resCh in order, so a sink's internal counters (e.g. jdbcSink's
	// rowsDeleted) never see concurrent access regardless of transformWorkers.
	// See TestEnterprise_CDC_DeleteCapableSink_ParallelTransformWorkers
	// (integration package), which runs under -race to prove it.
	txWorkers := def.Performance.TransformWorkers
	if txWorkers < 1 {
		txWorkers = 1
	}
	parallelEligible := txWorkers > 1 &&
		hwmProvider == nil && dqProvider == nil && winOp == nil &&
		def.SchemaDrift.Policy == "" &&
		!pipelineHasSinkOwner(txPipeline) &&
		(def.Checkpoint.Type == "" || def.Checkpoint.Type == "none")
	if txWorkers > 1 {
		if parallelEligible {
			runLog.Infof("transformWorkers=%d — parallel transform across %d workers with ordered output", txWorkers, txWorkers)
		} else {
			runLog.Infof("transformWorkers=%d — ordered processing required (checkpoint/HWM/DQ/drift); transform runs single-threaded, reads overlapped via read-ahead", txWorkers)
		}
	}

	// drain processes a single already-open source to exhaustion, updating the
	// shared Output. It is invoked once for single mode and once per assigned
	// partition for sharded/distributed modes (ckptKey namespaces the checkpoint
	// so partitions resume independently). A reader goroutine decouples reads
	// from transform+write so the two overlap; transform+write stay sequential to
	// preserve transform state (hwm/dq accumulators) and checkpoint-cursor order.
	drain := func(pSrc source.Source, ckptKey string) error {
		// Resume from checkpoint (per-source key).
		token, lErr := ckpt.Load(ckptKey)
		if lErr != nil {
			return fmt.Errorf("checkpoint load: %w", lErr)
		}
		if token != nil && token.RowsProcessed > 0 {
			runLog.Infof("resuming %q — skipping %d already-processed rows", ckptKey, token.RowsProcessed)
			if skipErr := pSrc.Skip(token.RowsProcessed); skipErr != nil {
				return fmt.Errorf("source skip: %w", skipErr)
			}
			out.RowsProcessed += token.RowsProcessed
		}
		// Restore window operator state captured at the same offset as the token so
		// open windows and their aggregations resume in sync with the source cursor.
		if token != nil && len(token.WindowState) > 0 && winOp != nil {
			if rErr := winOp.Restore(token.WindowState); rErr != nil {
				runLog.Warnf("window state restore failed (starting fresh): %v", rErr)
			} else {
				runLog.Infof("resuming %q — restored window state (%d bytes)", ckptKey, len(token.WindowState))
			}
		}

		// resumeBaseline is out.RowsProcessed after any resume skip; subtracting it
		// yields this run's processed-row count, which aligns with a Committer
		// source's run-local returned-row marks for read-ahead-safe offset commits.
		resumeBaseline := out.RowsProcessed
		var rowsSinceCkpt int64
		lastCkpt := time.Now()

		// Read-ahead: a dedicated goroutine reads batches into readCh while the
		// main goroutine transforms+writes. A byte semaphore enforces maxMemoryMB.
		type readItem struct {
			batch   *record.RecordBatch
			bytes   int64 // budget charged at read time; released verbatim
			readDur time.Duration
			err     error
		}
		readCh := make(chan readItem, bufDepth)
		budget := newMemBudget(maxInFlightBytes)
		readCtx, cancelRead := context.WithCancel(ctx)
		defer cancelRead()
		// On any return, stop the reader and wait for it to leave ReadBatch
		// before the source can be closed — sources are not safe for a Close
		// concurrent with ReadBatch. Batches it still delivers are recycled.
		// The wait is bounded so a source stuck in ReadBatch cannot hang
		// shutdown (its own poll/request timeout normally ends the call).
		defer func() {
			cancelRead()
			timeout := time.NewTimer(30 * time.Second)
			defer timeout.Stop()
			for {
				select {
				case it, ok := <-readCh:
					if !ok {
						return
					}
					if it.batch != nil {
						record.Put(it.batch)
					}
				case <-timeout.C:
					runLog.Warnf("source reader did not stop within 30s of shutdown; closing the source anyway")
					return
				}
			}
		}()

		// Write stage: a single writer goroutine overlaps sink I/O with the next
		// batch's transform. Order is preserved, and syncWrites is a barrier
		// taken before every sink Flush/checkpoint and before returning, so
		// checkpoint semantics are unchanged (see writeStage).
		ws := newWriteStage(sinkDepth, func(b *record.RecordBatch, o *Output) (int, error) {
			t2 := time.Now()
			n, err := a.rowLevelWrite(runLog, snk, dlq, b, runID, o)
			if err != nil {
				return 0, err
			}
			d := time.Since(t2)
			a.recorder.BatchWritten(a.ref, def.Sink.Type, n, d)
			a.checkStall(runLog, "write", d)
			return n, nil
		})
		wsClosed := false
		defer func() {
			if !wsClosed {
				// Aborting: recycle queued batches unwritten, but still count
				// what the writer already delivered.
				w, f := ws.close(true)
				out.RowsWritten += w
				out.RowsFailed += f
			}
		}()
		syncWrites := func() error {
			w, f, err := ws.sync()
			out.RowsWritten += w
			out.RowsFailed += f
			return err
		}

		go func() {
			defer close(readCh)
			for {
				// Backpressure: block while the buffer already holds ~maxMemoryMB.
				if !budget.waitForRoom(readCtx) {
					return
				}
				t0 := time.Now()
				b, rErr := pSrc.ReadBatch(batchSize)
				rd := time.Since(t0)
				var nb int64
				if maxInFlightBytes > 0 && b != nil && b.Len() > 0 {
					nb = batchBytes(b)
					budget.acquire(nb)
				}
				select {
				case readCh <- readItem{batch: b, bytes: nb, readDur: rd, err: rErr}:
				case <-readCtx.Done():
					return
				}
				if rErr != nil {
					return // EOF or hard error — stop reading.
				}
			}
		}()

		for item := range readCh {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			batch, readErr, readDur := item.batch, item.err, item.readDur
			if readErr != nil && readErr != io.EOF {
				return fmt.Errorf("source read: %w", readErr)
			}

			// Decided up front: once processed, batch is owned by the write
			// stage or recycled and must not be read again.
			idle := batch == nil || batch.Len() == 0
			if !idle {
				batchLen := int64(batch.Len())
				out.RowsProcessed += batchLen
				rowsSinceCkpt += batchLen

				a.recorder.BatchRead(a.ref, def.Source.Type, int(batchLen), readDur)
				a.checkStall(runLog, "read", readDur)

				// --- transform ---
				t1 := time.Now()
				transformed, txErr := txPipeline.Apply(batch)
				txDur := time.Since(t1)
				if transformed != batch {
					record.Put(batch)
				}
				// Batch fully consumed from the buffer — release its bytes.
				budget.release(item.bytes)

				if txErr != nil && ctx.Err() != nil {
					// The run is being cancelled (e.g. lookups aborted
					// mid-flight). Not a row failure: stop before any
					// checkpoint can advance past this batch, so it replays.
					return ctx.Err()
				}
				if txErr != nil {
					runLog.Warnf("transform error on batch of %d rows (routing to failed): %v", batchLen, txErr)
					a.recorder.BatchFailed(a.ref, int(batchLen))
					out.RowsFailed += batchLen
				} else if transformed == nil {
					a.recorder.BatchTransformed(a.ref, int(batchLen), 0, txDur)
					a.checkStall(runLog, "transform", txDur)
					out.RowsDropped += batchLen
				} else if transformed.Len() > 0 {
					txOut := int(transformed.Len())
					a.recorder.BatchTransformed(a.ref, int(batchLen), txOut, txDur)
					a.checkStall(runLog, "transform", txDur)

					// --- write (async, with per-row DLQ fallback) ---
					// A batch transformed while the run was being cancelled may
					// be degraded (e.g. lookups aborted mid-flight); it is
					// replayed from the last checkpoint, so never write it.
					if ctx.Err() != nil {
						record.Put(transformed)
						return ctx.Err()
					}
					if !ws.submit(transformed) {
						// An earlier write failed; surface it now.
						if err := syncWrites(); err != nil {
							return err
						}
					}
				} else {
					a.recorder.BatchTransformed(a.ref, int(batchLen), 0, txDur)
					a.checkStall(runLog, "transform", txDur)
					record.Put(transformed)
				}

			} else if batch != nil {
				record.Put(batch) // empty poll result (streaming source idle)
			}

			{
				// Checkpoint at configured row interval or time interval. The
				// time trigger is also evaluated on idle polls, so a quiet
				// stream still persists its tail (rows processed since the
				// last checkpoint, or source progress such as skipped Kafka
				// messages) instead of replaying it after every restart.
				rowTrigger := !idle && ckptIntervalRows > 0 && rowsSinceCkpt >= ckptIntervalRows
				timeTrigger := ckptIntervalSecs > 0 && time.Since(lastCkpt) >= time.Duration(ckptIntervalSecs)*time.Second &&
					(!idle || rowsSinceCkpt > 0 || hasUncommitted(pSrc))
				if (rowTrigger || timeTrigger) && ctx.Err() != nil {
					// Never checkpoint (or commit source offsets) once cancelled.
					return ctx.Err()
				}
				if rowTrigger || timeTrigger {
					if err := syncWrites(); err != nil {
						return err
					}
					if err := a.flushSink(runLog, snk, dlq, out); err != nil {
						return fmt.Errorf("sink flush (checkpoint): %w", err)
					}
					// Surface the checkpoint so operators can see progress being
					// persisted and which trigger fired (row count vs. time).
					reason := "rows"
					if timeTrigger && !rowTrigger {
						reason = "time"
					}
					if cerr := saveCheckpoint(runLog, ckpt, ckptKey, def.Source.Type, out.RowsProcessed, pSrc, winOp, out.RowsProcessed-resumeBaseline); cerr != nil {
						if ferr := checkpointFailed(runLog, def, "checkpoint", cerr); ferr != nil {
							return ferr
						}
					} else {
						a.recorder.CheckpointSaved(a.ref)
						runLog.Infof("checkpoint saved — %d rows processed (trigger=%s)", out.RowsProcessed, reason)
					}
					saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)
					a.emitWindowMetrics(winOp)
					rowsSinceCkpt = 0
					lastCkpt = time.Now()
				}
			}
		}
		// readCh also closes when the reader stops because the run was
		// cancelled. That is not end-of-input: returning nil here would report
		// success and let the end-of-run checkpoint commit offsets of batches
		// the reader dropped on the way out.
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syncWrites(); err != nil {
			return err
		}
		wsClosed = true
		ws.close(false)
		return nil
	}

	// --- execution: single-instance vs horizontally scaled ---
	scMode := def.Scaling.Mode
	if scMode == "" {
		scMode = "single"
	}
	// drainParallel is the ordered-parallel transform path (see parallelEligible).
	// A reader feeds sequence-numbered batches to a pool of workers, each with a
	// private transform pipeline; a collector reassembles results in read order
	// so the sink write + counter updates stay single-threaded. It carries no
	// checkpoint/watermark logic because parallelEligible already excludes those.
	drainParallel := func(pSrc source.Source) error {
		type result struct {
			seq         int64
			src         *record.RecordBatch
			transformed *record.RecordBatch
			batchLen    int64
			txErr       error
			txDur       time.Duration
			readDur     time.Duration
			bytes       int64
		}
		type job struct {
			seq     int64
			batch   *record.RecordBatch
			readDur time.Duration
			bytes   int64 // budget charged at read time; released verbatim
		}

		// One private transform pipeline per worker so concurrent Apply calls
		// never share mutable transform state.
		pipes := make([]*transform.Pipeline, txWorkers)
		for i := range pipes {
			p, bErr := transform.Build(transformCfgs)
			if bErr != nil {
				for j := 0; j < i; j++ {
					_ = pipes[j].Close()
				}
				return fmt.Errorf("transforms build (worker %d): %w", i, bErr)
			}
			p.BindContext(ctx)
			pipes[i] = p
		}
		defer func() {
			for _, p := range pipes {
				_ = p.Close()
			}
		}()

		jobCh := make(chan job, txWorkers)
		resCh := make(chan result, txWorkers*2)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()

		budget := newMemBudget(maxInFlightBytes)
		go func() {
			defer close(jobCh)
			var seq int64
			for {
				if !budget.waitForRoom(wctx) {
					return
				}
				t0 := time.Now()
				b, rErr := pSrc.ReadBatch(batchSize)
				rd := time.Since(t0)
				if b != nil && b.Len() > 0 {
					var nb int64
					if maxInFlightBytes > 0 {
						nb = batchBytes(b)
						budget.acquire(nb)
					}
					select {
					case jobCh <- job{seq: seq, batch: b, readDur: rd, bytes: nb}:
						seq++
					case <-wctx.Done():
						return
					}
				}
				if rErr != nil {
					return
				}
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < txWorkers; i++ {
			wg.Add(1)
			go func(p *transform.Pipeline) {
				defer wg.Done()
				for j := range jobCh {
					batchLen := int64(j.batch.Len())
					t1 := time.Now()
					tOut, err := p.Apply(j.batch)
					resCh <- result{
						seq: j.seq, src: j.batch, transformed: tOut,
						batchLen: batchLen, txErr: err,
						txDur: time.Since(t1), readDur: j.readDur, bytes: j.bytes,
					}
				}
			}(pipes[i])
		}
		go func() { wg.Wait(); close(resCh) }()

		pending := make(map[int64]result)
		var next int64
		var writeErr error
		process := func(r result) {
			out.RowsProcessed += r.batchLen
			a.recorder.BatchRead(a.ref, def.Source.Type, int(r.batchLen), r.readDur)
			a.checkStall(runLog, "read", r.readDur)
			if r.transformed != r.src {
				record.Put(r.src)
			}
			budget.release(r.bytes)
			switch {
			case r.txErr != nil:
				runLog.Warnf("transform error on batch of %d rows (routing to failed): %v", r.batchLen, r.txErr)
				a.recorder.BatchFailed(a.ref, int(r.batchLen))
				out.RowsFailed += r.batchLen
			case r.transformed == nil:
				a.recorder.BatchTransformed(a.ref, int(r.batchLen), 0, r.txDur)
				a.checkStall(runLog, "transform", r.txDur)
				out.RowsDropped += r.batchLen
			case r.transformed.Len() == 0:
				a.recorder.BatchTransformed(a.ref, int(r.batchLen), 0, r.txDur)
				a.checkStall(runLog, "transform", r.txDur)
				record.Put(r.transformed)
			default:
				txOut := int(r.transformed.Len())
				a.recorder.BatchTransformed(a.ref, int(r.batchLen), txOut, r.txDur)
				a.checkStall(runLog, "transform", r.txDur)
				t2 := time.Now()
				written, wErr := a.rowLevelWrite(runLog, snk, dlq, r.transformed, runID, out)
				writeDur := time.Since(t2)
				record.Put(r.transformed)
				if wErr != nil {
					writeErr = wErr
					cancel()
					return
				}
				out.RowsWritten += int64(written)
				a.recorder.BatchWritten(a.ref, def.Sink.Type, written, writeDur)
				a.checkStall(runLog, "write", writeDur)
			}
		}
		for r := range resCh {
			if writeErr != nil {
				continue // keep draining results after an error so goroutines exit
			}
			pending[r.seq] = r
			for {
				nr, ok := pending[next]
				if !ok {
					break
				}
				delete(pending, next)
				process(nr)
				next++
			}
		}
		if writeErr == nil {
			// Cancellation closes jobCh/resCh just like end-of-input; never
			// report it as a completed run.
			writeErr = ctx.Err()
		}
		return writeErr
	}

	// resumeKey is the checkpoint key for single-instance runs. Streaming
	// pipelines set checkpoint.key to a stable value so window state and source
	// position resume across restarts; batch runs default to the per-run id.
	resumeKey := runID
	if def.Checkpoint.Key != "" {
		resumeKey = def.Checkpoint.Key
	}

	// stageStats for the Studio graph run overlay are collected only from the
	// single-path txPipeline; the parallel/scaled paths build their own per-worker
	// pipelines, so per-stage counts are omitted there (the overlay falls back to
	// the aggregate source/sink/dlq badges).
	singlePipeline := false
	switch {
	case scMode == "sharded" || scMode == "distributed":
		if err := a.runScaled(ctx, runLog, def, scMode, src, srcCfg, runID, drain); err != nil {
			return out, err
		}
	case parallelEligible:
		if err := drainParallel(src); err != nil {
			return out, err
		}
	default:
		singlePipeline = true
		if err := drain(src, resumeKey); err != nil {
			return out, err
		}
	}

	// Emit any windows still open once the source is exhausted (bounded-source EOF).
	// The window transform fires closed windows incrementally during Apply; Flush
	// drains whatever remains and writes it through the normal sink path.
	if winOp != nil {
		flushed, ferr := winOp.Flush()
		if ferr != nil {
			return out, fmt.Errorf("window flush: %w", ferr)
		}
		if flushed != nil && flushed.Len() > 0 {
			written, werr := a.rowLevelWrite(runLog, snk, dlq, flushed, runID, out)
			if werr != nil {
				return out, werr
			}
			out.RowsWritten += int64(written)
		}
		if flushed != nil {
			record.Put(flushed)
		}
		st := winOp.Stats()
		runLog.Infof("window stats — emitted=%d openAtEnd=%d lateDropped=%d evicted=%d spillFailures=%d watermark=%dms",
			st.WindowsEmitted, st.OpenWindows, st.LateDropped, st.StateEvicted, st.SpillFailures, st.WatermarkMillis)
		if st.SpillFailures > 0 {
			runLog.Warnf("window state: %d keys dropped because spill-to-disk failed — results may be incomplete (check disk space/permissions)", st.SpillFailures)
		}
		if out.Metadata == nil {
			out.Metadata = make(map[string]interface{})
		}
		out.Metadata["windowStats"] = map[string]interface{}{
			"windowsEmitted":  st.WindowsEmitted,
			"lateDropped":     st.LateDropped,
			"stateEvicted":    st.StateEvicted,
			"openWindows":     st.OpenWindows,
			"spillFailures":   st.SpillFailures,
			"watermarkMillis": st.WatermarkMillis,
		}
		a.emitWindowMetrics(winOp)
	}

	// Final flush.
	if err := a.flushSink(runLog, snk, dlq, out); err != nil {
		return out, fmt.Errorf("sink final flush: %w", err)
	}

	// Contract reconciliation (optional, before the sink closes — it reuses
	// the sink's own open connection). See ContractDef.Reconciliation.
	// Must run AFTER Flush() (buffered writes aren't durable/visible to a
	// fresh COUNT(*) query until flushed) and BEFORE Close() (which drops the
	// connection ReconciliationCount needs).
	a.runReconciliation(runLog, def, src, snk, out)

	// Explicitly close the sink and surface any error. This is critical for sinks
	// whose durable write happens at Close (e.g. the object-store sink uploads the
	// buffered object on Close) — relying only on the deferred Close would discard
	// that error and mis-report a failed upload as a successful run. Closing here
	// makes the deferred snk.Close() a harmless no-op (idempotent close).
	if err := snk.Close(); err != nil {
		return out, fmt.Errorf("sink close: %w", err)
	}
	sinkClosed = true

	// Delete-capable/CDC-target sinks (e.g. jdbc's opColumn mode) optionally
	// report a rows-deleted count distinct from RowsWritten, so lineage/audit
	// output for a CDC run shows "N rows deleted" instead of folding deletes
	// into an undifferentiated write total. Sinks without a delete concept
	// simply don't implement StatsReporter, so out.RowsDeleted stays 0 and no
	// lineage facet is emitted for it (out.Metadata["cdcStats"] is the signal
	// lineageFinish uses to decide whether the sink is delete-capable at all,
	// distinct from "delete-capable but zero deletes this run").
	if reporter, ok := snk.(sink.StatsReporter); ok {
		if st := reporter.SinkStats(); st.RowsDeleted >= 0 {
			out.RowsDeleted = st.RowsDeleted
			if out.Metadata == nil {
				out.Metadata = make(map[string]interface{})
			}
			out.Metadata["cdcStats"] = map[string]interface{}{"rowsDeleted": st.RowsDeleted}
		}
	}

	if def.Checkpoint.DeleteOnSuccess {
		// A failed delete leaves a stale checkpoint behind, which is harmless in
		// isolation (a fresh run of a completed pipeline simply doesn't need it,
		// and any FUTURE run overwrites it before ever reading it) but worth
		// surfacing rather than silently swallowing, matching saveCheckpoint's
		// own "warn and continue" pattern below.
		if err := ckpt.Delete(resumeKey); err != nil {
			runLog.Warnf("checkpoint delete failed (continuing; a stale checkpoint may remain): %v", err)
		}
	} else {
		if cerr := saveCheckpoint(runLog, ckpt, resumeKey, def.Source.Type, out.RowsProcessed, src, winOp, math.MaxInt64); cerr != nil {
			if ferr := checkpointFailed(runLog, def, "final checkpoint", cerr); ferr != nil {
				return out, ferr
			}
		} else {
			a.recorder.CheckpointSaved(a.ref)
		}
	}

	// Always persist the final high-water mark on successful completion,
	// regardless of deleteOnSuccess — the watermark must survive a checkpoint
	// reset so subsequent runs still load incrementally.
	saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)

	// Collect DQ gate report into Output.Metadata so downstream flows can
	// inspect quality scores without parsing logs. The report is included
	// regardless of PassedGate — callers can inspect PassedGate themselves.
	if dqProvider != nil {
		if report := dqProvider.LastRunReport(); report != nil {
			if out.Metadata == nil {
				out.Metadata = make(map[string]interface{})
			}
			report.PipelineRef = a.ref
			out.Metadata["dqReport"] = report
		}
	}

	// Per-stage row counters for the Studio graph run overlay (single-path runs).
	if singlePipeline {
		if stats := txPipeline.StageStats(); len(stats) > 0 {
			if out.Metadata == nil {
				out.Metadata = make(map[string]interface{})
			}
			out.Metadata["stageStats"] = stats
		}
	}

	return out, nil
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

// saveCheckpoint persists a checkpoint token (cursor + window state) and, only
// once it is durable, acknowledges the source. It returns a non-nil error when
// nothing was persisted (snapshot or backend failure); the previous checkpoint
// and uncommitted source offsets then remain in force.
func saveCheckpoint(runLog corelog.Logger, ckpt checkpoint.Checkpoint, runID, format string, rowsProcessed int64, src source.Source, winOp wintransform.WindowedOperator, committerRows int64) error {
	var cp checkpoint.CursorProvider
	if p, ok := src.(checkpoint.CursorProvider); ok {
		cp = p
	}
	t := checkpoint.NewToken(format, rowsProcessed, cp)
	// Attach a window-state snapshot (if this pipeline has a window transform) so
	// it is persisted atomically with the source cursor and replayed on resume.
	if winOp != nil {
		snap, err := winOp.Snapshot()
		if err != nil {
			// Saving the cursor without the open windows would make a resume skip
			// their already-consumed input — silent data loss. Keep the previous
			// durable checkpoint (and uncommitted source offsets) instead; the next
			// interval retries.
			return fmt.Errorf("window snapshot: %w", err)
		}
		t.WindowState = snap
	}
	if err := ckpt.Save(runID, t); err != nil {
		return fmt.Errorf("save: %w", err)
	}
	// Advance the source's external acknowledgement (e.g. Kafka offsets) only
	// after the token is durable, so a crash re-delivers everything after this
	// checkpoint. committerRows is the pipeline's processed-row boundary, so the
	// commit never runs ahead of processed (read-ahead-safe) — no lost records.
	if c, ok := src.(source.Committer); ok {
		if err := c.Commit(committerRows); err != nil {
			runLog.Warnf("source commit failed (continuing): %v", err)
		}
	}
	return nil
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
	write := func(part [][]interface{}) error {
		chunk.Rows = part
		werr := snk.WriteBatch(chunk)
		if werr == nil {
			werr = snk.Flush()
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
