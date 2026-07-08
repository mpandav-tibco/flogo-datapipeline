package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/app/resource"
	"github.com/project-flogo/core/data/metadata"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	dqtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	hwmtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
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
	wmStore  watermark.Store  // never nil; NoopStore when incremental is disabled
	recorder metrics.Recorder // never nil; NoopRecorder when unconfigured
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

// newRunLog builds a per-run child logger that permanently binds the pipeline
// name and run ID as structured fields — identical to the flogo-agent pattern
// where every log line carries {"agent": "...", "session": "..."}.
func newRunLog(pipelineRef, runID string) corelog.Logger {
	return corelog.ChildLoggerWithFields(flog,
		corelog.FieldString("pipeline", shortName(pipelineRef)),
		corelog.FieldString("runId", runID),
	)
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

	runLog := newRunLog(a.ref, runID)
	runLog.Infof("pipeline run started — source=%s sink=%s", a.def.Source.Type, a.def.Sink.Type)

	start := time.Now()
	out, runErr := a.executePipeline(ctx, in, runLog, runID)
	out.DurationMs = time.Since(start).Milliseconds()

	if runErr != nil {
		out.Status = "failed"
		out.Error = runErr.Error()
		runLog.Errorf("pipeline run failed after %dms: %v", out.DurationMs, runErr)
	} else if out.RowsFailed > 0 {
		out.Status = "partial"
	} else {
		out.Status = "completed"
	}
	runLog.Infof("pipeline run %s — processed=%d written=%d failed=%d dropped=%d duration=%dms",
		out.Status, out.RowsProcessed, out.RowsWritten, out.RowsFailed, out.RowsDropped, out.DurationMs)

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

	go func() {
		runLog := newRunLog(a.ref, runID)
		runLog.Infof("pipeline run started — source=%s sink=%s", a.def.Source.Type, a.def.Sink.Type)

		start := time.Now()
		out, runErr := a.executePipeline(ctx, in, runLog, runID)
		totalDur := time.Since(start)
		out.DurationMs = totalDur.Milliseconds()

		if runErr != nil {
			out.Status = "failed"
			out.Error = runErr.Error()
			runLog.Errorf("pipeline run failed after %dms: %v", out.DurationMs, runErr)
		} else if out.RowsFailed > 0 {
			out.Status = "partial"
		} else {
			out.Status = "completed"
		}
		runLog.Infof("pipeline run %s — processed=%d written=%d failed=%d dropped=%d duration=%dms",
			out.Status, out.RowsProcessed, out.RowsWritten, out.RowsFailed, out.RowsDropped, out.DurationMs)

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
	return a.recorder.Shutdown(ctx)
}

// executePipeline runs the full source → transform → sink loop.
// Called from the goroutine spawned by Run() and directly from RunDirect().
// runLog carries the per-run structured fields {"pipeline": "...", "runId": "..."}.
func (a *DataPipelineAction) executePipeline(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (*Output, error) {
	def := a.def
	out := &Output{}

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
	src, err := source.New(def.Source.Type, srcCfg)
	if err != nil {
		return out, fmt.Errorf("source open: %w", err)
	}
	defer src.Close()

	// Resume from checkpoint.
	token, err := ckpt.Load(runID)
	if err != nil {
		return out, fmt.Errorf("checkpoint load: %w", err)
	}
	if token != nil && token.RowsProcessed > 0 {
		runLog.Infof("resuming — skipping %d already-processed rows", token.RowsProcessed)
		if skipErr := src.Skip(token.RowsProcessed); skipErr != nil {
			return out, fmt.Errorf("source skip: %w", skipErr)
		}
		out.RowsProcessed = token.RowsProcessed
	}

	// --- transforms ---
	// Build the user-declared transform configs.
	transformCfgs := make([]map[string]interface{}, 0, len(def.Transforms)+1)

	// Auto-prepend the schemadrift transform when a drift policy is configured.
	// The schemadrift package must be imported (blank import) alongside this
	// package in the main program; if it is not imported the Build call below
	// will return an "unknown type" error with a clear message.
	if def.SchemaDrift.Policy != "" {
		driftCfg := map[string]interface{}{
			"type":    "schemadrift",
			"policy":  def.SchemaDrift.Policy,
			"onDrift": def.SchemaDrift.OnDrift,
		}
		transformCfgs = append(transformCfgs, driftCfg)
	}

	for _, td := range def.Transforms {
		transformCfgs = append(transformCfgs, td.Extra)
	}

	txPipeline, err := transform.Build(transformCfgs)
	if err != nil {
		return out, fmt.Errorf("transforms build: %w", err)
	}
	defer txPipeline.Close()

	// Locate the hwm transform (if present) so the engine can read MaxSeen()
	// after each checkpoint interval. A nil hwmProvider means no hwm transform
	// was declared; watermark persistence is skipped silently.
	hwmProvider := findHWMTransform(txPipeline)

	// Locate the dq transform (if present) so the engine can include the last
	// DQ gate report in Output.Metadata after the pipeline completes.
	// A nil dqProvider means no dq transform was declared; metadata is skipped.
	dqProvider := findDQGateProvider(txPipeline)

	// --- sink ---
	snk, err := sink.New(def.Sink.Type, def.Sink.Options)
	if err != nil {
		return out, fmt.Errorf("sink open: %w", err)
	}
	defer snk.Close()

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
	var rowsSinceCkpt int64

	for {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}

		// --- read ---
		t0 := time.Now()
		batch, readErr := src.ReadBatch(batchSize)
		readDur := time.Since(t0)

		if readErr != nil && readErr != io.EOF {
			return out, fmt.Errorf("source read: %w", readErr)
		}
		done := readErr == io.EOF

		if batch != nil && batch.Len() > 0 {
			batchLen := int64(batch.Len())
			out.RowsProcessed += batchLen
			rowsSinceCkpt += batchLen

			// Emit read metric and check stall before touching the batch further.
			// Note: readDur includes backpressure wait time in any future buffered
			// pipeline design — this is intentional (wall-clock latency, not CPU time).
			a.recorder.BatchRead(a.ref, def.Source.Type, int(batchLen), readDur)
			a.checkStall(runLog, "read", readDur)

			// --- transform ---
			t1 := time.Now()
			transformed, txErr := txPipeline.Apply(batch)
			txDur := time.Since(t1)
			record.Put(batch)

			if txErr != nil {
				runLog.Warnf("transform error on batch of %d rows (routing to failed): %v", batchLen, txErr)
				a.recorder.BatchFailed(a.ref, int(batchLen))
				out.RowsFailed += batchLen
			} else if transformed == nil {
				// A nil batch with no error means the transform pipeline deliberately
				// dropped the batch (e.g. schemadrift policy=drop, or a filter that
				// eliminated all rows). Track these rows as dropped rather than failed
				// so callers can distinguish intentional drops from processing errors.
				a.recorder.BatchTransformed(a.ref, int(batchLen), 0, txDur)
				a.checkStall(runLog, "transform", txDur)
				out.RowsDropped += batchLen
			} else if transformed.Len() > 0 {
				txOut := int(transformed.Len())
				a.recorder.BatchTransformed(a.ref, int(batchLen), txOut, txDur)
				a.checkStall(runLog, "transform", txDur)

				// --- write (with per-row DLQ fallback) ---
				t2 := time.Now()
				written, writeErr := a.rowLevelWrite(runLog, snk, dlq, transformed, runID, out)
				writeDur := time.Since(t2)
				record.Put(transformed)
				if writeErr != nil {
					// rowLevelWrite only returns an error for unrecoverable problems
					// (e.g. the DLQ sink itself failed).  Individual row failures are
					// counted in out.RowsFailed and do not abort the loop.
					return out, writeErr
				}
				out.RowsWritten += int64(written)
				a.recorder.BatchWritten(a.ref, def.Sink.Type, written, writeDur)
				a.checkStall(runLog, "write", writeDur)
			} else {
				// transformed is non-nil but empty — all rows were filtered out and
				// the transform returned an empty batch rather than nil.
				a.recorder.BatchTransformed(a.ref, int(batchLen), 0, txDur)
				a.checkStall(runLog, "transform", txDur)
				record.Put(transformed)
			}

			// Checkpoint at configured row interval.
			if ckptIntervalRows > 0 && rowsSinceCkpt >= ckptIntervalRows {
				if err := snk.Flush(); err != nil {
					return out, fmt.Errorf("sink flush (checkpoint): %w", err)
				}
				saveCheckpoint(runLog, ckpt, runID, def.Source.Type, out.RowsProcessed, src)
				a.recorder.CheckpointSaved(a.ref)
				saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)
				rowsSinceCkpt = 0
			}
		}

		if done {
			break
		}
	}

	// Final flush.
	if err := snk.Flush(); err != nil {
		return out, fmt.Errorf("sink final flush: %w", err)
	}

	if def.Checkpoint.DeleteOnSuccess {
		_ = ckpt.Delete(runID)
	} else {
		saveCheckpoint(runLog, ckpt, runID, def.Source.Type, out.RowsProcessed, src)
		a.recorder.CheckpointSaved(a.ref)
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

	return out, nil
}

// buildSourceConfig merges definition config with runtime overrides.
// a.def.Source.Columns is []ColumnDef; we convert to []record.Column here so
// that parseColumnDefs in the csv (and other) source packages always receives
// the concrete type it already handles, avoiding a type-switch miss that would
// silently drop all explicit type/format/scale information.
func (a *DataPipelineAction) buildSourceConfig(in *Input) map[string]interface{} {
	cols := make([]record.Column, len(a.def.Source.Columns))
	for i, cd := range a.def.Source.Columns {
		cols[i] = record.Column{
			Name:   cd.Name,
			Type:   record.ColumnType(cd.Type),
			Format: cd.Format,
			Scale:  cd.Scale,
		}
	}
	cfg := map[string]interface{}{
		"type":     a.def.Source.Type,
		"encoding": a.def.Source.Encoding,
		"columns":  cols, // []record.Column — never a []ColumnDef type-switch miss
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
	}
	return checkpoint.New(cd.Type, cfg)
}

func saveCheckpoint(runLog corelog.Logger, ckpt checkpoint.Checkpoint, runID, format string, rowsProcessed int64, src source.Source) {
	var cp checkpoint.CursorProvider
	if p, ok := src.(checkpoint.CursorProvider); ok {
		cp = p
	}
	t := checkpoint.NewToken(format, rowsProcessed, cp)
	if err := ckpt.Save(runID, t); err != nil {
		runLog.Warnf("checkpoint save failed (continuing): %v", err)
	}
}

func newRunID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
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
// On batch-level error: falls back to writing each row individually in its own
// single-row batch.  Rows that succeed are accumulated into a "good" batch and
// written to snk at the end.  Rows that fail individually are accumulated into
// a "bad" batch and — when a DLQ sink is configured — forwarded to dlq via
// WriteBatchErrors so the error envelope carries the per-row message.
//
// rowLevelWrite always returns nil from the error position unless an
// unrecoverable infrastructure failure occurs (e.g. the DLQ sink itself
// returns an error).  Individual row failures are recorded in out.RowsFailed
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
	if batchErr := snk.WriteBatch(batch); batchErr == nil {
		return batch.Len(), nil
	} else {
		runLog.Warnf("batch write failed (%d rows), falling back to per-row isolation: %v",
			batch.Len(), batchErr)
	}

	// --- per-row isolation fallback ---
	// Build reusable single-row batches to avoid heap allocations in the loop.
	rowBatch := record.Get()
	rowBatch.Columns = batch.Columns
	rowBatch.Rows = make([][]interface{}, 1)

	goodBatch := record.Get()
	goodBatch.Columns = batch.Columns

	var badRows [][]interface{}
	var badErrs []string

	for i, row := range batch.Rows {
		rowBatch.Rows[0] = row

		if rowErr := snk.WriteBatch(rowBatch); rowErr == nil {
			goodBatch.Rows = append(goodBatch.Rows, row)
		} else {
			runLog.Debugf("row %d failed write (routing to DLQ): %v", i, rowErr)
			badRows = append(badRows, row)
			badErrs = append(badErrs, rowErr.Error())
			out.RowsFailed++
		}
	}

	// Release the single-row scratch batch — its Rows slice must not be
	// pooled because it was reused across iterations and may alias batch.Rows.
	rowBatch.Columns = nil
	rowBatch.Rows = nil
	record.Put(rowBatch)

	// Write the accumulated good rows to the primary sink.
	if len(goodBatch.Rows) > 0 {
		if flushErr := snk.WriteBatch(goodBatch); flushErr != nil {
			// Even the good-rows consolidated write failed; treat them all as failed
			// rather than masking the error, and route to DLQ with the consolidated msg.
			runLog.Warnf("consolidated good-rows write failed (%d rows): %v",
				len(goodBatch.Rows), flushErr)
			for _, r := range goodBatch.Rows {
				badRows = append(badRows, r)
				badErrs = append(badErrs, flushErr.Error())
				out.RowsFailed++
			}
			written = 0
		} else {
			written = len(goodBatch.Rows)
		}
	}
	record.Put(goodBatch)

	// Route bad rows to DLQ.
	if len(badRows) > 0 && dlq != nil {
		badBatch := record.Get()
		badBatch.Columns = batch.Columns
		badBatch.Rows = badRows

		if dlqErr := dlq.WriteBatchErrors(badBatch, badErrs); dlqErr != nil {
			// DLQ write failure is an unrecoverable infrastructure problem —
			// surface it so the caller can abort the pipeline rather than silently
			// losing the error evidence.
			record.Put(badBatch)
			return written, fmt.Errorf("dlq write: %w", dlqErr)
		}
		record.Put(badBatch)
		runLog.Infof("routed %d failed rows to DLQ", len(badRows))
	} else if len(badRows) > 0 {
		runLog.Warnf("%d rows failed write and no DLQ is configured — rows are lost", len(badRows))
	}

	return written, nil
}
