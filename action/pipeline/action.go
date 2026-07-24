package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/app/resource"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/engine"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
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

	// transformWorkers > 1 requests multi-core transform parallelism. It is only
	// safe when transforms are STATELESS and there is no ordered-cursor state to
	// protect, i.e. no HWM/DQ/schemadrift accumulator and no checkpointing. When
	// eligible we run an ordered-parallel path: N worker goroutines each own a
	// private transform pipeline, and a single collector reassembles results in
	// the original batch order before writing — so the sink write and all
	// counters stay single-threaded (no data race) while transforms use N cores.
	// When the constraints don't hold we keep transform single-threaded; reads
	// are still overlapped with transform+write by the read-ahead goroutine.
	txWorkers := def.Performance.TransformWorkers
	if txWorkers < 1 {
		txWorkers = 1
	}
	parallelEligible := txWorkers > 1 &&
		hwmProvider == nil && dqProvider == nil && winOp == nil &&
		def.SchemaDrift.Policy == "" &&
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

		var rowsSinceCkpt int64
		lastCkpt := time.Now()

		// Read-ahead: a dedicated goroutine reads batches into readCh while the
		// main goroutine transforms+writes. A byte semaphore enforces maxMemoryMB.
		type readItem struct {
			batch   *record.RecordBatch
			readDur time.Duration
			err     error
		}
		readCh := make(chan readItem, bufDepth)
		var inFlight int64 // bytes currently buffered (atomic)
		readCtx, cancelRead := context.WithCancel(ctx)
		defer cancelRead()

		go func() {
			defer close(readCh)
			for {
				if readCtx.Err() != nil {
					return
				}
				// Backpressure: block while the buffer already holds ~maxMemoryMB.
				for maxInFlightBytes > 0 && atomic.LoadInt64(&inFlight) >= maxInFlightBytes {
					if readCtx.Err() != nil {
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
				t0 := time.Now()
				b, rErr := pSrc.ReadBatch(batchSize)
				rd := time.Since(t0)
				if b != nil && b.Len() > 0 {
					atomic.AddInt64(&inFlight, batchBytes(b))
				}
				select {
				case readCh <- readItem{batch: b, readDur: rd, err: rErr}:
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

			if batch != nil && batch.Len() > 0 {
				bBytes := batchBytes(batch)
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
				atomic.AddInt64(&inFlight, -bBytes)

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

					// --- write (with per-row DLQ fallback) ---
					t2 := time.Now()
					written, writeErr := a.rowLevelWrite(runLog, snk, dlq, transformed, runID, out)
					writeDur := time.Since(t2)
					record.Put(transformed)
					if writeErr != nil {
						return writeErr
					}
					out.RowsWritten += int64(written)
					a.recorder.BatchWritten(a.ref, def.Sink.Type, written, writeDur)
					a.checkStall(runLog, "write", writeDur)
				} else {
					a.recorder.BatchTransformed(a.ref, int(batchLen), 0, txDur)
					a.checkStall(runLog, "transform", txDur)
					record.Put(transformed)
				}

				// Checkpoint at configured row interval or time interval.
				rowTrigger := ckptIntervalRows > 0 && rowsSinceCkpt >= ckptIntervalRows
				timeTrigger := ckptIntervalSecs > 0 && time.Since(lastCkpt) >= time.Duration(ckptIntervalSecs)*time.Second
				if rowTrigger || timeTrigger {
					if err := snk.Flush(); err != nil {
						return fmt.Errorf("sink flush (checkpoint): %w", err)
					}
					saveCheckpoint(runLog, ckpt, ckptKey, def.Source.Type, out.RowsProcessed, pSrc)
					a.recorder.CheckpointSaved(a.ref)
					saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)
					// Surface the checkpoint so operators can see progress being
					// persisted and which trigger fired (row count vs. time).
					reason := "rows"
					if timeTrigger && !rowTrigger {
						reason = "time"
					}
					runLog.Infof("checkpoint saved — %d rows processed (trigger=%s)", out.RowsProcessed, reason)
					rowsSinceCkpt = 0
					lastCkpt = time.Now()
				}
			}
		}
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
		}
		type job struct {
			seq     int64
			batch   *record.RecordBatch
			readDur time.Duration
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

		var inFlight int64
		go func() {
			defer close(jobCh)
			var seq int64
			for {
				if wctx.Err() != nil {
					return
				}
				for maxInFlightBytes > 0 && atomic.LoadInt64(&inFlight) >= maxInFlightBytes {
					if wctx.Err() != nil {
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
				t0 := time.Now()
				b, rErr := pSrc.ReadBatch(batchSize)
				rd := time.Since(t0)
				if b != nil && b.Len() > 0 {
					atomic.AddInt64(&inFlight, batchBytes(b))
					select {
					case jobCh <- job{seq: seq, batch: b, readDur: rd}:
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
					t1 := time.Now()
					tOut, err := p.Apply(j.batch)
					resCh <- result{
						seq: j.seq, src: j.batch, transformed: tOut,
						batchLen: int64(j.batch.Len()), txErr: err,
						txDur: time.Since(t1), readDur: j.readDur,
					}
				}
			}(pipes[i])
		}
		go func() { wg.Wait(); close(resCh) }()

		pending := make(map[int64]result)
		var next int64
		var writeErr error
		process := func(r result) {
			bBytes := batchBytes(r.src)
			out.RowsProcessed += r.batchLen
			a.recorder.BatchRead(a.ref, def.Source.Type, int(r.batchLen), r.readDur)
			a.checkStall(runLog, "read", r.readDur)
			if r.transformed != r.src {
				record.Put(r.src)
			}
			atomic.AddInt64(&inFlight, -bBytes)
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
		return writeErr
	}

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
		if err := drain(src, runID); err != nil {
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
		if lc := winOp.LateCount(); lc > 0 {
			runLog.Infof("window: %d late record(s) dropped (onLate=drop)", lc)
		}
	}

	// Final flush.
	if err := snk.Flush(); err != nil {
		return out, fmt.Errorf("sink final flush: %w", err)
	}

	// Explicitly close the sink and surface any error. This is critical for sinks
	// whose durable write happens at Close (e.g. the object-store sink uploads the
	// buffered object on Close) — relying only on the deferred Close would discard
	// that error and mis-report a failed upload as a successful run. Closing here
	// makes the deferred snk.Close() a harmless no-op (idempotent close).
	if err := snk.Close(); err != nil {
		return out, fmt.Errorf("sink close: %w", err)
	}
	sinkClosed = true

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

// batchBytes estimates the in-memory size of a batch for the maxMemoryMB
// backpressure budget. It is a cheap heuristic — a fixed per-cell estimate — not
// an exact sizeof; the goal is proportional accounting to bound the read-ahead
// buffer, not precise memory measurement.
func batchBytes(b *record.RecordBatch) int64 {
	if b == nil || len(b.Rows) == 0 {
		return 0
	}
	const bytesPerCell = 32 // rough average across numeric/string cells
	cols := int64(len(b.Columns))
	if cols == 0 && len(b.Rows) > 0 {
		cols = int64(len(b.Rows[0]))
	}
	return int64(len(b.Rows)) * cols * bytesPerCell
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
