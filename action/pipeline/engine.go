package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	dqtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	hwmtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
	wintransform "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// execute is the pipeline's single execution engine. Every definition is
// compiled to a graphexec plan and run by the same dataflow runtime:
//
//   - a linear definition becomes source → chain → sink, where chain is one
//     fused node running the whole transform pipeline (see chainOp);
//   - an explicit DAG (fan-out/fan-in, typed edges) is compiled node by node.
//
// Run semantics — checkpoints, watermarks, DLQ, metrics, scaling — are the
// same for both shapes. See docs/design/engine-unification.md.
func (a *DataPipelineAction) execute(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (*Output, error) {
	def := a.def
	out := &Output{}

	// A run cancelled before it starts must fail deterministically rather
	// than report an empty "completed" run.
	if err := ctx.Err(); err != nil {
		return out, err
	}

	dag := graphExecutable(def)
	if dag {
		runLog.Infof("executing DAG — %d nodes, %d edges", len(def.Graph.Nodes), len(def.Graph.Edges))
	}
	runLog.Infof("run config — mode=%s batchSize=%d bufferBatches=%d maxMemoryMB=%d transformWorkers=%d scaling=%s checkpoint=%s(rows=%d,secs=%d) incremental=%s metrics=%s",
		orDefault(def.Mode, "etl"), def.Performance.BatchSize, def.Performance.SourceBufferBatches,
		def.Performance.MaxMemoryMB, orDefaultInt(def.Performance.TransformWorkers, 1),
		orDefault(def.Scaling.Mode, "single"), orDefault(def.Checkpoint.Type, "none"),
		def.Checkpoint.IntervalRows, def.Checkpoint.IntervalSeconds,
		orDefault(def.Incremental.Mode, "none"), orDefault(def.Observability.Metrics, "none"))

	scMode := orDefault(def.Scaling.Mode, "single")
	scaled := scMode == "sharded" || scMode == "distributed"
	if scaled && dag && sourceNodeSelfContained(def) {
		runLog.Warnf("scaling.mode=%s needs the definition's source as the DAG's \"source\" node to partition it — running single-instance", scMode)
		scaled = false
	}
	checkpointing := def.Checkpoint.Type != "" && def.Checkpoint.Type != "none"
	resumeKey := runID
	if def.Checkpoint.Key != "" {
		resumeKey = def.Checkpoint.Key
	}

	// --- checkpoint + watermark stores (both no-ops when disabled) ---
	ckpt, err := openCheckpoint(def)
	if err != nil {
		return out, fmt.Errorf("checkpoint open: %w", err)
	}
	defer ckpt.Close()
	wmStore, err := openWatermarkStore(def)
	if err != nil {
		return out, fmt.Errorf("watermark store open: %w", err)
	}
	defer wmStore.Close()
	priorMark, err := a.loadPriorMark(runLog, wmStore)
	if err != nil {
		return out, err
	}
	if priorMark != nil && dag && scaled {
		runLog.Warnf("incremental.mode=hwm is not supported for a DAG with scaling.mode=%s — the watermark will not be applied or advanced", scMode)
		priorMark = nil
	}

	// --- primary source (the incremental / checkpoint cursor) ---
	srcCfg := a.buildSourceConfig(in)
	if priorMark != nil {
		srcCfg["hwmColumn"] = priorMark.Column
		srcCfg["hwmValue"] = priorMark.Value
		srcCfg["hwmValueType"] = priorMark.ValueType
	}
	// Checkpoint-mode Kafka: seek to the exact processed offsets recorded in
	// the token (captured atomically with any window state), independent of
	// whether the last external offset commit succeeded.
	if def.Source.Type == "kafka" && !scaled {
		if tok, tErr := ckpt.Load(resumeKey); tErr == nil && tok != nil && len(tok.Payload) > 0 {
			srcCfg["resumeCursor"] = tok.Payload
		}
	}
	var src source.Source
	if !dag || !sourceNodeSelfContained(def) {
		src, err = source.New(def.Source.Type, srcCfg)
		if err != nil {
			return out, fmt.Errorf("source open: %w", err)
		}
		defer src.Close()
		if ra, ok := src.(metrics.RecorderAware); ok {
			ra.SetRecorder(a.recorder)
		}
	}

	// --- shared run state ---
	env := &runEnv{a: a, runLog: runLog, runID: runID, counts: &runCounters{}}
	var dlq dlqWriteSink
	if d, derr := openDLQ(runLog, def, a.ref, runID); derr != nil {
		return out, fmt.Errorf("dlq open: %w", derr)
	} else if d != nil {
		dlq = &lockedDLQ{inner: d}
		defer dlq.Close()
	}

	// --- compile the plan ---
	var metered *meteredSource
	if src != nil {
		metered = &meteredSource{Source: src, env: env, typ: def.Source.Type}
	}
	var eg *graphexec.Graph
	var chain *chainOp
	if dag {
		eg, err = a.buildExecGraph(in, nil)
		if err != nil {
			return out, fmt.Errorf("graph build: %w", err)
		}
		for i := range eg.Nodes {
			n := &eg.Nodes[i]
			if n.ID != "source" || n.Kind != graphexec.KindSource {
				continue
			}
			if metered != nil {
				n.PreBuilt = metered
			} else if priorMark != nil && n.Cfg != nil {
				n.Cfg["hwmColumn"] = priorMark.Column
				n.Cfg["hwmValue"] = priorMark.Value
				n.Cfg["hwmValueType"] = priorMark.ValueType
			}
		}
	} else {
		eg, chain, err = a.buildLinearGraph(runLog, env, metered, dlq)
		if err != nil {
			return out, err
		}
	}
	plan, err := graphexec.Compile(eg, a.engineBuilders(env, dlq))
	if err != nil {
		if chain != nil && len(eg.Nodes) > 0 {
			// Compile closes what it built; the prebuilt chain/sink are ours.
			closePrebuilt(eg)
		}
		return out, fmt.Errorf("graph compile: %w", err)
	}
	defer plan.Close()
	// A stateless plan can finish cleanly on shutdown (sources report end of
	// input, everything read is written, final checkpoint). A stateful plan
	// would flush half-open windows instead; it is cancelled and resumes from
	// its checkpointed state.
	if !plan.HasStateful() {
		env.drain = drainSignal(ctx)
	}

	primary := source.Source(src)
	if primary == nil {
		if srcs := plan.Sources(); len(srcs) > 0 {
			primary, _ = unwrapSource(srcs[0]).(source.Source)
		}
	}
	var primarySink sink.Sink
	if s, ok := plan.Sinks()["sink"]; ok {
		primarySink, _ = unwrapSink(s).(sink.Sink)
	}
	hwmProvider, dqProvider, winOp := planCapabilities(plan)
	snapshotters := plan.Snapshotters()
	singleSource := len(plan.Sources()) == 1

	base := graphexec.Options{
		BatchSize:       def.Performance.BatchSize,
		BufferDepth:     def.Performance.SourceBufferBatches,
		SinkBufferDepth: def.Performance.SinkBufferBatches,
		KeepOpen:        true,
		// Linear scaled runs flush stateful state once after the last
		// partition (flushDeferred); DAG nodes flush per partition.
		DeferFlush: scaled && !dag,
	}
	if def.Performance.MaxMemoryMB > 0 {
		base.SizeOf = batchBytes
	}
	if scaled && plan.HasStateful() && checkpointing {
		runLog.Warnf("checkpoint/resume is not supported for a stateful (windowed) pipeline with scaling.mode=%s — running without mid-run checkpoints", scMode)
	}

	nodeTotals := map[string]*graphexec.NodeStat{}
	mergeStats := func(res *graphexec.Result) {
		for id, ns := range res.Nodes {
			t, ok := nodeTotals[id]
			if !ok {
				t = &graphexec.NodeStat{ID: ns.ID, Kind: ns.Kind}
				nodeTotals[id] = t
			}
			t.RowsIn += ns.RowsIn
			t.RowsOut += ns.RowsOut
			t.RowsWritten += ns.RowsWritten
			t.RowsErrored += ns.RowsErrored
		}
	}

	// runPartition runs the plan over one source (the primary source, or one
	// partition of it in scaled modes) under checkpoint key ckptKey.
	runPartition := func(pSrc source.Source, ckptKey string) error {
		opts := base
		// A fresh budget per Run: a failed partition can leave charged batches
		// in its channels, which must not starve the next partition.
		if mb := def.Performance.MaxMemoryMB; mb > 0 {
			opts.Budget = graphexec.NewMemoryBudget(int64(mb) << 20)
		}
		if scaled {
			if rerr := plan.RewireSource("source", &meteredSource{Source: pSrc, env: env, typ: def.Source.Type}); rerr != nil {
				return fmt.Errorf("rewire source: %w", rerr)
			}
		}
		stateful := plan.HasStateful()
		if checkpointing {
			tok, lErr := ckpt.Load(ckptKey)
			if lErr != nil {
				return fmt.Errorf("checkpoint load: %w", lErr)
			}
			if tok != nil && tok.RowsProcessed > 0 {
				runLog.Infof("resuming %q — skipping %d already-processed rows", ckptKey, tok.RowsProcessed)
				opts.ResumeFrom = tok.RowsProcessed
			}
			if tok != nil && len(tok.WindowState) > 0 && (!scaled || !stateful) {
				if rerr := restoreState(runLog, snapshotters, tok.WindowState, dag); rerr != nil {
					return rerr
				}
			}
			if (!scaled || !stateful) && singleSource {
				opts.CheckpointRows = def.Checkpoint.IntervalRows
				opts.CheckpointInterval = time.Duration(def.Checkpoint.IntervalSeconds) * time.Second
				opts.HasUncommitted = func() bool { return hasUncommitted(pSrc) }
				opts.OnCheckpoint = func(pos int64) error {
					state, serr := snapshotState(snapshotters, dag)
					if serr != nil {
						// Advancing the position without operator state would
						// lose its buffered input on resume.
						return checkpointFailed(runLog, def, "state snapshot", serr)
					}
					token := checkpoint.NewToken(def.Source.Type, pos, cursorOf(pSrc, pos-opts.ResumeFrom))
					token.WindowState = state
					if serr := ckpt.Save(ckptKey, token); serr != nil {
						return checkpointFailed(runLog, def, "checkpoint save", serr)
					}
					a.recorder.CheckpointSaved(a.ref)
					runLog.Infof("checkpoint saved — %d rows processed", pos)
					// Every row up to pos is flushed and the token is durable:
					// acknowledge the source (e.g. Kafka offsets). Commit takes
					// the run-local processed row count.
					if c, ok := pSrc.(source.Committer); ok {
						if cerr := c.Commit(pos - opts.ResumeFrom); cerr != nil {
							runLog.Warnf("source commit failed (continuing): %v", cerr)
						}
					}
					// The watermark is NOT advanced here: a resume re-runs the
					// source with the prior mark and skips pos rows, so an
					// advanced mark would make it skip unprocessed rows.
					a.emitWindowMetrics(winOp)
					return nil
				}
			}
		}
		res, runErr := plan.Run(ctx, opts)
		mergeStats(res)
		out.RowsProcessed += res.RowsRead + opts.ResumeFrom
		if runErr != nil {
			return runErr
		}
		drained := env.wasDrained()
		// Scaled runs persist each partition's final position so a restart
		// skips completed partitions' rows. A partition stopped by shutdown
		// keeps its position (never deleted) and is reported unfinished, so the
		// scaling driver does not mark it done.
		if scaled && checkpointing && !stateful {
			if def.Checkpoint.DeleteOnSuccess && !drained {
				_ = ckpt.Delete(ckptKey)
			} else if serr := ckpt.Save(ckptKey, checkpoint.NewToken(def.Source.Type, opts.ResumeFrom+res.RowsRead, nil)); serr != nil {
				if ferr := checkpointFailed(runLog, def, fmt.Sprintf("final checkpoint for partition %q", ckptKey), serr); ferr != nil {
					return ferr
				}
			} else {
				a.recorder.CheckpointSaved(a.ref)
			}
		}
		if drained {
			return errDrained
		}
		return nil
	}

	if scaled {
		err = a.runScaled(ctx, runLog, def, scMode, src, srcCfg, runID, runPartition)
	} else {
		err = runPartition(primary, resumeKey)
	}
	if dag {
		gs := make([]graphexec.NodeStat, 0, len(nodeTotals))
		for _, ns := range nodeTotals {
			gs = append(gs, *ns)
		}
		out.Metadata = map[string]interface{}{"graphStats": gs}
	}
	if err == nil && env.wasDrained() {
		err = errDrained
	}
	if errors.Is(err, errDrained) && !scaled {
		// Stopped by shutdown after draining: everything read is written.
		// Save the exact position so a restart resumes there — but this is
		// not a complete run: no deleteOnSuccess, watermark or reconciliation.
		env.counts.addTo(out)
		if cerr := plan.CloseSinks(); cerr != nil {
			return out, fmt.Errorf("sink close: %w", cerr)
		}
		if checkpointing {
			if cerr := a.saveFinalCheckpoint(runLog, ckpt, resumeKey, out.RowsProcessed, primary, snapshotters, dag); cerr != nil {
				runLog.Warnf("final checkpoint after drain failed (a restart replays from the previous one): %v", cerr)
			} else {
				a.recorder.CheckpointSaved(a.ref)
			}
		} else if c, ok := primary.(source.Committer); ok && primary != nil {
			// Everything read is written: acknowledge it, as at a clean end.
			if cerr := c.Commit(math.MaxInt64); cerr != nil {
				runLog.Warnf("source commit failed (continuing): %v", cerr)
			}
		}
		return out, err
	}
	if err != nil {
		env.counts.addTo(out)
		return out, err
	}

	// --- clean end of input ---
	// Scaled runs defer the stateful flush to here, once, across partitions.
	if scaled && chain != nil {
		if f, ok := interface{}(chainNode(plan)).(graphexec.Flusher); ok {
			if ferr := a.flushDeferred(ctx, f, plan, env); ferr != nil {
				env.counts.addTo(out)
				return out, ferr
			}
		}
	}
	// Sink counters are final now (the deferred flush above writes through
	// the managed sink too).
	env.counts.addTo(out)
	if winOp != nil {
		a.recordWindowStats(runLog, winOp, out)
	}

	// Reconciliation reuses the sink's connection: after the final flush (done
	// by the engine at end of input) and before Close.
	if primary != nil && primarySink != nil {
		a.runReconciliation(runLog, def, primary, primarySink, out)
	}
	// Close sinks explicitly so errors that only occur at Close (e.g. an
	// object-store upload) fail the run instead of being dropped.
	if cerr := plan.CloseSinks(); cerr != nil {
		return out, fmt.Errorf("sink close: %w", cerr)
	}
	if reporter, ok := primarySink.(sink.StatsReporter); ok {
		if st := reporter.SinkStats(); st.RowsDeleted >= 0 {
			out.RowsDeleted = st.RowsDeleted
			ensureMetadata(out)["cdcStats"] = map[string]interface{}{"rowsDeleted": st.RowsDeleted}
		}
	}

	// Final checkpoint (scaled runs saved theirs per partition).
	if checkpointing && !scaled {
		if def.Checkpoint.DeleteOnSuccess {
			if derr := ckpt.Delete(resumeKey); derr != nil {
				runLog.Warnf("checkpoint delete failed (continuing; a stale checkpoint may remain): %v", derr)
			}
		} else if cerr := a.saveFinalCheckpoint(runLog, ckpt, resumeKey, out.RowsProcessed, primary, snapshotters, dag); cerr != nil {
			if ferr := checkpointFailed(runLog, def, "final checkpoint", cerr); ferr != nil {
				return out, ferr
			}
		} else {
			a.recorder.CheckpointSaved(a.ref)
		}
	} else if !checkpointing && !scaled && primary != nil {
		// Without a checkpoint store, still acknowledge a committing source
		// once everything is written (e.g. Kafka commitMode checkpoint is
		// rejected without a store, but other committers may opt in).
		if c, ok := primary.(source.Committer); ok {
			if cerr := c.Commit(math.MaxInt64); cerr != nil {
				runLog.Warnf("source commit failed (continuing): %v", cerr)
			}
		}
	}

	// The watermark survives deleteOnSuccess so the next run stays incremental.
	saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)
	if dqProvider != nil {
		if report := dqProvider.LastRunReport(); report != nil {
			report.PipelineRef = a.ref
			ensureMetadata(out)["dqReport"] = report
		}
	}
	if chain != nil && len(chainReplicas(eg)) == 0 {
		if stats := chain.p.StageStats(); len(stats) > 0 {
			ensureMetadata(out)["stageStats"] = stats
		}
	}
	if dag {
		runLog.Infof("graph run complete — nodes=%d edges=%d read=%d written=%d",
			len(def.Graph.Nodes), len(def.Graph.Edges), out.RowsProcessed, out.RowsWritten)
	}
	return out, nil
}

// buildLinearGraph compiles a linear definition to source → chain → sink.
func (a *DataPipelineAction) buildLinearGraph(runLog corelog.Logger, env *runEnv, src *meteredSource, dlq dlqWriteSink) (*graphexec.Graph, *chainOp, error) {
	def := a.def
	cfgs := a.linearTransformConfigs(runLog)

	snk, err := sink.New(def.Sink.Type, def.Sink.Options)
	if err != nil {
		return nil, nil, fmt.Errorf("sink open: %w", err)
	}
	if ra, ok := snk.(metrics.RecorderAware); ok {
		ra.SetRecorder(a.recorder)
	}
	sinkNode := graphexec.Node{ID: "sink", Kind: graphexec.KindSink, Type: def.Sink.Type,
		Operator: &managedSink{snk: snk, dlq: dlq, env: env, typ: def.Sink.Type}}
	g := &graphexec.Graph{Nodes: []graphexec.Node{{ID: "source", Kind: graphexec.KindSource, Type: def.Source.Type, PreBuilt: src}}}
	if len(cfgs) == 0 {
		g.Nodes = append(g.Nodes, sinkNode)
		g.Edges = []graphexec.Edge{{From: "source", To: "sink"}}
		return g, nil, nil
	}

	p, err := transform.Build(cfgs)
	if err != nil {
		_ = snk.Close()
		return nil, nil, fmt.Errorf("transforms build: %w", err)
	}
	op := newChainOp(p, env, dlq)
	chain := chainOf(op)

	// transformWorkers > 1: ordered parallel replicas of the chain, when every
	// stage is stateless — no HWM/DQ/window accumulator, no schema-drift
	// tracking and no transform writing to its own sink (duplicate writers).
	var replicas []graphexec.TransformOp
	workers := def.Performance.TransformWorkers
	if workers > 1 {
		h, d, w := capabilitiesOf(p)
		eligible := h == nil && d == nil && w == nil && def.SchemaDrift.Policy == "" && !pipelineHasSinkOwner(p)
		if eligible {
			for i := 1; i < workers; i++ {
				rp, rerr := transform.Build(cfgs)
				if rerr != nil {
					for _, r := range replicas {
						_ = r.Close()
					}
					_ = p.Close()
					_ = snk.Close()
					return nil, nil, fmt.Errorf("transforms build (worker %d): %w", i, rerr)
				}
				replicas = append(replicas, newChainOp(rp, env, dlq))
			}
			runLog.Infof("transformWorkers=%d — parallel transform across %d workers with ordered output", workers, workers)
		} else {
			runLog.Infof("transformWorkers=%d — stateful or sink-owning transforms require ordered single-threaded processing", workers)
		}
	}
	g.Nodes = append(g.Nodes, graphexec.Node{ID: "chain", Kind: graphexec.KindTransform, Type: "chain", Operator: op, Replicas: replicas}, sinkNode)
	g.Edges = []graphexec.Edge{{From: "source", To: "chain"}, {From: "chain", To: "sink"}}
	return g, chain, nil
}

// linearTransformConfigs assembles the linear chain: optional schemadrift
// first, the declared transforms, then a contract dq gate — none of them in
// el/elt mode (transforms are not run in flight).
func (a *DataPipelineAction) linearTransformConfigs(runLog corelog.Logger) []map[string]interface{} {
	def := a.def
	mode := orDefault(def.Mode, "etl")
	inFlight := mode == "etl"
	if !inFlight && (len(def.Transforms) > 0 || def.SchemaDrift.Policy != "") {
		if mode == "elt" {
			runLog.Infof("mode=elt — %d declared transform(s) are NOT run in-flight; they are expected to execute in the target system", len(def.Transforms))
		} else {
			runLog.Warnf("mode=el — raw extract-load; %d declared transform(s) will be ignored", len(def.Transforms))
		}
	}
	if !inFlight {
		return nil
	}
	cfgs := make([]map[string]interface{}, 0, len(def.Transforms)+2)
	if def.SchemaDrift.Policy != "" {
		cfgs = append(cfgs, map[string]interface{}{"type": "schemadrift", "policy": def.SchemaDrift.Policy, "onDrift": def.SchemaDrift.OnDrift})
	}
	for _, td := range def.Transforms {
		cfgs = append(cfgs, td.Extra)
	}
	if def.Contract != nil && len(def.Contract.Expectations) > 0 {
		hasDQ := false
		for _, td := range def.Transforms {
			if td.Type == "dq" {
				hasDQ = true
				break
			}
		}
		if hasDQ {
			runLog.Infof("contract.expectations declared but the pipeline already has an explicit dq transform — not auto-appending a second gate")
		} else {
			c := map[string]interface{}{
				"type": "dq", "expectations": def.Contract.Expectations,
				"qualityThreshold": def.Contract.QualityThreshold, "onFail": def.Contract.OnViolation,
			}
			if def.Contract.ErrorSinkType != "" {
				c["errorSinkType"] = def.Contract.ErrorSinkType
				c["errorSinkConfig"] = def.Contract.ErrorSinkConfig
			}
			cfgs = append(cfgs, c)
		}
	}
	return cfgs
}

// engineBuilders constructs DAG node operators with the run's wrappers:
// metered sources and managed (DLQ-isolating) sinks.
func (a *DataPipelineAction) engineBuilders(env *runEnv, dlq dlqWriteSink) graphexec.Builders {
	return graphexec.Builders{
		Source: func(typ string, cfg map[string]interface{}) (graphexec.SourceOp, error) {
			s, err := source.New(typ, cfg)
			if err != nil {
				return nil, err
			}
			if ra, ok := s.(metrics.RecorderAware); ok {
				ra.SetRecorder(a.recorder)
			}
			return &meteredSource{Source: s, env: env, typ: typ}, nil
		},
		Transform: func(typ string, cfg map[string]interface{}) (graphexec.TransformOp, error) {
			return transform.New(typ, cfg)
		},
		Sink: func(typ string, cfg map[string]interface{}) (graphexec.SinkOp, error) {
			s, err := sink.New(typ, cfg)
			if err != nil {
				return nil, err
			}
			if ra, ok := s.(metrics.RecorderAware); ok {
				ra.SetRecorder(a.recorder)
			}
			return &managedSink{snk: s, dlq: dlq, env: env, typ: typ}, nil
		},
		Predicate: func(expr string) (graphexec.Pred, error) {
			p, err := filter.Compile(expr)
			if err != nil {
				return nil, err
			}
			return p.Eval, nil
		},
		BindPredicate: func(expr string) (graphexec.PredBinder, error) {
			p, err := filter.Compile(expr)
			if err != nil {
				return nil, err
			}
			return func(cols []record.Column) func([]interface{}) bool {
				return p.Bind(cols).EvalRow
			}, nil
		},
	}
}

// loadPriorMark loads the incremental high-water mark (nil when disabled or
// absent), synthesising one from incremental.initialValue on the first run.
func (a *DataPipelineAction) loadPriorMark(runLog corelog.Logger, wmStore watermark.Store) (*watermark.Mark, error) {
	def := a.def
	if def.Incremental.Mode != "hwm" {
		return nil, nil
	}
	mark, err := wmStore.Load(a.ref)
	if err != nil {
		return nil, fmt.Errorf("watermark load: %w", err)
	}
	if mark == nil && def.Incremental.InitialValue != "" {
		mark = &watermark.Mark{
			Column: def.Incremental.Column, Value: def.Incremental.InitialValue,
			ValueType: def.Incremental.ValueType, SavedAt: time.Now().UTC(),
		}
	}
	if mark != nil {
		runLog.Infof("incremental load — %s > %s (type: %s)", mark.Column, mark.Value, mark.ValueType)
	} else {
		runLog.Infof("incremental load — no prior mark, performing full-table load")
	}
	return mark, nil
}

// planCapabilities finds the HWM, DQ and window operators anywhere in the plan
// (inside a fused chain or as standalone nodes).
func planCapabilities(plan *graphexec.Plan) (hwmtransform.HWMProvider, dqtransform.DQGateProvider, wintransform.WindowedOperator) {
	var h hwmtransform.HWMProvider
	var d dqtransform.DQGateProvider
	var w wintransform.WindowedOperator
	for _, op := range plan.Transforms() {
		var ch, cd, cw = capabilitiesOfOp(op)
		if h == nil {
			h = ch
		}
		if d == nil {
			d = cd
		}
		if w == nil {
			w = cw
		}
	}
	return h, d, w
}

func capabilitiesOfOp(op graphexec.TransformOp) (hwmtransform.HWMProvider, dqtransform.DQGateProvider, wintransform.WindowedOperator) {
	if c := chainOf(op); c != nil {
		return capabilitiesOf(c.p)
	}
	h, _ := op.(hwmtransform.HWMProvider)
	d, _ := op.(dqtransform.DQGateProvider)
	w, _ := op.(wintransform.WindowedOperator)
	return h, d, w
}

func capabilitiesOf(p *transform.Pipeline) (hwmtransform.HWMProvider, dqtransform.DQGateProvider, wintransform.WindowedOperator) {
	return findHWMTransform(p), findDQGateProvider(p), findWindowedOperator(p)
}

// chainOf returns the chainOp behind a (possibly windowed) chain operator.
func chainOf(op interface{}) *chainOp {
	switch c := op.(type) {
	case *chainOp:
		return c
	case *windowedChainOp:
		return c.chainOp
	}
	return nil
}

// chainNode returns the plan's chain operator (linear plans only).
func chainNode(plan *graphexec.Plan) graphexec.TransformOp {
	for _, op := range plan.Transforms() {
		if chainOf(op) != nil {
			return op
		}
	}
	return nil
}

func chainReplicas(g *graphexec.Graph) []graphexec.TransformOp {
	for _, n := range g.Nodes {
		if n.ID == "chain" {
			return n.Replicas
		}
	}
	return nil
}

// closePrebuilt closes operators handed to a graph that failed to compile.
func closePrebuilt(g *graphexec.Graph) {
	for _, n := range g.Nodes {
		if c, ok := n.Operator.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		for _, r := range n.Replicas {
			_ = r.Close()
		}
	}
}

// flushDeferred emits a stateful chain's remaining output (scaled runs flush
// once after the last partition) through the primary sink.
func (a *DataPipelineAction) flushDeferred(ctx context.Context, f graphexec.Flusher, plan *graphexec.Plan, env *runEnv) error {
	// The last Plan.Run cancelled the context it bound to the chain; the
	// flushed rows pass through later stages (e.g. lookup), so re-bind them
	// to the still-live run context.
	if cb, ok := f.(interface{ BindContext(context.Context) }); ok {
		cb.BindContext(ctx)
	}
	b, err := f.Flush()
	if err != nil {
		return fmt.Errorf("window flush: %w", err)
	}
	s := plan.Sinks()["sink"]
	if b == nil || s == nil {
		return nil
	}
	defer record.Put(b)
	if b.Len() > 0 {
		if werr := s.WriteBatch(b); werr != nil {
			return werr
		}
	}
	if ferr := s.Flush(); ferr != nil {
		return fmt.Errorf("sink final flush: %w", ferr)
	}
	return nil
}

// recordWindowStats logs and records the window operator's end-of-run stats.
func (a *DataPipelineAction) recordWindowStats(runLog corelog.Logger, winOp wintransform.WindowedOperator, out *Output) {
	st := winOp.Stats()
	runLog.Infof("window stats — emitted=%d openAtEnd=%d lateDropped=%d evicted=%d spillFailures=%d watermark=%dms",
		st.WindowsEmitted, st.OpenWindows, st.LateDropped, st.StateEvicted, st.SpillFailures, st.WatermarkMillis)
	if st.SpillFailures > 0 {
		runLog.Warnf("window state: %d keys dropped because spill-to-disk failed — results may be incomplete (check disk space/permissions)", st.SpillFailures)
	}
	ensureMetadata(out)["windowStats"] = map[string]interface{}{
		"windowsEmitted": st.WindowsEmitted, "lateDropped": st.LateDropped, "stateEvicted": st.StateEvicted,
		"openWindows": st.OpenWindows, "spillFailures": st.SpillFailures, "watermarkMillis": st.WatermarkMillis,
	}
	a.emitWindowMetrics(winOp)
}

// saveFinalCheckpoint persists the end-of-run position and acknowledges
// everything the source returned (all of it is written by now).
func (a *DataPipelineAction) saveFinalCheckpoint(runLog corelog.Logger, ckpt checkpoint.Checkpoint, key string, pos int64, src source.Source, snapshotters map[string]graphexec.Snapshotter, dag bool) error {
	state, err := snapshotState(snapshotters, dag)
	if err != nil {
		return fmt.Errorf("state snapshot: %w", err)
	}
	token := checkpoint.NewToken(a.def.Source.Type, pos, cursorOf(src, math.MaxInt64))
	token.WindowState = state
	if err := ckpt.Save(key, token); err != nil {
		return fmt.Errorf("save: %w", err)
	}
	if c, ok := src.(source.Committer); ok {
		if cerr := c.Commit(math.MaxInt64); cerr != nil {
			runLog.Warnf("source commit failed (continuing): %v", cerr)
		}
	}
	return nil
}

// snapshotState captures stateful operator state for a checkpoint token. A
// linear plan stores its single chain's snapshot raw — the format the
// pre-unification linear engine wrote, so existing checkpoints resume — and a
// DAG stores a JSON object keyed by node id.
func snapshotState(snapshotters map[string]graphexec.Snapshotter, dag bool) (json.RawMessage, error) {
	if dag {
		return snapshotGraphState(snapshotters)
	}
	for _, s := range snapshotters {
		b, err := s.Snapshot()
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	return nil, nil
}

// restoreState is snapshotState's inverse. For a linear plan a failed restore
// starts the window fresh with a warning (the linear engine's behaviour); for
// a DAG it fails the run.
func restoreState(runLog corelog.Logger, snapshotters map[string]graphexec.Snapshotter, raw json.RawMessage, dag bool) error {
	if dag {
		if err := restoreGraphSnapshots(runLog, snapshotters, raw); err != nil {
			return fmt.Errorf("graph state restore: %w", err)
		}
		return nil
	}
	for _, s := range snapshotters {
		if err := s.Restore(raw); err != nil {
			runLog.Warnf("window state restore failed (starting fresh): %v", err)
		} else {
			runLog.Infof("restored window state (%d bytes)", len(raw))
		}
	}
	return nil
}

// cursorOf returns src's checkpoint cursor provider, if any, positioned at
// localRows run-local processed rows — the value then passed to Commit — so
// the token never carries the previous checkpoint's offsets.
func cursorOf(src source.Source, localRows int64) checkpoint.CursorProvider {
	if p, ok := src.(checkpoint.PositionedCursorProvider); ok {
		return positionedCursor{p, localRows}
	}
	if p, ok := src.(checkpoint.CursorProvider); ok {
		return p
	}
	return nil
}

type positionedCursor struct {
	p    checkpoint.PositionedCursorProvider
	rows int64
}

func (c positionedCursor) Cursor() json.RawMessage { return c.p.CursorAt(c.rows) }

// sourceNodeSelfContained reports whether a DAG's "source" node carries its
// own config (so the definition's source is not the primary source).
func sourceNodeSelfContained(def *Definition) bool {
	for _, n := range def.Graph.Nodes {
		if strings.EqualFold(n.ID, "source") {
			_, ok := selfContainedConfig(n)
			return ok
		}
	}
	return true // no "source" node: nothing to prebuild
}

func ensureMetadata(out *Output) map[string]interface{} {
	if out.Metadata == nil {
		out.Metadata = make(map[string]interface{})
	}
	return out.Metadata
}
