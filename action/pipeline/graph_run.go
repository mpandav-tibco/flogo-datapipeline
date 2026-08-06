package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	dqtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/dq"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	hwmtransform "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// This file wires the streaming graph runner (action/pipeline/graphexec) into
// the engine. Activation is AUTOMATIC but narrowly scoped: the runner takes over
// only when a pipeline's graph is NON-REDUCIBLE (it has conditional edges or true
// fan-out/fan-in that the linear source→transforms→sink chain cannot express).
// A pure linear chain — even one promoted to an explicit graph block — is left on
// the linear path, so its checkpoint/HWM/DQ/parallel-worker behaviour is
// untouched. This is the safe reading of "run the graph automatically".

// unsupportedGraphTransforms lists transform types the streaming runner cannot
// run with the same guarantees as the linear engine. It is now EMPTY — every
// transform is supported by the graph runner:
//   - window: the runner flushes still-open aggregates at end-of-input (Flusher).
//   - hwm:    runGraph injects the prior mark into the primary source and, after
//     a clean run, harvests MaxSeen() and persists the watermark (mirrors linear).
//   - dq:     runGraph surfaces the DQ gate report in Output.Metadata after the run.
//   - join/union: single-input transforms that self-load their bounded secondary
//     side, so they run as ordinary operators. (True multi-source consolidation
//     uses the graph's native fan-in, not these.)
//
// The map and the gate check in graphExecutable are kept so a future transform
// that needs extra wiring can be gated again without reworking the gate.
var unsupportedGraphTransforms = map[string]bool{}

// snapshotGraphState captures Snapshot() from every stateful node, keyed by
// node id, and marshals the result into one JSON blob for checkpoint.Token's
// WindowState field (which holds arbitrary JSON, not just a single window's
// state — a graph may have more than one stateful node). Returns nil (not an
// error) when there are no stateful nodes, so a plain graph's checkpoint stays
// exactly as before. A single node's Snapshot failure is logged by the caller
// and simply omitted, rather than aborting a healthy checkpoint save.
func snapshotGraphState(snapshotters map[string]graphexec.Snapshotter) (json.RawMessage, error) {
	if len(snapshotters) == 0 {
		return nil, nil
	}
	state := make(map[string]json.RawMessage, len(snapshotters))
	var firstErr error
	for id, s := range snapshotters {
		b, err := s.Snapshot()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("node %q: %w", id, err)
			}
			continue
		}
		state[id] = b
	}
	if len(state) == 0 {
		return nil, firstErr
	}
	b, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshal graph state: %w", err)
	}
	return b, firstErr
}

// restoreGraphSnapshots unmarshals a checkpoint token's WindowState (as saved
// by snapshotGraphState) and calls Restore on each matching stateful node. A
// node id present in the snapshot but absent from the CURRENT plan (the
// pipeline's graph changed since the checkpoint was taken) is logged and
// skipped rather than failing the whole resume.
func restoreGraphSnapshots(runLog corelog.Logger, snapshotters map[string]graphexec.Snapshotter, raw json.RawMessage) error {
	var state map[string]json.RawMessage
	if err := json.Unmarshal(raw, &state); err != nil {
		return fmt.Errorf("unmarshal graph state: %w", err)
	}
	for id, b := range state {
		s, ok := snapshotters[id]
		if !ok {
			runLog.Warnf("graph state restore: node %q from the checkpoint no longer exists in this graph — skipping its snapshot", id)
			continue
		}
		if err := s.Restore(b); err != nil {
			return fmt.Errorf("node %q: %w", id, err)
		}
	}
	return nil
}

// eofSourceOp is a placeholder graphexec.SourceOp used only to satisfy Compile
// for the "source" node of a scaled graph plan BEFORE the first partition's
// real source is wired in via Plan.RewireSource — Compile never reads from it.
type eofSourceOp struct{}

func (eofSourceOp) ReadBatch(int) (*record.RecordBatch, error) { return nil, io.EOF }
func (eofSourceOp) Skip(int64) error                           { return nil }
func (eofSourceOp) Close() error                               { return nil }

// graphExecutable reports whether the pipeline's graph both NEEDS the graph
// runner (non-reducible) and CAN be run: every node must be resolvable — either
// self-contained (it carries its own "config" object) or a standard linear node
// ("source", a transform "t{i}" in range, or "sink") — and no node may use a
// transform type the runner does not yet support. Multiple sinks/sources are
// allowed as long as the extra ones carry their own config.
func graphExecutable(def *Definition) bool {
	g := def.Graph
	if g == nil || len(g.Nodes) == 0 {
		return false
	}
	for _, n := range g.Nodes {
		typ, ok := nodeResolveType(def, n)
		if !ok {
			return false
		}
		if n.Kind == "transform" && unsupportedGraphTransforms[strings.ToLower(typ)] {
			return false
		}
	}
	return graphNonReducible(g)
}

// selfContainedConfig returns a graph node's embedded config object, if any. A
// self-contained node fully describes its own operator (no linear-model
// fallback), which is what lets a graph declare extra sinks (error / reject) or
// additional sources.
func selfContainedConfig(n GraphNodeDef) (map[string]interface{}, bool) {
	c, ok := n.Extra["config"].(map[string]interface{})
	return c, ok
}

// nodeResolveType returns a node's effective operator type and whether the node
// can be resolved (self-contained, or a standard linear node).
func nodeResolveType(def *Definition, n GraphNodeDef) (string, bool) {
	if _, ok := selfContainedConfig(n); ok {
		return n.Type, true
	}
	switch n.ID {
	case "source":
		return def.Source.Type, true
	case "sink":
		return def.Sink.Type, true
	default:
		idx, ok := transformIndex(n.ID)
		if ok && idx < len(def.Transforms) {
			return def.Transforms[idx].Type, true
		}
		return "", false
	}
}

// graphNonReducible reports whether the graph has structure the linear chain
// cannot represent: a typed edge (condition / otherwise / error), or any node
// with fan-out / fan-in ≥ 2.
func graphNonReducible(g *GraphDef) bool {
	outDeg := map[string]int{}
	inDeg := map[string]int{}
	for _, e := range g.Edges {
		switch e.Type {
		case "expression", "otherwise", "error":
			return true
		}
		outDeg[graphEndpointNode(e.From)]++
		inDeg[graphEndpointNode(e.To)]++
	}
	for _, d := range outDeg {
		if d >= 2 {
			return true
		}
	}
	for _, d := range inDeg {
		if d >= 2 {
			return true
		}
	}
	return false
}

// transformIndex parses a transform node id "t{N}" into its index N.
func transformIndex(id string) (int, bool) {
	if len(id) < 2 || id[0] != 't' {
		return 0, false
	}
	i, err := strconv.Atoi(id[1:])
	if err != nil || i < 0 {
		return 0, false
	}
	return i, true
}

// buildExecGraph turns the on-disk GraphDef into the executor's neutral plan. A
// self-contained node (one carrying its own "config" object) is built from that
// config; otherwise a standard node is resolved from the linear model by id
// (source → def.Source, t{i} → def.Transforms[i], sink → def.Sink). Each edge's
// type and condition are carried through. preBuiltSource, when non-nil, is
// wired directly as the "source" node's operator instead of its config —
// used for horizontally-scaled runs where the caller already opened a
// partition-scoped source (see runGraphScaled).
func (a *DataPipelineAction) buildExecGraph(in *Input, preBuiltSource graphexec.SourceOp) (*graphexec.Graph, error) {
	def := a.def
	eg := &graphexec.Graph{}
	for _, n := range def.Graph.Nodes {
		node := graphexec.Node{ID: n.ID, Kind: graphexec.NodeKind(n.Kind), Type: n.Type}
		if cfg, ok := selfContainedConfig(n); ok {
			node.Cfg = cfg
		} else {
			switch n.ID {
			case "source":
				node.Kind = graphexec.KindSource
				node.Type = def.Source.Type
				node.Cfg = a.buildSourceConfig(in)
				if preBuiltSource != nil {
					node.PreBuilt = preBuiltSource
				}
			case "sink":
				node.Kind = graphexec.KindSink
				node.Type = def.Sink.Type
				node.Cfg = def.Sink.Options
			default:
				idx, ok := transformIndex(n.ID)
				if !ok || idx >= len(def.Transforms) {
					return nil, fmt.Errorf("graph node %q cannot be resolved to a pipeline stage", n.ID)
				}
				node.Kind = graphexec.KindTransform
				node.Type = def.Transforms[idx].Type
				node.Cfg = def.Transforms[idx].Extra
			}
		}
		eg.Nodes = append(eg.Nodes, node)
	}
	for _, e := range def.Graph.Edges {
		edge := graphexec.Edge{From: e.From, To: e.To, Type: graphexec.EdgeType(e.Type)}
		if e.Type == "expression" {
			edge.Cond = e.Value
		}
		eg.Edges = append(eg.Edges, edge)
	}
	return eg, nil
}

// graphBuilders supplies the executor with the real engine constructors.
func graphBuilders() graphexec.Builders {
	return graphexec.Builders{
		Source: func(typ string, cfg map[string]interface{}) (graphexec.SourceOp, error) {
			return source.New(typ, cfg)
		},
		Transform: func(typ string, cfg map[string]interface{}) (graphexec.TransformOp, error) {
			return transform.New(typ, cfg)
		},
		Sink: func(typ string, cfg map[string]interface{}) (graphexec.SinkOp, error) {
			return sink.New(typ, cfg)
		},
		Predicate: func(expr string) (graphexec.Pred, error) {
			p, err := filter.Compile(expr)
			if err != nil {
				return nil, err
			}
			return p.Eval, nil
		},
	}
}

// runGraph builds, compiles and runs the graph, returning the run's row counts.
// The caller (RunDirect / Run) sets Status, DurationMs and lineage, exactly as
// it does for the linear path.
func (a *DataPipelineAction) runGraph(ctx context.Context, in *Input, runLog corelog.Logger, runID string) (*Output, error) {
	def := a.def
	out := &Output{}

	// Horizontally-scaled DAG execution: delegate to runGraphScaled, which
	// reuses the same scaling.Coordinator / partition-claim machinery as the
	// linear engine's runScaled. A "single" (default) pipeline stays below,
	// unchanged.
	scMode := def.Scaling.Mode
	if scMode == "" {
		scMode = "single"
	}
	if scMode == "sharded" || scMode == "distributed" {
		return a.runGraphScaled(ctx, in, runLog, runID, scMode)
	}

	// --- watermark store + prior mark (incremental HWM) — mirrors the linear path.
	// The store is a no-op unless incremental.mode == "hwm". A prior mark is
	// injected into the primary source config so an HWM-aware source (jdbc) can
	// append its WHERE predicate; the hwm transform's observed max is persisted
	// after a clean run (see saveWatermark below).
	wmStore, werr := openWatermarkStore(def)
	if werr != nil {
		return out, fmt.Errorf("watermark store open: %w", werr)
	}
	defer wmStore.Close()
	var priorMark *watermark.Mark
	if def.Incremental.Mode == "hwm" {
		priorMark, werr = wmStore.Load(a.ref)
		if werr != nil {
			return out, fmt.Errorf("watermark load: %w", werr)
		}
		if priorMark == nil && def.Incremental.InitialValue != "" {
			priorMark = &watermark.Mark{
				Column:    def.Incremental.Column,
				Value:     def.Incremental.InitialValue,
				ValueType: def.Incremental.ValueType,
				SavedAt:   time.Now().UTC(),
			}
		}
		if priorMark != nil {
			runLog.Infof("graph incremental load — %s > %s (type: %s)",
				priorMark.Column, priorMark.Value, priorMark.ValueType)
		} else {
			runLog.Infof("graph incremental load — no prior mark, performing full load")
		}
	}

	eg, err := a.buildExecGraph(in, nil)
	if err != nil {
		return out, fmt.Errorf("graph build: %w", err)
	}
	// Inject the high-water mark into the PRIMARY source node ("source"); extra
	// fan-in feeds are not the incremental cursor. Same keys the linear path adds.
	if priorMark != nil {
		for i := range eg.Nodes {
			if eg.Nodes[i].ID == "source" && eg.Nodes[i].Kind == graphexec.KindSource && eg.Nodes[i].Cfg != nil {
				eg.Nodes[i].Cfg["hwmColumn"] = priorMark.Column
				eg.Nodes[i].Cfg["hwmValue"] = priorMark.Value
				eg.Nodes[i].Cfg["hwmValueType"] = priorMark.ValueType
			}
		}
	}

	plan, err := graphexec.Compile(eg, graphBuilders())
	if err != nil {
		return out, fmt.Errorf("graph compile: %w", err)
	}

	// Locate the hwm / dq transforms among the plan's operators so the runner can
	// persist the watermark and surface the DQ gate report after the run — the
	// same post-run harvest the linear engine does (findHWMTransform /
	// findDQGateProvider). The plan hands back its operators; we own the
	// capability-interface assertions so graphexec stays decoupled.
	var hwmProvider hwmtransform.HWMProvider
	var dqProvider dqtransform.DQGateProvider
	for _, tx := range plan.Transforms() {
		if hwmProvider == nil {
			if h, ok := tx.(hwmtransform.HWMProvider); ok {
				hwmProvider = h
			}
		}
		if dqProvider == nil {
			if d, ok := tx.(dqtransform.DQGateProvider); ok {
				dqProvider = d
			}
		}
	}
	// snapshotters are the stateful nodes (e.g. window) whose state is captured
	// at each checkpoint barrier and restored on resume — see below.
	snapshotters := plan.Snapshotters()

	opts := graphexec.Options{
		BatchSize:   def.Performance.BatchSize,
		BufferDepth: def.Performance.SourceBufferBatches,
	}

	// checkpoint key mirrors the linear path: per-run id, or a stable configured
	// key so a streaming graph resumes across restarts.
	ckptKey := runID
	if def.Checkpoint.Key != "" {
		ckptKey = def.Checkpoint.Key
	}

	// Checkpoint/resume for the streaming DAG, including STATEFUL graphs (a
	// window node): the barrier fires only once every producer has paused (no
	// batch in flight anywhere), which is also the safe instant to call
	// Snapshot() on every stateful node — captured alongside the source
	// position so a crash resumes with the window aggregates and the source
	// cursor in sync, exactly mirroring the linear engine's own
	// winOp.Snapshot()/Restore() contract (see saveCheckpoint in action.go).
	var ckpt checkpoint.Checkpoint
	if ct := def.Checkpoint.Type; ct != "" && ct != "none" {
		c, cerr := openCheckpoint(def)
		if cerr != nil {
			_ = plan.Close()
			return out, fmt.Errorf("checkpoint open: %w", cerr)
		}
		ckpt = c
		defer ckpt.Close()

		// Resume: skip rows already durable before the last saved checkpoint,
		// and restore any stateful node's snapshot taken at that same position.
		tok, lErr := ckpt.Load(ckptKey)
		if lErr != nil {
			_ = plan.Close()
			return out, fmt.Errorf("checkpoint load: %w", lErr)
		}
		if tok != nil && tok.RowsProcessed > 0 {
			runLog.Infof("resuming graph %q — skipping %d already-processed rows", ckptKey, tok.RowsProcessed)
			opts.ResumeFrom = tok.RowsProcessed
		}
		if tok != nil && len(tok.WindowState) > 0 {
			if err := restoreGraphSnapshots(runLog, snapshotters, tok.WindowState); err != nil {
				_ = plan.Close()
				return out, fmt.Errorf("graph state restore: %w", err)
			}
		}
		// Enable barriers only when an interval is configured; otherwise the
		// checkpoint is written just once at end-of-run (below).
		if def.Checkpoint.IntervalRows > 0 {
			opts.CheckpointRows = def.Checkpoint.IntervalRows
			opts.OnCheckpoint = func(pos int64) error {
				state, serr := snapshotGraphState(snapshotters)
				if serr != nil {
					runLog.Warnf("graph state snapshot failed (checkpointing source position only): %v", serr)
				}
				token := checkpoint.NewToken(def.Source.Type, pos, nil)
				token.WindowState = state
				if serr := ckpt.Save(ckptKey, token); serr != nil {
					// Match the linear path: warn and keep running rather than
					// aborting a healthy load on a checkpoint-backend hiccup.
					runLog.Warnf("graph checkpoint save failed (continuing): %v", serr)
					return nil
				}
				a.recorder.CheckpointSaved(a.ref)
				runLog.Infof("graph checkpoint saved — %d rows processed", pos)
				return nil
			}
		}
	}

	res, runErr := plan.Run(ctx, opts)
	// Include any resumed rows so the reported processed count is the absolute
	// source position (RowsWritten stays this-run only, as on the linear path).
	out.RowsProcessed = res.RowsRead + opts.ResumeFrom
	out.RowsWritten = res.RowsWritten
	// Surface per-node counters for the Studio graph run overlay (present even on
	// a partial/failed run so the canvas can show how far it got).
	gs := make([]graphexec.NodeStat, 0, len(res.Nodes))
	for _, ns := range res.Nodes {
		gs = append(gs, *ns)
	}
	out.Metadata = map[string]interface{}{"graphStats": gs}
	if runErr != nil {
		return out, runErr
	}

	// Clean completion: finalize the checkpoint. The tail after the last barrier
	// was flushed by the sink-close path but not acked, so persist the FINAL
	// position now — a restart after a clean run then resumes at EOF (a no-op)
	// instead of reprocessing the tail. DeleteOnSuccess clears it for a fresh run.
	// (Any stateful node's Flush already ran as part of Run's node-completion, so
	// its Snapshot here is expected to be empty — nothing left to resume.)
	if ckpt != nil {
		if def.Checkpoint.DeleteOnSuccess {
			_ = ckpt.Delete(ckptKey)
		} else {
			state, serr := snapshotGraphState(snapshotters)
			if serr != nil {
				runLog.Warnf("graph state snapshot failed (final checkpoint saves source position only): %v", serr)
			}
			token := checkpoint.NewToken(def.Source.Type, out.RowsProcessed, nil)
			token.WindowState = state
			if serr := ckpt.Save(ckptKey, token); serr != nil {
				runLog.Warnf("final graph checkpoint save failed: %v", serr)
			} else {
				a.recorder.CheckpointSaved(a.ref)
			}
		}
	}

	// Post-run harvest — mirrors the linear engine. Persist the high-water mark
	// (a no-op unless incremental.mode == "hwm" and an hwm transform was present),
	// then surface the DQ gate report in Output.Metadata so downstream flows can
	// inspect quality scores without parsing logs.
	saveWatermark(runLog, wmStore, hwmProvider, def, a.ref)
	if dqProvider != nil {
		if report := dqProvider.LastRunReport(); report != nil {
			if out.Metadata == nil {
				out.Metadata = make(map[string]interface{})
			}
			report.PipelineRef = a.ref
			out.Metadata["dqReport"] = report
		}
	}

	runLog.Infof("graph run complete — nodes=%d edges=%d read=%d written=%d",
		len(eg.Nodes), len(eg.Edges), res.RowsRead, res.RowsWritten)
	return out, nil
}

// runGraphScaled executes a non-reducible graph across horizontal partitions
// of the PRIMARY source, for scaling.mode "sharded"/"distributed" — the graph
// counterpart of the linear engine's runScaled, reusing the SAME
// scaling.Coordinator / partition-claim machinery (a.runScaled) so
// static-shard assignment and dynamic-claim behavior are identical between
// the two execution paths.
//
// The graph (transform/sink operators) is compiled EXACTLY ONCE and reused
// across every partition this instance owns — Plan.RewireSource swaps in each
// partition's source before its Run call, and Options.KeepOpen keeps the
// compiled sink/transform chain open between calls — mirroring the linear
// engine's own drain(), which reuses one open sink across every partition on
// a shard rather than reopening it per partition (reopening a file-based sink
// per partition would truncate the previous partition's rows on the next
// open).
//
// NOT YET SUPPORTED here (documented, not silently mishandled):
//   - incremental HWM — each partition would need its own watermark
//     semantics, which is undefined; a configured hwm mode logs a warning and
//     is otherwise ignored by this path.
//   - the post-run DQ gate report / HWM harvest — mirrors the single-instance
//     graph path's harvest, but is not yet wired here.
//
// Per-node graphStats accumulate correctly across every partition this
// instance processes (see nodeTotals below), so the Studio run overlay still
// gets a cumulative, meaningful picture.
func (a *DataPipelineAction) runGraphScaled(ctx context.Context, in *Input, runLog corelog.Logger, runID string, scMode string) (*Output, error) {
	def := a.def
	out := &Output{}

	if def.Incremental.Mode == "hwm" {
		runLog.Warnf("graph runner: incremental.mode=hwm is not yet supported combined with scaling.mode=%s for a graph-executed pipeline — the watermark will NOT be read or advanced by this run", scMode)
	}

	srcCfg := a.buildSourceConfig(in)
	primarySrc, err := source.New(def.Source.Type, srcCfg)
	if err != nil {
		return out, fmt.Errorf("primary source open: %w", err)
	}
	defer primarySrc.Close()

	// Compile the transform/sink chain ONCE, wired to a placeholder source that
	// is immediately swapped out (via RewireSource) before the first partition
	// runs — Compile never reads from it, so an EOF stub satisfies it cleanly.
	eg, err := a.buildExecGraph(in, eofSourceOp{})
	if err != nil {
		return out, fmt.Errorf("graph build: %w", err)
	}
	plan, err := graphexec.Compile(eg, graphBuilders())
	if err != nil {
		return out, fmt.Errorf("graph compile: %w", err)
	}
	defer func() { _ = plan.Close() }()
	statefulGraph := plan.HasStateful()

	var ckpt checkpoint.Checkpoint
	if ct := def.Checkpoint.Type; ct != "" && ct != "none" {
		if statefulGraph {
			runLog.Warnf("graph runner: checkpoint/resume not yet supported for a windowed (stateful) graph — running scaled without it (configured checkpoint=%s)", ct)
		} else {
			c, cerr := openCheckpoint(def)
			if cerr != nil {
				return out, fmt.Errorf("checkpoint open: %w", cerr)
			}
			ckpt = c
			defer ckpt.Close()
		}
	}

	// nodeTotals accumulates per-node counters across every partition this
	// instance processes, for the final graphStats metadata.
	nodeTotals := make(map[string]*graphexec.NodeStat)
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
	finalize := func() {
		gs := make([]graphexec.NodeStat, 0, len(nodeTotals))
		for _, ns := range nodeTotals {
			gs = append(gs, *ns)
		}
		out.Metadata = map[string]interface{}{"graphStats": gs}
	}

	// drainOnce swaps pSrc into the ALREADY-COMPILED plan and runs it, keeping
	// the transform/sink chain open for the next partition. Mirrors the linear
	// engine's drain(pSrc, ckptKey), which reuses the same open sink per shard.
	drainOnce := func(pSrc source.Source, ckptKey string) error {
		if rerr := plan.RewireSource("source", pSrc); rerr != nil {
			return fmt.Errorf("graph rewire source: %w", rerr)
		}

		opts := graphexec.Options{
			BatchSize:   def.Performance.BatchSize,
			BufferDepth: def.Performance.SourceBufferBatches,
			KeepOpen:    true,
		}
		if ckpt != nil {
			tok, lErr := ckpt.Load(ckptKey)
			if lErr != nil {
				return fmt.Errorf("checkpoint load: %w", lErr)
			}
			if tok != nil && tok.RowsProcessed > 0 {
				runLog.Infof("resuming graph partition %q — skipping %d already-processed rows", ckptKey, tok.RowsProcessed)
				opts.ResumeFrom = tok.RowsProcessed
			}
			if def.Checkpoint.IntervalRows > 0 {
				opts.CheckpointRows = def.Checkpoint.IntervalRows
				opts.OnCheckpoint = func(pos int64) error {
					if serr := ckpt.Save(ckptKey, checkpoint.NewToken(def.Source.Type, pos, nil)); serr != nil {
						runLog.Warnf("graph checkpoint save failed (continuing): %v", serr)
						return nil
					}
					a.recorder.CheckpointSaved(a.ref)
					return nil
				}
			}
		}

		res, runErr := plan.Run(ctx, opts)
		mergeStats(res)
		out.RowsProcessed += res.RowsRead + opts.ResumeFrom
		out.RowsWritten += res.RowsWritten
		if runErr != nil {
			return runErr
		}
		if ckpt != nil {
			partitionRows := opts.ResumeFrom + res.RowsRead
			if def.Checkpoint.DeleteOnSuccess {
				_ = ckpt.Delete(ckptKey)
			} else if serr := ckpt.Save(ckptKey, checkpoint.NewToken(def.Source.Type, partitionRows, nil)); serr != nil {
				runLog.Warnf("final graph checkpoint save failed for partition %q: %v", ckptKey, serr)
			} else {
				a.recorder.CheckpointSaved(a.ref)
			}
		}
		return nil
	}

	if err := a.runScaled(ctx, runLog, def, scMode, primarySrc, srcCfg, runID, drainOnce); err != nil {
		finalize()
		return out, err
	}

	finalize()
	runLog.Infof("graph run complete (scaling.mode=%s) — read=%d written=%d", scMode, out.RowsProcessed, out.RowsWritten)
	return out, nil
}
