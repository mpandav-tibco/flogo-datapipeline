package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
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
	switch {
	case n.ID == "source":
		return def.Source.Type, true
	case n.ID == "sink":
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
// type and condition are carried through.
func (a *DataPipelineAction) buildExecGraph(in *Input) (*graphexec.Graph, error) {
	def := a.def
	eg := &graphexec.Graph{}
	for _, n := range def.Graph.Nodes {
		node := graphexec.Node{ID: n.ID, Kind: graphexec.NodeKind(n.Kind), Type: n.Type}
		if cfg, ok := selfContainedConfig(n); ok {
			node.Cfg = cfg
		} else {
			switch {
			case n.ID == "source":
				node.Kind = graphexec.KindSource
				node.Type = def.Source.Type
				node.Cfg = a.buildSourceConfig(in)
			case n.ID == "sink":
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

	eg, err := a.buildExecGraph(in)
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

	// Checkpoint/resume for the streaming DAG. STATELESS graphs get full no-loss
	// checkpointing via drain barriers: the source pauses at each interval, the
	// barrier flushes every sink, and the source position is persisted only once
	// ALL sinks are durable — so a crash resumes from the last saved position with
	// no committed rows lost (at-least-once for the in-flight tail). A STATEFUL
	// graph (a window operator) needs a coordinated state snapshot the barrier
	// does not yet capture, so it keeps running WITHOUT checkpoint (with a warn).
	var ckpt checkpoint.Checkpoint
	if ct := def.Checkpoint.Type; ct != "" && ct != "none" {
		if plan.HasStateful() {
			runLog.Warnf("graph runner: checkpoint/resume not yet supported for a windowed (stateful) graph — running without it (configured checkpoint=%s)", ct)
		} else {
			c, cerr := openCheckpoint(def)
			if cerr != nil {
				_ = plan.Close()
				return out, fmt.Errorf("checkpoint open: %w", cerr)
			}
			ckpt = c
			defer ckpt.Close()

			// Resume: skip rows already durable before the last saved checkpoint.
			tok, lErr := ckpt.Load(ckptKey)
			if lErr != nil {
				_ = plan.Close()
				return out, fmt.Errorf("checkpoint load: %w", lErr)
			}
			if tok != nil && tok.RowsProcessed > 0 {
				runLog.Infof("resuming graph %q — skipping %d already-processed rows", ckptKey, tok.RowsProcessed)
				opts.ResumeFrom = tok.RowsProcessed
			}
			// Enable barriers only when an interval is configured; otherwise the
			// checkpoint is written just once at end-of-run (below).
			if def.Checkpoint.IntervalRows > 0 {
				opts.CheckpointRows = def.Checkpoint.IntervalRows
				opts.OnCheckpoint = func(pos int64) error {
					if serr := ckpt.Save(ckptKey, checkpoint.NewToken(def.Source.Type, pos, nil)); serr != nil {
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
	if ckpt != nil {
		if def.Checkpoint.DeleteOnSuccess {
			_ = ckpt.Delete(ckptKey)
		} else {
			if serr := ckpt.Save(ckptKey, checkpoint.NewToken(def.Source.Type, out.RowsProcessed, nil)); serr != nil {
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
