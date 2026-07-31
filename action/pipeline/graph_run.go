package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
)

// This file wires the streaming graph runner (action/pipeline/graphexec) into
// the engine. Activation is AUTOMATIC but narrowly scoped: the runner takes over
// only when a pipeline's graph is NON-REDUCIBLE (it has conditional edges or true
// fan-out/fan-in that the linear source→transforms→sink chain cannot express).
// A pure linear chain — even one promoted to an explicit graph block — is left on
// the linear path, so its checkpoint/HWM/DQ/parallel-worker behaviour is
// untouched. This is the safe reading of "run the graph automatically".

// unsupportedGraphTransforms are transform types the streaming runner cannot yet
// execute with the same guarantees as the linear engine: they need end-of-input
// flush (window), post-run watermark persistence (hwm), gate-report metadata
// (dq), or secondary-stream buffering (join, union). A graph that uses one of
// them falls back to the linear path.
var unsupportedGraphTransforms = map[string]bool{
	"window": true,
	"hwm":    true,
	"dq":     true,
	"join":   true,
	"union":  true,
}

// graphExecutable reports whether the pipeline's graph both NEEDS the graph
// runner (non-reducible) and CAN be run by this increment: every node must be a
// standard, linearly-resolvable node (the single "source", a transform "t{i}"
// in range, or the single "sink"), with no unsupported transform. Branch, DLQ
// and multi-source graphs are deferred to a later increment and stay linear.
func graphExecutable(def *Definition) bool {
	g := def.Graph
	if g == nil || len(g.Nodes) == 0 {
		return false
	}
	sources, sinks := 0, 0
	for _, n := range g.Nodes {
		switch {
		case n.ID == "source":
			sources++
		case n.ID == "sink":
			sinks++
		default:
			idx, ok := transformIndex(n.ID)
			if !ok || idx >= len(def.Transforms) {
				return false
			}
			if unsupportedGraphTransforms[strings.ToLower(def.Transforms[idx].Type)] {
				return false
			}
		}
	}
	if sources != 1 || sinks != 1 {
		return false
	}
	return graphNonReducible(g)
}

// graphNonReducible reports whether the graph has structure the linear chain
// cannot represent: a conditional edge, or any node with fan-out / fan-in ≥ 2.
func graphNonReducible(g *GraphDef) bool {
	outDeg := map[string]int{}
	inDeg := map[string]int{}
	for _, e := range g.Edges {
		if e.Type == "expression" {
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

// buildExecGraph turns the on-disk GraphDef into the executor's neutral plan,
// resolving each node's config from the linear model by id (source → def.Source,
// t{i} → def.Transforms[i], sink → def.Sink) and carrying each edge's condition.
func (a *DataPipelineAction) buildExecGraph(in *Input) (*graphexec.Graph, error) {
	def := a.def
	eg := &graphexec.Graph{}
	for _, n := range def.Graph.Nodes {
		node := graphexec.Node{ID: n.ID}
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
		eg.Nodes = append(eg.Nodes, node)
	}
	for _, e := range def.Graph.Edges {
		edge := graphexec.Edge{From: e.From, To: e.To}
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
func (a *DataPipelineAction) runGraph(ctx context.Context, in *Input, runLog corelog.Logger) (*Output, error) {
	def := a.def
	out := &Output{}
	eg, err := a.buildExecGraph(in)
	if err != nil {
		return out, fmt.Errorf("graph build: %w", err)
	}
	plan, err := graphexec.Compile(eg, graphBuilders())
	if err != nil {
		return out, fmt.Errorf("graph compile: %w", err)
	}
	opts := graphexec.Options{
		BatchSize:   def.Performance.BatchSize,
		BufferDepth: def.Performance.SourceBufferBatches,
	}
	res, runErr := plan.Run(ctx, opts)
	out.RowsProcessed = res.RowsRead
	out.RowsWritten = res.RowsWritten
	if runErr != nil {
		return out, runErr
	}
	runLog.Infof("graph run complete — nodes=%d edges=%d read=%d written=%d",
		len(eg.Nodes), len(eg.Edges), res.RowsRead, res.RowsWritten)
	return out, nil
}
