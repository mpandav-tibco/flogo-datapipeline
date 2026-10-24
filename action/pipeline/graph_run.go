package pipeline

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline/graphexec"
)

// This file holds the DAG-specific helpers of the unified engine (engine.go):
// deciding whether an explicit graph must run node-per-node (it is
// NON-REDUCIBLE: conditional edges or true fan-out/fan-in), building its
// graphexec.Graph, and snapshotting/restoring its stateful nodes. A reducible
// graph — or no graph at all — runs as the fused source → chain → sink plan.

// unsupportedGraphTransforms lists transform types a DAG cannot run with the
// same guarantees as a linear plan. It is now EMPTY — every transform is
// supported node-per-node:
//   - window: still-open aggregates are flushed at end-of-input (Flusher).
//   - hwm:    the engine injects the prior mark into the primary source and,
//     after a clean run, harvests MaxSeen() and persists the watermark.
//   - dq:     the engine surfaces the DQ gate report in Output.Metadata.
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
// partition-scoped source (see execute in engine.go).
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
