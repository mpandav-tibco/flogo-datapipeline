package graphexec

import (
	"fmt"
	"strings"
)

// pedge is a compiled outbound edge: the target node, the edge's routing role,
// (for EdgeCondition only) its compiled row predicate, and (for an edge into a
// multi-input-active node) which named port it targets ("left" | "right").
type pedge struct {
	to     *pnode
	etype  EdgeType
	pred   Pred
	binder PredBinder // optional positional form of pred
	toPort string
}

// pnode is a compiled node: its operator (exactly one of src/tx/snk is set by
// kind) plus its outbound edges and inbound-edge count. multiIn is non-nil
// ONLY when tx implements MultiInputOp AND the node has ≥ 1 "right"-port
// inbound edge (see Compile's post-wiring pass) — that combination is what
// activates the dedicated left/right scheduling in runMultiInputTransform;
// otherwise the node runs through the ordinary single-input path unaffected.
type pnode struct {
	id         string
	kind       NodeKind
	src        SourceOp
	tx         TransformOp
	snk        SinkOp
	out        []*pedge
	inDeg      int // ALL inbound edges, for cycle detection only
	leftInDeg  int // multi-input nodes: "left"-port (or unlabeled) inbound edges
	rightInDeg int // multi-input nodes: "right"-port inbound edges
	hasErr     bool
	multiIn    MultiInputOp
	closer     interface{ Close() error }
	replicas   []TransformOp // extra parallel instances of tx (see Node.Replicas)
}

// Plan is a validated, ready-to-run graph: operators constructed, edges wired,
// predicates compiled, topology proven acyclic.
type Plan struct {
	nodes []*pnode
	byID  map[string]*pnode
}

// nodeOf strips a "node:port" endpoint down to the node id.
func nodeOf(endpoint string) string {
	if i := strings.IndexByte(endpoint, ':'); i >= 0 {
		return endpoint[:i]
	}
	return endpoint
}

// portOf returns the ":port" suffix of an endpoint ("" if none).
func portOf(endpoint string) string {
	if i := strings.IndexByte(endpoint, ':'); i >= 0 {
		return endpoint[i+1:]
	}
	return ""
}

// normEdgeType maps the neutral edge type onto a canonical routing role; an
// empty or "default" type is a success edge.
func normEdgeType(t EdgeType) EdgeType {
	if t == "" || t == "default" {
		return EdgeSuccess
	}
	return t
}

// Compile validates the graph and constructs every operator, returning a Plan
// ready for Run. On any failure it closes whatever it already built so no
// source/sink handles leak.
func Compile(g *Graph, b Builders) (*Plan, error) {
	if g == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	if len(g.Nodes) == 0 {
		return nil, fmt.Errorf("graph has no nodes")
	}
	if b.Source == nil || b.Transform == nil || b.Sink == nil || b.Predicate == nil {
		return nil, fmt.Errorf("graphexec: Builders must supply Source, Transform, Sink and Predicate")
	}

	p := &Plan{byID: make(map[string]*pnode, len(g.Nodes))}
	// A running list of constructed closers so we can unwind on error.
	built := make([]interface{ Close() error }, 0, len(g.Nodes))
	fail := func(format string, args ...interface{}) (*Plan, error) {
		for i := len(built) - 1; i >= 0; i-- {
			_ = built[i].Close()
		}
		return nil, fmt.Errorf(format, args...)
	}

	// 1) Index + construct nodes.
	for _, n := range g.Nodes {
		if n.ID == "" {
			return fail("graphexec: node with empty id")
		}
		if _, dup := p.byID[n.ID]; dup {
			return fail("graphexec: duplicate node id %q", n.ID)
		}
		pn := &pnode{id: n.ID, kind: n.Kind}
		switch n.Kind {
		case KindSource:
			if n.PreBuilt != nil {
				// Caller-owned lifecycle — do not register a closer.
				pn.src = n.PreBuilt
			} else {
				op, err := b.Source(n.Type, n.Cfg)
				if err != nil {
					return fail("graphexec: build source %q (%s): %w", n.ID, n.Type, err)
				}
				pn.src, pn.closer = op, op
			}
		case KindTransform:
			if op, ok := n.Operator.(TransformOp); ok {
				pn.tx, pn.closer = op, op
			} else {
				op, err := b.Transform(n.Type, n.Cfg)
				if err != nil {
					return fail("graphexec: build transform %q (%s): %w", n.ID, n.Type, err)
				}
				pn.tx, pn.closer = op, op
			}
			pn.replicas = n.Replicas
			for _, r := range n.Replicas {
				built = append(built, r)
			}
		case KindSink:
			if op, ok := n.Operator.(SinkOp); ok {
				pn.snk, pn.closer = op, op
			} else {
				op, err := b.Sink(n.Type, n.Cfg)
				if err != nil {
					return fail("graphexec: build sink %q (%s): %w", n.ID, n.Type, err)
				}
				pn.snk, pn.closer = op, op
			}
		default:
			return fail("graphexec: node %q has unknown kind %q", n.ID, n.Kind)
		}
		if pn.closer != nil {
			built = append(built, pn.closer)
		}
		p.byID[n.ID] = pn
		p.nodes = append(p.nodes, pn)
	}

	// 2) Wire edges, compiling any conditions.
	for _, e := range g.Edges {
		fromID, toID := nodeOf(e.From), nodeOf(e.To)
		from, ok := p.byID[fromID]
		if !ok {
			return fail("graphexec: edge %s→%s references unknown source node %q", e.From, e.To, fromID)
		}
		to, ok := p.byID[toID]
		if !ok {
			return fail("graphexec: edge %s→%s references unknown target node %q", e.From, e.To, toID)
		}
		if from == to {
			return fail("graphexec: edge %s→%s is a self-loop", e.From, e.To)
		}
		if from.kind == KindSink {
			return fail("graphexec: sink node %q cannot have an outbound edge", fromID)
		}
		if to.kind == KindSource {
			return fail("graphexec: source node %q cannot have an inbound edge", toID)
		}
		pe := &pedge{to: to, etype: normEdgeType(e.Type)}
		switch pe.etype {
		case EdgeCondition:
			if strings.TrimSpace(e.Cond) == "" {
				return fail("graphexec: condition edge %s→%s has an empty expression", e.From, e.To)
			}
			pred, err := b.Predicate(e.Cond)
			if err != nil {
				return fail("graphexec: compile condition %q on edge %s→%s: %w", e.Cond, e.From, e.To, err)
			}
			pe.pred = pred
			if b.BindPredicate != nil {
				binder, err := b.BindPredicate(e.Cond)
				if err != nil {
					return fail("graphexec: compile condition %q on edge %s→%s: %w", e.Cond, e.From, e.To, err)
				}
				pe.binder = binder
			}
		case EdgeSuccess, EdgeOtherwise, EdgeError:
			// no predicate
			if pe.etype == EdgeError {
				from.hasErr = true
			}
		default:
			return fail("graphexec: edge %s→%s has unknown type %q", e.From, e.To, e.Type)
		}
		pe.toPort = portOf(e.To)
		from.out = append(from.out, pe)
		to.inDeg++ // ALL inbound edges count here, regardless of port — cycle detection only.
		if pe.toPort == "right" {
			to.rightInDeg++
		} else {
			to.leftInDeg++
		}
	}

	// 2b) Activate the multi-input schedule ONLY for a node whose operator
	// implements MultiInputOp AND has \u2265 1 real "right"-port edge \u2014 e.g. an
	// existing join promoted into a graph with no right-port edges (still
	// configured with an inline "right" dataset) is left on the ordinary
	// single-input path, unaffected.
	for _, n := range p.nodes {
		if n.tx == nil || n.rightInDeg == 0 {
			continue
		}
		if mi, ok := n.tx.(MultiInputOp); ok {
			n.multiIn = mi
		}
	}

	// 3) Prove the graph is acyclic (Kahn) — a cycle would deadlock the runner.
	if err := p.checkAcyclic(); err != nil {
		return fail("%s", err.Error())
	}

	return p, nil
}

// checkAcyclic runs a Kahn topological pass over a scratch in-degree map; it
// mutates nothing on the plan.
func (p *Plan) checkAcyclic() error {
	indeg := make(map[string]int, len(p.nodes))
	for _, n := range p.nodes {
		indeg[n.id] = n.inDeg
	}
	queue := make([]*pnode, 0, len(p.nodes))
	for _, n := range p.nodes {
		if indeg[n.id] == 0 {
			queue = append(queue, n)
		}
	}
	visited := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited++
		for _, e := range n.out {
			indeg[e.to.id]--
			if indeg[e.to.id] == 0 {
				queue = append(queue, e.to)
			}
		}
	}
	if visited != len(p.nodes) {
		return fmt.Errorf("graphexec: graph is cyclic (%d of %d nodes reachable topologically)", visited, len(p.nodes))
	}
	return nil
}

// HasStateful reports whether any transform node buffers state across batches
// (implements Flusher) — e.g. a window operator. Such a node needs a state
// snapshot to resume correctly; see Snapshotters for the nodes that actually
// provide one (a Flusher without Snapshotter still runs, just without
// checkpoint/resume support for its buffered state).
func (p *Plan) HasStateful() bool {
	for _, n := range p.nodes {
		if n.tx == nil {
			continue
		}
		if _, ok := n.tx.(Flusher); ok {
			return true
		}
	}
	return false
}

// Snapshotters returns every transform node whose operator implements
// Snapshotter, keyed by node id — used by the caller to restore state before a
// resumed Run (once, from a persisted checkpoint) and to capture state at
// each checkpoint barrier (see the pipeline engine, action/pipeline/engine.go).
func (p *Plan) Snapshotters() map[string]Snapshotter {
	out := make(map[string]Snapshotter)
	for _, n := range p.nodes {
		if n.tx == nil {
			continue
		}
		if s, ok := n.tx.(Snapshotter); ok {
			out[n.id] = s
		}
	}
	return out
}

// RewireSource replaces the operator on an already-compiled KindSource node
// WITHOUT rebuilding the rest of the plan — used by callers that run the SAME
// compiled transform/sink chain across multiple partitions of a horizontally-
// scaled source (a fresh partition-scoped SourceOp per call, matching the
// linear engine's one-sink-per-shard reuse). The caller owns both the OLD and
// NEW source's lifecycle exactly like Node.PreBuilt: RewireSource neither
// closes the previous operator nor registers the new one as a plan-owned
// closer. Pair with Options.KeepOpen so intervening Run calls don't tear down
// the transform/sink chain this is meant to reuse.
func (p *Plan) RewireSource(nodeID string, src SourceOp) error {
	n, ok := p.byID[nodeID]
	if !ok {
		return fmt.Errorf("graphexec: RewireSource: no node %q", nodeID)
	}
	if n.kind != KindSource {
		return fmt.Errorf("graphexec: RewireSource: node %q is not a source", nodeID)
	}
	n.src = src
	return nil
}

// Transforms returns every transform operator in the plan, in node order. The
// caller can type-assert them to capability interfaces it owns (e.g. an HWM or
// DQ provider) to harvest post-run state, without this package importing those
// transform types — the same decoupling the Builders injection gives Compile.
func (p *Plan) Transforms() []TransformOp {
	ops := make([]TransformOp, 0, len(p.nodes))
	for _, n := range p.nodes {
		if n.tx != nil {
			ops = append(ops, n.tx)
		}
	}
	return ops
}

// Sources returns every source operator in the plan, in node order, for
// capability type-assertions (e.g. offset committers) by the caller.
func (p *Plan) Sources() []SourceOp {
	var ops []SourceOp
	for _, n := range p.nodes {
		if n.kind == KindSource && n.src != nil {
			ops = append(ops, n.src)
		}
	}
	return ops
}

// sinkCount returns the number of sink nodes (barriers the coordinator awaits).
func (p *Plan) sinkCount() int {
	c := 0
	for _, n := range p.nodes {
		if n.kind == KindSink {
			c++
		}
	}
	return c
}

// Close releases every operator in the plan (used when a plan is compiled but
// not run, or to clean up after Run).
func (p *Plan) Close() error {
	var first error
	for _, n := range p.nodes {
		if n.closer != nil {
			if err := n.closer.Close(); err != nil && first == nil {
				first = err
			}
		}
		for _, r := range n.replicas {
			if err := r.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

// CloseSinks closes every sink operator and returns the first error, so the
// caller can surface errors that only occur at Close (e.g. an object-store
// upload). Closed sinks are not closed again by Close.
func (p *Plan) CloseSinks() error {
	var first error
	for _, n := range p.nodes {
		if n.kind == KindSink && n.closer != nil {
			if err := n.closer.Close(); err != nil && first == nil {
				first = err
			}
			n.closer = nil
		}
	}
	return first
}

// Sinks returns every sink operator in the plan, keyed by node id.
func (p *Plan) Sinks() map[string]SinkOp {
	out := make(map[string]SinkOp)
	for _, n := range p.nodes {
		if n.kind == KindSink {
			out[n.id] = n.snk
		}
	}
	return out
}

// sourceCount returns the number of source nodes.
func (p *Plan) sourceCount() int {
	c := 0
	for _, n := range p.nodes {
		if n.kind == KindSource {
			c++
		}
	}
	return c
}
