package graphexec

import (
	"fmt"
	"strings"
)

// pedge is a compiled outbound edge: the target node, the edge's routing role,
// and (for EdgeCondition only) its compiled row predicate.
type pedge struct {
	to    *pnode
	etype EdgeType
	pred  Pred
}

// pnode is a compiled node: its operator (exactly one of src/tx/snk is set by
// kind) plus its outbound edges and inbound-edge count.
type pnode struct {
	id     string
	kind   NodeKind
	src    SourceOp
	tx     TransformOp
	snk    SinkOp
	out    []*pedge
	inDeg  int
	hasErr bool
	closer interface{ Close() error }
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
			op, err := b.Source(n.Type, n.Cfg)
			if err != nil {
				return fail("graphexec: build source %q (%s): %w", n.ID, n.Type, err)
			}
			pn.src, pn.closer = op, op
		case KindTransform:
			op, err := b.Transform(n.Type, n.Cfg)
			if err != nil {
				return fail("graphexec: build transform %q (%s): %w", n.ID, n.Type, err)
			}
			pn.tx, pn.closer = op, op
		case KindSink:
			op, err := b.Sink(n.Type, n.Cfg)
			if err != nil {
				return fail("graphexec: build sink %q (%s): %w", n.ID, n.Type, err)
			}
			pn.snk, pn.closer = op, op
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
		case EdgeSuccess, EdgeOtherwise, EdgeError:
			// no predicate
			if pe.etype == EdgeError {
				from.hasErr = true
			}
		default:
			return fail("graphexec: edge %s→%s has unknown type %q", e.From, e.To, e.Type)
		}
		from.out = append(from.out, pe)
		to.inDeg++
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
// snapshot to resume correctly, which the barrier checkpoint does not yet
// provide, so callers must not enable checkpointing for a plan with one.
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
	}
	return first
}
