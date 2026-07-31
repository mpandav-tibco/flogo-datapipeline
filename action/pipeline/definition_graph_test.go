package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

// ge builds a plain (unconditional) graph edge for tests.
func ge(from, to string) GraphEdge { return GraphEdge{From: from, To: to} }

// TestGraphValidate exercises the topology rules of the optional `graph` block:
// unique non-empty ids, known kinds, edge endpoints that reference declared
// nodes, and acyclicity.
func TestGraphValidate(t *testing.T) {
	n := func(id, kind string) GraphNodeDef { return GraphNodeDef{ID: id, Kind: kind} }
	cases := []struct {
		name    string
		graph   GraphDef
		wantErr bool
	}{
		{"linear ok", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source"), n("t", "transform"), n("k", "sink")},
			Edges: []GraphEdge{ge("s:rows", "t:in"), ge("t:out", "k:in")},
		}, false},
		{"fan-out + fan-in ok", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source"), n("r", "transform"), n("a", "sink"), n("b", "sink")},
			Edges: []GraphEdge{ge("s", "r"), ge("r:eu", "a"), ge("r:us", "b")},
		}, false},
		{"empty nodes rejected", GraphDef{Edges: []GraphEdge{ge("a", "b")}}, true},
		{"missing id rejected", GraphDef{Nodes: []GraphNodeDef{{Kind: "source"}}}, true},
		{"duplicate id rejected", GraphDef{Nodes: []GraphNodeDef{n("x", "source"), n("x", "sink")}}, true},
		{"bad kind rejected", GraphDef{Nodes: []GraphNodeDef{n("x", "widget")}}, true},
		{"edge to unknown node rejected", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source")},
			Edges: []GraphEdge{ge("s", "ghost")},
		}, true},
		{"self-loop rejected", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source")},
			Edges: []GraphEdge{ge("s:out", "s:in")},
		}, true},
		{"cycle rejected", GraphDef{
			Nodes: []GraphNodeDef{n("a", "transform"), n("b", "transform"), n("c", "transform")},
			Edges: []GraphEdge{ge("a", "b"), ge("b", "c"), ge("c", "a")},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := c.graph
			err := g.validate()
			if c.wantErr && err == nil {
				t.Fatalf("expected a validation error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
		})
	}
}

// TestDefinitionGraphIntegration proves back-compat: a definition with no graph
// validates exactly as before, a valid graph passes through Definition.Validate,
// and an invalid (cyclic) graph is rejected at the definition level.
func TestDefinitionGraphIntegration(t *testing.T) {
	base := func() *Definition {
		return &Definition{Source: SourceDef{Type: "csv"}, Sink: SinkDef{Type: "jsonlines"}}
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("a definition with no graph should be valid: %v", err)
	}

	ok := base()
	ok.Graph = &GraphDef{
		Nodes: []GraphNodeDef{{ID: "s", Kind: "source"}, {ID: "k", Kind: "sink"}},
		Edges: []GraphEdge{ge("s", "k")},
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("a definition with a valid graph should be valid: %v", err)
	}

	bad := base()
	bad.Graph = &GraphDef{
		Nodes: []GraphNodeDef{{ID: "a", Kind: "transform"}, {ID: "b", Kind: "transform"}},
		Edges: []GraphEdge{ge("a", "b"), ge("b", "a")},
	}
	if err := bad.Validate(); err == nil {
		t.Fatalf("a definition with a cyclic graph should be rejected by Validate")
	}
}

// TestGraphEdgeConditional confirms the conditional (object) edge form parses,
// validates and round-trips, while a plain array edge stays unconditional.
func TestGraphEdgeConditional(t *testing.T) {
	src := []byte(`{"nodes":[{"id":"r","kind":"transform"},{"id":"a","kind":"sink"},{"id":"b","kind":"sink"}],` +
		`"edges":[["r:out","a:in"],{"from":"r:out","to":"b:in","type":"expression","value":"amount > 1000"}]}`)
	var g GraphDef
	if err := json.Unmarshal(src, &g); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(g.Edges) != 2 {
		t.Fatalf("want 2 edges, got %d", len(g.Edges))
	}
	if g.Edges[0].Type != "" || g.Edges[0].From != "r:out" || g.Edges[0].To != "a:in" {
		t.Fatalf("plain edge parsed wrong: %+v", g.Edges[0])
	}
	if g.Edges[1].Type != "expression" || g.Edges[1].Value != "amount > 1000" || g.Edges[1].To != "b:in" {
		t.Fatalf("conditional edge parsed wrong: %+v", g.Edges[1])
	}
	if err := g.validate(); err != nil {
		t.Fatalf("conditional graph should validate: %v", err)
	}
	out, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `["r:out","a:in"]`) {
		t.Fatalf("plain edge should marshal to the array form: %s", out)
	}
	var g2 GraphDef
	if err := json.Unmarshal(out, &g2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if g2.Edges[0].Type != "" || g2.Edges[1].Type != "expression" || g2.Edges[1].Value != "amount > 1000" {
		t.Fatalf("edge types did not round-trip: %+v", g2.Edges)
	}
	bad := GraphDef{Nodes: []GraphNodeDef{{ID: "a", Kind: "source"}, {ID: "b", Kind: "sink"}}, Edges: []GraphEdge{{From: "a", To: "b", Type: "bogus"}}}
	if err := bad.validate(); err == nil {
		t.Fatalf("unknown edge type should be rejected")
	}
}

// TestGraphJSONRoundTrip confirms the graph block decodes from the board-08
// shape (nodes + 2-array edges), preserves node config in Extra, and re-marshals
// without dropping that config.
func TestGraphJSONRoundTrip(t *testing.T) {
	src := []byte(`{"nodes":[{"id":"s","kind":"source","type":"csv","path":"a.csv"},{"id":"k","kind":"sink","type":"jsonlines"}],"edges":[["s:rows","k:in"]]}`)
	var g GraphDef
	if err := json.Unmarshal(src, &g); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(g.Nodes) != 2 || g.Nodes[0].ID != "s" || g.Nodes[0].Type != "csv" {
		t.Fatalf("nodes not parsed: %+v", g.Nodes)
	}
	if g.Nodes[0].Extra["path"] != "a.csv" {
		t.Fatalf("node Extra config not preserved: %+v", g.Nodes[0].Extra)
	}
	if len(g.Edges) != 1 || g.Edges[0].From != "s:rows" || g.Edges[0].To != "k:in" {
		t.Fatalf("edges not parsed: %+v", g.Edges)
	}
	out, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `"path":"a.csv"`) {
		t.Fatalf("re-marshaled graph dropped node config: %s", out)
	}
}

// TestPromotedGraphShapeValidates proves the exact graph block Studio's
// deriveGraph emits (board-08 nodes + edges for a router/join/validate/dlq
// pipeline) passes the engine validator — verifying the Studio→engine round-trip.
func TestPromotedGraphShapeValidates(t *testing.T) {
	src := []byte(`{
      "source": {"type":"csv","path":"orders.csv"},
      "sink": {"type":"jsonlines"},
      "graph": {
        "nodes": [
          {"id":"source","kind":"source","type":"csv","ports":{"out":["rows"]}},
          {"id":"t0","kind":"transform","type":"join","ports":{"in":["left","right"],"out":["out"]}},
          {"id":"t1","kind":"transform","type":"validate","ports":{"in":["in"],"out":["pass","reject"]}},
          {"id":"t2","kind":"transform","type":"router","ports":{"in":["in"],"out":["eu","us","default","out"]}},
          {"id":"sink","kind":"sink","type":"jsonlines","ports":{"in":["in"]}},
          {"id":"source.err","kind":"sink","type":"csv","ports":{"in":["in"]}},
          {"id":"t0.right","kind":"source","type":"csv","ports":{"out":["rows"]}},
          {"id":"t1.reject","kind":"sink","type":"csv","ports":{"in":["in"]}},
          {"id":"t2.route0","kind":"sink","type":"jsonlines","ports":{"in":["in"]}},
          {"id":"t2.route1","kind":"sink","type":"jsonlines","ports":{"in":["in"]}},
          {"id":"t2.default","kind":"sink","type":"jsonlines","ports":{"in":["in"]}},
          {"id":"dlq","kind":"sink","type":"null","ports":{"in":["in"]}}
        ],
        "edges": [
          ["source:rows","t0:in"],
          ["t0:out","t1:in"],
          ["t1:out","t2:in"],
          ["t2:out","sink:in"],
          ["source:errors","source.err:in"],
          ["t0.right:rows","t0:right"],
          ["t1:reject","t1.reject:in"],
          ["t2:eu","t2.route0:in"],
          ["t2:us","t2.route1:in"],
          ["t2:default","t2.default:in"],
          ["sink:err","dlq:in"]
        ]
      }
    }`)
	var def Definition
	if err := json.Unmarshal(src, &def); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	def.Defaults()
	if err := def.Validate(); err != nil {
		t.Fatalf("promoted graph shape should validate, got: %v", err)
	}
	if def.Graph == nil || len(def.Graph.Nodes) != 12 || len(def.Graph.Edges) != 11 {
		t.Fatalf("graph not parsed as expected: %+v", def.Graph)
	}
}

