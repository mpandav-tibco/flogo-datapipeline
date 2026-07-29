package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

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
			Edges: []GraphEdge{{"s:rows", "t:in"}, {"t:out", "k:in"}},
		}, false},
		{"fan-out + fan-in ok", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source"), n("r", "transform"), n("a", "sink"), n("b", "sink")},
			Edges: []GraphEdge{{"s", "r"}, {"r:eu", "a"}, {"r:us", "b"}},
		}, false},
		{"empty nodes rejected", GraphDef{Edges: []GraphEdge{{"a", "b"}}}, true},
		{"missing id rejected", GraphDef{Nodes: []GraphNodeDef{{Kind: "source"}}}, true},
		{"duplicate id rejected", GraphDef{Nodes: []GraphNodeDef{n("x", "source"), n("x", "sink")}}, true},
		{"bad kind rejected", GraphDef{Nodes: []GraphNodeDef{n("x", "widget")}}, true},
		{"edge to unknown node rejected", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source")},
			Edges: []GraphEdge{{"s", "ghost"}},
		}, true},
		{"self-loop rejected", GraphDef{
			Nodes: []GraphNodeDef{n("s", "source")},
			Edges: []GraphEdge{{"s:out", "s:in"}},
		}, true},
		{"cycle rejected", GraphDef{
			Nodes: []GraphNodeDef{n("a", "transform"), n("b", "transform"), n("c", "transform")},
			Edges: []GraphEdge{{"a", "b"}, {"b", "c"}, {"c", "a"}},
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
		Edges: []GraphEdge{{"s", "k"}},
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("a definition with a valid graph should be valid: %v", err)
	}

	bad := base()
	bad.Graph = &GraphDef{
		Nodes: []GraphNodeDef{{ID: "a", Kind: "transform"}, {ID: "b", Kind: "transform"}},
		Edges: []GraphEdge{{"a", "b"}, {"b", "a"}},
	}
	if err := bad.Validate(); err == nil {
		t.Fatalf("a definition with a cyclic graph should be rejected by Validate")
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
	if len(g.Edges) != 1 || g.Edges[0].From() != "s:rows" || g.Edges[0].To() != "k:in" {
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
