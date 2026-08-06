package graphexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// countingCloseSource wraps memSource to prove a Node.PreBuilt source's
// lifecycle stays with the CALLER — Compile must not register it as a closer,
// so Plan.Run's internal Close() (called even on a clean finish) must not
// close it.
type countingCloseSource struct {
	*memSource
	closes int
}

func (c *countingCloseSource) Close() error {
	c.closes++
	return c.memSource.Close()
}

func TestCompile_PreBuiltSource_SkipsBuilderAndIsNotClosedByPlan(t *testing.T) {
	src := &countingCloseSource{memSource: &memSource{
		cols: []record.Column{{Name: "amount"}},
		rows: [][]interface{}{{1}, {2}, {3}},
	}}
	sinkOp := &memSink{}
	sourceBuilderCalled := false

	g := &Graph{
		Nodes: []Node{
			{ID: "source", Kind: KindSource, Type: "fake", PreBuilt: src},
			{ID: "sink", Kind: KindSink, Type: "mem"},
		},
		Edges: []Edge{{From: "source", To: "sink"}},
	}
	b := Builders{
		Source: func(typ string, cfg map[string]interface{}) (SourceOp, error) {
			sourceBuilderCalled = true
			return nil, fmt.Errorf("Builders.Source should not be called when PreBuilt is set")
		},
		Transform: func(typ string, cfg map[string]interface{}) (TransformOp, error) {
			return nil, fmt.Errorf("no transform expected in this graph")
		},
		Sink: func(typ string, cfg map[string]interface{}) (SinkOp, error) { return sinkOp, nil },
		Predicate: func(expr string) (Pred, error) {
			return nil, fmt.Errorf("no predicate expected in this graph")
		},
	}

	plan, err := Compile(g, b)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if sourceBuilderCalled {
		t.Fatal("Builders.Source was called even though the source node had PreBuilt set")
	}

	res, err := plan.Run(context.Background(), Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RowsRead != 3 {
		t.Fatalf("RowsRead = %d, want 3", res.RowsRead)
	}
	if sinkOp.rowCount() != 3 {
		t.Fatalf("sink rows = %d, want 3", sinkOp.rowCount())
	}
	if src.closes != 0 {
		t.Fatalf("PreBuilt source was closed %d times by the Plan — its lifecycle must stay with the caller", src.closes)
	}
}
