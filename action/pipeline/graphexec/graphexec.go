// Package graphexec is a streaming dataflow executor for the pipeline DAG.
//
// The shipping engine runs a single linear chain (source → transforms → sink).
// That is optimal for the common case but cannot express a true graph: multiple
// sources, conditional fan-out (route rows to different successors by a boolean
// predicate), or multiple terminal sinks fed by different edges. This package
// executes such a graph directly.
//
// Design goals:
//   - Pure and dependency-light: the executor imports only `record` (+ stdlib),
//     so it is trivially unit-testable and cannot form an import cycle with the
//     `pipeline` package that owns the on-disk GraphDef. All heavy constructors
//     (source/transform/sink factories, expression compiler) are INJECTED via
//     Builders, and real engine operators satisfy the narrow operator
//     interfaces here structurally.
//   - Streaming, not materialising: every edge is a bounded channel of batches,
//     so memory stays O(batch · edges), matching the linear engine's O(batch)
//     streaming rather than buffering whole datasets.
//   - Correct fan-out ownership: when a batch feeds more than one successor,
//     each successor receives its OWN batch (a copy of the rows that satisfy its
//     edge predicate), so the single-owner batch-pool contract still holds.
//
// The executor is gated behind an explicit graph: the linear path remains the
// default and is untouched.
package graphexec

import "github.com/mpandav-tibco/flogo-datapipeline/record"

// NodeKind is the role a node plays in the graph.
type NodeKind string

const (
	// KindSource reads batches from an external source (no inbound edges).
	KindSource NodeKind = "source"
	// KindTransform processes batches (inbound and outbound edges).
	KindTransform NodeKind = "transform"
	// KindSink writes batches to an external destination (no outbound edges).
	KindSink NodeKind = "sink"
)

// Node is a single vertex in the graph plan. Cfg is the flattened config passed
// verbatim to the matching factory (source.New / transform.New / sink.New).
type Node struct {
	ID   string
	Kind NodeKind
	Type string
	Cfg  map[string]interface{}
}

// Edge connects one node's output to another node's input. From and To may be
// bare node ids or port-qualified ("node:port"); the executor routes by node id
// (ports are retained for authoring/round-trip and future port-aware routing).
// Cond, when non-empty, is a boolean row predicate: only rows for which it
// evaluates true traverse this edge. An empty Cond is an unconditional edge.
type Edge struct {
	From string
	To   string
	Cond string
}

// Graph is the neutral plan the executor consumes. The pipeline package builds
// it from the on-disk GraphDef; tests build it directly.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

// SourceOp reads batches until it returns io.EOF. Satisfied structurally by
// source.Source (which additionally exposes Schema/Skip).
type SourceOp interface {
	ReadBatch(size int) (*record.RecordBatch, error)
	Close() error
}

// TransformOp processes one batch into another (nil = all rows consumed).
// Satisfied structurally by transform.Transform (which additionally exposes
// Name). Per the transform contract the caller owns the input batch and the
// operator owns the output.
type TransformOp interface {
	Apply(in *record.RecordBatch) (*record.RecordBatch, error)
	Close() error
}

// SinkOp writes batches to a destination. Satisfied structurally by sink.Sink.
type SinkOp interface {
	WriteBatch(b *record.RecordBatch) error
	Flush() error
	Close() error
}

// Pred is a compiled boolean row predicate used for conditional edges.
type Pred func(row map[string]interface{}) bool

// Builders supplies the concrete constructors the compiler needs. Injecting
// them keeps this package free of the source/sink/transform/filter imports and
// lets tests substitute fakes.
type Builders struct {
	Source    func(typ string, cfg map[string]interface{}) (SourceOp, error)
	Transform func(typ string, cfg map[string]interface{}) (TransformOp, error)
	Sink      func(typ string, cfg map[string]interface{}) (SinkOp, error)
	// Predicate compiles an edge condition into a reusable row predicate.
	Predicate func(expr string) (Pred, error)
}

// Options tunes the run. Zero values fall back to sensible defaults.
type Options struct {
	// BatchSize is the row count requested from each source ReadBatch.
	BatchSize int
	// BufferDepth is the per-node input channel capacity (backpressure window).
	BufferDepth int
}

func (o Options) batchSize() int {
	if o.BatchSize > 0 {
		return o.BatchSize
	}
	return 5000
}

func (o Options) bufferDepth() int {
	if o.BufferDepth > 0 {
		return o.BufferDepth
	}
	return 4
}

// NodeStat holds per-node row counters gathered during a run. Fields are
// accessed atomically while running and read after the run completes.
type NodeStat struct {
	RowsIn      int64
	RowsOut     int64
	RowsWritten int64
	ID          string
	Kind        NodeKind
}

// Result aggregates the outcome of a run. RowsRead/RowsWritten are the sums
// across all source/sink nodes; Nodes carries per-node detail.
type Result struct {
	RowsRead    int64
	RowsWritten int64
	Nodes       map[string]*NodeStat
}
