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

// EdgeType classifies an edge the way Flogo Flow classifies links between
// activities. Applied to a stream of rows leaving a node:
//
//   - EdgeSuccess    — every successfully-processed row (the main continuation;
//     tees when it coexists with condition edges).
//   - EdgeCondition  — successful rows for which Cond evaluates true (additive:
//     a matching row still takes any EdgeSuccess edge as well).
//   - EdgeOtherwise  — successful rows that matched NONE of the node's condition
//     edges (the "else" fallback).
//   - EdgeError      — rows/batches that FAILED at the node (routed to an error
//     sink / DLQ branch instead of aborting the run).
type EdgeType string

const (
	EdgeSuccess   EdgeType = "success"
	EdgeCondition EdgeType = "expression"
	EdgeOtherwise EdgeType = "otherwise"
	EdgeError     EdgeType = "error"
)

// Edge connects one node's output to another node's input. From and To may be
// bare node ids or port-qualified ("node:port"); the executor routes by node id
// (ports are retained for authoring/round-trip and future port-aware routing).
// Type selects the routing role; Cond holds the boolean row predicate for an
// EdgeCondition edge. An empty Type is treated as EdgeSuccess.
type Edge struct {
	From string
	To   string
	Type EdgeType
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

// Splitter is an OPTIONAL capability a transform may implement for per-row error
// routing: it partitions a batch into the rows that PASSED and the rows that were
// REJECTED, returning both as NEW batches (the caller owns the input). The graph
// runner calls ApplySplit instead of Apply when the node has an error edge, and
// routes the rejected rows down that edge — so e.g. a validate node emits its
// violations to an error sink row-by-row instead of to its own internal sink.
// Satisfied structurally (implementers do not import this package).
type Splitter interface {
	ApplySplit(in *record.RecordBatch) (pass *record.RecordBatch, reject *record.RecordBatch, err error)
}

// Flusher is an OPTIONAL capability a transform may implement to emit rows it has
// buffered once its input is exhausted — e.g. a window operator emitting its
// still-open aggregates at end-of-stream. The graph runner calls Flush after the
// node's input channel closes and forwards the returned batch downstream.
// Satisfied structurally by transform/window's WindowedOperator.Flush.
type Flusher interface {
	Flush() (*record.RecordBatch, error)
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
	RowsErrored int64
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
