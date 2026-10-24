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
//   - No-loss checkpointing (stateless graphs): the source can emit a drain
//     barrier every N rows — it pauses, the barrier flows through every edge,
//     fan-in nodes forward it once seen on all inputs, and each sink flushes and
//     acks. Only after every sink is durable does the coordinator persist the
//     source position, so a crash resumes from there with no committed rows lost
//     (see Options.CheckpointRows / OnCheckpoint / ResumeFrom). Stateful graphs
//     (a Flusher node) opt out via Plan.HasStateful until state snapshots land.
//
// It is the pipeline's only executor: a linear definition compiles to a
// three-node plan (source → fused transform chain → sink), an explicit
// non-reducible graph to one node per graph node.
package graphexec

import (
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

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
	// PreBuilt, when set on a KindSource node, is used directly as that node's
	// operator instead of calling Builders.Source(Type, Cfg) — used for
	// horizontally-scaled runs where the caller has already opened a
	// partition-scoped source. The CALLER owns PreBuilt's lifecycle: Compile
	// does not add it to the Plan's close list (mirrors how the linear engine's
	// scaling path closes a partition source itself — see runScaled).
	PreBuilt SourceOp
	// Operator, on a KindTransform or KindSink node, is a ready-made operator
	// (a TransformOp or SinkOp) used instead of calling the builder. Unlike
	// PreBuilt, the plan owns it and closes it with the plan.
	Operator interface{}
	// Replicas, on a KindTransform node, are additional independent instances
	// of the node's operator. The node then applies batches across all
	// instances in parallel and re-sequences the results, so downstream order
	// is unchanged (performance.transformWorkers). Only for stateless,
	// order-insensitive operators without error edges; the plan closes them.
	Replicas []TransformOp
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
// source.Source (which additionally exposes Schema). Skip advances past already-
// processed rows when resuming from a checkpoint.
type SourceOp interface {
	ReadBatch(size int) (*record.RecordBatch, error)
	Skip(n int64) error
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

// Snapshotter is an OPTIONAL capability a STATEFUL transform (e.g. window)
// implements to serialize/restore its keyed state across a checkpoint —
// mirroring the linear engine's own window Snapshot()/Restore() contract
// exactly (same method names; transform/window's windowTransform already
// satisfies this via structural typing, zero changes needed there).
//
// Snapshot is called by the checkpoint coordinator at each drain barrier —
// the instant every producer in the graph has paused and no batch is in
// flight anywhere (the same guarantee that lets every sink flush durably), so
// it's safe to call directly without additional synchronization. Restore is
// called once by the CALLER (graph_run.go) before the first Run, to prime a
// resumed node's state from a previously persisted snapshot — graphexec
// itself never calls Restore; see Plan.Snapshotters.
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// MultiInputOp is an OPTIONAL capability for a transform with two DISTINCT
// named input ports rather than one homogeneous stream — e.g. a keyed join's
// "left" (the streaming primary continuation) and "right" (a bounded
// reference dataset that must be FULLY LOADED before any left row can be
// matched against it, exactly like the existing config-driven join transform's
// own in-memory table — this just lets the "right" side be wired from an
// arbitrary upstream node/subgraph via a real graph edge instead of embedded
// config).
//
// A node is scheduled via this capability ONLY when its operator implements
// MultiInputOp AND it has at least one inbound edge targeting the "right"
// port by name (an edge whose To is "nodeID:right"). A MultiInputOp-capable
// operator used with NO "right"-port edges (e.g. an existing join promoted
// into a graph for unrelated reasons, still configured with an inline "right"
// dataset) runs through the ordinary TransformOp.Apply path, completely
// unaffected — see graphExecutable's reducible-graph fallback for the same
// "don't disturb pipelines that don't need this" principle.
//
// Execution order (see runtime.runMultiInputTransform): every right-port
// producer's batches are drained to LoadRight and CloseRight is called ONCE
// they've all finished — BEFORE any left-port batch reaches ApplyLeft. A
// checkpoint barrier arriving on the right channel is ignored (the bounded
// right side is not part of the primary checkpoint story); barriers are
// tracked against the LEFT port's producer count only.
type MultiInputOp interface {
	// LoadRight ingests one batch of the bounded reference side. Called
	// repeatedly until every right-port producer has finished.
	LoadRight(in *record.RecordBatch) error
	// CloseRight signals every right-port producer has finished — the
	// operator should finalize its in-memory index here.
	CloseRight() error
	// ApplyLeft processes one batch of the streaming primary side, exactly
	// like TransformOp.Apply, but is only called AFTER CloseRight.
	ApplyLeft(in *record.RecordBatch) (*record.RecordBatch, error)
}

// SinkOp writes batches to a destination. Satisfied structurally by sink.Sink.
type SinkOp interface {
	WriteBatch(b *record.RecordBatch) error
	Flush() error
	Close() error
}

// Pred is a compiled boolean row predicate used for conditional edges. The row
// map is reused across rows, so a Pred must not retain it after returning.
type Pred func(row map[string]interface{}) bool

// PredBinder resolves a condition against one column layout, returning a
// predicate over positional rows in that layout.
type PredBinder func(cols []record.Column) func(row []interface{}) bool

// Builders supplies the concrete constructors the compiler needs. Injecting
// them keeps this package free of the source/sink/transform/filter imports and
// lets tests substitute fakes.
type Builders struct {
	Source    func(typ string, cfg map[string]interface{}) (SourceOp, error)
	Transform func(typ string, cfg map[string]interface{}) (TransformOp, error)
	Sink      func(typ string, cfg map[string]interface{}) (SinkOp, error)
	// Predicate compiles an edge condition into a reusable row predicate.
	Predicate func(expr string) (Pred, error)
	// BindPredicate optionally compiles an edge condition into a schema-bindable
	// form. When supplied it is used instead of Predicate on the hot path, so
	// condition routing evaluates positional rows without building row maps.
	BindPredicate func(expr string) (PredBinder, error)
}

// Options tunes the run. Zero values fall back to sensible defaults.
type Options struct {
	// BatchSize is the row count requested from each source ReadBatch.
	BatchSize int
	// BufferDepth is the per-node input channel capacity (backpressure window).
	BufferDepth int
	// CheckpointRows, when > 0, makes the source emit a drain barrier every N
	// rows: it pauses, the barrier flushes every sink, and OnCheckpoint persists
	// the source position — so a crash resumes from there with no data loss.
	CheckpointRows int
	// OnCheckpoint persists the source row position once every sink has flushed
	// at a barrier. Required for checkpointing; ignored when CheckpointRows == 0.
	OnCheckpoint func(rowsProcessed int64) error
	// ResumeFrom, when > 0, skips the source that many rows before the first read
	// (resuming from a saved checkpoint).
	ResumeFrom int64
	// KeepOpen, when true, skips Run's internal Plan.Close() so the SAME compiled
	// transform/sink operators can be reused across multiple Run calls — used by
	// horizontally-scaled runs that swap the source (via Plan.RewireSource)
	// between partitions while keeping one open sink per shard, exactly like the
	// linear engine's runScaled/drain reuse one sink across every partition an
	// instance owns. The caller must call Plan.Close() itself once, after the
	// LAST Run call.
	KeepOpen bool

	// DeferFlush skips the end-of-input Flush of stateful transform nodes
	// (Flusher), for a caller running one plan over several partitions that
	// flushes once after the last one.
	DeferFlush bool
	// SinkBufferDepth, when > 0, is the input channel depth of sink nodes
	// (performance.sinkBufferBatches); other nodes use BufferDepth.
	SinkBufferDepth int
	// CheckpointInterval, when > 0, also emits a checkpoint barrier once this
	// much time has passed since the last one — including on idle polls, when
	// rows are pending or HasUncommitted reports unacknowledged source
	// progress (checkpoint.intervalSeconds). Requires OnCheckpoint.
	CheckpointInterval time.Duration
	HasUncommitted     func() bool
	// Budget, when set with SizeOf, bounds bytes read but not yet consumed
	// (performance.maxMemoryMB).
	Budget *MemoryBudget
	SizeOf func(*record.RecordBatch) int64
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
