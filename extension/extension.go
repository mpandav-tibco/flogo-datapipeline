// Package extension is the stable, public contract for writing flogo-datapipeline
// plugins — custom sources, sinks, and transforms — in your own Go module,
// without modifying the engine.
//
// # Model
//
// The engine discovers plugins through a registry. A plugin:
//
//  1. implements one of the three interfaces below (Source, Sink, or Transform),
//  2. registers a named factory in an init() function, and
//  3. is activated in the host binary by a blank import of your package.
//
// Adding, removing, or updating a plugin never touches the engine or any other
// plugin: the engine depends only on these interfaces, and each plugin lives in
// its own package. This is the same mechanism the Go standard library uses for
// database/sql drivers and image decoders.
//
// Every name here is a type alias to the engine's internal packages, so an
// extension.Source *is* a source.Source — there is no adapter layer and no copy.
// The aliases give external authors a single, documented import whose surface is
// meant to stay stable across releases.
//
// See docs/extending.md for the full guide and examples/extension-plugin for a
// runnable out-of-tree module.
package extension

import (
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// ─── Data contract ───────────────────────────────────────────────────────────

// RecordBatch is the unit of work every plugin operates on: a shared column
// schema plus rows of untyped values. Sources produce batches, transforms
// reshape them, sinks consume them.
type (
	RecordBatch = record.RecordBatch
	Column      = record.Column
	ColumnType  = record.ColumnType
)

// Column value types — the T in each field's ["null", T] schema.
const (
	TypeString  = record.TypeString
	TypeInt     = record.TypeInt
	TypeFloat   = record.TypeFloat
	TypeBool    = record.TypeBool
	TypeDate    = record.TypeDate
	TypeDecimal = record.TypeDecimal
	TypeAny     = record.TypeAny
)

// Batch-pool helpers. A source MAY use GetBatch/PutBatch to recycle allocations
// on hot paths, but a plain &RecordBatch{...} is equally valid.
//
// GetBatch acquires a pooled RecordBatch; release it with PutBatch once the next
// stage has fully consumed it.
func GetBatch() *RecordBatch  { return record.Get() }
func PutBatch(b *RecordBatch) { record.Put(b) }

// ─── Source contract ─────────────────────────────────────────────────────────

// Source reads an input and emits it as a stream of RecordBatches. Implement it
// to support a new format or system, then register with RegisterSource.
//
// The optional capability interfaces (Committer, PartitionableSource) are
// detected by the engine via type assertion, so implementing them is purely
// additive — a Source that omits them simply gets safe default behaviour.
type (
	Source              = source.Source
	Committer           = source.Committer
	PartitionableSource = source.PartitionableSource
	Partition           = source.Partition
	SourceFactory       = source.Factory
)

// RegisterSource registers a Source factory under a unique format name.
// Call it from your package's init(). It panics on a duplicate name (a
// programming error), which surfaces the clash at process start.
func RegisterSource(name string, f SourceFactory) { source.Register(name, f) }

// ─── Sink contract ───────────────────────────────────────────────────────────

// Sink writes RecordBatches to a destination. Implement it to support a new
// output, then register with RegisterSink.
type (
	Sink        = sink.Sink
	SinkFactory = sink.Factory
)

// RegisterSink registers a Sink factory under a unique type name.
// Call it from your package's init(). It panics on a duplicate name.
func RegisterSink(name string, f SinkFactory) { sink.Register(name, f) }

// ─── Transform contract ──────────────────────────────────────────────────────

// Transform reshapes a RecordBatch: filter, map, enrich, aggregate, or route.
// Implement it and register with RegisterTransform.
//
// A transform that writes to its own external sink(s) should also implement the
// optional SinkOwner interface so the engine keeps it on the single-threaded
// path (avoiding duplicate writers on the parallel path).
type (
	Transform        = transform.Transform
	SinkOwner        = transform.SinkOwner
	TransformFactory = transform.Factory
)

// RegisterTransform registers a Transform factory under a unique type name.
// Call it from your package's init(). It panics on a duplicate name.
func RegisterTransform(name string, f TransformFactory) { transform.Register(name, f) }
