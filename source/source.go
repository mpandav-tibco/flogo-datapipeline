// Package source defines the Source interface and the global format registry.
// Each file format (csv, parquet, fixedwidth, xml, jsonlines, …) ships its own
// sub-package that calls Register() in its init() function, so the caller only
// needs a blank import to activate a format.
package source

import (
	"fmt"
	"io"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Source reads data from a format-specific location and exposes it as a stream
// of RecordBatches. Implementations must be safe to call from a single
// goroutine (the pipeline reader goroutine); they need not be concurrent-safe.
type Source interface {
	// Schema returns the column definitions for all rows this source will emit.
	// Called once after Open, before the first ReadBatch.
	Schema() []record.Column

	// ReadBatch reads the next batch of up to size rows.
	// Returns io.EOF (with a nil batch) when the source is exhausted.
	// A partial batch (len < size) on the last call before EOF is valid.
	ReadBatch(size int) (*record.RecordBatch, error)

	// Skip advances the source by n rows without emitting them.
	// Used by the checkpoint layer to resume from a saved position.
	// Must be called before the first ReadBatch.
	Skip(n int64) error

	// Close releases any file handles or network connections held by the source.
	Close() error
}

// Committer is an optional Source interface for sources whose read position is
// acknowledged to an external system (e.g. a Kafka consumer committing offsets).
// When implemented, the pipeline engine calls Commit() at each checkpoint AFTER
// the sink is flushed and the checkpoint token (including this source's Cursor)
// is persisted. Advancing the external acknowledgement only after the token is
// durable guarantees at-least-once delivery: a crash re-delivers everything
// after the last checkpoint, so no in-flight record is lost. Sources without an
// external ack simply do not implement this (a clean no-op).
type Committer interface {
	// Commit durably acknowledges consumption up to the position reported by the
	// most recent ReadBatch. Called by the engine at checkpoint and completion.
	Commit() error
}

// PartitionableSource extends Source with the ability to enumerate and open
// independent sub-ranges of the data, enabling multi-instance parallel reads.
// Not all formats support partitioning; the pipeline falls back to single-
// instance mode when the source only implements Source.
type PartitionableSource interface {
	Source

	// Partitions enumerates the logical partitions of this source.
	// targetCount is a hint; the source may return more or fewer.
	Partitions(targetCount int) ([]Partition, error)

	// OpenPartition opens a new Source scoped to a single partition.
	// The returned Source must implement at least the base Source interface.
	OpenPartition(p Partition) (Source, error)
}

// Partition is the unit of work for multi-instance distributed execution.
// The Token field is format-specific and opaque to the pipeline engine;
// only the source that produced the partition can interpret it.
type Partition struct {
	// ID is a stable, deterministic identifier used as the checkpoint key.
	ID string `json:"id"`
	// Token carries the format-specific position information.
	// CSV/text: int64 byte offset. Parquet: int row-group index.
	// Multi-file: string file path.
	Token interface{} `json:"token"`
	// SizeHint is the estimated uncompressed byte size of this partition.
	// Used for load balancing when assigning partitions to instances.
	SizeHint int64 `json:"sizeHint"`
}

// Factory constructs a Source from a generic config map.
// The map keys match the JSON field names in the pipeline resource definition.
type Factory func(cfg map[string]interface{}) (Source, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a format name with its Factory.
// Called from each source sub-package's init() function.
// Panics on duplicate registration (programming error).
func Register(format string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[format]; ok {
		panic(fmt.Sprintf("source: format %q already registered", format))
	}
	registry[format] = f
}

// New creates a Source for the given format using the provided config.
func New(format string, cfg map[string]interface{}) (Source, error) {
	mu.RLock()
	f, ok := registry[format]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("source: unknown format %q — import the format sub-package", format)
	}
	return f(cfg)
}

// Formats returns the names of all registered source formats.
func Formats() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}

// Sentinel re-export so callers can use source.EOF without importing io.
var EOF = io.EOF
