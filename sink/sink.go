// Package sink defines the Sink interface and the global sink registry.
// Sinks receive RecordBatches from the transform pipeline and write them to
// their destination (database, file, Kafka topic, object storage, etc.).
package sink

import (
	"fmt"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Sink writes RecordBatches to a destination.
// Implementations must be safe to call from a single goroutine.
// For distributed writes (e.g. Kafka fan-out), use a wrapper that manages
// internal concurrency.
type Sink interface {
	// WriteBatch writes a batch of records to the destination.
	// Must be idempotent when called with the same batch (for retry safety).
	WriteBatch(batch *record.RecordBatch) error

	// Flush forces any buffered writes to the underlying destination.
	// Called after every checkpoint interval and at pipeline completion.
	Flush() error

	// Close flushes and releases all resources held by the sink.
	Close() error
}

// Stats carries write statistics returned by a sink at close time.
type Stats struct {
	RowsWritten int64
	RowsFailed  int64
	// RowsDeleted counts rows a delete-capable sink (e.g. the jdbc sink's
	// opColumn mode) actually removed from the destination — distinct from
	// RowsWritten, which counts rows submitted to WriteBatch regardless of
	// whether they were inserted/upserted or deleted. -1 means "not tracked"
	// (the sink doesn't implement StatsReporter, or has no delete concept).
	RowsDeleted int64
}

// StatsReporter is an optional Sink interface for sinks that track
// operation-level statistics beyond a single write count — most notably a
// delete-capable/CDC-target sink's insert-vs-delete breakdown. The engine
// queries this after Close(), if implemented, to enrich lineage/audit output,
// so a CDC run's "N rows deleted" is visible instead of folded into an
// undifferentiated RowsWritten total (a plain aggregate count cannot tell
// "1000 inserts" apart from "500 inserts + 500 deletes" otherwise). Sinks
// without a delete concept simply do not implement this.
type StatsReporter interface {
	SinkStats() Stats
}

// Reconciler is an optional Sink interface mirroring source.Reconciler — an
// independent trust-verification query run against the sink's own
// already-open connection, used by a data contract's optional post-run
// reconciliation check (action/pipeline's ContractDef.Reconciliation). Only
// jdbc implements this today; other sink types simply don't.
type Reconciler interface {
	// ReconciliationCount runs query (typically "SELECT COUNT(*) FROM ...")
	// against the sink's own connection and returns the single integer result.
	ReconciliationCount(query string) (int64, error)
}

// Factory constructs a Sink from a generic config map.
type Factory func(cfg map[string]interface{}) (Sink, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a sink type name with its Factory.
// Called from each sink sub-package's init() function.
// Panics on duplicate registration.
func Register(typ string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[typ]; ok {
		panic(fmt.Sprintf("sink: type %q already registered", typ))
	}
	registry[typ] = f
}

// New creates a Sink for the given type using the provided config.
func New(typ string, cfg map[string]interface{}) (Sink, error) {
	mu.RLock()
	f, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sink: unknown type %q — import the sink sub-package", typ)
	}
	return f(cfg)
}

// Types returns the names of all registered sink types.
func Types() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}

// UnflushedRowsError is returned by a buffering sink when a flush fails and
// rows accepted by EARLIER WriteBatch calls (already reported as written) were
// not persisted. Rows/Columns hand those rows back so the engine can isolate
// and dead-letter them instead of losing them silently. The rows of the
// current WriteBatch call are never included — the caller still owns them.
type UnflushedRowsError struct {
	Columns []record.Column
	Rows    [][]interface{}
	Err     error
}

func (e *UnflushedRowsError) Error() string {
	return fmt.Sprintf("%v (%d previously accepted rows not persisted)", e.Err, len(e.Rows))
}

func (e *UnflushedRowsError) Unwrap() error { return e.Err }

// AtomicWriter is implemented by sinks whose WriteBatch+Flush either persists
// every row of the batch or none of them (e.g. a single database
// transaction). The engine then isolates bad rows by bisection — retrying
// halves of a failed batch — instead of one row at a time, which is only safe
// when a failed attempt cannot have written part of the batch.
type AtomicWriter interface {
	AtomicWrites() bool
}
