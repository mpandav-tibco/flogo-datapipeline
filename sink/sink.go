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
