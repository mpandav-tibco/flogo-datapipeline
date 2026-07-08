// Package watermark defines the high-water-mark (HWM) persistence abstraction
// used by incremental-load pipelines.
//
// A watermark records the maximum value seen for a configured column (e.g.
// updated_at, seq_id) across all rows written in previous pipeline runs.
// On the next run the pipeline source injects a WHERE clause predicate
// "column > value" so only new or changed rows are read.
//
// Implementations are registered via Register() in each backend's init().
// Import a backend sub-package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/file"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/redis"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/watermark/postgres"
package watermark

import (
	"fmt"
	"sync"
	"time"
)

// Mark is the persisted high-water-mark value for one pipeline.
type Mark struct {
	// Column is the watermark column name (e.g. "updated_at", "seq_id").
	Column string `json:"column"`
	// Value is the string representation of the last seen max value.
	// Stored as string to be type-agnostic; the source coerces it to the
	// declared column type when injecting the WHERE predicate.
	Value string `json:"value"`
	// ValueType distinguishes the underlying type so the source can parse
	// Value correctly. One of "datetime" | "int" | "string".
	ValueType string `json:"valueType"`
	// SavedAt records when this mark was persisted (for audit).
	SavedAt time.Time `json:"savedAt"`
}

// Store persists and retrieves high-water marks.
// Implementations must be goroutine-safe.
type Store interface {
	// Load returns the current mark for pipelineID, or nil if none exists.
	// A nil mark means this is the first run; the source should do a full-table load.
	Load(pipelineID string) (*Mark, error)
	// Save atomically persists the mark for pipelineID.
	Save(pipelineID string, m *Mark) error
	// Close releases any resources held by this store (connections, file handles).
	Close() error
}

// Factory constructs a Store from a generic config map.
type Factory func(cfg map[string]interface{}) (Store, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a watermark store type name with its Factory.
// Called from each backend sub-package's init() function.
// Panics on duplicate registration (programming error).
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[name]; ok {
		panic(fmt.Sprintf("watermark: type %q already registered", name))
	}
	registry[name] = f
}

// New creates a Store for the given type using the provided config.
func New(name string, cfg map[string]interface{}) (Store, error) {
	mu.RLock()
	f, ok := registry[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("watermark: unknown store type %q — import the backend sub-package", name)
	}
	return f(cfg)
}

// NoopStore is used when incremental load is disabled (mode == "none").
// All operations are no-ops; Load always returns nil (full-table load every run).
type NoopStore struct{}

func (NoopStore) Load(_ string) (*Mark, error) { return nil, nil }
func (NoopStore) Save(_ string, _ *Mark) error { return nil }
func (NoopStore) Close() error                 { return nil }
