// Package transform defines the Transform interface, the Pipeline (a chain of
// transforms), and the global transform registry.
// Each transform receives a RecordBatch and produces a (possibly smaller or
// reshaped) RecordBatch. Transforms must not retain references to the input
// batch after returning — the pipeline engine may recycle it via record.Put.
package transform

import (
	"fmt"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Transform processes a RecordBatch and returns a new one.
// A transform may:
//   - filter rows (return a batch with fewer rows)
//   - map fields (return a batch with different columns)
//   - enrich rows (add columns via lookup)
//   - aggregate rows (reduce many rows to fewer)
//   - route bad rows to a dead-letter sink (validate)
//
// Returning a nil batch signals that all rows were filtered or routed out;
// the pipeline skips the batch rather than passing an empty one downstream.
type Transform interface {
	// Name returns the human-readable transform type for logging.
	Name() string
	// Apply processes the input batch and returns the output batch.
	// The transform owns the output batch; the caller owns the input batch.
	Apply(in *record.RecordBatch) (*record.RecordBatch, error)
	// Close releases any resources held by the transform (e.g. LRU cache,
	// database connection used by lookup transforms).
	Close() error
}

// Pipeline chains multiple transforms in sequence.
// The output of transform N is the input of transform N+1.
type Pipeline struct {
	stages []Transform
}

// NewPipeline constructs a Pipeline from an ordered slice of transforms.
func NewPipeline(stages []Transform) *Pipeline {
	return &Pipeline{stages: stages}
}

// Stages returns the ordered slice of transform stages.
// Used by the engine to locate specific transform implementations via type
// assertions (e.g. to find the hwm transform's HWMProvider interface).
func (p *Pipeline) Stages() []Transform { return p.stages }

// Apply runs the batch through all stages in order.
// Returns nil if any stage reduces the batch to zero rows.
func (p *Pipeline) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	cur := in
	for _, t := range p.stages {
		if cur == nil || cur.Len() == 0 {
			return nil, nil
		}
		out, err := t.Apply(cur)
		if err != nil {
			return nil, fmt.Errorf("transform %s: %w", t.Name(), err)
		}
		cur = out
	}
	return cur, nil
}

// Close closes all stages in reverse order.
func (p *Pipeline) Close() error {
	var last error
	for i := len(p.stages) - 1; i >= 0; i-- {
		if err := p.stages[i].Close(); err != nil {
			last = err
		}
	}
	return last
}

// Factory constructs a Transform from a generic config map.
type Factory func(cfg map[string]interface{}) (Transform, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a transform type name with its Factory.
// Called from each transform sub-package's init() function.
// Panics on duplicate registration.
func Register(typ string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[typ]; ok {
		panic(fmt.Sprintf("transform: type %q already registered", typ))
	}
	registry[typ] = f
}

// New creates a Transform for the given type using the provided config.
func New(typ string, cfg map[string]interface{}) (Transform, error) {
	mu.RLock()
	f, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transform: unknown type %q — import the transform sub-package", typ)
	}
	return f(cfg)
}

// Build constructs a Pipeline from a slice of transform configs.
// Each config map must have a "type" key identifying the transform.
func Build(configs []map[string]interface{}) (*Pipeline, error) {
	stages := make([]Transform, 0, len(configs))
	for i, cfg := range configs {
		typ, _ := cfg["type"].(string)
		if typ == "" {
			return nil, fmt.Errorf("transform[%d]: missing 'type' field", i)
		}
		t, err := New(typ, cfg)
		if err != nil {
			return nil, fmt.Errorf("transform[%d] (%s): %w", i, typ, err)
		}
		stages = append(stages, t)
	}
	return NewPipeline(stages), nil
}
