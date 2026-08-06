// Package transform defines the Transform interface, the Pipeline (a chain of
// transforms), and the global transform registry.
// Each transform receives a RecordBatch and produces a (possibly smaller or
// reshaped) RecordBatch. Transforms must not retain references to the input
// batch after returning — the pipeline engine may recycle it via record.Put.
package transform

import (
	"errors"
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

// SinkOwner marks a transform that writes rows to its own external sink(s) and
// therefore must run on the single-threaded transform path. The parallel path
// builds one private transform pipeline per worker (see the engine's
// drainParallel), which would open duplicate writers to the same destination —
// corrupting files or racing connections. Transforms that route rows to sinks
// (e.g. router) implement this so the engine forces ordered execution.
type SinkOwner interface {
	// OwnsExternalSink reports whether this transform owns external sink(s).
	OwnsExternalSink() bool
}

// Pipeline chains multiple transforms in sequence.
// The output of transform N is the input of transform N+1.
type Pipeline struct {
	stages  []Transform
	rowsIn  []int64 // per-stage rows consumed, accumulated across batches
	rowsOut []int64 // per-stage rows emitted, accumulated across batches
}

// NewPipeline constructs a Pipeline from an ordered slice of transforms.
func NewPipeline(stages []Transform) *Pipeline {
	return &Pipeline{
		stages:  stages,
		rowsIn:  make([]int64, len(stages)),
		rowsOut: make([]int64, len(stages)),
	}
}

// Stages returns the ordered slice of transform stages.
// Used by the engine to locate specific transform implementations via type
// assertions (e.g. to find the hwm transform's HWMProvider interface).
func (p *Pipeline) Stages() []Transform { return p.stages }

// Apply runs the batch through all stages in order.
// Returns nil if any stage reduces the batch to zero rows.
func (p *Pipeline) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	cur := in
	for i, t := range p.stages {
		if cur == nil || cur.Len() == 0 {
			if cur != nil && cur != in {
				record.Put(cur)
			}
			return nil, nil
		}
		inLen := cur.Len()
		out, err := t.Apply(cur)
		if err != nil {
			return nil, fmt.Errorf("transform %s: %w", t.Name(), err)
		}
		outLen := 0
		if out != nil {
			outLen = out.Len()
		}
		// Per-stage row accounting (RowsOut can exceed RowsIn for fan-out stages
		// such as join/explode, and be smaller for filter/validate/router).
		p.rowsIn[i] += int64(inLen)
		p.rowsOut[i] += int64(outLen)
		// Return the intermediate batch to the pool once we're done with it —
		// but ONLY when the stage produced a DISTINCT batch. Pass-through stages
		// (hwm, dq, schemadrift, validate-keep) return the SAME batch they were
		// given; recycling it here would clear the rows the next stage reads.
		if cur != in && cur != out {
			record.Put(cur)
		}
		cur = out
	}
	return cur, nil
}

// StageStat reports the rows a single transform stage consumed (RowsIn) and
// emitted (RowsOut) across the whole run, accumulated over every batch.
type StageStat struct {
	Name    string
	RowsIn  int64
	RowsOut int64
}

// StageStats returns the per-stage row counters accumulated by Apply. Read it
// after the run completes: the counters are written by Apply on the pipeline's
// own goroutine (the parallel engine gives each worker its own Pipeline), so no
// locking is needed for a post-run read. Aggregate across worker pipelines by
// summing matching stage indices.
func (p *Pipeline) StageStats() []StageStat {
	out := make([]StageStat, len(p.stages))
	for i, t := range p.stages {
		out[i] = StageStat{Name: t.Name(), RowsIn: p.rowsIn[i], RowsOut: p.rowsOut[i]}
	}
	return out
}

// Close closes all stages in reverse order, accumulating all errors.
func (p *Pipeline) Close() error {
	var errs []error
	for i := len(p.stages) - 1; i >= 0; i-- {
		if err := p.stages[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("transform %s close: %w", p.stages[i].Name(), err))
		}
	}
	return errors.Join(errs...)
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

// Types returns the names of all registered transform types.
func Types() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
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
