package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/app/resource"
	"github.com/project-flogo/core/data/metadata"
	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/audit"
	"github.com/mpandav-tibco/flogo-datapipeline/lineage"
	"github.com/mpandav-tibco/flogo-datapipeline/metrics"
)

var flog = corelog.ChildLogger(corelog.RootLogger(), "pipeline")

// globalFactory is the singleton Factory instance registered via action.Register.
// It is set during Initialize() so callers can obtain a reference after engine start.
var globalFactory *Factory

// GlobalFactory returns the singleton Factory that was registered and initialized
// by the Flogo engine. Returns nil before engine.New/Initialize has been called.
// Used by the REST API server to access the action cache without requiring the
// caller to thread a *Factory through the entire call stack.
func GlobalFactory() *Factory { return globalFactory }

// Factory implements action.Factory for DataPipelineAction.
// One Factory exists per Flogo app. It caches one DataPipelineAction per
// pipeline resource reference — multiple handlers pointing at the same
// res://pipeline:X share a single action instance.
type Factory struct {
	mu      sync.RWMutex
	actions map[string]*DataPipelineAction
	rm      *resource.Manager
}

// Settings holds the action-level settings parsed from the handler config.
type Settings struct {
	PipelineRef string `md:"pipelineRef"`
}

var _ action.Factory = (*Factory)(nil)

// Initialize is called once when the Flogo app starts.
// It stores the resource manager for later use in New().
func (f *Factory) Initialize(ctx action.InitContext) error {
	f.actions = make(map[string]*DataPipelineAction)
	f.rm = ctx.ResourceManager()
	globalFactory = f
	flog.Info("DataPipeline action factory initialized")
	return nil
}

// New constructs or returns a cached DataPipelineAction for the given config.
// The cache key is the pipelineRef setting — multiple handlers that reference
// the same pipeline resource share one action instance (and its stats counters).
func (f *Factory) New(cfg *action.Config) (action.Action, error) {
	s := &Settings{}
	if err := metadata.MapToStruct(cfg.Settings, s, true); err != nil {
		return nil, fmt.Errorf("pipeline factory: bad settings: %w", err)
	}
	if s.PipelineRef == "" {
		return nil, fmt.Errorf("pipeline factory: pipelineRef is required")
	}

	// Hold the write lock for the entire check-then-create sequence to prevent
	// two concurrent New() calls for the same ref from both creating a metrics
	// backend (which would try to bind the same port twice) and leaking the first.
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.actions[s.PipelineRef]; ok {
		return existing, nil
	}

	// Load and parse the pipeline resource.
	res := f.rm.GetResource(s.PipelineRef)
	if res == nil {
		return nil, fmt.Errorf("pipeline factory: resource %q not found", s.PipelineRef)
	}
	def, ok := res.Object().(*Definition)
	if !ok {
		return nil, fmt.Errorf("pipeline factory: resource %q is not a pipeline definition", s.PipelineRef)
	}

	// Build the metrics recorder from the observability config.
	// metrics.New returns NoopRecorder{} when Metrics is "" or "none", so the
	// recorder field is always non-nil and the engine calls it unconditionally.
	rec, err := metrics.New(def.Observability.Metrics, def.Observability.Options)
	if err != nil {
		// A bad metrics config is a startup error — fail fast rather than
		// silently falling back, so operators know their config is wrong.
		return nil, fmt.Errorf("pipeline factory: metrics backend %q: %w", def.Observability.Metrics, err)
	}

	aud, err := audit.New(def.Observability.Audit)
	if err != nil {
		return nil, fmt.Errorf("pipeline factory: audit log: %w", err)
	}
	lin, err := lineage.New(def.Observability.Lineage)
	if err != nil {
		return nil, fmt.Errorf("pipeline factory: lineage: %w", err)
	}

	act := &DataPipelineAction{
		def:      def,
		ref:      s.PipelineRef,
		ioMd:     buildIOMetadata(),
		recorder: rec,
		auditor:  aud,
		lineage:  lin,
	}

	f.actions[s.PipelineRef] = act

	flog.Infof("created DataPipelineAction for %s (source: %s → sink: %s, metrics: %s)",
		s.PipelineRef, def.Source.Type, def.Sink.Type,
		metricsLabel(def.Observability.Metrics))
	return act, nil
}

// Actions returns a snapshot of the ref → DataPipelineAction cache.
// The returned map is a shallow copy; callers must not mutate it.
// Used by the REST API server to list and operate on registered pipelines.
func (f *Factory) Actions() map[string]*DataPipelineAction {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*DataPipelineAction, len(f.actions))
	for ref, act := range f.actions {
		out[ref] = act
	}
	return out
}

// NewInline constructs a DataPipelineAction directly from a Definition without
// going through the Flogo resource manager or factory cache.
// It is used by the SDK Run() path for pipeline-as-code inline execution.
// The ref is an arbitrary identifier (e.g. "sdk://my-pipeline") used in logs
// and metric labels.
func NewInline(def *Definition, ref string) (*DataPipelineAction, error) {
	rec, err := metrics.New(def.Observability.Metrics, def.Observability.Options)
	if err != nil {
		return nil, fmt.Errorf("pipeline inline: metrics backend %q: %w",
			def.Observability.Metrics, err)
	}
	aud, err := audit.New(def.Observability.Audit)
	if err != nil {
		return nil, fmt.Errorf("pipeline inline: audit log: %w", err)
	}
	lin, err := lineage.New(def.Observability.Lineage)
	if err != nil {
		return nil, fmt.Errorf("pipeline inline: lineage: %w", err)
	}
	return &DataPipelineAction{
		def:      def,
		ref:      ref,
		ioMd:     buildIOMetadata(),
		recorder: rec,
		auditor:  aud,
		lineage:  lin,
	}, nil
}

// store inserts or replaces the action for ref, lazily creating the map so a
// zero-value Factory (e.g. in tests, or before Initialize) is safe to use.
func (f *Factory) store(ref string, act *DataPipelineAction) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actions == nil {
		f.actions = make(map[string]*DataPipelineAction)
	}
	f.actions[ref] = act
}

// Replace atomically swaps the cached action for ref with a freshly built one
// from def, so subsequent runs use the new definition without an engine
// restart. It creates the entry when ref is not yet registered. It is used by
// the REST API PUT /v1/pipelines/{ref} runtime-update path.
//
// Observability handling avoids churning the metrics backend's bound port:
//   - When the new definition's Observability config is unchanged, the existing
//     recorder/auditor/lineage are transferred to the new action (no rebind, no
//     dropped exporter), which also preserves metric continuity.
//   - Otherwise fresh recorders are built first; only after that succeeds is the
//     previous action shut down. A build failure therefore leaves the previous
//     action intact (the update is all-or-nothing).
//
// In-flight runs holding the previous action pointer complete unaffected.
func (f *Factory) Replace(ref string, def *Definition) (*DataPipelineAction, error) {
	if ref == "" {
		return nil, fmt.Errorf("pipeline factory: ref is required")
	}
	if def == nil {
		return nil, fmt.Errorf("pipeline factory: definition is required")
	}

	f.mu.RLock()
	old := f.actions[ref]
	f.mu.RUnlock()

	// Fast path: observability unchanged — reuse the live recorders so the
	// metrics port/exporter is neither rebound nor torn down.
	if old != nil && reflect.DeepEqual(old.def.Observability, def.Observability) {
		act := &DataPipelineAction{
			def:      def,
			ref:      ref,
			ioMd:     buildIOMetadata(),
			recorder: old.recorder,
			auditor:  old.auditor,
			lineage:  old.lineage,
		}
		f.store(ref, act)
		flog.Infof("replaced DataPipelineAction for %s (source: %s → sink: %s, observability unchanged)",
			ref, def.Source.Type, def.Sink.Type)
		return act, nil
	}

	// Observability changed (or brand-new ref): build fresh recorders. Because
	// the config differs, their ports do not collide with the previous action's,
	// so building before the old action is shut down is safe and gives clean
	// rollback if the build fails.
	act, err := NewInline(def, ref)
	if err != nil {
		return nil, err
	}

	f.store(ref, act)

	if old != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = old.Shutdown(ctx)
		cancel()
	}

	flog.Infof("replaced DataPipelineAction for %s (source: %s → sink: %s, metrics: %s)",
		ref, def.Source.Type, def.Sink.Type, metricsLabel(def.Observability.Metrics))
	return act, nil
}

// metricsLabel returns a display-friendly label for the metrics backend name.
func metricsLabel(name string) string {
	if name == "" || name == "none" {
		return "none (noop)"
	}
	return name
}
