// Package sdk provides a fluent Go API (pipeline-as-code) for constructing and
// running flogo-datapipeline pipelines without a Flogo app.json file.
//
// Quick start:
//
//	result, err := sdk.NewPipeline("daily-load").
//	    FromCSV("/data/orders.csv").
//	    Filter("amount > 0").
//	    Map(map[string]string{"order_id": "=$id", "total": "=$amount"}).
//	    ToJDBC("host=localhost dbname=orders", "orders_enriched").
//	    Run(ctx)
//
// The SDK builder pattern builds a pipeline.Definition and either:
//   - Runs it inline (Run) using a temporary DataPipelineAction — no Flogo engine,
//     no app.json, suitable for scripts, tests, and CI/CD pipelines.
//   - Serialises it to JSON (MarshalJSON) for use as a Flogo resource file that
//     can be imported into Flogo Studio or embedded in a Flogo app.json.
package sdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	apitypes "github.com/mpandav-tibco/flogo-datapipeline/api"
)

// PipelineBuilder is the entry point for the fluent pipeline-as-code DSL.
// Construct one via NewPipeline and chain builder methods to describe the
// pipeline topology. Call Build() to validate and produce a Definition, or
// Run() to execute inline without a Flogo app.
type PipelineBuilder struct {
	name string
	def  *pipeline.Definition
	errs []error // accumulated validation errors from builder methods
}

// NewPipeline creates a new PipelineBuilder with the given human-readable name.
// The name is used as the pipeline Definition.Name and as the resource ID when
// MarshalJSON serialises the definition to a Flogo resource file.
func NewPipeline(name string) *PipelineBuilder {
	return &PipelineBuilder{
		name: name,
		def: &pipeline.Definition{
			Name: name,
		},
	}
}

// addErr records a builder error for later reporting by Build().
func (b *PipelineBuilder) addErr(format string, args ...interface{}) {
	b.errs = append(b.errs, fmt.Errorf(format, args...))
}

// Build validates the accumulated configuration and returns the resulting
// pipeline.Definition. Returns an error if any builder methods recorded
// validation errors (e.g. duplicate source, missing required field).
//
// Build does NOT execute the pipeline. Use Run() to execute inline.
func (b *PipelineBuilder) Build() (*pipeline.Definition, error) {
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("pipeline %q: %w", b.name, b.errs[0])
	}
	if b.def.Source.Type == "" {
		return nil, fmt.Errorf("pipeline %q: source is required — call one of FromCSV / FromJDBC / FromKafka", b.name)
	}
	if b.def.Sink.Type == "" {
		return nil, fmt.Errorf("pipeline %q: sink is required — call one of ToJDBC / ToKafka / ToS3", b.name)
	}
	// Apply default values so the definition is immediately usable.
	b.def.Defaults()
	return b.def, nil
}

// MarshalJSON serialises the pipeline definition as a Flogo resource JSON object.
// The output is valid as the "data" field of a Flogo resource entry with id
// "pipeline:<name>", so it can be embedded directly into an app.json or written
// as a standalone resource file for import into Flogo Studio.
//
// Example app.json snippet:
//
//	{
//	  "id": "pipeline:daily-load",
//	  "data": <MarshalJSON output>
//	}
func (b *PipelineBuilder) MarshalJSON() ([]byte, error) {
	def, err := b.Build()
	if err != nil {
		return nil, err
	}
	return marshalDefinition(def)
}

// Run builds the pipeline definition, constructs a temporary DataPipelineAction,
// and executes the pipeline synchronously in the calling goroutine.
// This is the 'pipeline-as-code' path that does not require a Flogo app.json or
// a running Flogo engine — suitable for scripts, tests, and CI/CD jobs.
//
// The returned *apitypes.PipelineRunResponse always has a non-nil value; check
// Status for "completed" / "partial" / "failed". A non-nil error is returned
// only when the pipeline was aborted by a context cancellation or an
// unrecoverable infrastructure failure (open/close errors); business-logic
// errors (failed rows, DQ violations) are captured inside the response.
func (b *PipelineBuilder) Run(ctx context.Context) (*apitypes.PipelineRunResponse, error) {
	def, err := b.Build()
	if err != nil {
		return nil, err
	}

	// Generate a stable ref and run ID for this inline execution.
	ref := "sdk://" + b.name
	runID := newSDKRunID()

	act, err := pipeline.NewInline(def, ref)
	if err != nil {
		return nil, fmt.Errorf("pipeline %q: build inline action: %w", b.name, err)
	}

	in := &pipeline.Input{
		RunID: runID,
	}

	startedAt := time.Now().UTC()
	out, runErr := act.RunDirect(ctx, in)

	resp := &apitypes.PipelineRunResponse{
		RunID:         runID,
		Status:        out.Status,
		RowsProcessed: out.RowsProcessed,
		RowsWritten:   out.RowsWritten,
		RowsFailed:    out.RowsFailed,
		RowsDropped:   out.RowsDropped,
		DurationMs:    out.DurationMs,
		Error:         out.Error,
		StartedAt:     startedAt,
		FinishedAt:    startedAt.Add(time.Duration(out.DurationMs) * time.Millisecond),
	}
	return resp, runErr
}

// newSDKRunID generates a random 16-hex-character run ID for SDK-inline runs.
func newSDKRunID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("sdk-%d", time.Now().UnixNano())
	}
	return "sdk-" + hex.EncodeToString(b)
}
