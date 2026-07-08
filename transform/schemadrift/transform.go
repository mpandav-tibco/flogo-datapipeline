// Package schemadrift registers a schema-drift detection and remediation
// transform. Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/schemadrift"
//
// The transform is auto-inserted as the first pipeline stage by the action
// engine when `schemaDrift.policy` is set in the pipeline definition. It can
// also be added manually as a named stage in the `transforms` list.
//
// # Config keys
//
//	policy  string  "project" | "additive" | "strict" | "drop"  (default "project")
//	onDrift string  "log" | "alert" | "silent"                   (default "log")
//
// # Behaviour summary
//
//	additive : new columns are merged into the baseline and propagated downstream.
//	           Type changes for existing columns adopt the new type (promotion).
//	project  : only the original baseline columns are passed downstream.
//	           New columns are silently ignored; dropped source columns emit nil.
//	strict   : any schema change returns an error, aborting the pipeline run.
//	drop     : any batch whose schema deviates from the baseline is discarded in
//	           full (nil is returned, which causes the pipeline to skip the batch).
package schemadrift

import (
	"fmt"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/schema"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

var slog = corelog.ChildLogger(corelog.RootLogger(), "schemadrift")

func init() {
	transform.Register("schemadrift", newSchemaDriftTransform)
}

// schemaDriftTransform is a stateful transform that tracks the schema of the
// first batch it sees (the baseline) and applies a configurable DriftPolicy
// to subsequent batches whose schema deviates from that baseline.
//
// Lifecycle:
//   - baseline == nil  → first batch: baseline is established, batch passes through unchanged.
//   - baseline != nil  → subsequent batches: Diff is computed; policy is applied.
//
// The transform is safe for sequential (single-goroutine) use only. The action
// engine guarantees this: each pipeline run has exactly one goroutine calling Apply.
type schemaDriftTransform struct {
	// baseline is the canonical schema established from the first batch.
	// It grows in DriftPolicyAdditive mode when new columns are encountered.
	baseline []record.Column

	// policy determines what to do when drift is detected.
	policy schema.DriftPolicy

	// onDrift controls how detected changes are surfaced to the operator.
	// "log"    — emit Warn-level log entries for each ColumnChange.
	// "alert"  — same as "log" (reserved for future metric/alert integration).
	// "silent" — suppress all drift logging.
	onDrift string
}

// newSchemaDriftTransform is the Factory function registered for type "schemadrift".
func newSchemaDriftTransform(cfg map[string]interface{}) (transform.Transform, error) {
	policy, _ := cfg["policy"].(string)
	if policy == "" {
		policy = string(schema.DriftPolicyProject)
	}
	// Validate the policy value so bad configs surface at startup, not at runtime.
	switch schema.DriftPolicy(policy) {
	case schema.DriftPolicyAdditive, schema.DriftPolicyStrict,
		schema.DriftPolicyDrop, schema.DriftPolicyProject:
		// valid
	default:
		return nil, fmt.Errorf("schemadrift: unknown policy %q; valid values: additive|strict|drop|project", policy)
	}

	onDrift, _ := cfg["onDrift"].(string)
	if onDrift == "" {
		onDrift = "log"
	}
	switch onDrift {
	case "log", "alert", "silent":
		// valid
	default:
		return nil, fmt.Errorf("schemadrift: unknown onDrift %q; valid values: log|alert|silent", onDrift)
	}

	return &schemaDriftTransform{
		policy:  schema.DriftPolicy(policy),
		onDrift: onDrift,
	}, nil
}

// Name implements transform.Transform.
func (s *schemaDriftTransform) Name() string { return "schemadrift" }

// Apply implements transform.Transform.
//
// On the first call (baseline == nil) the baseline is established from the
// incoming batch's Columns and the batch is returned unchanged.
//
// On subsequent calls:
//   - schema.Diff detects any added, dropped, or retyped columns.
//   - If no changes are detected the batch is returned in the fast path.
//   - Otherwise the configured DriftPolicy is applied (see package doc).
//
// Returning nil signals to the pipeline engine that the batch should be
// skipped; this is the correct behaviour for DriftPolicyDrop.
func (s *schemaDriftTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	// First batch: establish the baseline schema and pass through unchanged.
	if s.baseline == nil {
		s.baseline = make([]record.Column, len(in.Columns))
		copy(s.baseline, in.Columns)
		return in, nil
	}

	// Detect schema drift between the baseline and the incoming batch.
	changes := schema.Diff(s.baseline, in.Columns)
	if len(changes) == 0 {
		// Fast path: no drift — return as-is.
		return in, nil
	}

	// Surface the changes to the operator according to the onDrift setting.
	s.notifyDrift(changes)

	switch s.policy {
	case schema.DriftPolicyStrict:
		// Abort the pipeline run. The action engine will mark the run as "failed"
		// and call handler.HandleResult with the error.
		return nil, fmt.Errorf("schemadrift: policy=strict — schema changed: %v", changes)

	case schema.DriftPolicyAdditive:
		// Grow the baseline by merging in new and retyped columns.
		s.mergeBaseline(changes)
		// Reproject the input batch onto the updated (possibly larger) baseline.
		// schema.Project fills nil for any source columns not yet present in the
		// batch, and drops source columns not in the baseline (there are none in
		// additive mode — the baseline always grows to include everything seen).
		projected := schema.Project(in, s.baseline)
		// Return the original input batch to the caller's record.Put; the
		// projected batch is the new pipeline currency.
		return projected, nil

	case schema.DriftPolicyProject:
		// Keep only the original baseline columns. New columns in the incoming
		// batch are silently dropped; missing source columns yield nil.
		return schema.Project(in, s.baseline), nil

	case schema.DriftPolicyDrop:
		// Discard the entire batch. Returning nil causes Pipeline.Apply to skip
		// all downstream stages. The action engine's RowsFailed counter is NOT
		// incremented for dropped batches; use RowsDropped instead.
		return nil, nil
	}

	// Unreachable after constructor validation, but satisfies the compiler.
	return in, nil
}

// Close implements transform.Transform. No resources to release.
func (s *schemaDriftTransform) Close() error { return nil }

// notifyDrift emits log warnings (or is silent) for each detected ColumnChange.
func (s *schemaDriftTransform) notifyDrift(changes []schema.ColumnChange) {
	if s.onDrift == "silent" {
		return
	}
	// "log" and "alert" both emit Warn-level entries. Future versions may hook
	// "alert" into a metrics counter or send a notification.
	for _, c := range changes {
		slog.Warnf("schema drift detected: column=%q kind=%s old_type=%s new_type=%s",
			c.Column, c.Kind, c.OldType, c.NewType)
	}
}

// mergeBaseline integrates a set of changes into the baseline schema.
// Only "added" and "retyped" changes modify the baseline; "dropped" changes
// leave the baseline column in place (it will be filled with nil by Project).
func (s *schemaDriftTransform) mergeBaseline(changes []schema.ColumnChange) {
	for _, c := range changes {
		switch c.Kind {
		case "added":
			// Append the new column to the end of the baseline.
			s.baseline = append(s.baseline, record.Column{
				Name: c.Column,
				Type: c.NewType,
			})
		case "retyped":
			// Update the type in-place; additive mode allows type promotion.
			for i, col := range s.baseline {
				if col.Name == c.Column {
					s.baseline[i].Type = c.NewType
					break
				}
			}
		// "dropped": intentionally not removed from baseline — Project will emit nil.
		}
	}
}
