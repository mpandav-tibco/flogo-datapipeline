package pipeline

import (
	"testing"
)

// TestContract_AutoAppendsDQGate_EnforcesQuality proves a Contract with
// Expectations set actually auto-appends a dq gate to the transform chain
// (with no user-declared dq transform present) and that gate really
// evaluates: a run whose data violates the contract's expectations is marked
// "failed" (onViolation: "fail"), and a run whose data satisfies them
// completes normally.
func TestContract_AutoAppendsDQGate_EnforcesQuality(t *testing.T) {
	mkDef := func(csv string) *Definition {
		src := writeTemp(t, "orders.csv", csv)
		outPath := writeTemp(t, "out.jsonl", "")
		return &Definition{
			Source: csvSource(src),
			Sink:   jsonlSink(outPath),
			Contract: &ContractDef{
				Expectations: []interface{}{
					map[string]interface{}{"name": "amount not null", "type": "expectColumnNotNull", "column": "amount"},
				},
				QualityThreshold: 1.0,
				OnViolation:      "fail",
			},
		}
	}

	t.Run("data satisfying the contract completes normally", func(t *testing.T) {
		out, err := runInline(t, mkDef("id,name,amount,currency\n1,Alice,100,USD\n2,Bob,50,EUR\n"))
		if err != nil {
			t.Fatalf("RunDirect: %v", err)
		}
		if out.Status != "completed" {
			t.Errorf("status = %q, want completed (error=%s)", out.Status, out.Error)
		}
	})

	t.Run("data violating the contract is rejected as partial", func(t *testing.T) {
		// amount is empty (null) for id=2 — violates expectColumnNotNull at
		// qualityThreshold 1.0. A dq gate's own "failure" is a batch-level
		// transform error (same as any other transform error), which the
		// engine routes to RowsFailed rather than aborting the whole run
		// outright — so the observable outcome is status "partial" with
		// RowsFailed > 0, proving the auto-appended contract gate actually
		// evaluated and rejected the batch.
		out, err := runInline(t, mkDef("id,name,amount,currency\n1,Alice,100,USD\n2,Bob,,EUR\n"))
		if err != nil {
			t.Fatalf("RunDirect: %v", err)
		}
		if out.Status != "partial" || out.RowsFailed == 0 {
			t.Errorf("status=%q rowsFailed=%d, want partial with RowsFailed>0 — the auto-appended contract dq gate should have caught the null amount", out.Status, out.RowsFailed)
		}
	})
}

// TestContract_DoesNotDoubleGate_WhenExplicitDQPresent proves that when the
// pipeline ALREADY has an explicit dq transform, the contract's own
// Expectations are NOT also auto-appended as a second gate. The contract's
// expectations below are deliberately impossible (would always fail) so that
// if double-gating were happening, the run would fail; since it's correctly
// skipped, only the lenient explicit dq transform runs and the run completes.
func TestContract_DoesNotDoubleGate_WhenExplicitDQPresent(t *testing.T) {
	src := writeTemp(t, "orders.csv", "id,name,amount,currency\n1,Alice,100,USD\n2,Bob,200,EUR\n")
	outPath := writeTemp(t, "out.jsonl", "")
	def := &Definition{
		Source: csvSource(src),
		Sink:   jsonlSink(outPath),
		Transforms: []TransformDef{
			{Type: "dq", Extra: map[string]interface{}{
				"type": "dq",
				"expectations": []interface{}{
					map[string]interface{}{"name": "amount not null (lenient, always passes)", "type": "expectColumnNotNull", "column": "amount"},
				},
				"qualityThreshold": 0.5,
				"onFail":           "warn",
			}},
		},
		Contract: &ContractDef{
			Expectations: []interface{}{
				// Impossible: "amount" never equals this bogus value, so this
				// expectation ALWAYS scores 0 — if this were (incorrectly)
				// auto-appended alongside the explicit dq stage above, the run
				// would fail.
				map[string]interface{}{"name": "impossible", "type": "expectColumnValuesInSet", "column": "amount", "values": []interface{}{"never-matches-anything"}},
			},
			QualityThreshold: 0.99,
			OnViolation:      "fail",
		},
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Errorf("status = %q (error=%s), want completed — the contract's impossible expectation must NOT have been auto-appended since an explicit dq transform is already present", out.Status, out.Error)
	}
}

// TestContract_MaxDurationMsSLA_MarksPartial proves finalizeStatus applies
// the contract's MaxDurationMs SLA check: an otherwise-"completed" run whose
// DurationMs exceeds the configured ceiling is reclassified "partial", while
// a run within budget (or with no contract/no ceiling) stays "completed".
// Tested directly against finalizeStatus (not a real slow pipeline) for a
// fast, deterministic assertion with no timing flakiness.
func TestContract_MaxDurationMsSLA_MarksPartial(t *testing.T) {
	newAction := func(contract *ContractDef) *DataPipelineAction {
		def := &Definition{
			Source:   SourceDef{Type: "csv"},
			Sink:     SinkDef{Type: "null"},
			Contract: contract,
		}
		def.Defaults()
		act, err := NewInline(def, "sdk://contract-sla-test")
		if err != nil {
			t.Fatalf("NewInline: %v", err)
		}
		return act
	}
	runLog := newRunLog("sdk://contract-sla-test", "test-run")

	t.Run("exceeding maxDurationMs marks an otherwise-completed run partial", func(t *testing.T) {
		act := newAction(&ContractDef{MaxDurationMs: 100})
		out := &Output{DurationMs: 500}
		act.finalizeStatus(runLog, out, nil)
		if out.Status != "partial" {
			t.Errorf("status = %q, want partial (500ms exceeds the 100ms SLA)", out.Status)
		}
	})

	t.Run("within budget stays completed", func(t *testing.T) {
		act := newAction(&ContractDef{MaxDurationMs: 1000})
		out := &Output{DurationMs: 500}
		act.finalizeStatus(runLog, out, nil)
		if out.Status != "completed" {
			t.Errorf("status = %q, want completed (500ms is within the 1000ms SLA)", out.Status)
		}
	})

	t.Run("no contract never applies the SLA check", func(t *testing.T) {
		act := newAction(nil)
		out := &Output{DurationMs: 999999}
		act.finalizeStatus(runLog, out, nil)
		if out.Status != "completed" {
			t.Errorf("status = %q, want completed (no contract = no SLA)", out.Status)
		}
	})

	t.Run("a failed run is not reclassified by the SLA check", func(t *testing.T) {
		act := newAction(&ContractDef{MaxDurationMs: 1})
		out := &Output{DurationMs: 999999}
		act.finalizeStatus(runLog, out, errRunFailedForTest)
		if out.Status != "failed" {
			t.Errorf("status = %q, want failed (runErr takes precedence over the SLA check)", out.Status)
		}
	})
}

var errRunFailedForTest = &testRunError{"simulated run failure"}

type testRunError struct{ msg string }

func (e *testRunError) Error() string { return e.msg }
