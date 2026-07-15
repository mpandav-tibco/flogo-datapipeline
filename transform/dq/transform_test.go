package dq

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func build(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := transform.New("dq", cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr
}

func mkBatch() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{
			{Name: "id", Type: record.TypeInt},
			{Name: "amount", Type: record.TypeFloat},
			{Name: "status", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{1, 100.0, "OK"},
			{2, 250.0, "OK"},
			{3, 900.0, "PENDING"},
		},
	}
}

func TestDQ_ConfigValidation(t *testing.T) {
	if _, err := transform.New("dq", map[string]interface{}{"onFail": "bogus",
		"expectations": []interface{}{map[string]interface{}{"type": "expectTableRowCountBetween", "min": 1, "max": 10}}}); err == nil {
		t.Error("expected error for bad onFail")
	}
	if _, err := transform.New("dq", map[string]interface{}{"expectations": []interface{}{}}); err == nil {
		t.Error("expected error when no expectations")
	}
}

func TestDQ_WarnPassesAllRows(t *testing.T) {
	tr := build(t, map[string]interface{}{
		"onFail":           "warn",
		"qualityThreshold": 0.95,
		"expectations": []interface{}{
			map[string]interface{}{"type": "expectColumnNotNull", "column": "id"},
			map[string]interface{}{"type": "expectColumnValuesBetween", "column": "amount", "min": 0.0, "max": 1000.0},
		},
	})
	out, err := tr.Apply(mkBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Len() != 3 {
		t.Errorf("warn should pass all 3 rows, got %d", out.Len())
	}
	rep := tr.(DQGateProvider).LastRunReport()
	if rep == nil || rep.RowsEvaluated != 3 {
		t.Errorf("report missing or wrong: %+v", rep)
	}
}

func TestDQ_FailAborts(t *testing.T) {
	tr := build(t, map[string]interface{}{
		"onFail":           "fail",
		"qualityThreshold": 0.99,
		"expectations": []interface{}{
			// amount 900 is out of [0,500] → violation → low score → abort.
			map[string]interface{}{"type": "expectColumnValuesBetween", "column": "amount", "min": 0.0, "max": 500.0},
		},
	})
	if _, err := tr.Apply(mkBatch()); err == nil {
		t.Error("onFail=fail should abort when score below threshold")
	}
}

func TestDQ_Drop(t *testing.T) {
	tr := build(t, map[string]interface{}{
		"onFail":           "drop",
		"qualityThreshold": 0.99,
		"expectations": []interface{}{
			map[string]interface{}{"type": "expectColumnValuesBetween", "column": "amount", "min": 0.0, "max": 500.0},
		},
	})
	out, err := tr.Apply(mkBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Errorf("drop should discard the batch (nil), got %+v", out)
	}
}

func TestDQ_ScoreColumn(t *testing.T) {
	tr := build(t, map[string]interface{}{
		"onFail":           "warn",
		"scoreColumn":      "_dq_score",
		"qualityThreshold": 0.0,
		"expectations": []interface{}{
			map[string]interface{}{"type": "expectColumnNotNull", "column": "id"},
		},
	})
	out, err := tr.Apply(mkBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.ColIndex("_dq_score") < 0 {
		t.Errorf("score column not appended: %+v", out.Columns)
	}
}

func TestDQ_RowCountAndInSet(t *testing.T) {
	tr := build(t, map[string]interface{}{
		"onFail":           "warn",
		"qualityThreshold": 0.5,
		"expectations": []interface{}{
			map[string]interface{}{"type": "expectTableRowCountBetween", "min": 1, "max": 100},
			map[string]interface{}{"type": "expectColumnValuesInSet", "column": "status", "values": []interface{}{"OK", "PENDING"}},
			map[string]interface{}{"type": "expectColumnUnique", "column": "id"},
		},
	})
	if _, err := tr.Apply(mkBatch()); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rep := tr.(DQGateProvider).LastRunReport()
	if len(rep.ExpectationResults) != 3 {
		t.Errorf("expected 3 expectation results, got %d", len(rep.ExpectationResults))
	}
	if !rep.PassedGate {
		t.Errorf("gate should pass: score=%.3f", rep.OverallScore)
	}
	if tr.Name() != "dq" || tr.Close() != nil {
		t.Error("Name/Close contract")
	}
}
