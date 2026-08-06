package dq

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"

	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
)

// captureSink records rows written to it so onFail="route" quarantining can
// be asserted without needing a live error-sink backend.
type captureSink struct{ rows [][]interface{} }

var lastCapture *captureSink

func init() {
	sink.Register("dq_capture", func(map[string]interface{}) (sink.Sink, error) {
		lastCapture = &captureSink{}
		return lastCapture, nil
	})
}

func (c *captureSink) WriteBatch(b *record.RecordBatch) error {
	c.rows = append(c.rows, b.Rows...)
	return nil
}
func (c *captureSink) Flush() error { return nil }
func (c *captureSink) Close() error { return nil }

func profileOf(b *record.RecordBatch) *BatchProfile {
	p := &BatchProfile{}
	p.Compute(b)
	return p
}

func sampleBatch() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{
			{Name: "id", Type: record.TypeInt},
			{Name: "amount", Type: record.TypeFloat},
			{Name: "status", Type: record.TypeString},
		},
		Rows: [][]interface{}{
			{1, 10.0, "OK"},
			{2, 20.0, "OK"},
			{3, nil, "BAD"},
			{4, 40.0, "OK"},
		},
	}
}

func eval(t *testing.T, cfg map[string]interface{}, b *record.RecordBatch) ExpectationResult {
	t.Helper()
	e, err := buildExpectation(cfg)
	if err != nil {
		t.Fatalf("buildExpectation(%v): %v", cfg, err)
	}
	return e.Evaluate(b, profileOf(b))
}

func TestProfiler_Basics(t *testing.T) {
	p := profileOf(sampleBatch())
	if p.RowCount != 4 {
		t.Fatalf("RowCount = %d", p.RowCount)
	}
	amt := p.ColumnStats["amount"]
	if amt.NullCount != 1 || amt.NonNullCount != 3 {
		t.Errorf("amount null/nonnull = %d/%d", amt.NullCount, amt.NonNullCount)
	}
	if amt.Min.(float64) != 10.0 || amt.Max.(float64) != 40.0 {
		t.Errorf("amount min/max = %v/%v", amt.Min, amt.Max)
	}
	// int(1)/int(2)... distinct count for id
	if got := p.ColumnStats["id"].DistinctCount(); got != 4 {
		t.Errorf("id distinct = %d, want 4", got)
	}
}

func TestExpect_ColumnNotNull(t *testing.T) {
	// amount has 1/4 null → completeness 0.75; threshold 1.0 fails.
	r := eval(t, map[string]interface{}{"type": "expectColumnNotNull", "column": "amount"}, sampleBatch())
	if r.Passed {
		t.Error("expected fail at threshold 1.0")
	}
	// threshold 0.7 passes
	r = eval(t, map[string]interface{}{"type": "expectColumnNotNull", "column": "amount", "threshold": 0.7}, sampleBatch())
	if !r.Passed {
		t.Errorf("expected pass at threshold 0.7, score=%v", r.Score)
	}
	// missing column fails
	r = eval(t, map[string]interface{}{"type": "expectColumnNotNull", "column": "nope"}, sampleBatch())
	if r.Passed {
		t.Error("missing column should fail")
	}
}

func TestExpect_ColumnUnique(t *testing.T) {
	// id is fully unique
	r := eval(t, map[string]interface{}{"type": "expectColumnUnique", "column": "id"}, sampleBatch())
	if !r.Passed {
		t.Errorf("id should be unique, score=%v", r.Score)
	}
	// status has duplicates
	r = eval(t, map[string]interface{}{"type": "expectColumnUnique", "column": "status"}, sampleBatch())
	if r.Passed {
		t.Error("status has duplicates, should fail at threshold 1.0")
	}
}

func TestExpect_ValuesBetween(t *testing.T) {
	r := eval(t, map[string]interface{}{"type": "expectColumnValuesBetween", "column": "amount", "min": 0.0, "max": 100.0}, sampleBatch())
	if !r.Passed {
		t.Errorf("all amounts in [0,100], score=%v", r.Score)
	}
	r = eval(t, map[string]interface{}{"type": "expectColumnValuesBetween", "column": "amount", "min": 0.0, "max": 15.0}, sampleBatch())
	if r.Passed {
		t.Error("amounts exceed 15, should fail")
	}
}

func TestExpect_ValuesInSet(t *testing.T) {
	r := eval(t, map[string]interface{}{"type": "expectColumnValuesInSet", "column": "status", "values": []interface{}{"OK", "BAD"}}, sampleBatch())
	if !r.Passed {
		t.Errorf("all statuses in set, score=%v", r.Score)
	}
	r = eval(t, map[string]interface{}{"type": "expectColumnValuesInSet", "column": "status", "values": []interface{}{"OK"}}, sampleBatch())
	if r.Passed {
		t.Error("BAD not in set, should fail")
	}
}

func TestExpect_RowCountBetween(t *testing.T) {
	r := eval(t, map[string]interface{}{"type": "expectTableRowCountBetween", "min": 1.0, "max": 10.0}, sampleBatch())
	if !r.Passed {
		t.Errorf("4 rows in [1,10] should pass")
	}
	r = eval(t, map[string]interface{}{"type": "expectTableRowCountBetween", "min": 10.0, "max": 20.0}, sampleBatch())
	if r.Passed {
		t.Error("4 rows not in [10,20]")
	}
}

func TestExpect_MeanBetween(t *testing.T) {
	// amount mean = (10+20+40)/3 ≈ 23.33
	r := eval(t, map[string]interface{}{"type": "expectColumnMeanBetween", "column": "amount", "min": 20.0, "max": 30.0}, sampleBatch())
	if !r.Passed {
		t.Errorf("mean ~23.3 in [20,30], score=%v obs=%v", r.Score, r.Observed)
	}
}

func TestExpect_StddevBelow(t *testing.T) {
	r := eval(t, map[string]interface{}{"type": "expectColumnStddevBelow", "column": "amount", "threshold": 1000.0}, sampleBatch())
	if !r.Passed {
		t.Errorf("stddev should be below 1000, obs=%v", r.Observed)
	}
}

// referenceCSV writes a small CSV reference dataset (header + rows) to a temp
// file and returns its path, for expectColumnValuesInReferenceDataset tests.
func referenceCSV(t *testing.T, header string, rows ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ref.csv")
	content := header + "\n"
	for _, r := range rows {
		content += r + "\n"
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write reference csv: %v", err)
	}
	return p
}

func TestExpect_ValuesInReferenceDataset(t *testing.T) {
	// sampleBatch's "id" column has values 1,2,3,4 (all non-null); the
	// reference dataset only has 1,2,4 — id=3 is an orphan (referential
	// integrity violation), giving score 3/4 = 0.75.
	refPath := referenceCSV(t, "ref_id", "1", "2", "4")

	r := eval(t, map[string]interface{}{
		"type":   "expectColumnValuesInReferenceDataset",
		"column": "id",
		"referenceDataset": map[string]interface{}{
			"type": "csv", "path": refPath, "hasHeader": true,
		},
		"referenceColumn": "ref_id",
	}, sampleBatch())
	if r.Passed {
		t.Errorf("expected fail at threshold 1.0 (id=3 is orphaned), score=%v", r.Score)
	}
	if r.Score != 0.75 {
		t.Errorf("score = %v, want 0.75", r.Score)
	}

	// A lower threshold tolerates the one orphan.
	r = eval(t, map[string]interface{}{
		"type":   "expectColumnValuesInReferenceDataset",
		"column": "id",
		"referenceDataset": map[string]interface{}{
			"type": "csv", "path": refPath, "hasHeader": true,
		},
		"referenceColumn": "ref_id",
		"threshold":       0.7,
	}, sampleBatch())
	if !r.Passed {
		t.Errorf("expected pass at threshold 0.7, score=%v", r.Score)
	}

	// referenceColumn defaults to column when omitted: reference file's own
	// header IS "id" in this case, so a bare column-name match still works.
	refPath2 := referenceCSV(t, "id", "1", "2", "3", "4")
	r = eval(t, map[string]interface{}{
		"type":   "expectColumnValuesInReferenceDataset",
		"column": "id",
		"referenceDataset": map[string]interface{}{
			"type": "csv", "path": refPath2, "hasHeader": true,
		},
	}, sampleBatch())
	if !r.Passed || r.Score != 1.0 {
		t.Errorf("expected full match (default referenceColumn=column), score=%v", r.Score)
	}
}

func TestExpect_ValuesInReferenceDataset_ConfigErrors(t *testing.T) {
	if _, err := buildExpectation(map[string]interface{}{"type": "expectColumnValuesInReferenceDataset", "column": "id"}); err == nil {
		t.Error("missing referenceDataset should error")
	}
	if _, err := buildExpectation(map[string]interface{}{
		"type": "expectColumnValuesInReferenceDataset", "column": "id",
		"referenceDataset": map[string]interface{}{},
	}); err == nil {
		t.Error("missing referenceDataset.type should error")
	}
	refPath := referenceCSV(t, "ref_id", "1", "2")
	if _, err := buildExpectation(map[string]interface{}{
		"type": "expectColumnValuesInReferenceDataset", "column": "id",
		"referenceDataset": map[string]interface{}{"type": "csv", "path": refPath, "hasHeader": true},
		"referenceColumn":  "nope",
	}); err == nil {
		t.Error("referenceColumn not found in reference dataset should error")
	}
	if _, err := buildExpectation(map[string]interface{}{
		"type": "expectColumnValuesInReferenceDataset", "column": "id",
		"referenceDataset": map[string]interface{}{"type": "csv", "path": refPath, "hasHeader": true},
		"referenceColumn":  "ref_id",
		"maxRows":          1.0,
	}); err == nil {
		t.Error("reference dataset exceeding maxRows should error")
	}
}

func TestBuildExpectation_Errors(t *testing.T) {
	if _, err := buildExpectation(map[string]interface{}{}); err == nil {
		t.Error("missing type should error")
	}
	if _, err := buildExpectation(map[string]interface{}{"type": "bogus"}); err == nil {
		t.Error("unknown type should error")
	}
}

// TestDQTransform_ReferentialIntegrity_RoutesOrphans proves the referential
// check participates in PER-ROW routing (onFail="route"), not just the
// dataset-level score — id=3 (the orphan) must be quarantined to the error
// sink while the other 3 rows pass through untouched.
func TestDQTransform_ReferentialIntegrity_RoutesOrphans(t *testing.T) {
	refPath := referenceCSV(t, "ref_id", "1", "2", "4")
	lastCapture = nil
	tr, err := transform.New("dq", map[string]interface{}{
		"expectations": []interface{}{
			map[string]interface{}{
				"type":   "expectColumnValuesInReferenceDataset",
				"column": "id",
				"referenceDataset": map[string]interface{}{
					"type": "csv", "path": refPath, "hasHeader": true,
				},
				"referenceColumn": "ref_id",
			},
		},
		"onFail":           "route",
		"qualityThreshold": 0.99, // 0.75 dataset score < 0.99 → gate fails → row-level routing kicks in
		"errorSinkType":    "dq_capture",
	})
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	out, err := tr.Apply(sampleBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 3 {
		t.Fatalf("good batch len = %v, want 3 (id=3 quarantined)", out.Len())
	}
	if lastCapture == nil || len(lastCapture.rows) != 1 {
		t.Fatalf("expected exactly 1 quarantined row, got %+v", lastCapture)
	}
	if lastCapture.rows[0][0] != 3 {
		t.Errorf("quarantined row id = %v, want 3 (the orphan)", lastCapture.rows[0][0])
	}
}

func TestDQTransform_WarnPassesThrough(t *testing.T) {
	tr, err := transform.New("dq", map[string]interface{}{
		"expectations":     []interface{}{map[string]interface{}{"type": "expectColumnNotNull", "column": "id"}},
		"onFail":           "warn",
		"qualityThreshold": 0.95,
	})
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	if tr.Name() != "dq" {
		t.Errorf("Name = %q", tr.Name())
	}
	out, err := tr.Apply(sampleBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 4 {
		t.Errorf("warn should pass all 4 rows, got %v", out)
	}
	if p, ok := tr.(DQGateProvider); ok {
		if p.LastRunReport() == nil {
			t.Error("LastRunReport should be populated after Apply")
		}
	}
	_ = tr.Close()
}

func TestDQTransform_ScoreColumn(t *testing.T) {
	tr, err := transform.New("dq", map[string]interface{}{
		"expectations":     []interface{}{map[string]interface{}{"type": "expectColumnNotNull", "column": "amount"}},
		"onFail":           "warn",
		"qualityThreshold": 0.0,
		"scoreColumn":      "_dq_score",
	})
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	out, err := tr.Apply(sampleBatch())
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.ColIndex("_dq_score") == -1 {
		t.Error("score column should be appended")
	}
}

func TestDQTransform_ConfigValidation(t *testing.T) {
	if _, err := transform.New("dq", map[string]interface{}{
		"expectations": []interface{}{map[string]interface{}{"type": "unknownExpectation"}},
	}); err == nil {
		t.Error("unknown expectation type should error at construction")
	}
}
