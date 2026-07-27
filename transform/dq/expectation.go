package dq

// This file defines the Expectation interface and all built-in expectation
// implementations. The design mirrors Great Expectations' DSL but uses
// camelCase JSON keys to stay consistent with the rest of the pipeline config.
//
// Each expectation receives the pre-computed BatchProfile (one aggregate pass
// over the batch) and returns an ExpectationResult describing whether the
// assertion was met, the observed vs. expected values, and an individual score
// (1.0 = fully satisfied, 0.0 = fully violated, fractional for threshold-based
// expectations).
//
// Built-in expectations:
//
//   expectColumnNotNull         — completeness ≥ threshold (default 1.0)
//   expectColumnUnique          — distinct/total ≥ threshold (default 1.0)
//   expectColumnValuesBetween   — min ≥ lo && max ≤ hi (row-level: fraction of rows in range)
//   expectColumnValuesInSet     — fraction of non-null values whose string repr is in allowed set
//   expectTableRowCountBetween  — row count within [lo, hi]
//   expectColumnMeanBetween     — column mean within [lo, hi]
//   expectColumnStddevBelow     — column stddev < threshold

import (
	"fmt"
	"math"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Expectation is the interface every built-in and user-defined expectation must
// implement. Evaluate is called once per batch with the pre-computed profile.
type Expectation interface {
	// Name returns the expectation type identifier (matches the "type" config key).
	Name() string
	// Evaluate applies the expectation against the batch profile and returns a
	// structured result. Evaluate must be safe to call concurrently.
	Evaluate(batch *record.RecordBatch, profile *BatchProfile) ExpectationResult
}

// ExpectationResult carries the outcome of a single expectation evaluation.
type ExpectationResult struct {
	// Name is the expectation type identifier.
	Name string `json:"name"`
	// Passed is true when the expectation was fully or partially satisfied
	// (score >= the expectation's own threshold, if applicable).
	Passed bool `json:"passed"`
	// Score is a 0.0–1.0 measure of how well the expectation was met.
	// 1.0 means fully satisfied; 0.0 means completely violated.
	Score float64 `json:"score"`
	// Observed is the actual value(s) measured in the batch.
	Observed interface{} `json:"observed"`
	// Expected is the threshold or range declared in the config.
	Expected interface{} `json:"expected"`
	// Message is a human-readable description of the result.
	Message string `json:"message"`
}

// ----------------------------------------------------------------------------
// expectColumnNotNull
// ----------------------------------------------------------------------------

// expectColumnNotNull asserts that at least `threshold` fraction of rows have a
// non-null value in `column`. threshold defaults to 1.0 (all rows non-null).
//
// Config keys:
//
//	column    string  Column name (required)
//	threshold float64 Minimum completeness ratio [0.0, 1.0] (default 1.0)
type expectColumnNotNull struct {
	column    string
	threshold float64
}

func newExpectColumnNotNull(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnNotNull")
	if err != nil {
		return nil, err
	}
	threshold := 1.0
	if t, ok := cfg["threshold"]; ok {
		if f, ok2 := toFloat64(t); ok2 {
			threshold = f
		}
	}
	return &expectColumnNotNull{column: col, threshold: threshold}, nil
}

func (e *expectColumnNotNull) Name() string { return "expectColumnNotNull" }

func (e *expectColumnNotNull) Evaluate(_ *record.RecordBatch, profile *BatchProfile) ExpectationResult {
	cs, ok := profile.ColumnStats[e.column]
	if !ok {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   false,
			Score:    0,
			Observed: nil,
			Expected: fmt.Sprintf("column %q exists with completeness >= %.4f", e.column, e.threshold),
			Message:  fmt.Sprintf("expectColumnNotNull: column %q not found in batch schema", e.column),
		}
	}
	total := cs.NullCount + cs.NonNullCount
	if total == 0 {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   true,
			Score:    1.0,
			Observed: "no rows",
			Expected: fmt.Sprintf("completeness >= %.4f", e.threshold),
			Message:  "expectColumnNotNull: empty batch — trivially satisfied",
		}
	}
	completeness := float64(cs.NonNullCount) / float64(total)
	passed := completeness >= e.threshold
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    completeness,
		Observed: fmt.Sprintf("completeness=%.4f (%d/%d non-null)", completeness, cs.NonNullCount, total),
		Expected: fmt.Sprintf("completeness >= %.4f", e.threshold),
		Message: fmt.Sprintf("expectColumnNotNull[%s]: completeness=%.4f, threshold=%.4f, passed=%v",
			e.column, completeness, e.threshold, passed),
	}
}

// ----------------------------------------------------------------------------
// expectColumnUnique
// ----------------------------------------------------------------------------

// expectColumnUnique asserts that distinct/total ≥ threshold for non-null values
// in `column`. threshold defaults to 1.0 (all non-null values are unique).
//
// Config keys:
//
//	column    string  Column name (required)
//	threshold float64 Minimum uniqueness ratio [0.0, 1.0] (default 1.0)
type expectColumnUnique struct {
	column    string
	threshold float64
}

func newExpectColumnUnique(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnUnique")
	if err != nil {
		return nil, err
	}
	threshold := 1.0
	if t, ok := cfg["threshold"]; ok {
		if f, ok2 := toFloat64(t); ok2 {
			threshold = f
		}
	}
	return &expectColumnUnique{column: col, threshold: threshold}, nil
}

func (e *expectColumnUnique) Name() string { return "expectColumnUnique" }

func (e *expectColumnUnique) Evaluate(_ *record.RecordBatch, profile *BatchProfile) ExpectationResult {
	cs, ok := profile.ColumnStats[e.column]
	if !ok {
		return columnNotFoundResult(e.Name(), e.column)
	}
	if cs.NonNullCount == 0 {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   true,
			Score:    1.0,
			Observed: "no non-null rows",
			Expected: fmt.Sprintf("uniqueness >= %.4f", e.threshold),
			Message:  "expectColumnUnique: no non-null values — trivially satisfied",
		}
	}
	uniqueness := float64(cs.DistinctCount()) / float64(cs.NonNullCount)
	passed := uniqueness >= e.threshold
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    uniqueness,
		Observed: fmt.Sprintf("uniqueness=%.4f (%d distinct / %d non-null)", uniqueness, cs.DistinctCount(), cs.NonNullCount),
		Expected: fmt.Sprintf("uniqueness >= %.4f", e.threshold),
		Message: fmt.Sprintf("expectColumnUnique[%s]: uniqueness=%.4f, threshold=%.4f, passed=%v",
			e.column, uniqueness, e.threshold, passed),
	}
}

// ----------------------------------------------------------------------------
// expectColumnValuesBetween
// ----------------------------------------------------------------------------

// expectColumnValuesBetween asserts that every numeric value in `column` falls
// within [min, max] (inclusive). The score is the fraction of non-null rows
// that satisfy the constraint.
//
// Config keys:
//
//	column string  Column name (required)
//	min    number  Lower bound (inclusive, required)
//	max    number  Upper bound (inclusive, required)
type expectColumnValuesBetween struct {
	column string
	lo     float64
	hi     float64
}

func newExpectColumnValuesBetween(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnValuesBetween")
	if err != nil {
		return nil, err
	}
	lo, err := requireFloat(cfg, "min", "expectColumnValuesBetween")
	if err != nil {
		return nil, err
	}
	hi, err := requireFloat(cfg, "max", "expectColumnValuesBetween")
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return nil, fmt.Errorf("expectColumnValuesBetween[%s]: min (%v) > max (%v)", col, lo, hi)
	}
	return &expectColumnValuesBetween{column: col, lo: lo, hi: hi}, nil
}

func (e *expectColumnValuesBetween) Name() string { return "expectColumnValuesBetween" }

func (e *expectColumnValuesBetween) Evaluate(batch *record.RecordBatch, _ *BatchProfile) ExpectationResult {
	colIdx := batch.ColIndex(e.column)
	if colIdx < 0 {
		return columnNotFoundResult(e.Name(), e.column)
	}

	var inRange, outRange int
	for _, row := range batch.Rows {
		if colIdx >= len(row) || row[colIdx] == nil {
			continue // null values are excluded from the range check
		}
		f, ok := toFloat64(row[colIdx])
		if !ok {
			continue // non-numeric values are excluded
		}
		if f >= e.lo && f <= e.hi {
			inRange++
		} else {
			outRange++
		}
	}

	total := inRange + outRange
	if total == 0 {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   true,
			Score:    1.0,
			Observed: "no numeric values",
			Expected: fmt.Sprintf("[%.6g, %.6g]", e.lo, e.hi),
			Message:  fmt.Sprintf("expectColumnValuesBetween[%s]: no numeric values to check", e.column),
		}
	}

	score := float64(inRange) / float64(total)
	passed := outRange == 0
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    score,
		Observed: fmt.Sprintf("%d/%d values in [%.6g, %.6g]", inRange, total, e.lo, e.hi),
		Expected: fmt.Sprintf("all values in [%.6g, %.6g]", e.lo, e.hi),
		Message: fmt.Sprintf("expectColumnValuesBetween[%s]: %d out-of-range values (score=%.4f), passed=%v",
			e.column, outRange, score, passed),
	}
}

// ----------------------------------------------------------------------------
// expectColumnValuesInSet
// ----------------------------------------------------------------------------

// expectColumnValuesInSet asserts that all non-null values in `column` are
// members of the declared allowed set. The score is the fraction of non-null
// rows that are in-set.
//
// Config keys:
//
//	column string   Column name (required)
//	values []string Allowed values; comparison is case-insensitive by default
//	caseSensitive bool If true, comparison is exact (default false)
type expectColumnValuesInSet struct {
	column        string
	allowed       map[string]struct{}
	caseSensitive bool
}

func newExpectColumnValuesInSet(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnValuesInSet")
	if err != nil {
		return nil, err
	}
	rawVals, ok := cfg["values"].([]interface{})
	if !ok || len(rawVals) == 0 {
		return nil, fmt.Errorf("expectColumnValuesInSet[%s]: 'values' must be a non-empty array", col)
	}
	caseSensitive := false
	if cs, ok := cfg["caseSensitive"].(bool); ok {
		caseSensitive = cs
	}
	allowed := make(map[string]struct{}, len(rawVals))
	for _, v := range rawVals {
		s := fmt.Sprintf("%v", v)
		if !caseSensitive {
			s = strings.ToUpper(s)
		}
		allowed[s] = struct{}{}
	}
	return &expectColumnValuesInSet{column: col, allowed: allowed, caseSensitive: caseSensitive}, nil
}

func (e *expectColumnValuesInSet) Name() string { return "expectColumnValuesInSet" }

func (e *expectColumnValuesInSet) Evaluate(batch *record.RecordBatch, _ *BatchProfile) ExpectationResult {
	colIdx := batch.ColIndex(e.column)
	if colIdx < 0 {
		return columnNotFoundResult(e.Name(), e.column)
	}

	var inSet, outSet int
	for _, row := range batch.Rows {
		if colIdx >= len(row) || row[colIdx] == nil {
			continue
		}
		s := fmt.Sprintf("%v", row[colIdx])
		key := s
		if !e.caseSensitive {
			key = strings.ToUpper(s)
		}
		if _, ok := e.allowed[key]; ok {
			inSet++
		} else {
			outSet++
		}
	}

	total := inSet + outSet
	if total == 0 {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   true,
			Score:    1.0,
			Observed: "no non-null values",
			Expected: fmt.Sprintf("values in set of %d", len(e.allowed)),
			Message:  fmt.Sprintf("expectColumnValuesInSet[%s]: no non-null values to check", e.column),
		}
	}

	score := float64(inSet) / float64(total)
	passed := outSet == 0
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    score,
		Observed: fmt.Sprintf("%d/%d values in set", inSet, total),
		Expected: fmt.Sprintf("all values in allowed set (size=%d)", len(e.allowed)),
		Message: fmt.Sprintf("expectColumnValuesInSet[%s]: %d out-of-set values (score=%.4f), passed=%v",
			e.column, outSet, score, passed),
	}
}

// ----------------------------------------------------------------------------
// expectTableRowCountBetween
// ----------------------------------------------------------------------------

// expectTableRowCountBetween asserts that the batch row count falls within [min, max].
//
// Config keys:
//
//	min int Lower bound (inclusive, required)
//	max int Upper bound (inclusive, required)
type expectTableRowCountBetween struct {
	lo int
	hi int
}

func newExpectTableRowCountBetween(cfg map[string]interface{}) (Expectation, error) {
	loF, err := requireFloat(cfg, "min", "expectTableRowCountBetween")
	if err != nil {
		return nil, err
	}
	hiF, err := requireFloat(cfg, "max", "expectTableRowCountBetween")
	if err != nil {
		return nil, err
	}
	lo, hi := int(loF), int(hiF)
	if lo > hi {
		return nil, fmt.Errorf("expectTableRowCountBetween: min (%d) > max (%d)", lo, hi)
	}
	return &expectTableRowCountBetween{lo: lo, hi: hi}, nil
}

func (e *expectTableRowCountBetween) Name() string { return "expectTableRowCountBetween" }

func (e *expectTableRowCountBetween) Evaluate(_ *record.RecordBatch, profile *BatchProfile) ExpectationResult {
	rc := profile.RowCount
	passed := rc >= e.lo && rc <= e.hi
	score := 1.0
	if !passed {
		// Score degrades linearly based on how far the count is from the valid range.
		if rc < e.lo {
			if e.lo == 0 {
				score = 0
			} else {
				score = float64(rc) / float64(e.lo)
			}
		} else {
			// rc > hi
			score = float64(e.hi) / float64(rc)
		}
		score = math.Max(0, score)
	}
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    score,
		Observed: rc,
		Expected: fmt.Sprintf("[%d, %d]", e.lo, e.hi),
		Message: fmt.Sprintf("expectTableRowCountBetween: rowCount=%d, range=[%d, %d], passed=%v",
			rc, e.lo, e.hi, passed),
	}
}

// ----------------------------------------------------------------------------
// expectColumnMeanBetween
// ----------------------------------------------------------------------------

// expectColumnMeanBetween asserts that the arithmetic mean of numeric values in
// `column` falls within [lo, hi].
//
// Config keys:
//
//	column string Column name (required)
//	min    number Lower bound (inclusive, required)
//	max    number Upper bound (inclusive, required)
type expectColumnMeanBetween struct {
	column string
	lo     float64
	hi     float64
}

func newExpectColumnMeanBetween(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnMeanBetween")
	if err != nil {
		return nil, err
	}
	lo, err := requireFloat(cfg, "min", "expectColumnMeanBetween")
	if err != nil {
		return nil, err
	}
	hi, err := requireFloat(cfg, "max", "expectColumnMeanBetween")
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return nil, fmt.Errorf("expectColumnMeanBetween[%s]: min (%v) > max (%v)", col, lo, hi)
	}
	return &expectColumnMeanBetween{column: col, lo: lo, hi: hi}, nil
}

func (e *expectColumnMeanBetween) Name() string { return "expectColumnMeanBetween" }

func (e *expectColumnMeanBetween) Evaluate(_ *record.RecordBatch, profile *BatchProfile) ExpectationResult {
	cs, ok := profile.ColumnStats[e.column]
	if !ok {
		return columnNotFoundResult(e.Name(), e.column)
	}
	mean := cs.Mean()
	if math.IsNaN(mean) {
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   false,
			Score:    0,
			Observed: "NaN (no numeric values)",
			Expected: fmt.Sprintf("mean in [%.6g, %.6g]", e.lo, e.hi),
			Message:  fmt.Sprintf("expectColumnMeanBetween[%s]: no numeric values — cannot compute mean", e.column),
		}
	}
	passed := mean >= e.lo && mean <= e.hi

	// Score: 1.0 at midpoint, degrades linearly towards 0 as mean moves away.
	mid := (e.lo + e.hi) / 2
	halfRange := (e.hi - e.lo) / 2
	var score float64
	if halfRange == 0 {
		if mean == e.lo {
			score = 1.0
		} else {
			score = 0.0
		}
	} else {
		dist := math.Abs(mean - mid)
		score = math.Max(0, 1.0-dist/halfRange)
	}

	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    score,
		Observed: fmt.Sprintf("mean=%.6g", mean),
		Expected: fmt.Sprintf("[%.6g, %.6g]", e.lo, e.hi),
		Message: fmt.Sprintf("expectColumnMeanBetween[%s]: mean=%.6g, range=[%.6g, %.6g], passed=%v",
			e.column, mean, e.lo, e.hi, passed),
	}
}

// ----------------------------------------------------------------------------
// expectColumnStddevBelow
// ----------------------------------------------------------------------------

// expectColumnStddevBelow asserts that the population standard deviation of
// numeric values in `column` is strictly less than `threshold`.
//
// Config keys:
//
//	column    string Column name (required)
//	threshold number Upper bound on stddev (exclusive, required)
type expectColumnStddevBelow struct {
	column    string
	threshold float64
}

func newExpectColumnStddevBelow(cfg map[string]interface{}) (Expectation, error) {
	col, err := requireString(cfg, "column", "expectColumnStddevBelow")
	if err != nil {
		return nil, err
	}
	threshold, err := requireFloat(cfg, "threshold", "expectColumnStddevBelow")
	if err != nil {
		return nil, err
	}
	if threshold <= 0 {
		return nil, fmt.Errorf("expectColumnStddevBelow[%s]: threshold must be > 0, got %v", col, threshold)
	}
	return &expectColumnStddevBelow{column: col, threshold: threshold}, nil
}

func (e *expectColumnStddevBelow) Name() string { return "expectColumnStddevBelow" }

func (e *expectColumnStddevBelow) Evaluate(_ *record.RecordBatch, profile *BatchProfile) ExpectationResult {
	cs, ok := profile.ColumnStats[e.column]
	if !ok {
		return columnNotFoundResult(e.Name(), e.column)
	}
	stddev := cs.Stddev()
	if math.IsNaN(stddev) {
		// Fewer than 2 numeric values — trivially satisfies any stddev constraint.
		return ExpectationResult{
			Name:     e.Name(),
			Passed:   true,
			Score:    1.0,
			Observed: "NaN (< 2 numeric values)",
			Expected: fmt.Sprintf("stddev < %.6g", e.threshold),
			Message:  fmt.Sprintf("expectColumnStddevBelow[%s]: < 2 numeric values — trivially satisfied", e.column),
		}
	}
	passed := stddev < e.threshold
	score := math.Max(0, 1.0-stddev/e.threshold)
	return ExpectationResult{
		Name:     e.Name(),
		Passed:   passed,
		Score:    score,
		Observed: fmt.Sprintf("stddev=%.6g", stddev),
		Expected: fmt.Sprintf("stddev < %.6g", e.threshold),
		Message: fmt.Sprintf("expectColumnStddevBelow[%s]: stddev=%.6g, threshold=%.6g, passed=%v",
			e.column, stddev, e.threshold, passed),
	}
}

// ----------------------------------------------------------------------------
// Registry and factory
// ----------------------------------------------------------------------------

// expectationFactories maps expectation type names to their constructor functions.
var expectationFactories = map[string]func(cfg map[string]interface{}) (Expectation, error){
	"expectColumnNotNull":        newExpectColumnNotNull,
	"expectColumnUnique":         newExpectColumnUnique,
	"expectColumnValuesBetween":  newExpectColumnValuesBetween,
	"expectColumnValuesInSet":    newExpectColumnValuesInSet,
	"expectTableRowCountBetween": newExpectTableRowCountBetween,
	"expectColumnMeanBetween":    newExpectColumnMeanBetween,
	"expectColumnStddevBelow":    newExpectColumnStddevBelow,
}

// buildExpectation constructs an Expectation from a raw config map.
// The "type" key selects the implementation.
func buildExpectation(cfg map[string]interface{}) (Expectation, error) {
	typ, _ := cfg["type"].(string)
	if typ == "" {
		return nil, fmt.Errorf("dq expectation: missing 'type' field")
	}
	factory, ok := expectationFactories[typ]
	if !ok {
		return nil, fmt.Errorf("dq expectation: unknown type %q", typ)
	}
	return factory(cfg)
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func requireString(cfg map[string]interface{}, key, expectationName string) (string, error) {
	v, ok := cfg[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("%s: %q is required", expectationName, key)
	}
	return v, nil
}

func requireFloat(cfg map[string]interface{}, key, expectationName string) (float64, error) {
	v, ok := cfg[key]
	if !ok {
		return 0, fmt.Errorf("%s: %q is required", expectationName, key)
	}
	f, ok2 := toFloat64(v)
	if !ok2 {
		return 0, fmt.Errorf("%s: %q must be a number, got %T", expectationName, key, v)
	}
	return f, nil
}

func columnNotFoundResult(expectationName, column string) ExpectationResult {
	return ExpectationResult{
		Name:     expectationName,
		Passed:   false,
		Score:    0,
		Observed: nil,
		Expected: fmt.Sprintf("column %q present in batch schema", column),
		Message:  fmt.Sprintf("%s: column %q not found in batch schema", expectationName, column),
	}
}
