// Package validate registers a data quality transform that checks rules on each
// row, routes violations to a dead-letter sink, and passes clean rows downstream.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/validate"
//
// Config:
//
//	{
//	  "type": "validate",
//	  "rules": [
//	    { "field": "amount",     "notNull": true, "min": 0 },
//	    { "field": "customerId", "notNull": true, "pattern": "^CUST-\\d{6}$" },
//	    { "field": "currency",   "in": ["USD", "EUR", "GBP"] }
//	  ],
//	  "onViolation": "route",
//	  "errorSink": "res://sink:dlq"
//	}
//
// onViolation: "route" sends bad rows to errorSink and passes good rows downstream.
//              "fail"  aborts the pipeline on the first violation.
//              "drop"  silently drops bad rows without routing them.
package validate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

var vlog = corelog.ChildLogger(corelog.RootLogger(), "validate")

func init() {
	transform.Register("validate", newValidateTransform)
}

type validationRule struct {
	field   string
	notNull bool
	min     *float64
	max     *float64
	minLen  *int
	maxLen  *int
	pattern *regexp.Regexp
	inSet   map[string]struct{}
}

type validateTransform struct {
	rules       []validationRule
	onViolation string // "route" | "fail" | "drop"
	errorSink   sink.Sink
}

func newValidateTransform(cfg map[string]interface{}) (transform.Transform, error) {
	rulesRaw, ok := cfg["rules"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("validate transform: 'rules' must be an array")
	}

	rules, err := parseRules(rulesRaw)
	if err != nil {
		return nil, err
	}

	onViolation, _ := cfg["onViolation"].(string)
	if onViolation == "" {
		onViolation = "route"
	}

	var errSink sink.Sink
	if sinkType, ok := cfg["errorSinkType"].(string); ok && sinkType != "" {
		sinkCfg, _ := cfg["errorSinkConfig"].(map[string]interface{})
		if sinkCfg == nil {
			sinkCfg = map[string]interface{}{}
		}
		errSink, err = sink.New(sinkType, sinkCfg)
		if err != nil {
			return nil, fmt.Errorf("validate transform: error sink: %w", err)
		}
	} else {
		// Default error sink: null (discard). Replace with a real DLQ sink in production.
		// If the null sink package is not blank-imported, fall back to the built-in
		// noopSink so that we never store a nil errorSink (which would panic in Apply).
		errSink, err = sink.New("null", nil)
		if err != nil || errSink == nil {
			errSink = &noopSink{}
		}
	}

	return &validateTransform{
		rules:       rules,
		onViolation: onViolation,
		errorSink:   errSink,
	}, nil
}

func (v *validateTransform) Name() string { return "validate" }

func (v *validateTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	good := in.Clone(in.Len())
	good.Columns = in.Columns
	var badRows [][]interface{}

	for i := range in.Rows {
		row := in.RowMap(i)
		if violations := v.check(row); len(violations) == 0 {
			good.Rows = append(good.Rows, in.Rows[i])
		} else {
			switch v.onViolation {
			case "fail":
				record.Put(good)
				return nil, fmt.Errorf("validate: row %d failed: %v", i, violations)
			case "route":
				badRows = append(badRows, in.Rows[i])
			// "drop": silently discard
			}
		}
	}

	// Route bad rows to error sink.
	if len(badRows) > 0 && v.onViolation == "route" && v.errorSink != nil {
		errBatch := record.Get()
		errBatch.Columns = in.Columns
		errBatch.Rows = badRows
		if err := v.errorSink.WriteBatch(errBatch); err != nil {
			vlog.Warnf("validate: error sink write failed (%d bad rows may be lost): %v", len(badRows), err)
		}
		record.Put(errBatch)
	}

	if len(good.Rows) == 0 {
		record.Put(good)
		return nil, nil
	}
	return good, nil
}

func (v *validateTransform) Close() error {
	if v.errorSink != nil {
		_ = v.errorSink.Flush()
		return v.errorSink.Close()
	}
	return nil
}

// check returns a list of violation strings for the given row. Empty = valid.
func (v *validateTransform) check(row map[string]interface{}) []string {
	var violations []string
	for _, r := range v.rules {
		val := row[r.field]
		if r.notNull && val == nil {
			violations = append(violations, r.field+": required (null)")
			continue
		}
		if val == nil {
			continue // null is allowed for remaining checks
		}
		str := fmt.Sprintf("%v", val)
		f, isNum := toFloat64(val)

		if r.min != nil && isNum && f < *r.min {
			violations = append(violations, fmt.Sprintf("%s: %v < min(%v)", r.field, f, *r.min))
		}
		if r.max != nil && isNum && f > *r.max {
			violations = append(violations, fmt.Sprintf("%s: %v > max(%v)", r.field, f, *r.max))
		}
		if r.minLen != nil && len(str) < *r.minLen {
			violations = append(violations, fmt.Sprintf("%s: length %d < minLen(%d)", r.field, len(str), *r.minLen))
		}
		if r.maxLen != nil && len(str) > *r.maxLen {
			violations = append(violations, fmt.Sprintf("%s: length %d > maxLen(%d)", r.field, len(str), *r.maxLen))
		}
		if r.pattern != nil && !r.pattern.MatchString(str) {
			violations = append(violations, fmt.Sprintf("%s: %q does not match pattern", r.field, str))
		}
		if len(r.inSet) > 0 {
			if _, ok := r.inSet[strings.ToUpper(str)]; !ok {
				violations = append(violations, fmt.Sprintf("%s: %q not in allowed set", r.field, str))
			}
		}
	}
	return violations
}

func parseRules(raw []interface{}) ([]validationRule, error) {
	rules := make([]validationRule, 0, len(raw))
	for i, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("validate: rule[%d] must be an object", i)
		}
		r := validationRule{}
		r.field, _ = m["field"].(string)
		if r.field == "" {
			return nil, fmt.Errorf("validate: rule[%d]: 'field' is required", i)
		}
		if nb, ok := m["notNull"].(bool); ok {
			r.notNull = nb
		}
		if minV, ok := m["min"]; ok {
			if f, ok := toFloat64Any(minV); ok {
				r.min = &f
			}
		}
		if maxV, ok := m["max"]; ok {
			if f, ok := toFloat64Any(maxV); ok {
				r.max = &f
			}
		}
		if minL, ok := m["minLength"]; ok {
			if n, ok := toInt(minL); ok {
				r.minLen = &n
			}
		}
		if maxL, ok := m["maxLength"]; ok {
			if n, ok := toInt(maxL); ok {
				r.maxLen = &n
			}
		}
		if pat, ok := m["pattern"].(string); ok && pat != "" {
			re, err := regexp.Compile(pat)
			if err != nil {
				return nil, fmt.Errorf("validate: rule[%d] pattern: %w", i, err)
			}
			r.pattern = re
		}
		if inList, ok := m["in"].([]interface{}); ok {
			r.inSet = make(map[string]struct{}, len(inList))
			for _, v := range inList {
				r.inSet[strings.ToUpper(fmt.Sprintf("%v", v))] = struct{}{}
			}
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func toFloat64Any(v interface{}) (float64, bool) { return toFloat64(v) }

func toInt(v interface{}) (int, bool) {
	f, ok := toFloat64(v)
	return int(f), ok
}

// noopSink is an inline discard sink used when the "null" sink package is not
// blank-imported. It prevents a nil errorSink from causing a panic in Apply().
type noopSink struct{}

func (*noopSink) WriteBatch(_ *record.RecordBatch) error { return nil }
func (*noopSink) Flush() error                           { return nil }
func (*noopSink) Close() error                           { return nil }
