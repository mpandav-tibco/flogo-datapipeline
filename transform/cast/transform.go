// Package cast registers a type-casting transform that converts named columns to
// a target type in every row and updates the batch schema accordingly. It is a
// pure map-side, per-row transform — no shuffle, no cross-row state.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/cast"
//
// Config:
//
//	{
//	  "type": "cast",
//	  "columns": [
//	    { "name": "amount", "type": "decimal", "scale": 2 },
//	    { "name": "qty",    "type": "int" },
//	    { "name": "ts",     "type": "date", "format": "2006-01-02" }
//	  ],
//	  "onError": "null"   // "null" (default) — unconvertible values become null;
//	                      // "fail" — the batch errors on the first bad value.
//	}
//
// A configured column that is absent from a batch is skipped (non-fatal), matching
// the pipeline's schema-tolerant style. Target types: string, int, float, decimal
// (with optional scale), bool, date (with optional format), any.
package cast

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("cast", newCastTransform)
}

type castTransform struct {
	targets []record.Column // desired name/type/format/scale per column
	onError string          // "null" | "fail"
}

func newCastTransform(cfg map[string]interface{}) (transform.Transform, error) {
	raw, ok := cfg["columns"]
	if !ok {
		return nil, fmt.Errorf("cast transform: 'columns' is required")
	}
	targets, err := parseTargets(raw)
	if err != nil {
		return nil, fmt.Errorf("cast transform: %w", err)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("cast transform: 'columns' is empty")
	}
	onError, _ := cfg["onError"].(string)
	if onError == "" {
		onError = "null"
	}
	if onError != "null" && onError != "fail" {
		return nil, fmt.Errorf("cast transform: onError must be 'null' or 'fail', got %q", onError)
	}
	return &castTransform{targets: targets, onError: onError}, nil
}

func (c *castTransform) Name() string { return "cast" }

// Apply converts each configured column's value in every row and returns a new
// batch whose schema reflects the target types. The input batch is not modified.
func (c *castTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	// Resolve each target to its column index in this batch and build the output
	// schema (a copy — in.Columns is shared and must not be mutated).
	idxByTarget := make([]int, len(c.targets))
	newCols := make([]record.Column, len(in.Columns))
	copy(newCols, in.Columns)
	for ti, tgt := range c.targets {
		idx := in.ColIndex(tgt.Name)
		idxByTarget[ti] = idx
		if idx >= 0 {
			newCols[idx].Type = tgt.Type
			newCols[idx].Format = tgt.Format
			newCols[idx].Scale = tgt.Scale
		}
	}

	out := in.Clone(in.Len())
	out.Columns = newCols
	for i := range in.Rows {
		src := in.Rows[i]
		dst := make([]interface{}, len(src))
		copy(dst, src)
		for ti, idx := range idxByTarget {
			if idx < 0 || idx >= len(dst) {
				continue // column absent from this batch — skip
			}
			v, ok := castValue(dst[idx], c.targets[ti])
			if !ok {
				if c.onError == "fail" {
					record.Put(out)
					return nil, fmt.Errorf("cast transform: column %q value %v cannot convert to %s",
						c.targets[ti].Name, dst[idx], c.targets[ti].Type)
				}
				v = nil // lenient: null out unconvertible values
			}
			dst[idx] = v
		}
		out.Rows = append(out.Rows, dst)
	}
	return out, nil
}

func (c *castTransform) Close() error { return nil }

// parseTargets extracts the desired column definitions from the config value,
// accepting the JSON-decoded []interface{} of objects as well as typed slices.
func parseTargets(raw interface{}) ([]record.Column, error) {
	switch v := raw.(type) {
	case []record.Column:
		for _, c := range v {
			if err := validateTarget(c); err != nil {
				return nil, err
			}
		}
		return v, nil
	case []interface{}:
		cols := make([]record.Column, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("each 'columns' entry must be an object")
			}
			c, err := columnFromMap(m)
			if err != nil {
				return nil, err
			}
			cols = append(cols, c)
		}
		return cols, nil
	case []map[string]interface{}:
		cols := make([]record.Column, 0, len(v))
		for _, m := range v {
			c, err := columnFromMap(m)
			if err != nil {
				return nil, err
			}
			cols = append(cols, c)
		}
		return cols, nil
	default:
		return nil, fmt.Errorf("'columns' must be a list of column objects")
	}
}

func columnFromMap(m map[string]interface{}) (record.Column, error) {
	name, _ := m["name"].(string)
	typ, _ := m["type"].(string)
	format, _ := m["format"].(string)
	scale := 0
	switch s := m["scale"].(type) {
	case float64: // JSON numbers decode as float64
		scale = int(s)
	case int:
		scale = s
	}
	col := record.Column{Name: name, Type: record.ColumnType(typ), Format: format, Scale: scale}
	if err := validateTarget(col); err != nil {
		return record.Column{}, err
	}
	return col, nil
}

func validateTarget(c record.Column) error {
	if c.Name == "" {
		return fmt.Errorf("each 'columns' entry needs a 'name'")
	}
	switch c.Type {
	case record.TypeString, record.TypeInt, record.TypeFloat, record.TypeBool,
		record.TypeDate, record.TypeDecimal, record.TypeAny:
		return nil
	default:
		return fmt.Errorf("column %q: unsupported target type %q", c.Name, c.Type)
	}
}

// castValue converts a single value to the target column's type. ok=false means
// the value could not be converted (the caller then nulls it or fails). A nil
// value always passes through as nil.
func castValue(v interface{}, target record.Column) (interface{}, bool) {
	if v == nil {
		return nil, true
	}
	switch target.Type {
	case record.TypeString:
		return castToString(v, target.Format), true
	case record.TypeInt:
		return castToInt(v)
	case record.TypeFloat:
		return castToFloat(v)
	case record.TypeDecimal:
		return castToDecimal(v, target.Scale)
	case record.TypeBool:
		return castToBool(v)
	case record.TypeDate:
		return castToDate(v, target.Format)
	default: // TypeAny or unknown — pass through untouched
		return v, true
	}
}

func castToString(v interface{}, format string) interface{} {
	switch n := v.(type) {
	case string:
		return n
	case record.Decimal:
		return n.String()
	case time.Time:
		if format == "" {
			format = time.RFC3339
		}
		return n.Format(format)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func castToInt(v interface{}) (interface{}, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case bool:
		if n {
			return int64(1), true
		}
		return int64(0), true
	case record.Decimal:
		if i, err := strconv.ParseInt(n.Round(0).String(), 10, 64); err == nil {
			return i, true
		}
	case string:
		s := strings.TrimSpace(n)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
	}
	return nil, false
}

func castToFloat(v interface{}) (interface{}, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case bool:
		if n {
			return 1.0, true
		}
		return 0.0, true
	case record.Decimal:
		return n.Float64(), true
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f, true
		}
	}
	return nil, false
}

func castToDecimal(v interface{}, scale int) (interface{}, bool) {
	d, ok := record.ToDecimal(v)
	if !ok {
		if b, isBool := v.(bool); isBool {
			if b {
				d, ok = record.NewDecimal(1, 0), true
			} else {
				d, ok = record.NewDecimal(0, 0), true
			}
		}
	}
	if !ok {
		return nil, false
	}
	if scale > 0 {
		// Normalise to exactly `scale` fractional digits (pads or rounds).
		d = record.NewDecimalFromBig(d.Coef(int32(scale)), int32(scale))
	}
	return d, true
}

func castToBool(v interface{}) (interface{}, bool) {
	switch n := v.(type) {
	case bool:
		return n, true
	case string:
		switch strings.ToLower(strings.TrimSpace(n)) {
		case "true", "1", "yes", "y", "t":
			return true, true
		case "false", "0", "no", "n", "f":
			return false, true
		}
	case int64:
		return n != 0, true
	case int:
		return n != 0, true
	case int32:
		return n != 0, true
	case float64:
		return n != 0, true
	case record.Decimal:
		return !n.IsZero(), true
	}
	return nil, false
}

func castToDate(v interface{}, format string) (interface{}, bool) {
	switch n := v.(type) {
	case time.Time:
		return n.UTC(), true
	case string:
		s := strings.TrimSpace(n)
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
		if format != "" {
			layouts = append([]string{format}, layouts...)
		}
		for _, l := range layouts {
			if t, err := time.Parse(l, s); err == nil {
				return t.UTC(), true
			}
		}
	}
	return nil, false
}
