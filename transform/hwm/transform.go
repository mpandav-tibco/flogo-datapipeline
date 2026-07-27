// Package hwm registers a high-water-mark observer transform.
// The transform scans each batch for the maximum value of a configured column
// and tracks an in-memory maximum that the engine reads after each checkpoint
// interval to persist a new watermark mark. The batch is passed through
// unmodified — hwm is a pure observer, not a filter or mapper.
//
// Config keys:
//
//	column    string  Column name to track (required)
//	valueType string  "datetime" | "int" | "string" | "decimal" (default "string")
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/hwm"
package hwm

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("hwm", newHWMTransform)
}

// HWMProvider is implemented by hwmTransform and exposed to the engine via a
// type assertion on each stage in the pipeline after it is built.
// The engine calls MaxSeen() after every checkpoint interval and at completion.
type HWMProvider interface {
	// MaxSeen returns the maximum watermark value observed since the transform
	// was created (or since the last call to Reset), or nil if no rows have
	// been scanned. The returned value has the concrete type dictated by
	// valueType: time.Time for "datetime", int64 for "int", string for "string",
	// record.Decimal for "decimal".
	MaxSeen() interface{}
	// WatermarkColumn returns the column name this transform is tracking.
	WatermarkColumn() string
	// WatermarkValueType returns the declared value type.
	WatermarkValueType() string
}

type hwmTransform struct {
	column    string
	valueType string // "datetime" | "int" | "string" | "decimal"
	mu        sync.Mutex
	maxSeen   interface{} // current in-memory maximum; nil until first non-null row
}

func newHWMTransform(cfg map[string]interface{}) (transform.Transform, error) {
	column, _ := cfg["column"].(string)
	if column == "" {
		return nil, fmt.Errorf("hwm transform: 'column' is required")
	}
	valueType, _ := cfg["valueType"].(string)
	if valueType == "" {
		valueType = "string"
	}
	switch valueType {
	case "datetime", "int", "string", "decimal":
		// valid
	default:
		return nil, fmt.Errorf("hwm transform: valueType must be 'datetime', 'int', 'string', or 'decimal', got %q", valueType)
	}
	return &hwmTransform{
		column:    column,
		valueType: valueType,
	}, nil
}

func (h *hwmTransform) Name() string { return "hwm" }

// Apply scans the batch for the maximum value of the configured column and
// updates the in-memory maximum. The batch is returned unmodified.
// Returns (in, nil) even when the column is absent — absence is non-fatal;
// it simply means no watermark update occurs for that batch.
func (h *hwmTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	colIdx := in.ColIndex(h.column)
	if colIdx < 0 {
		// Column absent from this batch schema — pass through without update.
		return in, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, row := range in.Rows {
		if colIdx >= len(row) || row[colIdx] == nil {
			continue
		}
		candidate := h.coerce(row[colIdx])
		if candidate == nil {
			continue
		}
		if h.maxSeen == nil || h.greater(candidate, h.maxSeen) {
			h.maxSeen = candidate
		}
	}
	return in, nil
}

// MaxSeen returns the maximum watermark value seen across all batches so far.
// Returns nil if no non-null values have been observed (e.g. empty run).
// Thread-safe: the engine may call this concurrently with Apply.
func (h *hwmTransform) MaxSeen() interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxSeen
}

// WatermarkColumn returns the column name this transform tracks.
func (h *hwmTransform) WatermarkColumn() string { return h.column }

// WatermarkValueType returns the declared value type.
func (h *hwmTransform) WatermarkValueType() string { return h.valueType }

// Close is a no-op — the HWM transform holds no external resources.
func (h *hwmTransform) Close() error { return nil }

// coerce normalises a raw interface{} value to the canonical type for this
// transform's valueType so that comparisons are type-safe.
//
//   - "datetime" → time.Time (UTC); accepts time.Time, string (RFC3339Nano)
//   - "int"      → int64; accepts any integer or float type, or numeric string
//   - "string"   → string; accepts anything via fmt.Sprintf
func (h *hwmTransform) coerce(v interface{}) interface{} {
	switch h.valueType {
	case "datetime":
		return coerceDatetime(v)
	case "int":
		return coerceInt(v)
	case "decimal":
		return coerceDecimalHWM(v)
	default: // "string"
		return coerceString(v)
	}
}

// greater returns true when a > b under the ordering for h.valueType.
// Both a and b must already be coerced (same concrete type).
func (h *hwmTransform) greater(a, b interface{}) bool {
	switch h.valueType {
	case "datetime":
		at, aOk := a.(time.Time)
		bt, bOk := b.(time.Time)
		if aOk && bOk {
			return at.After(bt)
		}
	case "int":
		ai, aOk := a.(int64)
		bi, bOk := b.(int64)
		if aOk && bOk {
			return ai > bi
		}
	case "decimal":
		ad, aOk := a.(record.Decimal)
		bd, bOk := b.(record.Decimal)
		if aOk && bOk {
			return ad.Cmp(bd) > 0
		}
	default: // "string"
		as, aOk := a.(string)
		bs, bOk := b.(string)
		if aOk && bOk {
			return as > bs
		}
	}
	return false
}

// coerceDecimalHWM normalises a raw value to an exact record.Decimal for decimal
// watermark columns (NUMERIC/DECIMAL cursors). Non-numeric values yield nil so
// they are skipped rather than corrupting the tracked maximum.
func coerceDecimalHWM(v interface{}) interface{} {
	if d, ok := record.ToDecimal(v); ok {
		return d
	}
	return nil
}

// --- type coercions ---

func coerceDatetime(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case *time.Time:
		if t == nil {
			return nil
		}
		return t.UTC()
	case string:
		// Accept RFC3339Nano first, then fall back to common layouts.
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.UTC()
			}
		}
	}
	return nil // unparseable — skip this value
}

func coerceInt(v interface{}) interface{} {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		// Guard against overflow: if the value exceeds MaxInt64, clamp rather
		// than silently wrapping (which would produce a negative max seen).
		const maxInt64 = int64(^uint64(0) >> 1)
		if n > uint64(maxInt64) {
			return maxInt64
		}
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int64(f)
		}
	}
	return nil
}

func coerceString(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return s
	}
	// For time.Time values use RFC3339Nano for lexicographic ordering to work
	// correctly with ISO-8601 strings.
	if t, ok := v.(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%v", v)
}
