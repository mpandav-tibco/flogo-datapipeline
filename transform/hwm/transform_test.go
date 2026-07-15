package hwm

import (
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func build(t *testing.T, cfg map[string]interface{}) *hwmTransform {
	t.Helper()
	tr, err := newHWMTransform(cfg)
	if err != nil {
		t.Fatalf("newHWMTransform: %v", err)
	}
	return tr.(*hwmTransform)
}

func batch(col string, typ record.ColumnType, vals ...interface{}) *record.RecordBatch {
	rows := make([][]interface{}, len(vals))
	for i, v := range vals {
		rows[i] = []interface{}{v}
	}
	return &record.RecordBatch{Columns: []record.Column{{Name: col, Type: typ}}, Rows: rows}
}

func TestHWM_ConfigValidation(t *testing.T) {
	if _, err := newHWMTransform(map[string]interface{}{}); err == nil {
		t.Error("expected error when column missing")
	}
	if _, err := newHWMTransform(map[string]interface{}{"column": "c", "valueType": "bogus"}); err == nil {
		t.Error("expected error for invalid valueType")
	}
	// default valueType is string
	h := build(t, map[string]interface{}{"column": "c"})
	if h.WatermarkValueType() != "string" {
		t.Errorf("default valueType = %q, want string", h.WatermarkValueType())
	}
	if h.WatermarkColumn() != "c" {
		t.Errorf("column = %q", h.WatermarkColumn())
	}
	if h.Name() != "hwm" {
		t.Errorf("Name = %q", h.Name())
	}
	if err := h.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestHWM_Int(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	if _, err := h.Apply(batch("id", record.TypeInt, 3, 10, 7, nil, "42", 5)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := h.MaxSeen(); got != int64(42) {
		t.Errorf("MaxSeen = %v (%T), want int64(42)", got, got)
	}
}

func TestHWM_Datetime(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "ts", "valueType": "datetime"})
	early := "2024-01-01T00:00:00Z"
	late := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	if _, err := h.Apply(batch("ts", record.TypeDate, early, late, "2023-01-01")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, ok := h.MaxSeen().(time.Time)
	if !ok || !got.Equal(late) {
		t.Errorf("MaxSeen = %v, want %v", h.MaxSeen(), late)
	}
}

func TestHWM_String(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "name", "valueType": "string"})
	if _, err := h.Apply(batch("name", record.TypeString, "alpha", "zulu", "mike")); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := h.MaxSeen(); got != "zulu" {
		t.Errorf("MaxSeen = %v, want zulu", got)
	}
}

func TestHWM_ColumnAbsent(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "missing", "valueType": "int"})
	out, err := h.Apply(batch("other", record.TypeInt, 1, 2))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 2 {
		t.Errorf("passthrough failed: %v", out)
	}
	if h.MaxSeen() != nil {
		t.Errorf("MaxSeen = %v, want nil when column absent", h.MaxSeen())
	}
}

func TestHWM_EmptyRun(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	if _, err := h.Apply(batch("id", record.TypeInt)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if h.MaxSeen() != nil {
		t.Errorf("MaxSeen = %v, want nil for empty batch", h.MaxSeen())
	}
}

func TestHWM_MonotonicAcrossBatches(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	_, _ = h.Apply(batch("id", record.TypeInt, 100))
	_, _ = h.Apply(batch("id", record.TypeInt, 50)) // lower — must not regress
	if got := h.MaxSeen(); got != int64(100) {
		t.Errorf("MaxSeen = %v, want 100 (no regression)", got)
	}
	_, _ = h.Apply(batch("id", record.TypeInt, 250))
	if got := h.MaxSeen(); got != int64(250) {
		t.Errorf("MaxSeen = %v, want 250", got)
	}
}

func TestHWM_IntCoercionTypes(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	// Exercise the many numeric input types coerceInt handles.
	_, _ = h.Apply(batch("id", record.TypeInt,
		int8(1), int16(2), int32(3), int64(4), uint(5), uint8(6),
		uint16(7), uint32(8), uint64(9), float32(10.9), float64(11.9), "12"))
	if got := h.MaxSeen(); got != int64(12) {
		t.Errorf("MaxSeen = %v, want 12", got)
	}
}

func TestHWM_StringFromTime(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "c", "valueType": "string"})
	tm := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	_, _ = h.Apply(batch("c", record.TypeString, tm, 42, true))
	// All coerced to string; the max is whichever sorts highest lexicographically.
	if h.MaxSeen() == nil {
		t.Error("MaxSeen should not be nil")
	}
}

func TestHWM_DatetimeUnparseableSkipped(t *testing.T) {
	h := build(t, map[string]interface{}{"column": "ts", "valueType": "datetime"})
	_, _ = h.Apply(batch("ts", record.TypeDate, "not-a-date", "also bad"))
	if h.MaxSeen() != nil {
		t.Errorf("MaxSeen = %v, want nil (all unparseable)", h.MaxSeen())
	}
}

// Registered factory is reachable through the transform registry.
func TestHWM_RegisteredFactory(t *testing.T) {
	tr, err := transform.New("hwm", map[string]interface{}{"column": "id", "valueType": "int"})
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	if _, ok := tr.(HWMProvider); !ok {
		t.Error("hwm transform does not implement HWMProvider")
	}
}
