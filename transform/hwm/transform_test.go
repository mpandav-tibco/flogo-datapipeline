package hwm

import (
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func build(t *testing.T, cfg map[string]interface{}) transform.Transform {
	t.Helper()
	tr, err := transform.New("hwm", cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return tr
}

func batch(col string, ct record.ColumnType, vals ...interface{}) *record.RecordBatch {
	rows := make([][]interface{}, len(vals))
	for i, v := range vals {
		rows[i] = []interface{}{v}
	}
	return &record.RecordBatch{
		Columns: []record.Column{{Name: col, Type: ct}},
		Rows:    rows,
	}
}

func TestHWM_ConfigValidation(t *testing.T) {
	if _, err := transform.New("hwm", map[string]interface{}{}); err == nil {
		t.Error("expected error when column missing")
	}
	if _, err := transform.New("hwm", map[string]interface{}{"column": "c", "valueType": "bogus"}); err == nil {
		t.Error("expected error for invalid valueType")
	}
	// Default valueType is string.
	tr := build(t, map[string]interface{}{"column": "c"})
	if p := tr.(HWMProvider); p.WatermarkValueType() != "string" {
		t.Errorf("default valueType = %q, want string", p.WatermarkValueType())
	}
}

func TestHWM_Int(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	p := tr.(HWMProvider)
	if p.MaxSeen() != nil {
		t.Error("MaxSeen should be nil before any rows")
	}
	if _, err := tr.Apply(batch("id", record.TypeInt, 3, 10, 7, "12", 5)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := p.MaxSeen(); got != int64(12) {
		t.Errorf("MaxSeen = %v, want 12", got)
	}
	if p.WatermarkColumn() != "id" {
		t.Errorf("column = %q", p.WatermarkColumn())
	}
}

func TestHWM_Datetime(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "ts", "valueType": "datetime"})
	p := tr.(HWMProvider)
	_, _ = tr.Apply(batch("ts", record.TypeDate,
		"2024-01-01T00:00:00Z", "2024-03-15T12:00:00Z", "2024-02-01T00:00:00Z"))
	got, ok := p.MaxSeen().(time.Time)
	if !ok {
		t.Fatalf("MaxSeen type = %T, want time.Time", p.MaxSeen())
	}
	if !got.Equal(time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("MaxSeen = %v", got)
	}
}

func TestHWM_String(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "name", "valueType": "string"})
	p := tr.(HWMProvider)
	_, _ = tr.Apply(batch("name", record.TypeString, "alpha", "zeta", "gamma"))
	if got := p.MaxSeen(); got != "zeta" {
		t.Errorf("MaxSeen = %v, want zeta", got)
	}
}

func TestHWM_NullsAndMissingColumn(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	p := tr.(HWMProvider)
	// Nulls are skipped.
	_, _ = tr.Apply(batch("id", record.TypeInt, nil, 4, nil, 9))
	if got := p.MaxSeen(); got != int64(9) {
		t.Errorf("MaxSeen with nulls = %v, want 9", got)
	}
	// A batch missing the column passes through and does not update.
	other := batch("unrelated", record.TypeInt, 100)
	out, err := tr.Apply(other)
	if err != nil || out != other {
		t.Errorf("missing column should pass through unchanged")
	}
	if got := p.MaxSeen(); got != int64(9) {
		t.Errorf("MaxSeen after missing-column batch = %v, want 9", got)
	}
}

func TestHWM_AcrossBatches(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	p := tr.(HWMProvider)
	_, _ = tr.Apply(batch("id", record.TypeInt, 5, 2))
	_, _ = tr.Apply(batch("id", record.TypeInt, 8))
	_, _ = tr.Apply(batch("id", record.TypeInt, 1)) // lower — no change
	if got := p.MaxSeen(); got != int64(8) {
		t.Errorf("MaxSeen across batches = %v, want 8", got)
	}
	if tr.Name() != "hwm" || tr.Close() != nil {
		t.Error("Name/Close contract broken")
	}
}

func TestHWM_UintOverflowClamp(t *testing.T) {
	tr := build(t, map[string]interface{}{"column": "id", "valueType": "int"})
	p := tr.(HWMProvider)
	_, _ = tr.Apply(batch("id", record.TypeInt, uint64(1)<<63))
	if got := p.MaxSeen(); got != int64(^uint64(0)>>1) {
		t.Errorf("uint64 overflow clamp = %v", got)
	}
}
