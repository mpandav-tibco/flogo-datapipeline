package window

import (
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// captureSink records rows written to it so late-routing can be asserted.
type captureSink struct{ rows int }

var lastCapture *captureSink

func init() {
	sink.Register("window_capture", func(map[string]interface{}) (sink.Sink, error) {
		lastCapture = &captureSink{}
		return lastCapture, nil
	})
}

func (c *captureSink) WriteBatch(b *record.RecordBatch) error { c.rows += b.Len(); return nil }
func (c *captureSink) Flush() error                           { return nil }
func (c *captureSink) Close() error                           { return nil }

func toF(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

func TestWindowConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
	}{
		{"missing window", map[string]interface{}{
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		}},
		{"tumbling missing size", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "tumbling"},
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		}},
		{"session missing gap", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "session"},
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		}},
		{"unknown kind", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "wobbly", "sizeMs": float64(1000)},
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		}},
		{"no aggregations", map[string]interface{}{
			"window": map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
		}},
		{"agg missing as", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
			"aggregations": []interface{}{map[string]interface{}{"fn": "count"}},
		}},
		{"agg missing fn", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
			"aggregations": []interface{}{map[string]interface{}{"as": "c"}},
		}},
		{"agg missing column for sum", map[string]interface{}{
			"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
			"aggregations": []interface{}{map[string]interface{}{"as": "s", "fn": "sum"}},
		}},
		{"route to unknown sink", map[string]interface{}{
			"window":        map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
			"aggregations":  []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
			"onLate":        "route",
			"errorSinkType": "does-not-exist-xyz",
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := newTransform(c.cfg); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestWindowGlobalAndSlidingConfigOK(t *testing.T) {
	// global needs no size; sliding needs size — both should construct cleanly.
	for _, cfg := range []map[string]interface{}{
		{
			"window":       map[string]interface{}{"kind": "global"},
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		},
		{
			"window":       map[string]interface{}{"kind": "sliding", "sizeMs": float64(1000), "slideMs": float64(500)},
			"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		},
	} {
		tr, err := newTransform(cfg)
		if err != nil {
			t.Fatalf("newTransform: %v", err)
		}
		_ = tr.Close()
	}
}

func TestWindowTumblingAggregation(t *testing.T) {
	tr, err := newTransform(map[string]interface{}{
		"window":    map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
		"eventTime": map[string]interface{}{"column": "ts", "format": "epochMillis"},
		"watermark": map[string]interface{}{"maxLatenessMs": float64(0)},
		"groupBy":   []interface{}{"k"},
		"aggregations": []interface{}{
			map[string]interface{}{"as": "cnt", "fn": "count"},
			map[string]interface{}{"as": "total", "fn": "sum", "column": "v"},
		},
	})
	if err != nil {
		t.Fatalf("newTransform: %v", err)
	}
	w := tr.(*windowTransform)
	if w.Name() != "window" {
		t.Errorf("Name = %q", w.Name())
	}

	cols := []record.Column{
		{Name: "k", Type: record.TypeString},
		{Name: "v", Type: record.TypeFloat},
		{Name: "ts", Type: record.TypeInt},
	}
	in := &record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		{"a", 10.0, int64(100)}, // window [0,1000)
		{"a", 20.0, int64(200)}, // window [0,1000)
		{"a", 5.0, int64(1500)}, // window [1000,2000) → advances watermark, closes [0,1000)
	}}

	var cntSum, valSum float64
	windows := 0
	collect := func(b *record.RecordBatch) {
		if b == nil {
			return
		}
		for i := 0; i < b.Len(); i++ {
			windows++
			cntSum += toF(b.Value(i, "cnt"))
			valSum += toF(b.Value(i, "total"))
		}
	}

	out, err := w.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	collect(out)

	// Snapshot/Restore round-trip while state still holds the open window.
	snap, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := w.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	flushed, err := w.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	collect(flushed)

	if windows != 2 {
		t.Errorf("emitted %d windows, want 2", windows)
	}
	if cntSum != 3 {
		t.Errorf("total count across windows = %v, want 3", cntSum)
	}
	if valSum != 35 {
		t.Errorf("total sum across windows = %v, want 35", valSum)
	}

	st := w.Stats()
	if st.WindowsEmitted != 2 {
		t.Errorf("Stats.WindowsEmitted = %d, want 2", st.WindowsEmitted)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestWindowLateRouting(t *testing.T) {
	tr, err := newTransform(map[string]interface{}{
		"window":        map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
		"eventTime":     map[string]interface{}{"column": "ts", "format": "epochMillis"},
		"watermark":     map[string]interface{}{"maxLatenessMs": float64(0)},
		"aggregations":  []interface{}{map[string]interface{}{"as": "cnt", "fn": "count"}},
		"onLate":        "route",
		"errorSinkType": "window_capture",
		"emit":          map[string]interface{}{"windowStartAs": "ws", "windowEndAs": "we"},
	})
	if err != nil {
		t.Fatalf("newTransform: %v", err)
	}
	w := tr.(*windowTransform)

	// First advance the watermark well past window [0,1000), then feed a row
	// whose window already closed (late) plus a row with an unusable event time.
	in := &record.RecordBatch{
		Columns: []record.Column{{Name: "ts", Type: record.TypeInt}},
		Rows: [][]interface{}{
			{int64(5000)}, // opens/advances; watermark → 5000
			{int64(100)},  // window [0,1000) already closed → late, routed
			{nil},         // null event time → unusable, routed
		},
	}
	if _, err := w.Apply(in); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if w.LateCount() < 1 {
		t.Errorf("LateCount = %d, want >= 1", w.LateCount())
	}
	if lastCapture == nil || lastCapture.rows < 1 {
		t.Errorf("expected late rows routed to capture sink, got %+v", lastCapture)
	}
	_ = w.Close()
}

func mkWT(t *testing.T, eventTime map[string]interface{}) *windowTransform {
	t.Helper()
	cfg := map[string]interface{}{
		"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
		"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
	}
	if eventTime != nil {
		cfg["eventTime"] = eventTime
	}
	tr, err := newTransform(cfg)
	if err != nil {
		t.Fatalf("newTransform: %v", err)
	}
	return tr.(*windowTransform)
}

func TestEventMillisTypes(t *testing.T) {
	proc := mkWT(t, nil) // no eventTime → processing time
	if ms, err := proc.eventMillis(map[string]interface{}{}); err != nil || ms <= 0 {
		t.Errorf("processing-time eventMillis = %d, %v", ms, err)
	}

	layout := mkWT(t, map[string]interface{}{"column": "ts", "format": "RFC3339"})
	ok := []struct {
		name string
		val  interface{}
	}{
		{"time.Time", time.Unix(1, 0)},
		{"int", 123},
		{"int32", int32(123)},
		{"int64", int64(123)},
		{"float64", 1.5},
		{"float32", float32(1.5)},
		{"rfc3339 string", "2024-01-02T03:04:05Z"},
	}
	for _, c := range ok {
		t.Run("ok/"+c.name, func(t *testing.T) {
			if _, err := layout.eventMillis(map[string]interface{}{"ts": c.val}); err != nil {
				t.Errorf("eventMillis(%v) err = %v", c.val, err)
			}
		})
	}

	bad := []struct {
		name string
		val  interface{}
	}{
		{"nil", nil},
		{"unparseable string", "not-a-time"},
		{"unsupported type", true},
	}
	for _, c := range bad {
		t.Run("bad/"+c.name, func(t *testing.T) {
			if _, err := layout.eventMillis(map[string]interface{}{"ts": c.val}); err == nil {
				t.Errorf("eventMillis(%v) expected error", c.val)
			}
		})
	}

	epoch := mkWT(t, map[string]interface{}{"column": "ts", "format": "epochMillis"})
	if ms, err := epoch.eventMillis(map[string]interface{}{"ts": "1500"}); err != nil || ms != 1500 {
		t.Errorf("epoch string eventMillis = %d, %v", ms, err)
	}
	if _, err := epoch.eventMillis(map[string]interface{}{"ts": "abc"}); err == nil {
		t.Error("epoch string 'abc' expected error")
	}
}

func TestGoLayout(t *testing.T) {
	cases := map[string]string{
		"":            time.RFC3339,
		"RFC3339":     time.RFC3339,
		"RFC3339Nano": time.RFC3339Nano,
		"DateTime":    time.DateTime,
		"DateOnly":    time.DateOnly,
		"custom-fmt":  "custom-fmt", // unknown returned as-is
	}
	for in, want := range cases {
		if got := goLayout(in); got != want {
			t.Errorf("goLayout(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAggColType(t *testing.T) {
	cases := map[string]record.ColumnType{
		"count": record.TypeInt,
		"sum":   record.TypeFloat,
		"avg":   record.TypeFloat,
		"min":   record.TypeFloat,
		"max":   record.TypeFloat,
		"first": record.TypeAny,
		"last":  record.TypeAny,
	}
	for fn, want := range cases {
		if got := aggColType(fn); got != want {
			t.Errorf("aggColType(%q) = %v, want %v", fn, got, want)
		}
	}
}

func TestWindowAdvancedConfigOK(t *testing.T) {
	// Exercises stateTtlMs, trigger (early firing + accumulation) and spillable state.
	tr, err := newTransform(map[string]interface{}{
		"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(1000)},
		"aggregations": []interface{}{map[string]interface{}{"as": "c", "fn": "count"}},
		"stateTtlMs":   float64(60000),
		"accumulation": "accumulating",
		"trigger":      map[string]interface{}{"earlyEveryMs": float64(500)},
		"state":        map[string]interface{}{"maxResidentKeys": float64(2), "spillDir": t.TempDir()},
	})
	if err != nil {
		t.Fatalf("newTransform advanced: %v", err)
	}
	if _, ok := tr.(transform.Transform); !ok {
		t.Fatal("does not implement transform.Transform")
	}
	_ = tr.Close()
}
