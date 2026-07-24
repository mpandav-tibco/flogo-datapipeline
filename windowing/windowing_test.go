package windowing

import (
	"testing"
)

func TestTumblingAssign(t *testing.T) {
	s := Spec{Kind: Tumbling, SizeMs: 1000}
	cases := []struct {
		ts         int64
		start, end int64
	}{
		{100, 0, 1000},
		{999, 0, 1000},
		{1000, 1000, 2000},
		{1200, 1000, 2000},
		{2000, 2000, 3000},
	}
	for _, c := range cases {
		ws := s.assign(c.ts)
		if len(ws) != 1 {
			t.Fatalf("ts=%d: want 1 window, got %d", c.ts, len(ws))
		}
		if ws[0].Start != c.start || ws[0].End != c.end {
			t.Errorf("ts=%d: want [%d,%d), got [%d,%d)", c.ts, c.start, c.end, ws[0].Start, ws[0].End)
		}
	}
}

func TestSlidingAssign(t *testing.T) {
	s := Spec{Kind: Sliding, SizeMs: 1000, SlideMs: 500}
	ws := s.assign(1200)
	// 1200 belongs to windows [1000,2000) and [500,1500).
	if len(ws) != 2 {
		t.Fatalf("want 2 windows, got %d: %+v", len(ws), ws)
	}
	want := map[Window]bool{{1000, 2000}: true, {500, 1500}: true}
	for _, w := range ws {
		if !want[w] {
			t.Errorf("unexpected window %+v", w)
		}
	}
}

func TestGlobalAssign(t *testing.T) {
	s := Spec{Kind: Global}
	ws := s.assign(42)
	if len(ws) != 1 || ws[0].Start != 0 || ws[0].End != globalWindowEnd {
		t.Fatalf("global window wrong: %+v", ws)
	}
}

func TestAccumulators(t *testing.T) {
	vals := []interface{}{10, 20, 5, 30}
	check := func(fn string, want interface{}) {
		acc, err := newAccumulator(fn)
		if err != nil {
			t.Fatalf("newAccumulator(%q): %v", fn, err)
		}
		for _, v := range vals {
			acc.Add(v)
		}
		if got := acc.Result(); got != want {
			t.Errorf("%s: want %v, got %v", fn, want, got)
		}
	}
	check("sum", float64(65))
	check("avg", float64(65)/4)
	check("min", float64(5))
	check("max", float64(30))
	check("count", int64(4))
	check("first", 10)
	check("last", 30)
}

func TestAccumulatorEmpty(t *testing.T) {
	for _, fn := range []string{"sum", "avg", "min", "max", "first", "last"} {
		acc, _ := newAccumulator(fn)
		if got := acc.Result(); got != nil {
			t.Errorf("%s empty: want nil, got %v", fn, got)
		}
	}
	acc, _ := newAccumulator("count")
	if got := acc.Result(); got != int64(0) {
		t.Errorf("count empty: want 0, got %v", got)
	}
}

func TestOperatorTumblingEventTime(t *testing.T) {
	op, err := New(
		Spec{Kind: Tumbling, SizeMs: 1000},
		[]AggSpec{{As: "total", Fn: "sum", Column: "amount"}, {As: "n", Fn: "count"}},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	add := func(sensor string, ts int64, amount int) {
		op.Add([]interface{}{sensor}, ts, map[string]interface{}{"amount": amount})
	}
	add("A", 100, 10)
	add("A", 200, 20)
	add("B", 150, 5)
	add("A", 1100, 100)

	// Close window [0,1000): A(total=30,n=2), B(total=5,n=1). [1000,2000) stays open.
	got := op.Emit(1000)
	if len(got) != 2 {
		t.Fatalf("emit: want 2 results, got %d: %+v", len(got), got)
	}
	// deterministic order: same window start, key A before B.
	if got[0].Key[0] != "A" || got[0].Aggs["total"] != float64(30) || got[0].Aggs["n"] != int64(2) {
		t.Errorf("result[0] wrong: %+v", got[0])
	}
	if got[1].Key[0] != "B" || got[1].Aggs["total"] != float64(5) || got[1].Aggs["n"] != int64(1) {
		t.Errorf("result[1] wrong: %+v", got[1])
	}
	if got[0].Window != (Window{0, 1000}) {
		t.Errorf("window wrong: %+v", got[0].Window)
	}

	// Flush remaining: A [1000,2000) total=100,n=1.
	rem := op.Flush()
	if len(rem) != 1 || rem[0].Key[0] != "A" || rem[0].Aggs["total"] != float64(100) {
		t.Fatalf("flush wrong: %+v", rem)
	}
	if rem[0].Window != (Window{1000, 2000}) {
		t.Errorf("flush window wrong: %+v", rem[0].Window)
	}
}

func TestOperatorLateDetection(t *testing.T) {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, []AggSpec{{As: "n", Fn: "count"}}, 0)
	op.Add([]interface{}{"A"}, 100, nil)
	op.Emit(1000) // close [0,1000)
	// A record at ts=500 now belongs only to the already-closed [0,1000).
	late := op.Add([]interface{}{"A"}, 500, nil)
	if !late {
		t.Errorf("want late=true for record in closed window")
	}
	// A record at ts=1500 is on time (window [1000,2000) still open).
	if op.Add([]interface{}{"A"}, 1500, nil) {
		t.Errorf("want late=false for record in open window")
	}
}

func TestWatermarkOutOfOrderness(t *testing.T) {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, []AggSpec{{As: "n", Fn: "count"}}, 200)
	if op.Watermark() != -1<<63 {
		// math.MinInt64 sentinel before any data
		t.Errorf("empty watermark: got %d", op.Watermark())
	}
	op.Add([]interface{}{"A"}, 1000, nil)
	if op.Watermark() != 800 {
		t.Errorf("watermark: want 800, got %d", op.Watermark())
	}
}
