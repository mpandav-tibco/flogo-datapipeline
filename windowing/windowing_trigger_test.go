package windowing

import "testing"

func triggerAdd(op *Operator, ts int64, v int) {
	op.Add([]interface{}{"A"}, ts, map[string]interface{}{"v": v})
}

func TestOperatorEarlyTriggerAccumulating(t *testing.T) {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, []AggSpec{{As: "total", Fn: "sum", Column: "v"}}, 0)
	op.SetTrigger(200, true) // accumulating: each firing = running total

	triggerAdd(op, 100, 10) // window [0,1000)
	r1 := op.Emit(300)      // open; early fire (watermark 300 >= 0+200) → total 10
	if len(r1) != 1 || r1[0].Aggs["total"] != float64(10) || r1[0].Window != (Window{0, 1000}) {
		t.Fatalf("early fire 1: %+v", r1)
	}

	triggerAdd(op, 400, 20) // running total now 30
	r2 := op.Emit(500)      // early fire (500 >= 300+200) → running total 30
	if len(r2) != 1 || r2[0].Aggs["total"] != float64(30) {
		t.Fatalf("early fire 2 (accumulating): %+v", r2)
	}

	r3 := op.Emit(1000) // End<=watermark → final fire → 30
	if len(r3) != 1 || r3[0].Aggs["total"] != float64(30) {
		t.Fatalf("final fire: %+v", r3)
	}
}

func TestOperatorEarlyTriggerDiscarding(t *testing.T) {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, []AggSpec{{As: "total", Fn: "sum", Column: "v"}}, 0)
	op.SetTrigger(200, false) // discarding: each firing = delta since last

	triggerAdd(op, 100, 10)
	r1 := op.Emit(300) // early → 10, then reset
	if len(r1) != 1 || r1[0].Aggs["total"] != float64(10) {
		t.Fatalf("early fire 1: %+v", r1)
	}

	triggerAdd(op, 400, 20)
	r2 := op.Emit(500) // early → delta 20, reset
	if len(r2) != 1 || r2[0].Aggs["total"] != float64(20) {
		t.Fatalf("early fire 2 (discarding delta): %+v", r2)
	}

	triggerAdd(op, 900, 5)
	r3 := op.Emit(1000) // final → delta 5
	if len(r3) != 1 || r3[0].Aggs["total"] != float64(5) {
		t.Fatalf("final fire (discarding delta): %+v", r3)
	}
	// 10 + 20 + 5 == 35 == the true total (downstream re-sums discarding panes).
}

func TestOperatorNoEarlyTriggerUnchanged(t *testing.T) {
	// Without SetTrigger, a window fires exactly once on close (regression guard).
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, []AggSpec{{As: "total", Fn: "sum", Column: "v"}}, 0)
	triggerAdd(op, 100, 10)
	if got := op.Emit(300); len(got) != 0 { // window still open, no early trigger
		t.Fatalf("want no emission before close, got %+v", got)
	}
	got := op.Emit(1000)
	if len(got) != 1 || got[0].Aggs["total"] != float64(10) {
		t.Fatalf("single final fire: %+v", got)
	}
}
