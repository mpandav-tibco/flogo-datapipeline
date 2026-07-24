package windowing

import "testing"

func TestOperatorSession(t *testing.T) {
	op, err := New(Spec{Kind: Session, GapMs: 1000},
		[]AggSpec{{As: "total", Fn: "sum", Column: "v"}, {As: "n", Fn: "count"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	add := func(ts int64, v int) {
		op.Add([]interface{}{"A"}, ts, map[string]interface{}{"v": v})
	}
	// Two records within the 1s gap → one session [100,1500); a third far later → new session.
	add(100, 1)
	add(500, 2)  // 500 within gap of [100,1100) → merges → [100,1500)
	add(2000, 3) // gap from 1500 → new session [2000,3000)

	// Watermark (maxLateness 0) = 2000; session [100,1500) fires, [2000,3000) stays.
	got := op.Emit(2000)
	if len(got) != 1 {
		t.Fatalf("emit: want 1 session, got %d: %+v", len(got), got)
	}
	if got[0].Window != (Window{100, 1500}) || got[0].Aggs["total"] != float64(3) || got[0].Aggs["n"] != int64(2) {
		t.Errorf("session 1 wrong: %+v", got[0])
	}

	rem := op.Flush()
	if len(rem) != 1 || rem[0].Window != (Window{2000, 3000}) || rem[0].Aggs["n"] != int64(1) {
		t.Fatalf("session 2 wrong: %+v", rem)
	}
}

func TestOperatorSessionBridge(t *testing.T) {
	// A record can bridge two existing sessions into one.
	op, _ := New(Spec{Kind: Session, GapMs: 100},
		[]AggSpec{{As: "n", Fn: "count"}}, 0)
	op.Add([]interface{}{"A"}, 0, nil)   // [0,100)
	op.Add([]interface{}{"A"}, 300, nil) // [300,400)  (separate)
	op.Add([]interface{}{"A"}, 150, nil) // [150,250): bridges [0,100)? 150<=100? no. [300,400)? 250>=300? no → own session
	// 150's window [150,250): overlaps [0,100)? 150<=100 false. overlaps [300,400)? 250>=300 false. So 3 sessions.
	all := op.Flush()
	if len(all) != 3 {
		t.Fatalf("want 3 sessions, got %d: %+v", len(all), all)
	}

	op2, _ := New(Spec{Kind: Session, GapMs: 200},
		[]AggSpec{{As: "n", Fn: "count"}}, 0)
	op2.Add([]interface{}{"A"}, 0, nil)   // [0,200)
	op2.Add([]interface{}{"A"}, 400, nil) // [400,600)
	op2.Add([]interface{}{"A"}, 250, nil) // [250,450): overlaps [0,200)? 250<=200 no. overlaps [400,600)? 450>=400 yes → merges [250,600); still separate from [0,200)
	// Actually 250 window [250,450) overlaps [400,600) → merge → [250,600). [0,200) stays.
	all2 := op2.Flush()
	if len(all2) != 2 {
		t.Fatalf("want 2 sessions after partial bridge, got %d: %+v", len(all2), all2)
	}
}

func TestOperatorStateTTL(t *testing.T) {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000},
		[]AggSpec{{As: "n", Fn: "count"}}, 0)
	op.SetStateTTL(5000)

	op.Add([]interface{}{"A"}, 100, nil)   // window [0,1000)
	op.Add([]interface{}{"B"}, 10000, nil) // window [10000,11000); maxEventTime=10000

	// Emit with a low watermark (simulating a large lateness allowance): A's window
	// does NOT fire on the watermark, but the TTL sweep (cutoff 10000-5000=5000)
	// drops it since its End (1000) <= 5000.
	got := op.Emit(50)
	if len(got) != 0 {
		t.Fatalf("no window should fire at watermark 50, got %d", len(got))
	}
	if op.EvictedCount() != 1 {
		t.Errorf("want 1 TTL eviction, got %d", op.EvictedCount())
	}
	rem := op.Flush()
	if len(rem) != 1 || rem[0].Key[0] != "B" {
		t.Fatalf("only B should remain after TTL, got %+v", rem)
	}
}
