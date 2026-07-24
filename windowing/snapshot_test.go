package windowing

import "testing"

func newSumCountOp() *Operator {
	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000},
		[]AggSpec{{As: "total", Fn: "sum", Column: "v"}, {As: "n", Fn: "count"}}, 0)
	return op
}

func TestOperatorSnapshotRestore(t *testing.T) {
	op := newSumCountOp()
	op.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 10})
	op.Add([]interface{}{"A"}, 200, map[string]interface{}{"v": 20})
	op.Add([]interface{}{"B"}, 1100, map[string]interface{}{"v": 5})

	data, err := op.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh operator and confirm it flushes identical results.
	op2 := newSumCountOp()
	if err := op2.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	r1 := op.Flush()
	r2 := op2.Flush()
	if len(r1) != len(r2) || len(r1) != 2 {
		t.Fatalf("flush lengths: orig=%d restored=%d", len(r1), len(r2))
	}
	for i := range r1 {
		if r1[i].Key[0] != r2[i].Key[0] || r1[i].Window != r2[i].Window ||
			r1[i].Aggs["total"] != r2[i].Aggs["total"] || r1[i].Aggs["n"] != r2[i].Aggs["n"] {
			t.Errorf("result %d mismatch: orig=%+v restored=%+v", i, r1[i], r2[i])
		}
	}
}

func TestOperatorRestoreThenContinue(t *testing.T) {
	op := newSumCountOp()
	op.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 10})
	op.Add([]interface{}{"A"}, 200, map[string]interface{}{"v": 20})
	data, _ := op.Snapshot()

	// Simulate a restart: restore, then keep adding to the SAME window.
	op3 := newSumCountOp()
	if err := op3.Restore(data); err != nil {
		t.Fatal(err)
	}
	op3.Add([]interface{}{"A"}, 300, map[string]interface{}{"v": 5}) // A [0,1000) now 35 / n3

	got := op3.Emit(1000) // fire [0,1000)
	if len(got) != 1 {
		t.Fatalf("want 1 result, got %d: %+v", len(got), got)
	}
	if got[0].Aggs["total"] != float64(35) || got[0].Aggs["n"] != int64(3) {
		t.Errorf("resumed aggregate wrong: %+v", got[0])
	}
}
