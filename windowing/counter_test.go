package windowing

import "testing"

// TestOpenWindowCounter guards the incrementally-maintained open-window counter
// (OpenWindows is O(1) and must not drift from the actual window count) across
// Add, Emit, Flush, and session merges.
func TestOpenWindowCounter(t *testing.T) {
	aggs := []AggSpec{{As: "n", Fn: "count"}}

	op, _ := New(Spec{Kind: Tumbling, SizeMs: 1000}, aggs, 0)
	op.Add([]interface{}{"A"}, 100, nil)  // A [0,1000)
	op.Add([]interface{}{"A"}, 1100, nil) // A [1000,2000)
	op.Add([]interface{}{"B"}, 200, nil)  // B [0,1000)
	if op.OpenWindows() != 3 {
		t.Fatalf("after adds: OpenWindows=%d, want 3", op.OpenWindows())
	}
	op.Emit(1000) // fires A[0,1000) and B[0,1000)
	if op.OpenWindows() != 1 {
		t.Fatalf("after emit(1000): OpenWindows=%d, want 1", op.OpenWindows())
	}
	op.Flush()
	if op.OpenWindows() != 0 {
		t.Fatalf("after flush: OpenWindows=%d, want 0", op.OpenWindows())
	}

	// Session merges/splits.
	sop, _ := New(Spec{Kind: Session, GapMs: 500}, aggs, 0)
	sop.Add([]interface{}{"X"}, 100, nil) // [100,600)
	sop.Add([]interface{}{"X"}, 300, nil) // overlaps → merged, still 1
	if sop.OpenWindows() != 1 {
		t.Errorf("session merged: OpenWindows=%d, want 1", sop.OpenWindows())
	}
	sop.Add([]interface{}{"X"}, 5000, nil) // disjoint → 2
	if sop.OpenWindows() != 2 {
		t.Errorf("session disjoint: OpenWindows=%d, want 2", sop.OpenWindows())
	}
}
