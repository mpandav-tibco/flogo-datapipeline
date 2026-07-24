package windowing

import "testing"

// resultsEqual compares two already-sorted Result slices (Flush/Emit sort
// deterministically) for key, window, and aggregate equality.
func resultsEqual(a, b []Result) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if keyString(a[i].Key) != keyString(b[i].Key) || a[i].Window != b[i].Window {
			return false
		}
		if len(a[i].Aggs) != len(b[i].Aggs) {
			return false
		}
		for k, v := range a[i].Aggs {
			if b[i].Aggs[k] != v {
				return false
			}
		}
	}
	return true
}

func addRows(op *Operator) {
	op.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 10})
	op.Add([]interface{}{"A"}, 200, map[string]interface{}{"v": 20})
	op.Add([]interface{}{"B"}, 300, map[string]interface{}{"v": 5})
	op.Add([]interface{}{"C"}, 1100, map[string]interface{}{"v": 7})
	op.Add([]interface{}{"A"}, 1200, map[string]interface{}{"v": 30})
	op.Add([]interface{}{"B"}, 1300, map[string]interface{}{"v": 8})
}

// TestSpillStoreMatchesMemStore: a maxResident=1 spill store (constant paging)
// must produce byte-for-byte the same window results as the in-memory store.
func TestSpillStoreMatchesMemStore(t *testing.T) {
	spec := Spec{Kind: Tumbling, SizeMs: 1000}
	aggs := []AggSpec{{As: "sum", Fn: "sum", Column: "v"}, {As: "n", Fn: "count"}}

	memOp, _ := New(spec, aggs, 0)
	addRows(memOp)
	memRes := memOp.Flush()

	spillOp, _ := New(spec, aggs, 0)
	if err := spillOp.UseSpillableState(t.TempDir(), 1); err != nil {
		t.Fatal(err)
	}
	defer spillOp.Close()
	addRows(spillOp)
	spillRes := spillOp.Flush()

	if !resultsEqual(memRes, spillRes) {
		t.Errorf("spill results differ from mem:\n  mem=%v\n  spill=%v", memRes, spillRes)
	}
}

// TestSpillStoreEmitMatchesMem exercises the watermark-driven Emit path (fire +
// remove of spilled keys) under constant spill.
func TestSpillStoreEmitMatchesMem(t *testing.T) {
	spec := Spec{Kind: Tumbling, SizeMs: 1000}
	aggs := []AggSpec{{As: "sum", Fn: "sum", Column: "v"}}
	feed := func(op *Operator) []Result {
		var out []Result
		op.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 1})
		op.Add([]interface{}{"B"}, 200, map[string]interface{}{"v": 2})
		op.Add([]interface{}{"A"}, 1100, map[string]interface{}{"v": 3})
		out = append(out, op.Emit(1100)...)
		op.Add([]interface{}{"C"}, 2100, map[string]interface{}{"v": 4})
		out = append(out, op.Emit(2100)...)
		out = append(out, op.Flush()...)
		return out
	}
	memOp, _ := New(spec, aggs, 0)
	spillOp, _ := New(spec, aggs, 0)
	if err := spillOp.UseSpillableState(t.TempDir(), 1); err != nil {
		t.Fatal(err)
	}
	defer spillOp.Close()
	if !resultsEqual(feed(memOp), feed(spillOp)) {
		t.Error("spill Emit path differs from mem")
	}
}

// TestSpillStoreEvictsAndRetrieves checks the store mechanics directly: keys
// beyond maxResident are paged to disk, remain retrievable with state intact,
// and remove clears both tiers.
func TestSpillStoreEvictsAndRetrieves(t *testing.T) {
	aggs := []AggSpec{{As: "n", Fn: "count"}}
	s, err := newSpillStore(aggs, 2, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	mk := func(key string) *keyState {
		acc, _ := newAccumulator("count")
		acc.Add(nil)
		return &keyState{
			keyVals: []interface{}{key},
			windows: map[Window][]Accumulator{{Start: 0, End: 1000}: {acc}},
		}
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		s.put(k, mk(k))
	}
	if s.len() != 4 {
		t.Fatalf("len = %d, want 4", s.len())
	}
	if len(s.hot) != 2 || len(s.disk) != 2 {
		t.Fatalf("resident=%d spilled=%d, want 2/2", len(s.hot), len(s.disk))
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		ks, ok := s.get(k)
		if !ok {
			t.Fatalf("get %q missing", k)
		}
		var total int64
		for _, accs := range ks.windows {
			total += accs[0].Result().(int64)
		}
		if total != 1 {
			t.Errorf("key %q count = %d, want 1", k, total)
		}
	}
	s.remove("a")
	if _, ok := s.get("a"); ok {
		t.Error("removed key still present")
	}
	if s.len() != 3 {
		t.Errorf("len after remove = %d, want 3", s.len())
	}
}

// TestSpillStoreSnapshotRestore proves Snapshot captures spilled keys (via
// keys()+get) and Restore repopulates a spill-backed operator.
func TestSpillStoreSnapshotRestore(t *testing.T) {
	spec := Spec{Kind: Tumbling, SizeMs: 1000}
	aggs := []AggSpec{{As: "sum", Fn: "sum", Column: "v"}, {As: "n", Fn: "count"}}

	op, _ := New(spec, aggs, 0)
	if err := op.UseSpillableState(t.TempDir(), 1); err != nil {
		t.Fatal(err)
	}
	defer op.Close()
	op.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 10})
	op.Add([]interface{}{"B"}, 200, map[string]interface{}{"v": 20})
	op.Add([]interface{}{"C"}, 300, map[string]interface{}{"v": 30})
	data, err := op.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	op2, _ := New(spec, aggs, 0)
	if err := op2.UseSpillableState(t.TempDir(), 1); err != nil {
		t.Fatal(err)
	}
	defer op2.Close()
	if err := op2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if !resultsEqual(op.Flush(), op2.Flush()) {
		t.Error("restore into spill operator lost or changed state")
	}
}
