package windowing

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// emitScanReference is the pre-index Emit: a full scan of every key and window
// for final + early firing, followed by a full-scan TTL sweep. It is kept only
// as a reference implementation for the differential test below.
func emitScanReference(o *Operator, watermark int64) []Result {
	if watermark > o.currentWatermark {
		o.currentWatermark = watermark
	}
	var out []Result
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		for w, accs := range st.windows {
			if w.End <= watermark {
				out = append(out, o.buildResult(st.keyVals, w, accs))
				delete(st.windows, w)
				o.openWindowCount--
				if st.earlyFired != nil {
					delete(st.earlyFired, w)
				}
				continue
			}
			if o.earlyEveryMs <= 0 {
				continue
			}
			last, ok := st.earlyFired[w]
			if !ok {
				last = w.Start
			}
			if watermark >= last+o.earlyEveryMs {
				out = append(out, o.buildResult(st.keyVals, w, accs))
				if st.earlyFired == nil {
					st.earlyFired = make(map[Window]int64)
				}
				st.earlyFired[w] = watermark
				if !o.accumulating {
					fresh := make([]Accumulator, len(o.aggs))
					for i, ag := range o.aggs {
						fresh[i], _ = newAccumulator(ag.Fn)
					}
					st.windows[w] = fresh
				}
			}
		}
		if len(st.windows) == 0 {
			o.store.remove(key)
		} else {
			o.store.put(key, st)
		}
	}
	if o.stateTtlMs > 0 && o.hasEvent {
		cutoff := o.maxEventTime - o.stateTtlMs
		for _, key := range o.store.keys() {
			st, ok := o.store.get(key)
			if !ok {
				continue
			}
			for w := range st.windows {
				if w.End <= cutoff {
					delete(st.windows, w)
					if st.earlyFired != nil {
						delete(st.earlyFired, w)
					}
					o.ttlEvicted++
					o.openWindowCount--
				}
			}
			if len(st.windows) == 0 {
				o.store.remove(key)
			} else {
				o.store.put(key, st)
			}
		}
	}
	keys := make([]string, len(out))
	for i := range out {
		keys[i] = keyString(out[i].Key)
	}
	sortResultsKeyed(out, keys)
	return out
}

func TestEmitIndexMatchesFullScan(t *testing.T) {
	specs := []Spec{
		{Kind: Tumbling, SizeMs: 100},
		{Kind: Sliding, SizeMs: 100, SlideMs: 25},
		{Kind: Session, GapMs: 40},
	}
	aggs := []AggSpec{{As: "n", Fn: "count"}, {As: "s", Fn: "sum", Column: "v"}, {As: "f", Fn: "first", Column: "v"}}
	type knobs struct {
		ttl, early int64
		accum      bool
		spill      int
	}
	variants := []knobs{{}, {ttl: 150}, {ttl: 30}, {early: 30, accum: true}, {early: 30}, {ttl: 60, early: 20}, {spill: 3}, {spill: 2, ttl: 120, early: 25}}
	for _, spec := range specs {
		for vi, k := range variants {
			t.Run(fmt.Sprintf("%s/%d", spec.Kind, vi), func(t *testing.T) {
				mk := func() *Operator {
					o, err := New(spec, aggs, 20)
					if err != nil {
						t.Fatal(err)
					}
					o.SetStateTTL(k.ttl)
					o.SetTrigger(k.early, k.accum)
					if k.spill > 0 {
						if err := o.UseSpillableState(t.TempDir(), k.spill); err != nil {
							t.Fatal(err)
						}
					}
					t.Cleanup(func() { _ = o.Close() })
					return o
				}
				idx, ref := mk(), mk()
				rng := rand.New(rand.NewSource(int64(vi*31 + len(spec.Kind))))
				ts := int64(1000)
				for step := 0; step < 600; step++ {
					ts += int64(rng.Intn(15))
					evTs := ts - int64(rng.Intn(60)) // out-of-order arrivals
					key := []interface{}{fmt.Sprintf("k%d", rng.Intn(6)), int64(rng.Intn(2))}
					row := map[string]interface{}{"v": float64(rng.Intn(100))}
					if a, b := idx.Add(key, evTs, row), ref.Add(key, evTs, row); a != b {
						t.Fatalf("step %d: late mismatch idx=%v ref=%v", step, a, b)
					}
					if step%7 == 0 {
						got, want := idx.Emit(idx.Watermark()), emitScanReference(ref, ref.Watermark())
						if !reflect.DeepEqual(got, want) {
							for i := range got { if i < len(want) && !reflect.DeepEqual(got[i], want[i]) { t.Fatalf("step %d first diff at %d:\n idx=%#v\n ref=%#v", step, i, got[i], want[i]) } }; t.Fatalf("len %d vs %d", len(got), len(want))
						}
						if idx.OpenWindows() != ref.OpenWindows() || idx.EvictedCount() != ref.EvictedCount() {
							t.Fatalf("step %d: open %d/%d evicted %d/%d", step, idx.OpenWindows(), ref.OpenWindows(), idx.EvictedCount(), ref.EvictedCount())
						}
					}
				}
				if got, want := idx.Flush(), ref.Flush(); !reflect.DeepEqual(got, want) {
					t.Fatalf("flush mismatch\n idx=%v\n ref=%v", got, want)
				}
			})
		}
	}
}

func TestDeadlineIndexSurvivesRestore(t *testing.T) {
	o, _ := New(Spec{Kind: Tumbling, SizeMs: 100}, []AggSpec{{As: "n", Fn: "count"}}, 0)
	for ts := int64(0); ts < 450; ts += 10 {
		o.Add([]interface{}{"k"}, ts, nil)
	}
	_ = o.Emit(200)
	snap, err := o.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := New(Spec{Kind: Tumbling, SizeMs: 100}, []AggSpec{{As: "n", Fn: "count"}}, 0)
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	got := r.Emit(1000)
	var starts []int64
	for _, res := range got {
		starts = append(starts, res.Window.Start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	if !reflect.DeepEqual(starts, []int64{200, 300, 400}) {
		t.Fatalf("restored operator fired windows %v; want [200 300 400]", starts)
	}
}

func TestDeadlineIndexStaysBounded(t *testing.T) {
	// Session merges leave stale entries; compaction must keep the index
	// proportional to live state.
	o, _ := New(Spec{Kind: Session, GapMs: 1_000_000}, []AggSpec{{As: "n", Fn: "count"}}, 0)
	for ts := int64(0); ts < 50_000; ts++ {
		o.Add([]interface{}{"k"}, ts, nil)
	}
	if len(o.due) > 4*o.openWindowCount+1024 {
		t.Fatalf("deadline index grew to %d entries for %d open windows", len(o.due), o.openWindowCount)
	}
}
