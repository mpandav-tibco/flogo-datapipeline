package windowing

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Result is one emitted window aggregation for one key.
type Result struct {
	Key    []interface{}          // groupBy values, in groupBy order
	Window Window                 // the closed window
	Aggs   map[string]interface{} // output column name → aggregated value
}

// Operator holds keyed, windowed accumulator state. It is engine-agnostic: the
// caller feeds rows via Add, advances event time via Emit (watermark-driven), and
// drains remaining windows at end-of-input via Flush.
//
// Not safe for concurrent use; the pipeline drives one operator from a single
// goroutine (windowed pipelines are excluded from the parallel transform path).
type Operator struct {
	spec          Spec
	aggs          []AggSpec
	maxLatenessMs int64

	store stateStore

	maxEventTime int64
	hasEvent     bool
	// currentWatermark is the highest watermark passed to Emit so far; windows at
	// or below it are considered closed, and records for them are "late".
	currentWatermark int64

	// stateTtlMs, when > 0, bounds memory by dropping windows whose End falls more
	// than stateTtlMs behind the max event time (a safety valve for high-churn keys
	// and long-idle session windows). ttlEvicted counts such drops.
	stateTtlMs int64
	ttlEvicted int64

	// openWindowCount tracks the total number of (key,window) entries in state,
	// maintained incrementally so OpenWindows() is O(1) instead of scanning every
	// key (which, under spill, would page the whole cold set back from disk).
	openWindowCount int

	// earlyEveryMs, when > 0, emits partial results for still-open windows every
	// earlyEveryMs of watermark (event-time) progress. accumulating selects whether
	// each firing is the running total (accumulating) or the delta since the last
	// firing (discarding — accumulators are reset after each early fire).
	earlyEveryMs int64
	accumulating bool

	// due indexes open windows by End so Emit visits only windows that are
	// actually due instead of scanning every key (see deadline.go).
	due deadlineHeap
}

type keyState struct {
	keyVals    []interface{}
	windows    map[Window][]Accumulator
	earlyFired map[Window]int64 // last watermark a window early-fired at (lazy)
}

// New constructs an Operator. It validates the window spec and aggregate function
// names up front so a bad config fails at build time rather than panicking on the
// first row.
func New(spec Spec, aggs []AggSpec, maxLatenessMs int64) (*Operator, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	if maxLatenessMs < 0 {
		return nil, fmt.Errorf("windowing: maxLatenessMs must be >= 0, got %d (a negative value pushes the watermark ahead of event time and fires windows early)", maxLatenessMs)
	}
	for _, ag := range aggs {
		if _, err := newAccumulator(ag.Fn); err != nil {
			return nil, err
		}
	}
	return &Operator{
		spec:             spec,
		aggs:             aggs,
		maxLatenessMs:    maxLatenessMs,
		store:            newMemStore(),
		currentWatermark: math.MinInt64,
	}, nil
}

// SetStateTTL sets the state time-to-live in millis (0 disables). Windows whose
// End falls more than ttlMs behind the max event time are dropped on Emit.
func (o *Operator) SetStateTTL(ttlMs int64) { o.stateTtlMs = ttlMs }

// SetTrigger configures early firing: still-open windows emit a partial result
// every earlyEveryMs of watermark progress (0 disables). accumulating=true emits
// the running total each firing; false (discarding) emits the delta since the
// previous firing.
func (o *Operator) SetTrigger(earlyEveryMs int64, accumulating bool) {
	o.earlyEveryMs = earlyEveryMs
	o.accumulating = accumulating
}

// EvictedCount returns the number of windows dropped by the state-TTL sweep.
func (o *Operator) EvictedCount() int64 { return o.ttlEvicted }

// UseSpillableState swaps the default in-memory state store for a spillable one
// that keeps at most maxResidentKeys keys in memory and pages colder keys to
// disk under baseDir (empty = OS temp dir). This bounds heap for high-cardinality
// key spaces at the cost of disk I/O on cold-key access. Call once, right after
// New and before any rows are added.
func (o *Operator) UseSpillableState(baseDir string, maxResidentKeys int) error {
	ss, err := newSpillStore(o.aggs, maxResidentKeys, baseDir)
	if err != nil {
		return err
	}
	o.store = ss
	return nil
}

// Close releases resources held by the state store (e.g. the spill directory).
// A no-op for the default in-memory store.
func (o *Operator) Close() error { return o.store.close() }

// OpenWindows returns the number of windows currently held in state (not yet
// closed). Useful for observability — an ever-growing count on a stream signals
// key churn without a state TTL. Windows lost when a spillable store fails to
// page a key to disk (see SpillFailures) are discounted so the gauge stays
// accurate even on that error path.
func (o *Operator) OpenWindows() int { return o.openWindowCount - int(o.store.droppedWindowCount()) }

// SpillFailures returns the cumulative number of keys the spillable state store
// failed to page to disk (e.g. disk full) and therefore dropped — a correctness
// signal that should be zero. Always 0 for the in-memory store.
func (o *Operator) SpillFailures() int64 { return o.store.evictFailures() }

// Add folds a row (identified by its groupBy key values and event time ts) into
// the windows it belongs to. It returns late=true only when EVERY window the row
// belongs to has already closed (End <= current watermark) — the caller may then
// route or drop the row.
func (o *Operator) Add(keyVals []interface{}, ts int64, row map[string]interface{}) (late bool) {
	if !o.hasEvent || ts > o.maxEventTime {
		o.maxEventTime = ts
		o.hasEvent = true
	}

	if o.spec.Kind == Session {
		return o.addSession(keyVals, ts, row)
	}

	windows := o.spec.assign(ts)
	late = true
	key := keyString(keyVals)
	var ks *keyState
	for _, w := range windows {
		if w.End <= o.currentWatermark {
			continue // this window already fired — row is late for it
		}
		late = false

		if ks == nil {
			if existing, ok := o.store.get(key); ok {
				ks = existing
			} else {
				ks = &keyState{keyVals: cloneVals(keyVals), windows: make(map[Window][]Accumulator)}
			}
		}
		accs := ks.windows[w]
		if accs == nil {
			accs = make([]Accumulator, len(o.aggs))
			for i, ag := range o.aggs {
				accs[i], _ = newAccumulator(ag.Fn) // validated in New
			}
			ks.windows[w] = accs
			o.openWindowCount++
			o.schedule(key, w)
		}
		for i, ag := range o.aggs {
			if ag.Fn == "count" {
				accs[i].Add(nil)
				continue
			}
			accs[i].Add(row[ag.Column])
		}
	}
	if ks != nil {
		o.store.put(key, ks)
	}
	return late
}

// addSession folds a record into session windows, merging any existing windows for
// the key that overlap (or are within GapMs of) the new record's provisional
// window [ts, ts+gap). Merged accumulators combine the overlapping windows in
// start order, then the new record is added — so first/last follow window start
// order (best-effort for out-of-order arrivals).
func (o *Operator) addSession(keyVals []interface{}, ts int64, row map[string]interface{}) (late bool) {
	newStart, newEnd := ts, ts+o.spec.GapMs

	key := keyString(keyVals)
	ks, ok := o.store.get(key)
	if !ok {
		ks = &keyState{keyVals: cloneVals(keyVals), windows: make(map[Window][]Accumulator)}
	}

	var overlap []Window
	mergedStart, mergedEnd := newStart, newEnd
	for w := range ks.windows {
		if w.Start <= newEnd && w.End >= newStart { // overlapping or adjacent
			overlap = append(overlap, w)
			if w.Start < mergedStart {
				mergedStart = w.Start
			}
			if w.End > mergedEnd {
				mergedEnd = w.End
			}
		}
	}

	// A session entirely at/below the watermark has already fired — treat as late.
	if mergedEnd <= o.currentWatermark {
		return true
	}

	sort.Slice(overlap, func(i, j int) bool { return overlap[i].Start < overlap[j].Start })
	accs := make([]Accumulator, len(o.aggs))
	for i, ag := range o.aggs {
		accs[i], _ = newAccumulator(ag.Fn)
	}
	for _, w := range overlap {
		for i := range o.aggs {
			accs[i].Merge(ks.windows[w][i])
		}
		delete(ks.windows, w)
		if ks.earlyFired != nil {
			delete(ks.earlyFired, w) // merged window is replaced; drop stale early-fire mark
		}
	}
	for i, ag := range o.aggs {
		if ag.Fn == "count" {
			accs[i].Add(nil)
			continue
		}
		accs[i].Add(row[ag.Column])
	}
	merged := Window{Start: mergedStart, End: mergedEnd}
	ks.windows[merged] = accs
	o.openWindowCount += 1 - len(overlap)
	o.store.put(key, ks)
	// A merge that kept the bounds of an existing window is already scheduled.
	scheduled := false
	for _, w := range overlap {
		if w == merged {
			scheduled = true
			break
		}
	}
	if !scheduled {
		o.schedule(key, merged)
	}
	return false
}

// Watermark returns the current event-time watermark: the highest event time
// seen minus the configured out-of-orderness allowance. Returns math.MinInt64
// when no rows have been observed.
func (o *Operator) Watermark() int64 {
	if !o.hasEvent {
		return math.MinInt64
	}
	return o.maxEventTime - o.maxLatenessMs
}

// Emit fires windows and returns their results. A window whose End <= watermark
// fires finally and is removed. A still-open window fires early (a partial result)
// when early triggers are enabled and the watermark has advanced earlyEveryMs past
// its last firing; in discarding mode its accumulators are then reset. Results are
// returned in deterministic order (window start, then key).
//
// Final firing and TTL eviction are driven by the End-ordered deadline index, so
// cost is proportional to the windows that close, not to total retained state.
// Only early triggers (which must inspect every open window) still scan.
func (o *Operator) Emit(watermark int64) []Result {
	if watermark > o.currentWatermark {
		o.currentWatermark = watermark
	}
	var out []Result
	var sortKeys []string
	o.popDue(watermark, func(key string, st *keyState, w Window, accs []Accumulator) {
		out = append(out, o.buildResult(st.keyVals, w, accs))
		sortKeys = append(sortKeys, key)
	})
	if o.earlyEveryMs > 0 {
		out, sortKeys = o.fireEarly(watermark, out, sortKeys)
	}
	o.sweepTTL()
	sortResultsKeyed(out, sortKeys)
	return out
}

// fireEarly emits partial results for still-open windows whose trigger cadence
// has elapsed.
func (o *Operator) fireEarly(watermark int64, out []Result, sortKeys []string) ([]Result, []string) {
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		changed := false
		for w, accs := range st.windows {
			last, ok := st.earlyFired[w]
			if !ok {
				last = w.Start
			}
			if watermark < last+o.earlyEveryMs {
				continue
			}
			out = append(out, o.buildResult(st.keyVals, w, accs))
			sortKeys = append(sortKeys, key)
			if st.earlyFired == nil {
				st.earlyFired = make(map[Window]int64)
			}
			st.earlyFired[w] = watermark
			changed = true
			if !o.accumulating {
				// discarding: reset so the next pane is a delta
				fresh := make([]Accumulator, len(o.aggs))
				for i, ag := range o.aggs {
					fresh[i], _ = newAccumulator(ag.Fn)
				}
				st.windows[w] = fresh
			}
		}
		if changed {
			o.store.put(key, st)
		}
	}
	return out, sortKeys
}

// sweepTTL drops windows abandoned far behind the max event time, bounding memory
// for high-churn keys and long-idle sessions. In normal operation the watermark
// fires windows first, so this only catches pathological/very-late state.
func (o *Operator) sweepTTL() {
	if o.stateTtlMs <= 0 || !o.hasEvent {
		return
	}
	o.popDue(o.maxEventTime-o.stateTtlMs, func(string, *keyState, Window, []Accumulator) {
		o.ttlEvicted++
	})
}

// Flush closes and returns ALL remaining windows regardless of watermark (the
// end-of-input path for bounded sources), clearing state.
func (o *Operator) Flush() []Result {
	var out []Result
	var sortKeys []string
	for _, key := range o.store.keys() {
		st, ok := o.store.get(key)
		if !ok {
			continue
		}
		for w, accs := range st.windows {
			out = append(out, o.buildResult(st.keyVals, w, accs))
			sortKeys = append(sortKeys, key)
		}
	}
	o.store.clear()
	o.openWindowCount = 0
	o.due = o.due[:0]
	sortResultsKeyed(out, sortKeys)
	return out
}

func (o *Operator) buildResult(keyVals []interface{}, w Window, accs []Accumulator) Result {
	aggs := make(map[string]interface{}, len(o.aggs))
	for i, ag := range o.aggs {
		aggs[ag.As] = accs[i].Result()
	}
	return Result{Key: cloneVals(keyVals), Window: w, Aggs: aggs}
}

// sortResultsKeyed orders by window start, then window end, then key string —
// stable, deterministic output for reproducible sinks and tests. keys[i] is
// the precomputed keyString of rs[i], so comparisons never rebuild it.
func sortResultsKeyed(rs []Result, keys []string) {
	sort.Sort(keyedResults{rs, keys})
}

type keyedResults struct {
	rs   []Result
	keys []string
}

func (k keyedResults) Len() int { return len(k.rs) }
func (k keyedResults) Swap(i, j int) {
	k.rs[i], k.rs[j] = k.rs[j], k.rs[i]
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
}
func (k keyedResults) Less(i, j int) bool {
	a, b := k.rs[i].Window, k.rs[j].Window
	if a.Start != b.Start {
		return a.Start < b.Start
	}
	if a.End != b.End {
		return a.End < b.End
	}
	return k.keys[i] < k.keys[j]
}

// keyString builds a stable map key from grouping values. 0x1f (unit separator)
// avoids collisions between values whose string forms would otherwise concatenate
// ambiguously.
func keyString(vals []interface{}) string {
	var b strings.Builder
	for i, v := range vals {
		if i > 0 {
			b.WriteByte(0x1f)
		}
		b.WriteString(valString(v))
	}
	return b.String()
}

func valString(v interface{}) string {
	if v == nil {
		return "\x00nil"
	}
	return record.FormatValue(v) // identical to %v, without fmt for common types
}

func cloneVals(vals []interface{}) []interface{} {
	c := make([]interface{}, len(vals))
	copy(c, vals)
	return c
}
