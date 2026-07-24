package windowing

import (
	"fmt"
	"math"
	"sort"
	"strings"
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

	state map[string]*keyState

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
}

type keyState struct {
	keyVals []interface{}
	windows map[Window][]Accumulator
}

// New constructs an Operator. It validates the aggregate function names up front
// so a bad config fails at build time rather than at first row.
func New(spec Spec, aggs []AggSpec, maxLatenessMs int64) (*Operator, error) {
	for _, ag := range aggs {
		if _, err := newAccumulator(ag.Fn); err != nil {
			return nil, err
		}
	}
	return &Operator{
		spec:             spec,
		aggs:             aggs,
		maxLatenessMs:    maxLatenessMs,
		state:            make(map[string]*keyState),
		currentWatermark: math.MinInt64,
	}, nil
}

// SetStateTTL sets the state time-to-live in millis (0 disables). Windows whose
// End falls more than ttlMs behind the max event time are dropped on Emit.
func (o *Operator) SetStateTTL(ttlMs int64) { o.stateTtlMs = ttlMs }

// EvictedCount returns the number of windows dropped by the state-TTL sweep.
func (o *Operator) EvictedCount() int64 { return o.ttlEvicted }

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
	for _, w := range windows {
		if w.End <= o.currentWatermark {
			continue // this window already fired — row is late for it
		}
		late = false

		ks := o.state[keyString(keyVals)]
		if ks == nil {
			ks = &keyState{keyVals: cloneVals(keyVals), windows: make(map[Window][]Accumulator)}
			o.state[keyString(keyVals)] = ks
		}
		accs := ks.windows[w]
		if accs == nil {
			accs = make([]Accumulator, len(o.aggs))
			for i, ag := range o.aggs {
				accs[i], _ = newAccumulator(ag.Fn) // validated in New
			}
			ks.windows[w] = accs
		}
		for i, ag := range o.aggs {
			if ag.Fn == "count" {
				accs[i].Add(nil)
				continue
			}
			accs[i].Add(row[ag.Column])
		}
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

	ks := o.state[keyString(keyVals)]
	if ks == nil {
		ks = &keyState{keyVals: cloneVals(keyVals), windows: make(map[Window][]Accumulator)}
		o.state[keyString(keyVals)] = ks
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
	}
	for i, ag := range o.aggs {
		if ag.Fn == "count" {
			accs[i].Add(nil)
			continue
		}
		accs[i].Add(row[ag.Column])
	}
	ks.windows[Window{Start: mergedStart, End: mergedEnd}] = accs
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

// Emit closes and returns every window whose End <= watermark, removing them from
// state (discarding accumulation: each window fires exactly once). Results are
// returned in deterministic order (window start, then key).
func (o *Operator) Emit(watermark int64) []Result {
	if watermark > o.currentWatermark {
		o.currentWatermark = watermark
	}
	var out []Result
	for ks, st := range o.state {
		for w, accs := range st.windows {
			if w.End <= watermark {
				out = append(out, o.buildResult(st.keyVals, w, accs))
				delete(st.windows, w)
			}
		}
		if len(st.windows) == 0 {
			delete(o.state, ks)
		}
	}
	o.sweepTTL()
	sortResults(out)
	return out
}

// sweepTTL drops windows abandoned far behind the max event time, bounding memory
// for high-churn keys and long-idle sessions. In normal operation the watermark
// fires windows first, so this only catches pathological/very-late state.
func (o *Operator) sweepTTL() {
	if o.stateTtlMs <= 0 || !o.hasEvent {
		return
	}
	cutoff := o.maxEventTime - o.stateTtlMs
	for ks, st := range o.state {
		for w := range st.windows {
			if w.End <= cutoff {
				delete(st.windows, w)
				o.ttlEvicted++
			}
		}
		if len(st.windows) == 0 {
			delete(o.state, ks)
		}
	}
}

// Flush closes and returns ALL remaining windows regardless of watermark (the
// end-of-input path for bounded sources), clearing state.
func (o *Operator) Flush() []Result {
	var out []Result
	for _, st := range o.state {
		for w, accs := range st.windows {
			out = append(out, o.buildResult(st.keyVals, w, accs))
		}
	}
	o.state = make(map[string]*keyState)
	sortResults(out)
	return out
}

func (o *Operator) buildResult(keyVals []interface{}, w Window, accs []Accumulator) Result {
	aggs := make(map[string]interface{}, len(o.aggs))
	for i, ag := range o.aggs {
		aggs[ag.As] = accs[i].Result()
	}
	return Result{Key: cloneVals(keyVals), Window: w, Aggs: aggs}
}

// sortResults orders by window start, then window end, then key string — stable,
// deterministic output for reproducible sinks and tests.
func sortResults(rs []Result) {
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Window.Start != rs[j].Window.Start {
			return rs[i].Window.Start < rs[j].Window.Start
		}
		if rs[i].Window.End != rs[j].Window.End {
			return rs[i].Window.End < rs[j].Window.End
		}
		return keyString(rs[i].Key) < keyString(rs[j].Key)
	})
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
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprintf("%v", s)
	}
}

func cloneVals(vals []interface{}) []interface{} {
	c := make([]interface{}, len(vals))
	copy(c, vals)
	return c
}
