package windowing

import (
	"fmt"
	"strconv"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// Accumulator incrementally aggregates values within a single window.
// Implementations are NOT safe for concurrent use; the operator serialises access
// per key.
type Accumulator interface {
	// Add folds one input value into the accumulator. nil values are ignored by
	// numeric accumulators; count counts every row.
	Add(v interface{})
	// Result returns the aggregated value (nil when no contributing value was seen).
	Result() interface{}
	// Merge folds another accumulator of the SAME concrete type into this one. Used
	// to combine session windows that overlap and merge into a single session.
	Merge(other Accumulator)
	// snap/load serialise the accumulator's internal state for durable snapshots.
	snap() accState
	load(s accState)
}

// AggSpec declares one output aggregation column.
type AggSpec struct {
	As     string // output column name
	Fn     string // sum | avg | min | max | count | first | last
	Column string // input column (ignored for count)
}

// newAccumulator returns a fresh Accumulator for the given function name.
func newAccumulator(fn string) (Accumulator, error) {
	switch fn {
	case "sum":
		return &sumAcc{}, nil
	case "avg":
		return &avgAcc{}, nil
	case "min":
		return &minAcc{}, nil
	case "max":
		return &maxAcc{}, nil
	case "count":
		return &countAcc{}, nil
	case "first":
		return &firstAcc{}, nil
	case "last":
		return &lastAcc{}, nil
	default:
		return nil, fmt.Errorf("windowing: unknown aggregate function %q (want sum|avg|min|max|count|first|last)", fn)
	}
}

// toFloat coerces numeric-ish values to float64, mirroring the filter transform's
// permissive coercion so string-typed numeric columns aggregate correctly.
func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case interface{ Float64() float64 }:
		// Exact-numeric values (e.g. record.Decimal) aggregate via their float
		// value here; the windowing core stays engine-agnostic (no record import).
		return n.Float64(), true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// decAvgScale picks the fractional-digit count for a decimal average: at least
// 10 (division rarely terminates), more if the running sum already carries more.
func decAvgScale(sum record.Decimal) int32 {
	if s := sum.Scale(); s > 10 {
		return s
	}
	return 10
}

type sumAcc struct {
	sum   float64
	dsum  record.Decimal
	isDec bool
	seen  bool
}

func (a *sumAcc) Add(v interface{}) {
	if a.isDec {
		if d, ok := record.ToDecimal(v); ok {
			a.dsum = a.dsum.Add(d)
			a.seen = true
		}
		return
	}
	if !a.seen {
		if d, ok := v.(record.Decimal); ok {
			a.dsum, a.isDec, a.seen = d, true, true
			return
		}
	}
	if f, ok := toFloat(v); ok {
		a.sum += f
		a.seen = true
	}
}
func (a *sumAcc) Result() interface{} {
	if !a.seen {
		return nil
	}
	if a.isDec {
		return a.dsum
	}
	return a.sum
}

type avgAcc struct {
	sum   float64
	dsum  record.Decimal
	isDec bool
	n     int64
}

func (a *avgAcc) Add(v interface{}) {
	if a.isDec {
		if d, ok := record.ToDecimal(v); ok {
			a.dsum = a.dsum.Add(d)
			a.n++
		}
		return
	}
	if a.n == 0 {
		if d, ok := v.(record.Decimal); ok {
			a.dsum, a.isDec, a.n = d, true, 1
			return
		}
	}
	if f, ok := toFloat(v); ok {
		a.sum += f
		a.n++
	}
}
func (a *avgAcc) Result() interface{} {
	if a.n == 0 {
		return nil
	}
	if a.isDec {
		q, _ := a.dsum.DivRound(record.NewDecimal(a.n, 0), decAvgScale(a.dsum))
		return q
	}
	return a.sum / float64(a.n)
}

type minAcc struct {
	min   float64
	dmin  record.Decimal
	isDec bool
	seen  bool
}

func (a *minAcc) Add(v interface{}) {
	if a.isDec {
		if d, ok := record.ToDecimal(v); ok && a.dmin.Cmp(d) > 0 {
			a.dmin = d
		}
		return
	}
	if !a.seen {
		if d, ok := v.(record.Decimal); ok {
			a.dmin, a.isDec, a.seen = d, true, true
			return
		}
	}
	if f, ok := toFloat(v); ok {
		if !a.seen || f < a.min {
			a.min = f
		}
		a.seen = true
	}
}
func (a *minAcc) Result() interface{} {
	if !a.seen {
		return nil
	}
	if a.isDec {
		return a.dmin
	}
	return a.min
}

type maxAcc struct {
	max   float64
	dmax  record.Decimal
	isDec bool
	seen  bool
}

func (a *maxAcc) Add(v interface{}) {
	if a.isDec {
		if d, ok := record.ToDecimal(v); ok && a.dmax.Cmp(d) < 0 {
			a.dmax = d
		}
		return
	}
	if !a.seen {
		if d, ok := v.(record.Decimal); ok {
			a.dmax, a.isDec, a.seen = d, true, true
			return
		}
	}
	if f, ok := toFloat(v); ok {
		if !a.seen || f > a.max {
			a.max = f
		}
		a.seen = true
	}
}
func (a *maxAcc) Result() interface{} {
	if !a.seen {
		return nil
	}
	if a.isDec {
		return a.dmax
	}
	return a.max
}

// countAcc counts every row added (including nil values), matching SQL COUNT(*).
type countAcc struct{ n int64 }

func (a *countAcc) Add(interface{})     { a.n++ }
func (a *countAcc) Result() interface{} { return a.n }

type firstAcc struct {
	val interface{}
	set bool
}

func (a *firstAcc) Add(v interface{}) {
	if !a.set {
		a.val = v
		a.set = true
	}
}
func (a *firstAcc) Result() interface{} { return a.val }

type lastAcc struct {
	val interface{}
	set bool
}

func (a *lastAcc) Add(v interface{}) {
	a.val = v
	a.set = true
}
func (a *lastAcc) Result() interface{} { return a.val }
