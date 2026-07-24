// Package window registers a stateful, event-time windowed aggregation transform.
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
//
// Unlike the stateless row transforms, this stage buffers keyed window state and
// emits one output row per (key, closed window). It implements WindowedOperator so
// the pipeline engine can Flush() remaining open windows at end-of-input (a bounded
// source) — the same operator also fires windows incrementally as the event-time
// watermark advances (a streaming source).
//
// Config (see docs/stream-processing.md §9):
//
//	{
//	  "type": "window",
//	  "eventTime": { "column": "ts", "format": "RFC3339" },   // omit → processing time
//	  "watermark": { "strategy": "boundedOutOfOrderness", "maxLatenessMs": 5000 },
//	  "window":    { "kind": "tumbling", "sizeMs": 60000 },    // sliding{sizeMs,slideMs} | global
//	  "groupBy":   ["sensor_id"],
//	  "aggregations": [ { "as": "avg_temp", "fn": "avg", "column": "temperature" } ],
//	  "emit":      { "windowStartAs": "window_start", "windowEndAs": "window_end" },
//	  "onLate":    "drop"                                      // drop (default)
//	}
package window

import (
	"fmt"
	"strconv"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/windowing"
)

// WindowedOperator is implemented by the window transform. The pipeline engine
// locates it in the transform chain and calls Flush() after the source is
// exhausted, writing the remaining open windows to the sink.
type WindowedOperator interface {
	// Flush emits every window still open (end-of-input). Returns nil when there
	// are no open windows.
	Flush() (*record.RecordBatch, error)
	// LateCount reports how many records arrived after their window had closed.
	LateCount() int64
	// Stats returns window-operator counters for observability.
	Stats() Stats
	// Snapshot/Restore persist and restore window state (durable streaming resume).
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

func init() {
	transform.Register("window", newTransform)
}

type windowTransform struct {
	op          *windowing.Operator
	groupBy     []string
	aggs        []windowing.AggSpec
	eventCol    string    // "" → processing time
	layout      string    // Go time layout for string event-time columns
	epochMillis bool      // event-time column holds epoch-millis (numeric or numeric-string)
	onLate      string    // drop (default) | route
	lateSink    sink.Sink // non-nil when onLate=route

	outCols        []record.Column
	lateCount      int64
	windowsEmitted int64
}

func newTransform(cfg map[string]interface{}) (transform.Transform, error) {
	// --- window spec ---
	wm := asMap(cfg["window"])
	if wm == nil {
		return nil, fmt.Errorf("window transform: 'window' config is required")
	}
	spec := windowing.Spec{
		Kind:    windowing.Kind(asString(wm["kind"])),
		SizeMs:  asInt64(wm["sizeMs"]),
		SlideMs: asInt64(wm["slideMs"]),
		GapMs:   asInt64(wm["gapMs"]),
	}
	switch spec.Kind {
	case windowing.Tumbling, windowing.Sliding:
		if spec.SizeMs <= 0 {
			return nil, fmt.Errorf("window transform: window.sizeMs must be > 0 for %s", spec.Kind)
		}
	case windowing.Session:
		if spec.GapMs <= 0 {
			return nil, fmt.Errorf("window transform: window.gapMs must be > 0 for session windows")
		}
	case windowing.Global:
		// no size needed
	default:
		return nil, fmt.Errorf("window transform: unknown window.kind %q (want tumbling|sliding|session|global)", spec.Kind)
	}

	// --- aggregations ---
	aggsRaw := asSlice(cfg["aggregations"])
	if len(aggsRaw) == 0 {
		return nil, fmt.Errorf("window transform: at least one aggregation is required")
	}
	aggs := make([]windowing.AggSpec, 0, len(aggsRaw))
	for i, a := range aggsRaw {
		am := asMap(a)
		as, fn, col := asString(am["as"]), asString(am["fn"]), asString(am["column"])
		if as == "" {
			return nil, fmt.Errorf("window transform: aggregations[%d].as is required", i)
		}
		if fn == "" {
			return nil, fmt.Errorf("window transform: aggregations[%d].fn is required", i)
		}
		if fn != "count" && col == "" {
			return nil, fmt.Errorf("window transform: aggregations[%d].column is required for fn %q", i, fn)
		}
		aggs = append(aggs, windowing.AggSpec{As: as, Fn: fn, Column: col})
	}

	// --- watermark ---
	var maxLatenessMs int64
	if wmk := asMap(cfg["watermark"]); wmk != nil {
		maxLatenessMs = asInt64(wmk["maxLatenessMs"])
	}

	op, err := windowing.New(spec, aggs, maxLatenessMs)
	if err != nil {
		return nil, fmt.Errorf("window transform: %w", err)
	}
	if ttl := asInt64(cfg["stateTtlMs"]); ttl > 0 {
		op.SetStateTTL(ttl)
	}
	// trigger (early firing) + accumulation mode
	accumulating := asString(cfg["accumulation"]) == "accumulating"
	var earlyMs int64
	if tr := asMap(cfg["trigger"]); tr != nil {
		earlyMs = asInt64(tr["earlyEveryMs"])
	}
	if earlyMs > 0 || accumulating {
		op.SetTrigger(earlyMs, accumulating)
	}

	// spillable state: bound heap for high-cardinality key spaces by keeping only
	// maxResidentKeys keys in memory and paging colder keys to disk under spillDir
	// (default OS temp dir). Omit or set maxResidentKeys<=0 to keep all state in
	// memory (the default).
	if stcfg := asMap(cfg["state"]); stcfg != nil {
		if maxKeys := asInt64(stcfg["maxResidentKeys"]); maxKeys > 0 {
			if err := op.UseSpillableState(asString(stcfg["spillDir"]), int(maxKeys)); err != nil {
				return nil, fmt.Errorf("window transform: spillable state: %w", err)
			}
		}
	}

	// --- groupBy ---
	groupBy := asStringSlice(cfg["groupBy"])

	// --- event time ---
	var eventCol, layout string
	var epochMillis bool
	if et := asMap(cfg["eventTime"]); et != nil {
		eventCol = asString(et["column"])
		f := asString(et["format"])
		if f == "epochMillis" || f == "epoch" {
			epochMillis = true
		} else {
			layout = goLayout(f)
		}
	}
	if eventCol == "" {
		corelog.RootLogger().Warnf("window transform: no eventTime.column configured — using processing time; late/out-of-order data will be mis-bucketed")
	}

	// --- emit column names ---
	wStart, wEnd := "window_start", "window_end"
	if em := asMap(cfg["emit"]); em != nil {
		if s := asString(em["windowStartAs"]); s != "" {
			wStart = s
		}
		if s := asString(em["windowEndAs"]); s != "" {
			wEnd = s
		}
	}

	onLate := asString(cfg["onLate"])
	if onLate == "" {
		onLate = "drop"
	}
	var lateSink sink.Sink
	if onLate == "route" {
		st := asString(cfg["errorSinkType"])
		if st == "" {
			st = "null"
		}
		ls, err := sink.New(st, asMap(cfg["errorSinkConfig"]))
		if err != nil {
			return nil, fmt.Errorf("window transform: open late-route sink %q: %w", st, err)
		}
		lateSink = ls
	}

	// --- precompute the (static) output schema ---
	outCols := make([]record.Column, 0, len(groupBy)+2+len(aggs))
	for _, g := range groupBy {
		outCols = append(outCols, record.Column{Name: g, Type: record.TypeAny})
	}
	outCols = append(outCols,
		record.Column{Name: wStart, Type: record.TypeInt},
		record.Column{Name: wEnd, Type: record.TypeInt},
	)
	for _, ag := range aggs {
		outCols = append(outCols, record.Column{Name: ag.As, Type: aggColType(ag.Fn)})
	}

	return &windowTransform{
		op: op, groupBy: groupBy, aggs: aggs,
		eventCol: eventCol, layout: layout, epochMillis: epochMillis, onLate: onLate,
		lateSink: lateSink, outCols: outCols,
	}, nil
}

func (w *windowTransform) Name() string { return "window" }

// Apply folds every input row into its windows and emits any windows that the
// advancing watermark has closed. It returns a NON-nil (possibly empty) batch so
// the engine does not mis-count buffered rows as "dropped" — buffered rows are
// held in window state and emitted on a later batch or at Flush.
func (w *windowTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	var lateBatch *record.RecordBatch // lazily built when onLate=route
	for i := range in.Rows {
		row := in.RowMap(i)
		ts, err := w.eventMillis(row)
		if err != nil {
			w.lateCount++ // unparseable/absent event time: treat as unusable
			lateBatch = w.collectLate(lateBatch, in, i)
			continue
		}
		keyVals := make([]interface{}, len(w.groupBy))
		for j, g := range w.groupBy {
			keyVals[j] = row[g]
		}
		if late := w.op.Add(keyVals, ts, row); late {
			w.lateCount++ // the row's window had already closed
			lateBatch = w.collectLate(lateBatch, in, i)
		}
	}
	if lateBatch != nil && lateBatch.Len() > 0 {
		err := w.lateSink.WriteBatch(lateBatch)
		record.Put(lateBatch)
		if err != nil {
			return nil, fmt.Errorf("window: route late rows: %w", err)
		}
	}
	results := w.op.Emit(w.op.Watermark())
	return w.toBatch(results, true), nil
}

// collectLate appends input row i to a late-route batch (nil-safe, lazily
// created). Returns b unchanged when no late sink is configured (onLate != route).
func (w *windowTransform) collectLate(b *record.RecordBatch, in *record.RecordBatch, i int) *record.RecordBatch {
	if w.lateSink == nil {
		return b
	}
	if b == nil {
		b = record.Get()
		b.Columns = in.Columns
		b.Rows = b.Rows[:0]
	}
	b.Rows = append(b.Rows, in.Rows[i])
	return b
}

// Flush emits all remaining open windows (end-of-input).
func (w *windowTransform) Flush() (*record.RecordBatch, error) {
	return w.toBatch(w.op.Flush(), false), nil
}

// LateCount returns the number of records dropped for arriving after their window closed.
func (w *windowTransform) LateCount() int64 { return w.lateCount }

// Stats is a snapshot of window-operator counters for observability.
type Stats struct {
	WindowsEmitted  int64
	LateDropped     int64
	StateEvicted    int64
	OpenWindows     int64
	WatermarkMillis int64
	SpillFailures   int64
}

// Stats returns the current window-operator counters.
func (w *windowTransform) Stats() Stats {
	return Stats{
		WindowsEmitted:  w.windowsEmitted,
		LateDropped:     w.lateCount,
		StateEvicted:    w.op.EvictedCount(),
		OpenWindows:     int64(w.op.OpenWindows()),
		WatermarkMillis: w.op.Watermark(),
		SpillFailures:   w.op.SpillFailures(),
	}
}

// Snapshot serialises the window operator's keyed state for durable streaming resume.
func (w *windowTransform) Snapshot() ([]byte, error) { return w.op.Snapshot() }

// Restore restores window operator state from a Snapshot (same config assumed).
func (w *windowTransform) Restore(data []byte) error { return w.op.Restore(data) }

func (w *windowTransform) Close() error {
	var err error
	if w.lateSink != nil {
		_ = w.lateSink.Flush()
		err = w.lateSink.Close()
	}
	if cerr := w.op.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// toBatch materialises window results into a RecordBatch with the precomputed
// schema. When emptyNonNil is true and there are no results, it returns an empty
// (len 0) but non-nil batch so the engine treats the input as buffered, not dropped.
func (w *windowTransform) toBatch(results []windowing.Result, emptyNonNil bool) *record.RecordBatch {
	if len(results) == 0 {
		if !emptyNonNil {
			return nil
		}
		b := record.Get()
		b.Columns = w.outCols
		b.Rows = b.Rows[:0]
		return b
	}
	w.windowsEmitted += int64(len(results))
	b := record.Get()
	b.Columns = w.outCols
	rows := make([][]interface{}, 0, len(results))
	for _, r := range results {
		row := make([]interface{}, 0, len(w.outCols))
		row = append(row, r.Key...)
		row = append(row, r.Window.Start, r.Window.End)
		for _, ag := range w.aggs {
			row = append(row, r.Aggs[ag.As])
		}
		rows = append(rows, row)
	}
	b.Rows = rows
	return b
}

// eventMillis extracts the event time (epoch millis) for a row. With no eventTime
// column it falls back to processing time (wall clock).
func (w *windowTransform) eventMillis(row map[string]interface{}) (int64, error) {
	if w.eventCol == "" {
		return time.Now().UnixMilli(), nil
	}
	switch v := row[w.eventCol].(type) {
	case nil:
		return 0, fmt.Errorf("eventTime column %q is null", w.eventCol)
	case time.Time:
		return v.UnixMilli(), nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		if w.epochMillis {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse eventTime %q as epoch millis: %w", v, err)
			}
			return ms, nil
		}
		t, err := time.Parse(w.layout, v)
		if err != nil {
			return 0, fmt.Errorf("parse eventTime %q with layout %q: %w", v, w.layout, err)
		}
		return t.UnixMilli(), nil
	default:
		return 0, fmt.Errorf("unsupported eventTime type %T for column %q", v, w.eventCol)
	}
}

// aggColType maps an aggregate function to its output column type.
func aggColType(fn string) record.ColumnType {
	switch fn {
	case "count":
		return record.TypeInt
	case "sum", "avg", "min", "max":
		return record.TypeFloat
	default: // first, last
		return record.TypeAny
	}
}

// goLayout maps a friendly format name to a Go reference-time layout. An unknown
// value is returned as-is (assumed to already be a Go layout). Empty defaults to
// RFC3339.
func goLayout(format string) string {
	switch format {
	case "", "RFC3339":
		return time.RFC3339
	case "RFC3339Nano":
		return time.RFC3339Nano
	case "DateTime":
		return time.DateTime // "2006-01-02 15:04:05"
	case "DateOnly":
		return time.DateOnly // "2006-01-02"
	default:
		return format
	}
}
