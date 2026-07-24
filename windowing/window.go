// Package windowing is the engine-agnostic core for event-time windowed
// aggregation: window assignment, typed accumulators, watermark tracking, and a
// keyed operator. It has NO dependency on the pipeline engine, so it is trivially
// unit-testable and can be reused by both the datapipeline `window` transform and
// (later) a first-class streaming action.
//
// All times are epoch milliseconds. Windows are half-open intervals [Start, End).
package windowing

// Kind enumerates the supported window shapes.
type Kind string

const (
	// Tumbling: fixed-size, non-overlapping windows.
	Tumbling Kind = "tumbling"
	// Sliding: fixed-size windows that advance by SlideMs (may overlap).
	Sliding Kind = "sliding"
	// Session: windows that group records within GapMs of each other (merged by
	// the operator).
	Session Kind = "session"
	// Global: a single all-time window (fires only on Flush / explicit trigger).
	Global Kind = "global"
)

// globalWindowEnd is the End of the single Global window — far enough in the
// future that no watermark closes it before Flush.
const globalWindowEnd int64 = 1 << 62

// Spec configures window assignment. Times are epoch milliseconds.
type Spec struct {
	Kind    Kind
	SizeMs  int64 // tumbling/sliding window width
	SlideMs int64 // sliding step; defaults to SizeMs (→ tumbling) when <= 0
	GapMs   int64 // session inactivity gap
}

// Window is a half-open interval [Start, End) in event-time millis.
type Window struct {
	Start int64
	End   int64
}

// assign returns the window(s) a record at event time ts belongs to.
//
// For Session windows this returns a provisional single window [ts, ts+GapMs);
// the operator merges overlapping session windows per key.
func (s Spec) assign(ts int64) []Window {
	switch s.Kind {
	case Sliding:
		size := s.SizeMs
		slide := s.SlideMs
		if slide <= 0 {
			slide = size
		}
		// Windows whose start is a multiple of slide and that contain ts:
		// start <= ts < start+size, i.e. start in (ts-size, ts].
		floorStart := floorDiv(ts, slide) * slide
		var ws []Window
		for start := floorStart; start > ts-size; start -= slide {
			ws = append(ws, Window{Start: start, End: start + size})
		}
		return ws
	case Session:
		return []Window{{Start: ts, End: ts + s.GapMs}}
	case Global:
		return []Window{{Start: 0, End: globalWindowEnd}}
	default: // Tumbling
		start := floorDiv(ts, s.SizeMs) * s.SizeMs
		return []Window{{Start: start, End: start + s.SizeMs}}
	}
}

// floorDiv is integer division that floors toward negative infinity, so window
// alignment is correct for negative event times (Go's / truncates toward zero).
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
