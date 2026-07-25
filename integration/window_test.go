package integration

import (
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/window"

	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

// windowCSV: sensor readings across three one-minute event-time windows.
//
//	10:00 window: A=20,22 (sum 42, n 2) · B=15 (sum 15, n 1)
//	10:01 window: A=25       · B=17
//	10:02 window: A=30 (emitted only at Flush — its window is still open when the
//	              stream ends, since watermark = max event time = 10:02:03)
const windowCSV = `sensor,ts,temperature
A,2026-07-24T10:00:01Z,20
A,2026-07-24T10:00:30Z,22
B,2026-07-24T10:00:10Z,15
A,2026-07-24T10:01:05Z,25
B,2026-07-24T10:01:20Z,17
A,2026-07-24T10:02:03Z,30
`

// TestPipeline_CSV_Window_EventTime drives a real CSV source through the window
// transform and asserts event-time tumbling aggregation, including the EOF Flush
// of the still-open final window.
func TestPipeline_CSV_Window_EventTime(t *testing.T) {
	path, cleanup := writeTempCSV(t, windowCSV)
	defer cleanup()

	src, err := source.New("csv", map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	p, err := transform.Build([]map[string]interface{}{
		{
			"type":      "window",
			"eventTime": map[string]interface{}{"column": "ts", "format": "RFC3339"},
			"window":    map[string]interface{}{"kind": "tumbling", "sizeMs": float64(60000)},
			"groupBy":   []interface{}{"sensor"},
			"aggregations": []interface{}{
				map[string]interface{}{"as": "total", "fn": "sum", "column": "temperature"},
				map[string]interface{}{"as": "avg_t", "fn": "avg", "column": "temperature"},
				map[string]interface{}{"as": "n", "fn": "count"},
			},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build(window): %v", err)
	}
	defer p.Close()

	var got []string // "sensor|total|n" tuples
	collect := func(b *record.RecordBatch) {
		if b == nil {
			return
		}
		for i := range b.Rows {
			m := b.RowMap(i)
			total, _ := toF(m["total"])
			n, _ := toI(m["n"])
			got = append(got, fmt.Sprintf("%v|%.0f|%d", m["sensor"], total, n))
		}
	}

	// Stream batches through the transform (windows fire as the watermark advances).
	for {
		batch, rErr := src.ReadBatch(1000)
		if batch != nil && batch.Len() > 0 {
			res, aErr := p.Apply(batch)
			if aErr != nil {
				t.Fatalf("window Apply: %v", aErr)
			}
			collect(res)
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			t.Fatalf("ReadBatch: %v", rErr)
		}
	}

	// EOF: flush the still-open final window via the WindowedOperator contract the
	// engine relies on.
	winOp, ok := p.Stages()[0].(window.WindowedOperator)
	if !ok {
		t.Fatalf("stage 0 does not implement window.WindowedOperator")
	}
	flushed, fErr := winOp.Flush()
	if fErr != nil {
		t.Fatalf("window Flush: %v", fErr)
	}
	collect(flushed)

	want := []string{"A|42|2", "A|25|1", "A|30|1", "B|15|1", "B|17|1"}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("want %d window rows, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: want %q, got %q (all: %v)", i, want[i], got[i], got)
		}
	}
	if winOp.LateCount() != 0 {
		t.Errorf("unexpected late count: %d", winOp.LateCount())
	}
}

func toF(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

func toI(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}
