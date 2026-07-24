package integration

import (
	"os"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"github.com/mpandav-tibco/flogo-datapipeline/transform/window"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
)

// Row 2 (event time 10:00) arrives after row 1 (10:05) advanced the watermark, so
// its window [10:00,10:01) has already closed → it is late and must be routed.
const lateCSV = `sensor,ts,v
A,2026-07-24T10:05:00Z,1
A,2026-07-24T10:00:00Z,2
A,2026-07-24T10:05:30Z,3
`

func TestPipeline_Window_OnLateRoute(t *testing.T) {
	path, cleanup := writeTempCSV(t, lateCSV)
	defer cleanup()
	lateFile, err := os.CreateTemp("", "late_*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lateFile.Close()
	defer os.Remove(lateFile.Name())

	src, err := source.New("csv", map[string]interface{}{"path": path, "hasHeader": true})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	p, err := transform.Build([]map[string]interface{}{{
		"type":            "window",
		"eventTime":       map[string]interface{}{"column": "ts", "format": "RFC3339"},
		"window":          map[string]interface{}{"kind": "tumbling", "sizeMs": float64(60000)},
		"groupBy":         []interface{}{"sensor"},
		"aggregations":    []interface{}{map[string]interface{}{"as": "n", "fn": "count"}},
		"onLate":          "route",
		"errorSinkType":   "jsonlines",
		"errorSinkConfig": map[string]interface{}{"path": lateFile.Name()},
	}})
	if err != nil {
		t.Fatalf("transform.Build(window): %v", err)
	}

	// One row per batch so the watermark advances between rows (row 2 is late).
	for {
		b, rErr := src.ReadBatch(1)
		if b != nil && b.Len() > 0 {
			if _, aErr := p.Apply(b); aErr != nil {
				t.Fatalf("Apply: %v", aErr)
			}
			record.Put(b)
		}
		if rErr != nil {
			break
		}
	}
	winOp := p.Stages()[0].(window.WindowedOperator)
	if _, err := winOp.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := p.Close(); err != nil { // flush + close the route sink
		t.Fatalf("Close: %v", err)
	}

	if winOp.LateCount() != 1 {
		t.Errorf("want LateCount 1, got %d", winOp.LateCount())
	}
	if winOp.Stats().LateDropped != 1 {
		t.Errorf("want stats.LateDropped 1, got %d", winOp.Stats().LateDropped)
	}
	data, _ := os.ReadFile(lateFile.Name())
	if !strings.Contains(string(data), "2026-07-24T10:00:00Z") {
		t.Errorf("late row not routed to error sink; content: %q", string(data))
	}
}
