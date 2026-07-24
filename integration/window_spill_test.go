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

// TestPipeline_CSV_Window_Spill runs the same event-time windowed aggregation as
// TestPipeline_CSV_Window_EventTime but forces the spillable state backend
// (maxResidentKeys=1), proving the config wiring enables spill and that spilling
// yields identical results to the in-memory path.
func TestPipeline_CSV_Window_Spill(t *testing.T) {
	path, cleanup := writeTempCSV(t, windowCSV)
	defer cleanup()

	src, err := source.New("csv", map[string]interface{}{"path": path, "hasHeader": true})
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
			"state":     map[string]interface{}{"maxResidentKeys": float64(1), "spillDir": t.TempDir()},
			"aggregations": []interface{}{
				map[string]interface{}{"as": "total", "fn": "sum", "column": "temperature"},
				map[string]interface{}{"as": "n", "fn": "count"},
			},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build(window+spill): %v", err)
	}
	defer p.Close()

	var got []string
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
}
