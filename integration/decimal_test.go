package integration

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	// csv source + null sink are blank-imported in pipeline_test.go; add jsonlines.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
)

// TestPipeline_DecimalRoundTrip proves that TypeDecimal values survive a full
// CSV → JSONL pass with EXACT precision — including values float64 cannot
// represent — so monetary/NUMERIC data no longer silently drifts through the
// pipeline.
func TestPipeline_DecimalRoundTrip(t *testing.T) {
	// 12345678901234567.89 has 19 significant digits; float64 carries ~15–17 and
	// would round it to 12345678901234568. 0.1 and 100.00 (trailing-zero scale)
	// are the everyday cases.
	csv := "id,price\n1,12345678901234567.89\n2,0.1\n3,100.00\n"
	in, cleanup := writeTempCSV(t, csv)
	defer cleanup()

	out := in + ".out.jsonl"
	defer os.Remove(out)

	src, err := source.New("csv", map[string]interface{}{
		"path":      in,
		"hasHeader": true,
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "price", "type": "decimal"},
		},
	})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	snk, err := sink.New("jsonlines", map[string]interface{}{"path": out})
	if err != nil {
		t.Fatalf("sink.New(jsonlines): %v", err)
	}

	for {
		b, rerr := src.ReadBatch(10)
		if b != nil && b.Len() > 0 {
			if werr := snk.WriteBatch(b); werr != nil {
				t.Fatalf("WriteBatch: %v", werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("ReadBatch: %v", rerr)
		}
	}
	if err := snk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	got := string(data)
	for _, want := range []string{"12345678901234567.89", "0.1", "100.00"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing exact decimal %q; got:\n%s", want, got)
		}
	}
	// The float64 rendering of the high-precision value must NOT appear — that
	// would mean the value was truncated through float64 somewhere in the path.
	if strings.Contains(got, "12345678901234568") {
		t.Errorf("high-precision decimal was truncated through float64:\n%s", got)
	}
}
