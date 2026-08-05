package acmeplugins

import (
	"io"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// TestOutOfTreePipeline proves a source, transform, and sink defined ENTIRELY in
// this external module — using only the public extension contract — register into
// the engine's registries and interoperate, with no change to the engine.
func TestOutOfTreePipeline(t *testing.T) {
	src, err := source.New("rangesource", map[string]interface{}{"count": 3})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	xf, err := transform.New("uppercase", map[string]interface{}{"column": "label"})
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	snk, err := sink.New("capture", nil)
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}

	for {
		batch, err := src.ReadBatch(2)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		out, err := xf.Apply(batch)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if err := snk.WriteBatch(out); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	}
	_ = snk.Flush()
	_ = snk.Close()
	_ = src.Close()
	_ = xf.Close()

	got := snk.(*captureSink)
	if len(got.Rows) != 3 {
		t.Fatalf("captured %d rows, want 3", len(got.Rows))
	}
	if v := got.Rows[0][1]; v != "ITEM-0" {
		t.Errorf("row0 label = %v, want ITEM-0", v)
	}
	if v := got.Rows[2][1]; v != "ITEM-2" {
		t.Errorf("row2 label = %v, want ITEM-2", v)
	}
}
