package pipeline

import (
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// TestRunDirect_StageStats verifies the single-path run publishes per-stage row
// counters into Output.Metadata["stageStats"] for the Studio graph overlay.
func TestRunDirect_StageStats(t *testing.T) {
	src := writeTemp(t, "in.csv", ordersCSV)
	outPath := filepath.Join(t.TempDir(), "out.jsonl")

	def := &Definition{
		Source: csvSource(src),
		Transforms: []TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
			{Type: "mapper", Extra: map[string]interface{}{
				"type":       "mapper",
				"fields":     map[string]interface{}{"customerId": "=$id"},
				"fieldOrder": []interface{}{"customerId"},
			}},
		},
		Sink: jsonlSink(outPath),
	}

	out, err := runInline(t, def)
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("status = %q, want completed (err=%s)", out.Status, out.Error)
	}

	raw, ok := out.Metadata["stageStats"]
	if !ok {
		t.Fatalf("stageStats missing from Output.Metadata (keys: %v)", out.Metadata)
	}
	stats, ok := raw.([]transform.StageStat)
	if !ok {
		t.Fatalf("stageStats type = %T, want []transform.StageStat", raw)
	}
	if len(stats) != 2 {
		t.Fatalf("want 2 stage stats, got %d: %+v", len(stats), stats)
	}
	// 5 rows read; filter (amount>0) keeps 3 (Alice/Carol/Eve), drops Bob/Dave.
	if stats[0].Name != "filter" || stats[0].RowsIn != 5 || stats[0].RowsOut != 3 {
		t.Errorf("filter stage = %+v; want {filter 5 3}", stats[0])
	}
	// mapper is a projection: 3 in, 3 out.
	if stats[1].RowsIn != 3 || stats[1].RowsOut != 3 {
		t.Errorf("mapper stage = %+v; want in 3 out 3", stats[1])
	}
}
