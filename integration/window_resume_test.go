package integration

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/windowing"

	_ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/file"
)

// TestWindowStateSurvivesCheckpointRestart proves the durable-state seam end to
// end: an operator's open-window state is snapshotted into a checkpoint Token,
// persisted through the real file backend, then loaded and restored into a fresh
// operator that resumes the SAME open window — the final aggregate must include
// both the pre-restart and post-restart events, with no double-count or loss.
func TestWindowStateSurvivesCheckpointRestart(t *testing.T) {
	dir := t.TempDir()
	ckpt, err := checkpoint.New("file", map[string]interface{}{"path": dir})
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer ckpt.Close()

	spec := windowing.Spec{Kind: windowing.Tumbling, SizeMs: 1000}
	aggs := []windowing.AggSpec{
		{As: "total", Fn: "sum", Column: "v"},
		{As: "n", Fn: "count"},
	}

	// --- run 1: buffer two events into the open window [0,1000), then checkpoint
	// (window state + source position) and "crash". ---
	op1, err := windowing.New(spec, aggs, 0)
	if err != nil {
		t.Fatal(err)
	}
	op1.Add([]interface{}{"A"}, 100, map[string]interface{}{"v": 10})
	op1.Add([]interface{}{"A"}, 200, map[string]interface{}{"v": 20})
	snap, err := op1.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := ckpt.Save("run-1", &checkpoint.Token{
		Format:        "kafka",
		RowsProcessed: 2,
		WindowState:   snap,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// --- run 2: a brand-new process loads the checkpoint from disk and resumes. ---
	loaded, err := ckpt.Load("run-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded == nil || len(loaded.WindowState) == 0 {
		t.Fatalf("checkpoint did not carry window state: %+v", loaded)
	}
	op2, err := windowing.New(spec, aggs, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := op2.Restore(loaded.WindowState); err != nil {
		t.Fatalf("restore: %v", err)
	}
	op2.Add([]interface{}{"A"}, 300, map[string]interface{}{"v": 5}) // same window [0,1000)

	got := op2.Emit(1000) // advance watermark past window end → fire [0,1000)
	if len(got) != 1 {
		t.Fatalf("want 1 fired window after resume, got %d: %+v", len(got), got)
	}
	if got[0].Aggs["total"] != float64(35) || got[0].Aggs["n"] != int64(3) {
		t.Errorf("resumed window aggregate wrong (double-count or loss): %+v", got[0].Aggs)
	}
}
