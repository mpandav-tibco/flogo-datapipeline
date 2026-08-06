package pipeline

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	_ "github.com/mpandav-tibco/flogo-datapipeline/checkpoint/file"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/window"
)

// flakySource emits one row per ReadBatch call (so a small dataset spans
// MULTIPLE batches/checkpoint barriers instead of arriving as one EOF-bundled
// batch) from a fixed set of event-time rows, and fails with a hard
// (non-io.EOF) error after emitting `errorAfter` rows — used to deterministically
// simulate a mid-stream crash at an exact row count, without timing races.
type flakySource struct {
	total      int
	errorAfter int
	pos        int
}

func (f *flakySource) Schema() []record.Column {
	return []record.Column{{Name: "ts", Type: record.TypeString}, {Name: "sensor", Type: record.TypeString}}
}
func (f *flakySource) ReadBatch(size int) (*record.RecordBatch, error) {
	if f.pos >= f.errorAfter {
		return nil, fmt.Errorf("flakySource: simulated crash after %d rows", f.errorAfter)
	}
	if f.pos >= f.total {
		return nil, io.EOF
	}
	ts := time.Date(2020, 1, 1, 0, 0, f.pos*10, 0, time.UTC).Format(time.RFC3339)
	b := record.Get()
	b.Columns = f.Schema()
	b.Rows = append(b.Rows, []interface{}{ts, "A"})
	f.pos++
	if f.pos >= f.total {
		return b, io.EOF
	}
	return b, nil
}
func (f *flakySource) Skip(n int64) error { f.pos += int(n); return nil }
func (f *flakySource) Close() error       { return nil }

func init() {
	source.Register("flaky", func(cfg map[string]interface{}) (source.Source, error) {
		total, _ := cfg["rows"].(int)
		errorAfter, _ := cfg["errorAfter"].(int)
		if errorAfter <= 0 {
			errorAfter = total + 1 // never errors
		}
		return &flakySource{total: total, errorAfter: errorAfter}, nil
	})
}

func windowDef(srcCfg map[string]interface{}, ckptPath, outPath string) *Definition {
	return &Definition{
		Source: SourceDef{Type: "flaky", Options: srcCfg},
		Transforms: []TransformDef{
			{Type: "window", Extra: map[string]interface{}{
				"type":         "window",
				"eventTime":    map[string]interface{}{"column": "ts", "format": "RFC3339"},
				"window":       map[string]interface{}{"kind": "tumbling", "sizeMs": float64(600000)}, // wide enough to keep all rows in ONE window
				"groupBy":      []interface{}{"sensor"},
				"aggregations": []interface{}{map[string]interface{}{"as": "n", "fn": "count"}},
			}},
		},
		Sink:        jsonlSink(outPath),
		Checkpoint:  CheckpointDef{Type: "file", Path: ckptPath, Key: "win-resume-key", IntervalRows: 1},
		Performance: PerformanceDef{BatchSize: 1},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "flaky"},
				{ID: "t0", Kind: "transform", Type: "window"},
				{ID: "sink", Kind: "sink", Type: "jsonlines"},
				{ID: "sink2", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": outPath + ".2"}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "t0:in"},
				{From: "t0:out", To: "sink:in"},
				{From: "t0:out", To: "sink2:in"}, // fan-out makes the graph non-reducible -> graph runner
			},
		},
	}
}

// TestGraphRun_StatefulCheckpoint_ResumesWindowState proves a graph-executed
// windowed pipeline can now checkpoint/resume: a simulated mid-stream crash
// after 2 of 3 rows persists a NON-EMPTY, partial window-state snapshot; a
// second run resuming from it (restoring state + reading only the remaining
// row) produces the SAME final aggregate as an uninterrupted run — proving the
// restored state was genuinely combined with the newly-read row, not discarded
// or double-counted.
func TestGraphRun_StatefulCheckpoint_ResumesWindowState(t *testing.T) {
	ckptDir := t.TempDir()
	outPath := filepath.Join(t.TempDir(), "agg.jsonl")

	// Run 1: crashes after 2 of 3 rows (hard error, not EOF).
	def1 := windowDef(map[string]interface{}{"rows": 3, "errorAfter": 2}, ckptDir, outPath)
	def1.Defaults()
	act1, err := NewInline(def1, "sdk://test")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	_, err1 := act1.RunDirect(t.Context(), &Input{RunID: "test-run"})
	if err1 == nil {
		t.Fatal("run 1 should have failed (simulated crash), got nil error")
	}

	ckpt, err := openCheckpoint(def1)
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer ckpt.Close()
	tok, err := ckpt.Load("win-resume-key")
	if err != nil {
		t.Fatalf("checkpoint Load: %v", err)
	}
	if tok == nil {
		t.Fatal("expected a checkpoint token to be saved before the simulated crash")
	}
	if tok.RowsProcessed != 2 {
		t.Fatalf("checkpoint RowsProcessed = %d, want 2", tok.RowsProcessed)
	}
	if len(tok.WindowState) == 0 {
		t.Fatal("checkpoint WindowState is empty — the partial window aggregate was not persisted")
	}

	// Run 2: does NOT crash (errorAfter effectively disabled); resumes from the
	// saved checkpoint (Skip(2) + restored window state) and reads only the 1
	// remaining row.
	def2 := windowDef(map[string]interface{}{"rows": 3, "errorAfter": 0}, ckptDir, outPath)
	def2.Defaults()
	act2, err := NewInline(def2, "sdk://test")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	out2, err2 := act2.RunDirect(t.Context(), &Input{RunID: "test-run"})
	if err2 != nil {
		t.Fatalf("run 2 (resume): %v", err2)
	}
	if out2.Status != "completed" {
		t.Fatalf("run 2 status = %q, want completed (err=%s)", out2.Status, out2.Error)
	}

	rows := readJSONL(t, outPath)
	if len(rows) != 1 {
		t.Fatalf("aggregate rows = %d, want 1: %v", len(rows), rows)
	}
	// count fields decode as float64 via JSON.
	n, _ := rows[0]["n"].(float64)
	if n != 3 {
		t.Fatalf("resumed aggregate count = %v, want 3 (2 restored + 1 newly read) — got full row: %v", n, rows[0])
	}
}
