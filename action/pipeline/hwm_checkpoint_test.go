package pipeline

import (
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// TestHWM_NotAdvancedByMidRunCheckpoint guards against data loss on resume:
// a resumed run re-queries the source with the stored watermark AND skips the
// checkpointed row count, so a watermark advanced at a mid-run checkpoint
// would skip rows that were never processed. Only a clean run may advance it.
func TestHWM_NotAdvancedByMidRunCheckpoint(t *testing.T) {
	ckptDir, wmDir := t.TempDir(), t.TempDir()
	def := &Definition{
		Source: SourceDef{Type: "flaky", Options: map[string]interface{}{"rows": 3, "errorAfter": 2}},
		Incremental: IncrementalDef{
			Mode: "hwm", Column: "sensor", ValueType: "string",
			StoreType: "file", StoreOptions: map[string]interface{}{"path": wmDir},
		},
		Transforms: []TransformDef{
			{Type: "hwm", Extra: map[string]interface{}{"type": "hwm", "column": "sensor", "valueType": "string"}},
		},
		Sink:        jsonlSink(filepath.Join(t.TempDir(), "out.jsonl")),
		Checkpoint:  CheckpointDef{Type: "file", Path: ckptDir, Key: "hwm-ckpt", IntervalRows: 1},
		Performance: PerformanceDef{BatchSize: 1},
	}
	if _, err := runInline(t, def); err == nil {
		t.Fatal("run should fail (simulated crash after 2 rows)")
	}

	ckpt, err := openCheckpoint(def)
	if err != nil {
		t.Fatalf("openCheckpoint: %v", err)
	}
	defer ckpt.Close()
	tok, err := ckpt.Load("hwm-ckpt")
	if err != nil || tok == nil || tok.RowsProcessed == 0 {
		t.Fatalf("expected a mid-run checkpoint, got tok=%+v err=%v", tok, err)
	}

	store, err := watermark.New("file", map[string]interface{}{"path": wmDir})
	if err != nil {
		t.Fatalf("watermark.New: %v", err)
	}
	defer store.Close()
	mark, err := store.Load("sdk://test")
	if err != nil {
		t.Fatalf("watermark load: %v", err)
	}
	if mark != nil {
		t.Fatalf("watermark advanced by a failed run (mark=%q) — a resume would skip unprocessed rows", mark.Value)
	}
}
