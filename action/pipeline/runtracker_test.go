package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

// endlessSource emits a few rows per poll forever, like a busy stream.
type endlessSource struct{ n int }

func init() {
	source.Register("endlesstest", func(map[string]interface{}) (source.Source, error) { return &endlessSource{}, nil })
}
func (s *endlessSource) Schema() []record.Column { return []record.Column{{Name: "v"}} }
func (s *endlessSource) ReadBatch(int) (*record.RecordBatch, error) {
	b := record.Get()
	b.Columns = s.Schema()
	for i := 0; i < 3; i++ {
		s.n++
		b.Rows = append(b.Rows, []interface{}{s.n})
	}
	time.Sleep(5 * time.Millisecond)
	return b, nil
}
func (s *endlessSource) Skip(int64) error { return nil }
func (s *endlessSource) Close() error     { return nil }

// Shutdown drains a stateless streaming run: the source stops reading,
// everything read is written, and the run ends with a final checkpoint at
// exactly the written position — a restart neither loses nor replays rows.
func TestShutdownRunsDrainsStreamingRun(t *testing.T) {
	t.Cleanup(func() {
		activeRuns = &runTracker{cancels: map[int]context.CancelFunc{}, drains: map[int]chan struct{}{}}
	})
	out := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source:     SourceDef{Type: "endlesstest", Options: map[string]interface{}{}},
		Sink:       jsonlSink(out),
		Checkpoint: CheckpointDef{Type: "file", Path: t.TempDir(), Key: "drain", IntervalRows: 1000000},
	}
	def.Defaults()
	act, err := NewInline(def, "sdk://test")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *Output, 1)
	go func() {
		o, _ := act.RunDirect(context.Background(), &Input{RunID: "stream"})
		done <- o
	}()
	time.Sleep(300 * time.Millisecond)

	inFlight, left := ShutdownRuns(5 * time.Second)
	if inFlight != 1 || left != 0 {
		t.Fatalf("ShutdownRuns = (%d in flight, %d left); want (1, 0)", inFlight, left)
	}
	var o *Output
	select {
	case o = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after ShutdownRuns")
	}
	// Not a complete run (no watermark / deleteOnSuccess), but nothing read
	// is left unwritten.
	if o.Status != "cancelled" || o.RowsProcessed == 0 || o.RowsWritten != o.RowsProcessed {
		t.Fatalf("drained run: status=%q read=%d written=%d; want cancelled with read == written", o.Status, o.RowsProcessed, o.RowsWritten)
	}
	if rows := readJSONL(t, out); int64(len(rows)) != o.RowsWritten {
		t.Fatalf("sink holds %d rows, run reported %d written", len(rows), o.RowsWritten)
	}
	ckpt, err := openCheckpoint(def)
	if err != nil {
		t.Fatal(err)
	}
	defer ckpt.Close()
	tok, err := ckpt.Load("drain")
	if err != nil || tok == nil || tok.RowsProcessed != o.RowsWritten {
		t.Fatalf("final checkpoint = %+v (err %v); want RowsProcessed = %d", tok, err, o.RowsWritten)
	}
	// New runs after shutdown start cancelled.
	ctx, finish := activeRuns.start(context.Background())
	defer finish()
	if ctx.Err() == nil {
		t.Fatal("run started after shutdown was not cancelled")
	}
}

// A run stopped by shutdown is not a complete run: an incremental watermark
// must not advance (it would skip rows the run never read).
func TestDrainedRunDoesNotAdvanceWatermark(t *testing.T) {
	t.Cleanup(func() {
		activeRuns = &runTracker{cancels: map[int]context.CancelFunc{}, drains: map[int]chan struct{}{}}
	})
	wmDir := t.TempDir()
	def := &Definition{
		Source: SourceDef{Type: "endlesstest", Options: map[string]interface{}{}},
		Sink:   jsonlSink(filepath.Join(t.TempDir(), "out.jsonl")),
		Incremental: IncrementalDef{Mode: "hwm", Column: "v", ValueType: "int",
			StoreType: "file", StoreOptions: map[string]interface{}{"path": wmDir}},
		Transforms: []TransformDef{{Type: "hwm", Extra: map[string]interface{}{"type": "hwm", "column": "v", "valueType": "int"}}},
	}
	def.Defaults()
	act, err := NewInline(def, "sdk://test")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = act.RunDirect(context.Background(), &Input{RunID: "wm"}); close(done) }()
	time.Sleep(200 * time.Millisecond)
	ShutdownRuns(5 * time.Second)
	<-done
	store, err := watermark.New("file", map[string]interface{}{"path": wmDir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if mark, _ := store.Load("sdk://test"); mark != nil {
		t.Fatalf("drained run advanced the watermark to %q", mark.Value)
	}
}
