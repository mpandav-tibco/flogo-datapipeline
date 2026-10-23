package pipeline

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
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

func TestShutdownRunsCancelsAndFlushesStreamingRun(t *testing.T) {
	t.Cleanup(func() { activeRuns = &runTracker{cancels: map[int]context.CancelFunc{}} })
	out := filepath.Join(t.TempDir(), "out.jsonl")
	def := &Definition{
		Source: SourceDef{Type: "endlesstest", Options: map[string]interface{}{}},
		Sink:   jsonlSink(out),
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
	select {
	case o := <-done:
		if o.Status == "completed" {
			t.Fatalf("cancelled run reported completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after ShutdownRuns")
	}
	// The sink was closed cleanly: every line is a complete JSON record.
	rows := readJSONL(t, out)
	if len(rows) == 0 {
		t.Fatal("no rows flushed to the sink")
	}
	// New runs after shutdown start cancelled.
	ctx, finish := activeRuns.start(context.Background())
	defer finish()
	if ctx.Err() == nil {
		t.Fatal("run started after shutdown was not cancelled")
	}
}
