package pipeline

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func oneRowBatch(v interface{}) *record.RecordBatch {
	b := record.Get()
	b.Columns = []record.Column{{Name: "v"}}
	b.Rows = append(b.Rows, []interface{}{v})
	return b
}

func TestWriteStage_OrderAndBarrier(t *testing.T) {
	var mu sync.Mutex
	var got []interface{}
	ws := newWriteStage(2, func(b *record.RecordBatch, o *Output) (int, error) {
		time.Sleep(time.Millisecond)
		mu.Lock()
		got = append(got, b.Rows[0][0])
		mu.Unlock()
		return 1, nil
	})
	for i := 0; i < 20; i++ {
		ws.submit(oneRowBatch(i))
	}
	w, f, err := ws.sync()
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if err != nil || w != 20 || f != 0 || n != 20 {
		t.Fatalf("after barrier: written=%d failed=%d err=%v delivered=%d; want 20/0/nil/20", w, f, err, n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("out of order at %d: %v", i, got)
		}
	}
	if w, _, _ := ws.sync(); w != 0 {
		t.Fatalf("sync must return deltas; second sync written=%d", w)
	}
	ws.close(false)
}

func TestWriteStage_ErrorStopsFurtherWrites(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	ws := newWriteStage(1, func(b *record.RecordBatch, o *Output) (int, error) {
		calls++
		o.RowsFailed = 2
		if calls == 2 {
			return 0, boom
		}
		return 1, nil
	})
	for i := 0; i < 5; i++ {
		ws.submit(oneRowBatch(i))
	}
	w, f, err := ws.sync()
	if !errors.Is(err, boom) || w != 1 || calls != 2 || f != 4 {
		t.Fatalf("written=%d failed=%d calls=%d err=%v; want 1/4/2/boom", w, f, calls, err)
	}
	if ws.submit(oneRowBatch(9)) {
		t.Fatal("submit accepted a batch after a failed write")
	}
	ws.close(false)
}

func TestWriteStage_CloseDiscard(t *testing.T) {
	release := make(chan struct{})
	calls := 0
	ws := newWriteStage(4, func(b *record.RecordBatch, o *Output) (int, error) {
		calls++
		<-release
		return 1, nil
	})
	for i := 0; i < 4; i++ {
		ws.submit(oneRowBatch(i))
	}
	time.Sleep(10 * time.Millisecond) // first write is now in flight
	go func() { time.Sleep(10 * time.Millisecond); close(release) }()
	ws.close(true)
	if calls != 1 {
		t.Fatalf("discarding close wrote %d batches; want only the in-flight one", calls)
	}
}

// flushAuditSink records, at every Flush, how many rows it has received, so a
// test can prove each checkpoint flush happens after all prior writes landed.
type flushAuditSink struct {
	mu        sync.Mutex
	rows      int
	atFlushes []int
}

var lastFlushAudit *flushAuditSink

func init() {
	sink.Register("flushaudit", func(map[string]interface{}) (sink.Sink, error) {
		lastFlushAudit = &flushAuditSink{}
		return lastFlushAudit, nil
	})
}

func (s *flushAuditSink) WriteBatch(b *record.RecordBatch) error {
	time.Sleep(2 * time.Millisecond) // slow sink: writes lag behind transforms
	s.mu.Lock()
	s.rows += b.Len()
	s.mu.Unlock()
	return nil
}
func (s *flushAuditSink) Flush() error {
	s.mu.Lock()
	s.atFlushes = append(s.atFlushes, s.rows)
	s.mu.Unlock()
	return nil
}
func (s *flushAuditSink) Close() error { return nil }

func TestRunDirect_CheckpointFlushSeesAllPriorWrites(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name,amount\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "%d,n%d,%d\n", i, i, i)
	}
	src := writeTemp(t, "in.csv", sb.String())
	def := &Definition{
		Source:      csvSource(src),
		Sink:        SinkDef{Type: "flushaudit", Options: map[string]interface{}{}},
		Performance: PerformanceDef{BatchSize: 10, SinkBufferBatches: 4},
		Checkpoint:  CheckpointDef{Type: "file", Path: filepath.Join(t.TempDir(), "ck"), IntervalRows: 20},
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatal(err)
	}
	if out.RowsWritten != 100 {
		t.Fatalf("written=%d want 100", out.RowsWritten)
	}
	s := lastFlushAudit
	if len(s.atFlushes) < 5 {
		t.Fatalf("expected >=5 checkpoint flushes, got %v", s.atFlushes)
	}
	for i, n := range s.atFlushes[:5] {
		if want := (i + 1) * 20; n != want {
			t.Fatalf("checkpoint flush %d saw %d rows written; want %d (writes must drain before checkpoint)", i, n, want)
		}
	}
}

func TestWriteStage_PanicBecomesError(t *testing.T) {
	ws := newWriteStage(1, func(b *record.RecordBatch, o *Output) (int, error) {
		var m map[string]int
		m["boom"]++ // nil map write panics
		return 1, nil
	})
	ws.submit(oneRowBatch(1))
	_, _, err := ws.sync()
	if err == nil || !strings.Contains(err.Error(), "sink panicked") {
		t.Fatalf("want recovered panic error, got %v", err)
	}
	ws.close(false)
}

func TestWriteStage_CloseReportsUnsyncedCounts(t *testing.T) {
	ws := newWriteStage(4, func(b *record.RecordBatch, o *Output) (int, error) {
		o.RowsFailed = 1
		return 2, nil
	})
	for i := 0; i < 3; i++ {
		ws.submit(oneRowBatch(i))
	}
	if w, f := ws.close(false); w != 6 || f != 3 {
		t.Fatalf("close counts written=%d failed=%d; want 6/3", w, f)
	}
}

// panicSink panics on the 3rd write; the run must fail cleanly (recovered),
// not crash the process, and report the rows delivered before the panic.
type panicSink struct{ n int }

func init() {
	sink.Register("panictest", func(map[string]interface{}) (sink.Sink, error) { return &panicSink{}, nil })
}
func (s *panicSink) WriteBatch(b *record.RecordBatch) error {
	s.n++
	if s.n == 3 {
		panic("sink exploded")
	}
	return nil
}
func (s *panicSink) Flush() error { return nil }
func (s *panicSink) Close() error { return nil }

func TestRunDirect_SinkPanicFailsRunAndCountsDelivered(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name,amount\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "%d,n%d,%d\n", i, i, i)
	}
	def := &Definition{
		Source:      csvSource(writeTemp(t, "in.csv", sb.String())),
		Sink:        SinkDef{Type: "panictest", Options: map[string]interface{}{}},
		Performance: PerformanceDef{BatchSize: 10},
	}
	out, err := runInline(t, def)
	msg := out.Error
	if err != nil {
		msg = err.Error()
	}
	if !strings.Contains(msg, "sink exploded") {
		t.Fatalf("want run failure mentioning the panic, got err=%v out.Error=%q status=%q", err, out.Error, out.Status)
	}
	if out.RowsWritten != 20 {
		t.Fatalf("written=%d; want 20 (two batches delivered before the panic)", out.RowsWritten)
	}
}
