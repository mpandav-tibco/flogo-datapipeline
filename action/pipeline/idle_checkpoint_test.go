package pipeline

import (
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// idleStreamSource returns 3 rows, then empty polls (an idle stream) for
// ~2.5s, then EOF. It records how many checkpoints had been saved when it
// went idle and when it hit EOF.
type idleStreamSource struct {
	sent        bool
	idleSince   time.Time
	savesAtIdle atomic.Int64
	savesAtEOF  atomic.Int64
}

var lastIdle atomic.Pointer[idleStreamSource]

func init() {
	source.Register("idletest", func(map[string]interface{}) (source.Source, error) {
		s := &idleStreamSource{}
		lastIdle.Store(s)
		return s, nil
	})
}

func (s *idleStreamSource) Schema() []record.Column { return []record.Column{{Name: "v"}} }
func (s *idleStreamSource) ReadBatch(int) (*record.RecordBatch, error) {
	b := record.Get()
	b.Columns = s.Schema()
	if !s.sent {
		s.sent = true
		s.idleSince = time.Now()
		s.savesAtIdle.Store(atomic.LoadInt64(&countedSaves))
		b.Rows = append(b.Rows, []interface{}{1}, []interface{}{2}, []interface{}{3})
		return b, nil
	}
	if time.Since(s.idleSince) > 2500*time.Millisecond {
		s.savesAtEOF.Store(atomic.LoadInt64(&countedSaves))
		record.Put(b)
		return nil, io.EOF
	}
	time.Sleep(50 * time.Millisecond) // poll window
	return b, nil
}
func (s *idleStreamSource) Skip(int64) error { return nil }
func (s *idleStreamSource) Close() error     { return nil }

// An idle stream must still take its time-based checkpoint for rows processed
// since the last one (previously the time trigger only ran on non-empty
// batches, so a quiet stream never persisted its tail).
func TestIdleStreamTakesTimeCheckpoint(t *testing.T) {
	atomic.StoreInt64(&countedSaves, 0)
	def := &Definition{
		Source:     SourceDef{Type: "idletest", Options: map[string]interface{}{}},
		Sink:       jsonlSink(filepath.Join(t.TempDir(), "out.jsonl")),
		Checkpoint: CheckpointDef{Type: "counttest", IntervalRows: 1000, IntervalSeconds: 1},
	}
	if _, err := runInline(t, def); err != nil {
		t.Fatal(err)
	}
	s := lastIdle.Load()
	if idle, eof := s.savesAtIdle.Load(), s.savesAtEOF.Load(); eof-idle < 1 {
		t.Fatalf("no checkpoint while idle (saves at idle=%d, at EOF=%d)", idle, eof)
	}
}
