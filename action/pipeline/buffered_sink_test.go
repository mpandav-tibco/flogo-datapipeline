package pipeline

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

// bufferedAtomicSink mimics the jdbc sink: it buffers rows ACROSS WriteBatch
// calls, flushes once `threshold` rows are buffered, clears the buffer before
// "executing" the flush, and a flush containing a row named BAD* persists
// nothing. Rows from earlier calls in a failed flush are handed back via
// sink.UnflushedRowsError.
type bufferedAtomicSink struct {
	threshold int
	buf       [][]interface{}
	cols      []record.Column
	persisted []string
	writes    int
}

var lastBuffered *bufferedAtomicSink

func init() {
	sink.Register("bufferedtest", func(map[string]interface{}) (sink.Sink, error) {
		lastBuffered = &bufferedAtomicSink{threshold: 12}
		return lastBuffered, nil
	})
}

func (s *bufferedAtomicSink) AtomicWrites() bool { return true }

func (s *bufferedAtomicSink) WriteBatch(b *record.RecordBatch) error {
	s.cols = b.Columns
	prev := len(s.buf)
	for _, r := range b.Rows {
		s.buf = append(s.buf, append([]interface{}(nil), r...))
	}
	if len(s.buf) >= s.threshold {
		return s.flush(prev)
	}
	return nil
}

func (s *bufferedAtomicSink) flush(prev int) error {
	rows := s.buf
	s.buf = nil
	if len(rows) == 0 {
		return nil
	}
	s.writes++
	for _, r := range rows {
		if strings.HasPrefix(fmt.Sprint(r[1]), "BAD") {
			err := errors.New("constraint violation")
			if prev > 0 {
				return &sink.UnflushedRowsError{Columns: s.cols, Rows: rows[:prev], Err: err}
			}
			return err
		}
	}
	for _, r := range rows {
		s.persisted = append(s.persisted, fmt.Sprint(r[1]))
	}
	return nil
}
func (s *bufferedAtomicSink) Flush() error { return s.flush(len(s.buf)) }
func (s *bufferedAtomicSink) Close() error { return nil }

// With a buffering sink, rows accepted by earlier WriteBatch calls used to be
// lost (yet counted written) when a later flush failed. Every good row must
// land exactly once, every bad row must reach the DLQ, and counts must match.
func TestRunDirect_BufferedSinkFailedFlushLosesNothing(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name,amount\n")
	var good, bad []string
	for i := 0; i < 200; i++ {
		name := fmt.Sprintf("r%03d", i)
		if i%37 == 5 {
			name = fmt.Sprintf("BAD%03d", i)
			bad = append(bad, name)
		} else {
			good = append(good, name)
		}
		fmt.Fprintf(&sb, "%d,%s,%d\n", i, name, i)
	}
	dlqPath := filepath.Join(t.TempDir(), "dlq.jsonl")
	def := &Definition{
		Source:      csvSource(writeTemp(t, "in.csv", sb.String())),
		Sink:        SinkDef{Type: "bufferedtest", Options: map[string]interface{}{}},
		DLQ:         DLQDef{Type: "jsonlines", Options: map[string]interface{}{"path": dlqPath}},
		Performance: PerformanceDef{BatchSize: 5}, // < sink threshold: rows buffer across calls
	}
	out, err := runInline(t, def)
	if err != nil {
		t.Fatal(err)
	}
	got := append([]string(nil), lastBuffered.persisted...)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(good, ",") {
		t.Fatalf("persisted %d rows, want exactly the %d good rows once each\n got=%v", len(got), len(good), got)
	}
	var dlqNames []string
	for _, r := range readJSONL(t, dlqPath) {
		if row, ok := r["row"].(map[string]interface{}); ok {
			dlqNames = append(dlqNames, fmt.Sprint(row["name"]))
		} else {
			dlqNames = append(dlqNames, fmt.Sprint(r["name"]))
		}
	}
	sort.Strings(dlqNames)
	if strings.Join(dlqNames, ",") != strings.Join(bad, ",") {
		t.Fatalf("DLQ=%v want %v", dlqNames, bad)
	}
	if out.RowsWritten != int64(len(good)) || out.RowsFailed != int64(len(bad)) {
		t.Fatalf("written=%d failed=%d; want %d/%d", out.RowsWritten, out.RowsFailed, len(good), len(bad))
	}
}
