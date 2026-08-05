package broadcast

import (
	"errors"
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// Compile-time checks that broadcast satisfies both contracts.
var (
	_ transform.Transform = (*broadcastTransform)(nil)
	_ transform.SinkOwner = (*broadcastTransform)(nil)
)

// captureSink is an in-memory test sink that records the rows written to it,
// keyed by an "id" in its config so each sink/test can inspect its own stream.
type captureSink struct{ id string }

var (
	capMu       sync.Mutex
	capDests    = map[string]*[][]interface{}{}
	capClosed   = map[string]bool{}
	capFailNext = map[string]bool{} // if true, the next WriteBatch on this id fails
)

func init() {
	sink.Register("captureTest", func(cfg map[string]interface{}) (sink.Sink, error) {
		id, _ := cfg["id"].(string)
		capMu.Lock()
		if _, ok := capDests[id]; !ok {
			capDests[id] = &[][]interface{}{}
		}
		capMu.Unlock()
		return &captureSink{id: id}, nil
	})
}

func (c *captureSink) WriteBatch(b *record.RecordBatch) error {
	capMu.Lock()
	defer capMu.Unlock()
	if capFailNext[c.id] {
		capFailNext[c.id] = false
		return errors.New("simulated sink failure")
	}
	d := capDests[c.id]
	for _, row := range b.Rows {
		cp := make([]interface{}, len(row)) // copy: the batch is pooled/recycled after WriteBatch
		copy(cp, row)
		*d = append(*d, cp)
	}
	return nil
}
func (c *captureSink) Flush() error { return nil }
func (c *captureSink) Close() error {
	capMu.Lock()
	capClosed[c.id] = true
	capMu.Unlock()
	return nil
}

func capturedRows(id string) [][]interface{} {
	capMu.Lock()
	defer capMu.Unlock()
	if d, ok := capDests[id]; ok {
		return *d
	}
	return nil
}

func capSink(name, id string) map[string]interface{} {
	return map[string]interface{}{
		"name": name, "sinkType": "captureTest", "sinkConfig": map[string]interface{}{"id": id},
	}
}

var testCols = []record.Column{
	{Name: "amount", Type: record.TypeInt},
	{Name: "region", Type: record.TypeString},
}

func batch(rows ...[]interface{}) *record.RecordBatch {
	return &record.RecordBatch{Columns: testCols, Rows: rows}
}

// TestBroadcast_AllSinksGetEveryRow proves every row is duplicated to EVERY
// configured sink (not split/routed exclusively like router), and the
// original batch continues downstream unchanged (passthrough defaults true).
func TestBroadcast_AllSinksGetEveryRow(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{
			capSink("archive", "t1-archive"),
			capSink("notify", "t1-notify"),
		},
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	defer tf.Close()

	in := batch(
		[]interface{}{int64(100), "US"},
		[]interface{}{int64(200), "EU"},
	)
	out, err := tf.Apply(in)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := len(capturedRows("t1-archive")); got != 2 {
		t.Fatalf("archive sink got %d rows, want 2 (every row, not a filtered subset)", got)
	}
	if got := len(capturedRows("t1-notify")); got != 2 {
		t.Fatalf("notify sink got %d rows, want 2 (every row, not a filtered subset)", got)
	}
	if out == nil || len(out.Rows) != 2 {
		t.Fatalf("passthrough batch = %v, want the original 2 rows unchanged (default passthrough=true)", out)
	}
}

// TestBroadcast_PassthroughFalse proves setting passthrough:false makes
// broadcast terminal — the batch does not continue downstream after being
// fanned out, even though every configured sink still received it.
func TestBroadcast_PassthroughFalse(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks":       []interface{}{capSink("archive", "t2-archive")},
		"passthrough": false,
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	defer tf.Close()

	out, err := tf.Apply(batch([]interface{}{int64(100), "US"}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != nil {
		t.Fatalf("passthrough batch = %v, want nil (passthrough=false makes broadcast terminal)", out)
	}
	if got := len(capturedRows("t2-archive")); got != 1 {
		t.Fatalf("archive sink got %d rows, want 1", got)
	}
}

// TestBroadcast_OneSinkFailureFailsTheBatch proves broadcast does not
// silently swallow a failing destination — if any sink write fails, the whole
// Apply call fails (the run reports the error), because the point of fan-out
// is that EVERY destination receives every row; a silently-skipped sink would
// be a silent replication gap.
func TestBroadcast_OneSinkFailureFailsTheBatch(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{
			capSink("first", "t3-first"),
			capSink("second", "t3-second"),
		},
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	defer tf.Close()

	capMu.Lock()
	capFailNext["t3-second"] = true
	capMu.Unlock()

	_, err = tf.Apply(batch([]interface{}{int64(100), "US"}))
	if err == nil {
		t.Fatal("expected an error when a broadcast sink write fails")
	}
	if got := len(capturedRows("t3-first")); got != 1 {
		t.Fatalf("first sink (written before the failing one) got %d rows, want 1", got)
	}
}

// TestBroadcast_EmptyOrNilBatchIsANoOp proves an empty/nil input batch neither
// panics nor writes empty batches to the configured sinks.
func TestBroadcast_EmptyOrNilBatchIsANoOp(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{capSink("archive", "t4-archive")},
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	defer tf.Close()

	if out, err := tf.Apply(nil); err != nil || out != nil {
		t.Fatalf("Apply(nil) = (%v, %v), want (nil, nil)", out, err)
	}
	if out, err := tf.Apply(&record.RecordBatch{Columns: testCols}); err != nil || out == nil || len(out.Rows) != 0 {
		t.Fatalf("Apply(empty) = (%v, %v), want an empty passthrough batch", out, err)
	}
	if got := len(capturedRows("t4-archive")); got != 0 {
		t.Fatalf("archive sink got %d rows, want 0 (no rows to broadcast)", got)
	}
}

// TestBroadcast_RequiresNonEmptySinks proves a missing or empty "sinks" config
// is rejected at construction, not silently accepted as a no-op transform.
func TestBroadcast_RequiresNonEmptySinks(t *testing.T) {
	if _, err := newBroadcast(map[string]interface{}{}); err == nil {
		t.Fatal("expected an error when 'sinks' is missing")
	}
	if _, err := newBroadcast(map[string]interface{}{"sinks": []interface{}{}}); err == nil {
		t.Fatal("expected an error when 'sinks' is empty")
	}
}

// TestBroadcast_RequiresSinkType proves each sink entry must name a sinkType.
func TestBroadcast_RequiresSinkType(t *testing.T) {
	_, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{map[string]interface{}{"name": "no-type"}},
	})
	if err == nil {
		t.Fatal("expected an error when a sink entry is missing 'sinkType'")
	}
}

// TestBroadcast_UnnamedSinksGetDefaultNames proves sinks without an explicit
// "name" still work (defaulted to sink<N> for logging), matching router's
// route<N> default-naming convention.
func TestBroadcast_UnnamedSinksGetDefaultNames(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{
			map[string]interface{}{"sinkType": "captureTest", "sinkConfig": map[string]interface{}{"id": "t5-unnamed"}},
		},
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	defer tf.Close()
	bt := tf.(*broadcastTransform)
	if bt.sinks[0].name != "sink0" {
		t.Errorf("unnamed sink[0].name = %q, want %q", bt.sinks[0].name, "sink0")
	}
}

// TestBroadcast_CloseClosesEverySink proves Close() flushes and closes every
// configured sink, matching router's cleanup contract.
func TestBroadcast_CloseClosesEverySink(t *testing.T) {
	tf, err := newBroadcast(map[string]interface{}{
		"sinks": []interface{}{
			capSink("a", "t6-a"),
			capSink("b", "t6-b"),
		},
	})
	if err != nil {
		t.Fatalf("newBroadcast: %v", err)
	}
	if err := tf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	capMu.Lock()
	defer capMu.Unlock()
	if !capClosed["t6-a"] || !capClosed["t6-b"] {
		t.Error("expected both sinks to be closed")
	}
}
