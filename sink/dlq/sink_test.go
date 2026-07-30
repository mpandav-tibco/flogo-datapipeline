package dlq

import (
	"encoding/json"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

// captureSink is a test-only inner sink that deep-copies every batch it receives
// so the DLQ envelope output can be inspected (the DLQ sink recycles the batch
// via record.Put immediately after the inner write returns).
type captureSink struct {
	batches [][]map[string]interface{}
	cols    []record.Column
	flushed bool
	closed  bool
}

var lastCapture *captureSink

func init() {
	sink.Register("dlq_capture", func(map[string]interface{}) (sink.Sink, error) {
		lastCapture = &captureSink{}
		return lastCapture, nil
	})
}

func (c *captureSink) WriteBatch(b *record.RecordBatch) error {
	c.cols = append([]record.Column(nil), b.Columns...)
	rows := make([]map[string]interface{}, b.Len())
	for i := 0; i < b.Len(); i++ {
		rows[i] = b.RowMap(i)
	}
	c.batches = append(c.batches, rows)
	return nil
}
func (c *captureSink) Flush() error { c.flushed = true; return nil }
func (c *captureSink) Close() error { c.closed = true; return nil }

func newBatch(cols []string, rows ...[]interface{}) *record.RecordBatch {
	c := make([]record.Column, len(cols))
	for i, n := range cols {
		c[i] = record.Column{Name: n, Type: record.TypeString}
	}
	return &record.RecordBatch{Columns: c, Rows: rows}
}

func newDLQ(t *testing.T, cfg map[string]interface{}) *dlqSink {
	t.Helper()
	cfg["innerType"] = "dlq_capture"
	lastCapture = nil
	raw, err := newDLQSink(cfg)
	if err != nil {
		t.Fatalf("newDLQSink: %v", err)
	}
	return raw.(*dlqSink)
}

func TestDLQ_EnvelopeEnrichment(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{})
	d.InjectContext("res://pipeline:orders", "run-42")

	b := newBatch([]string{"id", "amount"}, []interface{}{"1", "100"}, []interface{}{"2", "bad"})
	if err := d.WriteBatchErrors(b, []string{"", "amount not numeric"}); err != nil {
		t.Fatalf("WriteBatchErrors: %v", err)
	}
	if lastCapture == nil || len(lastCapture.batches) != 1 {
		t.Fatalf("inner sink did not receive one batch: %+v", lastCapture)
	}
	rows := lastCapture.batches[0]
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	// Original columns preserved + envelope column added.
	if rows[0]["id"] != "1" {
		t.Fatalf("original column lost: %v", rows[0])
	}
	envRaw, ok := rows[1]["_dlq_error"].(string)
	if !ok {
		t.Fatalf("envelope column missing: %v", rows[1])
	}
	var env ErrorEnvelope
	if err := json.Unmarshal([]byte(envRaw), &env); err != nil {
		t.Fatalf("parse envelope: %v (%s)", err, envRaw)
	}
	if env.Pipeline != "res://pipeline:orders" || env.RunID != "run-42" {
		t.Fatalf("envelope context wrong: %+v", env)
	}
	if env.ErrorMessage != "amount not numeric" {
		t.Fatalf("envelope errorMessage = %q, want 'amount not numeric'", env.ErrorMessage)
	}
	if env.RowIndex != 1 {
		t.Fatalf("envelope rowIndex = %d, want 1", env.RowIndex)
	}
	// OriginalRow captured as a map.
	orig, ok := env.OriginalRow.(map[string]interface{})
	if !ok || orig["amount"] != "bad" {
		t.Fatalf("envelope originalRow wrong: %v", env.OriginalRow)
	}
}

func TestDLQ_CustomEnvelopeColumn(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{"envelopeColumn": "_err"})
	b := newBatch([]string{"id"}, []interface{}{"1"})
	if err := d.WriteBatch(b); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if _, ok := lastCapture.batches[0][0]["_err"]; !ok {
		t.Fatalf("custom envelope column '_err' missing: %v", lastCapture.batches[0][0])
	}
}

func TestDLQ_MaxRowsPerBatch_Drop(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{"maxRowsPerBatch": 2, "onFull": "drop"})
	b := newBatch([]string{"id"},
		[]interface{}{"1"}, []interface{}{"2"}, []interface{}{"3"}, []interface{}{"4"})
	if err := d.WriteBatch(b); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if got := len(lastCapture.batches[0]); got != 2 {
		t.Fatalf("onFull=drop wrote %d rows, want 2 (overflow dropped)", got)
	}
}

func TestDLQ_MaxRowsPerBatch_BlockKeepsAll(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{"maxRowsPerBatch": 2, "onFull": "block"})
	b := newBatch([]string{"id"},
		[]interface{}{"1"}, []interface{}{"2"}, []interface{}{"3"}, []interface{}{"4"})
	if err := d.WriteBatch(b); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if got := len(lastCapture.batches[0]); got != 4 {
		t.Fatalf("onFull=block wrote %d rows, want 4 (no drop)", got)
	}
}

func TestDLQ_LazyOpen_NoWriteNoInner(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{})
	// Flush + Close before any row is routed must be a no-op (inner never opened).
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush on unused DLQ: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close on unused DLQ: %v", err)
	}
	if lastCapture != nil {
		t.Fatal("inner sink was opened despite no rows being routed")
	}
}

func TestDLQ_EmptyBatchIsNoop(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{})
	if err := d.WriteBatch(newBatch([]string{"id"})); err != nil {
		t.Fatalf("empty WriteBatch: %v", err)
	}
	if lastCapture != nil {
		t.Fatal("empty batch should not open the inner sink")
	}
}

func TestDLQ_FlushClosePropagate(t *testing.T) {
	d := newDLQ(t, map[string]interface{}{})
	if err := d.WriteBatch(newBatch([]string{"id"}, []interface{}{"1"})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !lastCapture.flushed || !lastCapture.closed {
		t.Fatalf("Flush/Close not propagated to inner: flushed=%v closed=%v", lastCapture.flushed, lastCapture.closed)
	}
}

func TestDLQ_ValidationErrors(t *testing.T) {
	if _, err := newDLQSink(map[string]interface{}{}); err == nil {
		t.Fatal("expected error when innerType is missing")
	}
	if _, err := newDLQSink(map[string]interface{}{"innerType": "null", "onFull": "explode"}); err == nil {
		t.Fatal("expected error for invalid onFull value")
	}
}
