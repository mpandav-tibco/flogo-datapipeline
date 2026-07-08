package null

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

// ─── null sink is a no-op ─────────────────────────────────────────────────────

func TestNullSink_WriteBatch_NoOp(t *testing.T) {
	s := &nullSink{}
	batch := &record.RecordBatch{
		Columns: []record.Column{{Name: "x", Type: record.TypeString}},
		Rows:    [][]interface{}{{"hello"}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Errorf("WriteBatch: %v", err)
	}
}

func TestNullSink_Flush_NoOp(t *testing.T) {
	s := &nullSink{}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
}

func TestNullSink_Close_NoOp(t *testing.T) {
	s := &nullSink{}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// ─── registered as "null" and "discard" in sink registry ─────────────────────

func TestNullSink_Types(t *testing.T) {
	types := sink.Types()
	nullFound := false
	discardFound := false
	for _, typ := range types {
		if typ == "null" {
			nullFound = true
		}
		if typ == "discard" {
			discardFound = true
		}
	}
	if !nullFound {
		t.Error("'null' not found in sink.Types()")
	}
	if !discardFound {
		t.Error("'discard' not found in sink.Types()")
	}
}

// ─── created via registry ─────────────────────────────────────────────────────

func TestNullSink_ViaRegistry(t *testing.T) {
	s, err := sink.New("null", nil)
	if err != nil {
		t.Fatalf("sink.New(null): %v", err)
	}
	if s == nil {
		t.Fatal("sink.New(null) returned nil")
	}
	// Must implement the full Sink interface — compile-time guarantee via the
	// function signature, but let's exercise all three methods.
	batch := &record.RecordBatch{
		Columns: []record.Column{{Name: "y", Type: record.TypeInt}},
		Rows:    [][]interface{}{{42}},
	}
	if err := s.WriteBatch(batch); err != nil {
		t.Errorf("WriteBatch via registry: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush via registry: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close via registry: %v", err)
	}
}

func TestDiscardSink_ViaRegistry(t *testing.T) {
	s, err := sink.New("discard", map[string]interface{}{})
	if err != nil {
		t.Fatalf("sink.New(discard): %v", err)
	}
	if s == nil {
		t.Fatal("sink.New(discard) returned nil")
	}
	if err := s.WriteBatch(nil); err != nil {
		t.Errorf("WriteBatch(nil) via discard: %v", err)
	}
}

// ─── Implements sink.Sink interface (compile-time check) ─────────────────────

var _ sink.Sink = (*nullSink)(nil)
