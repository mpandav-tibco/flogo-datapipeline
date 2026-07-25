package jsonlines

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func batch() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}},
		Rows:    [][]interface{}{{1, "Ada"}, {2, "Bob"}},
	}
}

func TestJSONLinesSink_ConfigValidation(t *testing.T) {
	if _, err := newJSONLinesSink(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
}

func TestJSONLinesSink_Write(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.ndjson")
	s, err := newJSONLinesSink(map[string]interface{}{"path": p})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := s.WriteBatch(batch()); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, _ := os.ReadFile(p)
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2:\n%s", len(lines), got)
	}
	if !strings.Contains(lines[0], `"name":"Ada"`) || !strings.Contains(lines[0], `"id":1`) {
		t.Errorf("line0 = %q", lines[0])
	}
}

func TestJSONLinesSink_NilBatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "n.ndjson")
	s, _ := newJSONLinesSink(map[string]interface{}{"path": p})
	if err := s.WriteBatch(nil); err != nil {
		t.Errorf("nil batch: %v", err)
	}
	_ = s.Close()
	got, _ := os.ReadFile(p)
	if len(got) != 0 {
		t.Errorf("nil batch should write nothing, got %q", got)
	}
}
