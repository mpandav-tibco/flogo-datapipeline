package csv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func batch() *record.RecordBatch {
	return &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}},
		Rows:    [][]interface{}{{1, "Ada"}, {2, "Bob"}, {3, nil}},
	}
}

func TestCSVSink_ConfigValidation(t *testing.T) {
	if _, err := newCSVSink(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
}

func TestCSVSink_WriteWithHeader(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.csv")
	s, err := newCSVSink(map[string]interface{}{"path": p})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := s.WriteBatch(batch()); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, _ := os.ReadFile(p)
	want := "id,name\n1,Ada\n2,Bob\n3,\n"
	if string(got) != want {
		t.Errorf("csv =\n%q\nwant\n%q", got, want)
	}
}

func TestCSVSink_NoHeader(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nohdr.csv")
	s, _ := newCSVSink(map[string]interface{}{"path": p, "writeHeader": false})
	_ = s.WriteBatch(batch())
	_ = s.Close()
	got, _ := os.ReadFile(p)
	if strings.HasPrefix(string(got), "id,name") {
		t.Errorf("header should be suppressed, got %q", got)
	}
}

func TestCSVSink_TSVDelimiter(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out.tsv")
	s, err := sink.New("tsv", map[string]interface{}{"path": p})
	if err != nil {
		t.Fatalf("sink.New(tsv): %v", err)
	}
	_ = s.WriteBatch(batch())
	_ = s.Close()
	got, _ := os.ReadFile(p)
	if !strings.Contains(string(got), "id\tname") {
		t.Errorf("expected tab-delimited header, got %q", got)
	}
}

func TestCSVSink_EmptyBatchNoop(t *testing.T) {
	p := filepath.Join(t.TempDir(), "empty.csv")
	s, _ := newCSVSink(map[string]interface{}{"path": p})
	if err := s.WriteBatch(nil); err != nil {
		t.Errorf("nil batch: %v", err)
	}
	if err := s.WriteBatch(&record.RecordBatch{}); err != nil {
		t.Errorf("empty batch: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
	_ = s.Close()
	got, _ := os.ReadFile(p)
	if len(got) != 0 {
		t.Errorf("empty writes should produce empty file, got %q", got)
	}
}

func TestCSVSink_CustomDelimiter(t *testing.T) {
	p := filepath.Join(t.TempDir(), "semi.csv")
	s, _ := newCSVSink(map[string]interface{}{"path": p, "delimiter": ";"})
	_ = s.WriteBatch(batch())
	_ = s.Close()
	got, _ := os.ReadFile(p)
	if !strings.Contains(string(got), "id;name") {
		t.Errorf("expected semicolon delimiter, got %q", got)
	}
}
