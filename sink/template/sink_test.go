package template

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
		Rows:    [][]interface{}{{1, "ada"}, {2, "bob"}},
	}
}

func TestTemplateSink_ConfigValidation(t *testing.T) {
	if _, err := newTemplateSink(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
	if _, err := newTemplateSink(map[string]interface{}{"path": "out.txt"}); err == nil {
		t.Error("missing template should error")
	}
	if _, err := newTemplateSink(map[string]interface{}{"path": "out.txt", "template": "{{ .Bad"}); err == nil {
		t.Error("bad template should error")
	}
}

func TestTemplateSink_Render(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "report.txt")
	tmpl := "COUNT={{ .Count }}\n{{- range .Rows }}\n{{ .id }}|{{ upper .name }}{{- end }}\n"
	s, err := newTemplateSink(map[string]interface{}{"path": p, "template": tmpl})
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
	out := string(got)
	if !strings.Contains(out, "COUNT=2") {
		t.Errorf("missing count: %q", out)
	}
	if !strings.Contains(out, "1|ADA") || !strings.Contains(out, "2|BOB") {
		t.Errorf("rows not rendered/uppercased: %q", out)
	}
}

func TestTemplateSink_PathTemplatedOnAttr(t *testing.T) {
	dir := t.TempDir()
	// path uses the first row's id attribute
	pathSpec := filepath.Join(dir, `{{ index .Attr "id" }}.txt`)
	s, err := newTemplateSink(map[string]interface{}{"path": pathSpec, "template": "{{ .Count }}"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.WriteBatch(batch())
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// first row id=1 → file "1.txt"
	if _, err := os.Stat(filepath.Join(dir, "1.txt")); err != nil {
		t.Errorf("expected templated output file 1.txt: %v", err)
	}
}

func TestTemplateSink_TemplatePath(t *testing.T) {
	dir := t.TempDir()
	tpath := filepath.Join(dir, "layout.tmpl")
	_ = os.WriteFile(tpath, []byte("rows={{ .Count }}"), 0o644)
	out := filepath.Join(dir, "o.txt")
	s, err := newTemplateSink(map[string]interface{}{"path": out, "templatePath": tpath})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.WriteBatch(batch())
	_ = s.Close()
	got, _ := os.ReadFile(out)
	if string(got) != "rows=2" {
		t.Errorf("templatePath render = %q", got)
	}
}

func TestTemplateSink_EmptyBatchNoop(t *testing.T) {
	s, _ := newTemplateSink(map[string]interface{}{"path": filepath.Join(t.TempDir(), "x.txt"), "template": "{{ .Count }}"})
	if err := s.WriteBatch(nil); err != nil {
		t.Errorf("nil batch: %v", err)
	}
}
