package jsonlines

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func writeTmp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	return p
}

func TestJSONLines_ConfigValidation(t *testing.T) {
	if _, err := newJSONLinesSource(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
	if _, err := newJSONLinesSource(map[string]interface{}{"path": "/no/such/file.ndjson"}); err == nil {
		t.Error("nonexistent file should error")
	}
}

func TestJSONLines_DeclaredFields(t *testing.T) {
	path := writeTmp(t, "e.ndjson", `{"id":1,"event":"click"}
{"id":2,"event":"view"}
`)
	src, err := newJSONLinesSource(map[string]interface{}{"path": path, "fields": []interface{}{"id", "event"}})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer src.Close()
	if len(src.Schema()) != 2 {
		t.Fatalf("schema len = %d", len(src.Schema()))
	}
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 2 {
		t.Fatalf("rows = %d, want 2", b.Len())
	}
	if b.Value(0, "event") != "click" {
		t.Errorf("row0 event = %v", b.Value(0, "event"))
	}
}

func TestJSONLines_InferSchema(t *testing.T) {
	path := writeTmp(t, "i.ndjson", `{"a":1,"b":"x"}
`)
	src, _ := newJSONLinesSource(map[string]interface{}{"path": path})
	defer src.Close()
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 1 || len(b.Columns) != 2 {
		t.Errorf("inferred schema wrong: cols=%d rows=%d", len(b.Columns), b.Len())
	}
}

func TestJSONLines_SkipBlankAndMalformed(t *testing.T) {
	path := writeTmp(t, "m.ndjson", `{"id":1}

not-json
{"id":2}
`)
	src, _ := newJSONLinesSource(map[string]interface{}{"path": path, "fields": []interface{}{"id"}})
	defer src.Close()
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 2 {
		t.Errorf("rows = %d, want 2 (blank + malformed skipped)", b.Len())
	}
}

func TestJSONLines_EOF(t *testing.T) {
	path := writeTmp(t, "empty.ndjson", "")
	src, _ := newJSONLinesSource(map[string]interface{}{"path": path, "fields": []interface{}{"id"}})
	defer src.Close()
	if _, err := src.ReadBatch(10); err != io.EOF {
		t.Errorf("empty file should return EOF, got %v", err)
	}
}

func TestJSONLines_Skip(t *testing.T) {
	path := writeTmp(t, "s.ndjson", `{"id":1}
{"id":2}
{"id":3}
`)
	src, _ := newJSONLinesSource(map[string]interface{}{"path": path, "fields": []interface{}{"id"}})
	defer src.Close()
	if err := src.Skip(2); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 1 {
		t.Errorf("after skip(2), rows = %d, want 1", b.Len())
	}
}

func TestJSONLines_RegisteredFactory(t *testing.T) {
	path := writeTmp(t, "r.ndjson", `{"id":1}
`)
	for _, typ := range []string{"jsonlines", "ndjson"} {
		src, err := source.New(typ, map[string]interface{}{"path": path, "fields": []interface{}{"id"}})
		if err != nil {
			t.Fatalf("source.New(%q): %v", typ, err)
		}
		src.Close()
	}
}
