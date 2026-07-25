package xml

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func tmpXML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "data.xml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

func fields() []interface{} {
	return []interface{}{
		map[string]interface{}{"name": "id", "xpath": "id", "type": "int"},
		map[string]interface{}{"name": "name", "xpath": "name", "type": "string"},
		map[string]interface{}{"name": "city", "xpath": "address/city", "type": "string"},
		map[string]interface{}{"name": "qty", "xpath": "stock/@quantity", "type": "int"},
	}
}

func TestXML_ConfigValidation(t *testing.T) {
	if _, err := newXMLSource(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
	p := tmpXML(t, "<root/>")
	if _, err := newXMLSource(map[string]interface{}{"path": p}); err == nil {
		t.Error("missing recordElement should error")
	}
	if _, err := newXMLSource(map[string]interface{}{"path": p, "recordElement": "item"}); err == nil {
		t.Error("missing fields should error")
	}
}

func TestXML_ReadRecords(t *testing.T) {
	content := `<catalog>
  <product><id>1</id><name>Widget</name><address><city>Oslo</city></address><stock quantity="7"/></product>
  <product><id>2</id><name>Gadget</name><address><city>Bergen</city></address><stock quantity="0"/></product>
</catalog>`
	p := tmpXML(t, content)
	src, err := newXMLSource(map[string]interface{}{"path": p, "recordElement": "product", "fields": fields()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer src.Close()
	if len(src.Schema()) != 4 {
		t.Fatalf("schema len = %d", len(src.Schema()))
	}
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 2 {
		t.Fatalf("rows = %d, want 2", b.Len())
	}
	if b.Value(0, "id") != int64(1) {
		t.Errorf("id = %v, want 1", b.Value(0, "id"))
	}
	if b.Value(0, "name") != "Widget" {
		t.Errorf("name = %v, want Widget", b.Value(0, "name"))
	}
	if b.Value(1, "city") != "Bergen" {
		t.Errorf("city = %v, want Bergen", b.Value(1, "city"))
	}
	if b.Value(0, "qty") != int64(7) {
		t.Errorf("qty = %v, want 7 (child-element attribute)", b.Value(0, "qty"))
	}
}

func TestXML_EOF(t *testing.T) {
	p := tmpXML(t, `<catalog></catalog>`)
	src, _ := newXMLSource(map[string]interface{}{"path": p, "recordElement": "product", "fields": fields()})
	defer src.Close()
	if _, err := src.ReadBatch(10); err != io.EOF {
		t.Errorf("no records should EOF, got %v", err)
	}
}

func TestXML_Skip(t *testing.T) {
	content := `<c><product><id>1</id><name>A</name></product><product><id>2</id><name>B</name></product><product><id>3</id><name>C</name></product></c>`
	p := tmpXML(t, content)
	src, _ := newXMLSource(map[string]interface{}{"path": p, "recordElement": "product", "fields": fields()})
	defer src.Close()
	if err := src.Skip(2); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 1 || b.Value(0, "id") != int64(3) {
		t.Errorf("after skip(2): %v", b.Rows)
	}
}
