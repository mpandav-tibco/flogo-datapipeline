package sectioned

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func tmpDoc(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "report.txt")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

// A document with a key/value header (double-space separated) then a body
// whose column names appear on a marker line.
const doc = `Report Name    Sales Q1
Region         EMEA

id    product    qty
1     Widget     10
2     Gadget     5
`

func TestSectioned_ConfigValidation(t *testing.T) {
	if _, err := newSectionedSource(map[string]interface{}{}); err == nil {
		t.Error("missing path should error")
	}
	p := tmpDoc(t, doc)
	if _, err := newSectionedSource(map[string]interface{}{"path": p}); err == nil {
		t.Error("missing bodyColumnMarker should error")
	}
	if _, err := newSectionedSource(map[string]interface{}{"path": p, "bodyColumnMarker": "["}); err == nil {
		t.Error("invalid regex marker should error")
	}
}

func TestSectioned_ReadWithBroadcast(t *testing.T) {
	p := tmpDoc(t, doc)
	src, err := newSectionedSource(map[string]interface{}{
		"path":             p,
		"bodyColumnMarker": `^id\s+product\s+qty`,
		"broadcastHeader":  true,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer src.Close()

	schema := src.Schema()
	// body cols id,product,qty + header keys Report_Name, Region
	if len(schema) < 3 {
		t.Fatalf("schema too short: %v", schema)
	}
	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if b.Len() != 2 {
		t.Fatalf("rows = %d, want 2", b.Len())
	}
	if b.Value(0, "product") != "Widget" || b.Value(1, "qty") != "5" {
		t.Errorf("body values wrong: %v", b.Rows)
	}
	// broadcast header value present on every row
	if b.Value(0, "Region") != "EMEA" || b.Value(1, "Region") != "EMEA" {
		t.Errorf("broadcast header wrong: %v", b.Rows)
	}
}

func TestSectioned_NoBroadcast(t *testing.T) {
	p := tmpDoc(t, doc)
	src, err := newSectionedSource(map[string]interface{}{
		"path":             p,
		"bodyColumnMarker": `^id\s+product\s+qty`,
		"broadcastHeader":  false,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer src.Close()
	if len(src.Schema()) != 3 {
		t.Errorf("without broadcast, schema should be 3 body cols, got %d", len(src.Schema()))
	}
}

func TestSectioned_MarkerNoMatch(t *testing.T) {
	p := tmpDoc(t, doc)
	_, err := newSectionedSource(map[string]interface{}{
		"path":             p,
		"bodyColumnMarker": `^THIS_WILL_NEVER_MATCH`,
	})
	if err == nil {
		t.Error("marker matching no line should error")
	}
}
