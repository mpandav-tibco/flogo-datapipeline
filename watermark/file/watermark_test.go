package file

import (
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/watermark"
)

func TestFileWatermark_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	st, err := newFileStore(map[string]interface{}{"path": dir})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer st.Close()

	// Missing → nil.
	if m, err := st.Load("p1"); err != nil || m != nil {
		t.Fatalf("Load missing = %v,%v; want nil,nil", m, err)
	}

	mark := &watermark.Mark{Column: "updated_at", Value: "2025-01-01T00:00:00Z", ValueType: "datetime", SavedAt: time.Now().UTC()}
	if err := st.Save("p1", mark); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := st.Load("p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Column != "updated_at" || got.Value != "2025-01-01T00:00:00Z" || got.ValueType != "datetime" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestFileWatermark_IDEscaping(t *testing.T) {
	dir := t.TempDir()
	st, _ := newFileStore(map[string]interface{}{"path": dir})
	defer st.Close()
	a := "tenant/a/pipe:1"
	b := "tenant/b/pipe:1"
	_ = st.Save(a, &watermark.Mark{Column: "c", Value: "1"})
	_ = st.Save(b, &watermark.Mark{Column: "c", Value: "2"})
	ma, _ := st.Load(a)
	mb, _ := st.Load(b)
	if ma == nil || mb == nil || ma.Value != "1" || mb.Value != "2" {
		t.Errorf("ids collided: a=%+v b=%+v", ma, mb)
	}
}

func TestFileWatermark_ViaRegistry(t *testing.T) {
	dir := t.TempDir()
	st, err := watermark.New("file", map[string]interface{}{"path": dir})
	if err != nil {
		t.Fatalf("watermark.New: %v", err)
	}
	defer st.Close()
	if err := st.Save("x", &watermark.Mark{Column: "seq", Value: "99", ValueType: "int"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if m, _ := st.Load("x"); m == nil || m.Value != "99" {
		t.Errorf("registry round-trip failed: %+v", m)
	}
}
