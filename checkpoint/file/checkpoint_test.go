package file

import (
	"encoding/json"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
)

func TestFileCheckpoint_SaveLoadDelete(t *testing.T) {
	dir := t.TempDir()
	ck, err := newFileCheckpoint(map[string]interface{}{"path": dir})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer ck.Close()

	// Load missing → nil, no error.
	if tok, err := ck.Load("p1"); err != nil || tok != nil {
		t.Fatalf("Load missing = %v, %v; want nil,nil", tok, err)
	}

	tok := &checkpoint.Token{Format: "csv", RowsProcessed: 42, Payload: json.RawMessage(`{"offset":100}`)}
	if err := ck.Save("p1", tok); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := ck.Load("p1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Format != "csv" || got.RowsProcessed != 42 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	// Payload is re-indented by MarshalIndent on Save; compare semantically.
	var pv map[string]interface{}
	if err := json.Unmarshal(got.Payload, &pv); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if pv["offset"] != float64(100) {
		t.Errorf("payload offset = %v, want 100", pv["offset"])
	}

	if err := ck.Delete("p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if tok, _ := ck.Load("p1"); tok != nil {
		t.Error("token should be gone after Delete")
	}
	// Delete of missing is a no-op.
	if err := ck.Delete("p1"); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
}

func TestFileCheckpoint_PartitionIDEscaping(t *testing.T) {
	dir := t.TempDir()
	ck, _ := newFileCheckpoint(map[string]interface{}{"path": dir})
	defer ck.Close()
	// Two IDs that would collide under filepath.Base must stay distinct.
	a := "region/us-east/shard-0"
	b := "region/eu-west/shard-0"
	_ = ck.Save(a, &checkpoint.Token{Format: "a", RowsProcessed: 1})
	_ = ck.Save(b, &checkpoint.Token{Format: "b", RowsProcessed: 2})
	ta, _ := ck.Load(a)
	tb, _ := ck.Load(b)
	if ta == nil || tb == nil || ta.Format != "a" || tb.Format != "b" {
		t.Errorf("partition IDs collided: a=%+v b=%+v", ta, tb)
	}
}

func TestFileCheckpoint_DefaultDir(t *testing.T) {
	// Empty path falls back to os.TempDir — should still construct.
	if _, err := newFileCheckpoint(map[string]interface{}{}); err != nil {
		t.Errorf("default dir: %v", err)
	}
}

func TestFileCheckpoint_ViaRegistry(t *testing.T) {
	dir := t.TempDir()
	ck, err := checkpoint.New("file", map[string]interface{}{"path": dir})
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer ck.Close()
	if err := ck.Save("x", checkpoint.NewToken("csv", 7, nil)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if tok, _ := ck.Load("x"); tok == nil || tok.RowsProcessed != 7 {
		t.Errorf("registry round-trip failed: %+v", tok)
	}
}
