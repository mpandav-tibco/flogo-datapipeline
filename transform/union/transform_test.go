package union

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func str(v interface{}) string { return fmt.Sprintf("%v", v) }

// TestUnion_AppendsSecondarySource checks that a bounded `with` source is merged
// into the primary stream on the first batch, then the stream passes through.
func TestUnion_AppendsSecondarySource(t *testing.T) {
	dir := t.TempDir()
	extra := filepath.Join(dir, "extra.csv")
	if err := os.WriteFile(extra, []byte("id,name\n10,Xavier\n11,Yolanda\n"), 0o644); err != nil {
		t.Fatalf("write extra.csv: %v", err)
	}

	tf, err := transform.New("union", map[string]interface{}{
		"with": []interface{}{
			map[string]interface{}{"type": "csv", "path": extra, "hasHeader": true},
		},
	})
	if err != nil {
		t.Fatalf("new union: %v", err)
	}
	defer tf.Close()

	// Primary batch: one row with the same columns.
	in := record.Get()
	in.Columns = []record.Column{{Name: "id"}, {Name: "name"}}
	in.Rows = [][]interface{}{{"1", "Alice"}}

	out, err := tf.Apply(in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Len() != 3 {
		t.Fatalf("union output rows = %d, want 3 (2 secondary + 1 primary)", out.Len())
	}
	got := map[string]string{}
	for i := 0; i < out.Len(); i++ {
		m := out.RowMap(i)
		got[str(m["id"])] = str(m["name"])
	}
	for id, name := range map[string]string{"1": "Alice", "10": "Xavier", "11": "Yolanda"} {
		if got[id] != name {
			t.Errorf("id %q → name %q, want %q", id, got[id], name)
		}
	}

	// A second primary batch passes straight through (no more secondary rows).
	in2 := record.Get()
	in2.Columns = []record.Column{{Name: "id"}, {Name: "name"}}
	in2.Rows = [][]interface{}{{"2", "Bob"}}
	out2, err := tf.Apply(in2)
	if err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	if out2.Len() != 1 {
		t.Fatalf("second batch should pass through unchanged (1 row), got %d", out2.Len())
	}
}

// TestUnion_RequiresWith rejects a config with no secondary sources.
func TestUnion_RequiresWith(t *testing.T) {
	if _, err := transform.New("union", map[string]interface{}{}); err == nil {
		t.Fatal("expected an error when 'with' is missing")
	}
}
