package schema

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func col(name string, typ record.ColumnType) record.Column {
	return record.Column{Name: name, Type: typ}
}

func TestDiff_NoChange(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	cur := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	if changes := Diff(base, cur); changes != nil {
		t.Errorf("expected nil changes, got %v", changes)
	}
}

func TestDiff_Added(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt)}
	cur := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	changes := Diff(base, cur)
	if len(changes) != 1 || changes[0].Kind != "added" || changes[0].Column != "b" {
		t.Fatalf("unexpected changes: %v", changes)
	}
	if changes[0].NewType != record.TypeString {
		t.Errorf("NewType = %v", changes[0].NewType)
	}
}

func TestDiff_Dropped(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	cur := []record.Column{col("a", record.TypeInt)}
	changes := Diff(base, cur)
	if len(changes) != 1 || changes[0].Kind != "dropped" || changes[0].Column != "b" {
		t.Fatalf("unexpected changes: %v", changes)
	}
	if changes[0].OldType != record.TypeString {
		t.Errorf("OldType = %v", changes[0].OldType)
	}
}

func TestDiff_Retyped(t *testing.T) {
	base := []record.Column{col("a", record.TypeInt)}
	cur := []record.Column{col("a", record.TypeString)}
	changes := Diff(base, cur)
	if len(changes) != 1 || changes[0].Kind != "retyped" {
		t.Fatalf("unexpected changes: %v", changes)
	}
	if changes[0].OldType != record.TypeInt || changes[0].NewType != record.TypeString {
		t.Errorf("types wrong: %+v", changes[0])
	}
}

func TestProject_CopyAndFillAndDrop(t *testing.T) {
	src := &record.RecordBatch{
		Columns: []record.Column{col("a", record.TypeInt), col("extra", record.TypeString)},
		Rows:    [][]interface{}{{1, "x"}, {2, "y"}},
	}
	target := []record.Column{col("a", record.TypeInt), col("b", record.TypeString)}
	out := Project(src, target)
	defer record.Put(out)

	if len(out.Columns) != 2 {
		t.Fatalf("columns = %d", len(out.Columns))
	}
	// a copied by name
	if out.Value(0, "a") != 1 || out.Value(1, "a") != 2 {
		t.Errorf("a not copied: %v", out.Rows)
	}
	// b missing in source -> nil
	if out.Value(0, "b") != nil {
		t.Errorf("b should be nil, got %v", out.Value(0, "b"))
	}
	// extra dropped
	if out.ColIndex("extra") != -1 {
		t.Error("extra column should be dropped")
	}
}

func TestProject_OwnsColumnSlice(t *testing.T) {
	src := &record.RecordBatch{Columns: []record.Column{col("a", record.TypeInt)}, Rows: [][]interface{}{{1}}}
	target := []record.Column{col("a", record.TypeInt)}
	out := Project(src, target)
	defer record.Put(out)
	// Mutating the caller's target slice must not affect the output.
	target[0].Name = "mutated"
	if out.Columns[0].Name != "a" {
		t.Error("Project output must own an independent column slice")
	}
}
