package csv

import (
	"io"
	"os"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// writeTempCSV writes content to a temp file and returns its path.
// The caller must delete the file when done.
func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "csv_test_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return f.Name()
}

// ─── Parse with header (schema inference) ────────────────────────────────────

func TestCSVSource_WithHeader(t *testing.T) {
	data := "id,name,amount\n1,Alice,10.50\n2,Bob,20.00\n"
	path := writeTempCSV(t, data)
	defer os.Remove(path)

	src, err := newCSVSource(map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("newCSVSource: %v", err)
	}
	defer src.Close()

	schema := src.Schema()
	if len(schema) != 3 {
		t.Fatalf("schema len = %d, want 3", len(schema))
	}
	wantCols := []string{"id", "name", "amount"}
	for i, col := range schema {
		if col.Name != wantCols[i] {
			t.Errorf("schema[%d].Name = %q, want %q", i, col.Name, wantCols[i])
		}
		if col.Type != record.TypeString {
			t.Errorf("schema[%d].Type = %q, want string (inferred)", i, col.Type)
		}
	}

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if batch == nil {
		t.Fatal("expected non-nil batch")
	}
	if batch.Len() != 2 {
		t.Errorf("rows = %d, want 2", batch.Len())
	}
	if batch.Value(0, "name") != "Alice" {
		t.Errorf("row 0 name = %v, want Alice", batch.Value(0, "name"))
	}
	if batch.Value(1, "name") != "Bob" {
		t.Errorf("row 1 name = %v, want Bob", batch.Value(1, "name"))
	}
}

// ─── Skip N rows ──────────────────────────────────────────────────────────────

func TestCSVSource_SkipRows(t *testing.T) {
	data := "id,name\n1,Alice\n2,Bob\n3,Carol\n"
	path := writeTempCSV(t, data)
	defer os.Remove(path)

	src, err := newCSVSource(map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("newCSVSource: %v", err)
	}
	defer src.Close()

	// Skip first 2 data rows.
	if err := src.Skip(2); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch after Skip: %v", err)
	}
	if batch == nil || batch.Len() != 1 {
		rows := 0
		if batch != nil {
			rows = batch.Len()
		}
		t.Fatalf("expected 1 row after skip(2), got %d", rows)
	}
	if batch.Value(0, "name") != "Carol" {
		t.Errorf("expected Carol after skip, got %v", batch.Value(0, "name"))
	}
}

// ─── EOF on empty reader ──────────────────────────────────────────────────────

func TestCSVSource_EOFOnHeaderOnly(t *testing.T) {
	// Header row but no data rows.
	data := "id,name\n"
	path := writeTempCSV(t, data)
	defer os.Remove(path)

	src, err := newCSVSource(map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("newCSVSource: %v", err)
	}
	defer src.Close()

	batch, err := src.ReadBatch(10)
	if err != io.EOF {
		t.Errorf("expected io.EOF for header-only CSV, got err=%v batch=%v", err, batch)
	}
	if batch != nil {
		t.Errorf("expected nil batch for empty CSV, got %v rows", batch.Len())
	}
}

// ─── Schema inference from columns config ─────────────────────────────────────

func TestCSVSource_ExplicitColumnDefs(t *testing.T) {
	data := "1,Alice,100\n2,Bob,200\n"
	path := writeTempCSV(t, data)
	defer os.Remove(path)

	src, err := newCSVSource(map[string]interface{}{
		"path":      path,
		"hasHeader": false,
		"columns": []record.Column{
			{Name: "id", Type: record.TypeInt},
			{Name: "name", Type: record.TypeString},
			{Name: "amount", Type: record.TypeInt},
		},
	})
	if err != nil {
		t.Fatalf("newCSVSource: %v", err)
	}
	defer src.Close()

	schema := src.Schema()
	if len(schema) != 3 {
		t.Fatalf("schema len = %d, want 3", len(schema))
	}
	if schema[0].Type != record.TypeInt {
		t.Errorf("id type = %q, want int", schema[0].Type)
	}

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if batch == nil || batch.Len() != 2 {
		t.Fatalf("expected 2 rows, got %v", batch)
	}
	// id should have been coerced to int64.
	if batch.Value(0, "id") != int64(1) {
		t.Errorf("row 0 id = %v (%T), want int64(1)", batch.Value(0, "id"), batch.Value(0, "id"))
	}
}

// ─── source.Formats() contains "csv" after init ───────────────────────────────

func TestCSVSource_RegisteredFormats(t *testing.T) {
	formats := source.Formats()
	csvFound := false
	tsvFound := false
	for _, f := range formats {
		if f == "csv" {
			csvFound = true
		}
		if f == "tsv" {
			tsvFound = true
		}
	}
	if !csvFound {
		t.Error("'csv' not found in source.Formats()")
	}
	if !tsvFound {
		t.Error("'tsv' not found in source.Formats()")
	}
}

// ─── Missing path error ───────────────────────────────────────────────────────

func TestCSVSource_MissingPathError(t *testing.T) {
	_, err := newCSVSource(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing path, got nil")
	}
}

// ─── Batch size splits correctly ──────────────────────────────────────────────

func TestCSVSource_BatchSizeSplit(t *testing.T) {
	data := "a\n1\n2\n3\n4\n5\n"
	path := writeTempCSV(t, data)
	defer os.Remove(path)

	src, err := newCSVSource(map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("newCSVSource: %v", err)
	}
	defer src.Close()

	// First batch: 3 rows.
	b1, err := src.ReadBatch(3)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch(3): %v", err)
	}
	if b1 == nil || b1.Len() != 3 {
		t.Fatalf("first batch rows = %d, want 3", b1.Len())
	}

	// Second batch: 2 remaining rows.
	b2, err := src.ReadBatch(3)
	if err != io.EOF {
		t.Fatalf("second ReadBatch should return io.EOF, got %v", err)
	}
	if b2 == nil || b2.Len() != 2 {
		t.Fatalf("second batch rows = %d, want 2", b2.Len())
	}
}
