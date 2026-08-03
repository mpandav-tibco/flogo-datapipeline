package avro_test

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/avro"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/avro"
)

func codecLabel(c string) string {
	if c == "" {
		return "default"
	}
	return c
}

// TestAvroRoundTrip writes a batch through the avro sink and reads it back
// through the avro source for every supported codec, asserting types, values
// and nullable-union round-tripping all survive.
func TestAvroRoundTrip(t *testing.T) {
	for _, codec := range []string{"", "null", "deflate", "snappy", "zstandard"} {
		codec := codec
		t.Run("codec="+codecLabel(codec), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.avro")

			cols := []record.Column{
				{Name: "id", Type: record.TypeInt},
				{Name: "amount", Type: record.TypeFloat},
				{Name: "active", Type: record.TypeBool},
				{Name: "name", Type: record.TypeString},
			}
			rows := [][]interface{}{
				{int64(1), 10.5, true, "alice"},
				{int64(2), 20.0, false, "bob"},
				{nil, nil, nil, nil}, // all-null row exercises the nullable union
			}

			sk, err := sink.New("avro", map[string]interface{}{"path": path, "compression": codec})
			if err != nil {
				t.Fatalf("sink.New: %v", err)
			}
			if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: rows}); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}
			if err := sk.Close(); err != nil {
				t.Fatalf("sink Close: %v", err)
			}

			src, err := source.New("avro", map[string]interface{}{"path": path})
			if err != nil {
				t.Fatalf("source.New: %v", err)
			}
			defer src.Close()

			gotCols := src.Schema()
			if len(gotCols) != len(cols) {
				t.Fatalf("schema len = %d, want %d", len(gotCols), len(cols))
			}
			for i, c := range cols {
				if gotCols[i].Name != c.Name {
					t.Errorf("col %d name = %q, want %q", i, gotCols[i].Name, c.Name)
				}
				if gotCols[i].Type != c.Type {
					t.Errorf("col %d (%s) type = %q, want %q", i, c.Name, gotCols[i].Type, c.Type)
				}
			}

			batch, err := src.ReadBatch(10)
			if err != nil && err != io.EOF {
				t.Fatalf("ReadBatch: %v", err)
			}
			if batch.Len() != 3 {
				t.Fatalf("read %d rows, want 3", batch.Len())
			}

			if got := batch.Value(0, "id"); got != int64(1) {
				t.Errorf("row0 id = %v (%T), want int64(1)", got, got)
			}
			if got := batch.Value(0, "amount"); got != 10.5 {
				t.Errorf("row0 amount = %v, want 10.5", got)
			}
			if got := batch.Value(0, "active"); got != true {
				t.Errorf("row0 active = %v, want true", got)
			}
			if got := batch.Value(0, "name"); got != "alice" {
				t.Errorf("row0 name = %v, want alice", got)
			}
			if got := batch.Value(1, "id"); got != int64(2) {
				t.Errorf("row1 id = %v, want int64(2)", got)
			}
			for _, c := range []string{"id", "amount", "active", "name"} {
				if got := batch.Value(2, c); got != nil {
					t.Errorf("row2 %s = %v, want nil", c, got)
				}
			}

			// A subsequent read drains the source to EOF.
			if _, err := src.ReadBatch(10); err != io.EOF {
				t.Errorf("second ReadBatch err = %v, want io.EOF", err)
			}
		})
	}
}

// TestAvroFieldNameSanitized verifies a column name that is not a valid avro
// identifier is written (and read back) with invalid characters replaced.
func TestAvroFieldNameSanitized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.avro")
	cols := []record.Column{{Name: "order id", Type: record.TypeInt}}
	rows := [][]interface{}{{int64(7)}}

	sk, err := sink.New("avro", map[string]interface{}{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: rows}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Close(); err != nil {
		t.Fatal(err)
	}

	src, err := source.New("avro", map[string]interface{}{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	got := src.Schema()
	if len(got) != 1 || got[0].Name != "order_id" {
		t.Fatalf("schema = %+v, want single column order_id", got)
	}
	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if v := batch.Value(0, "order_id"); v != int64(7) {
		t.Errorf("order_id = %v, want int64(7)", v)
	}
}

// TestAvroStringCoercion checks that non-string values in a string column and
// mismatched numeric strings are coerced to the schema branch type rather than
// failing the strict avro encoder.
func TestAvroStringCoercion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.avro")
	cols := []record.Column{
		{Name: "n", Type: record.TypeInt},
		{Name: "s", Type: record.TypeString},
	}
	rows := [][]interface{}{
		{"42", 99}, // int-as-string into TypeInt; int into TypeString
	}

	sk, err := sink.New("avro", map[string]interface{}{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	if err := sk.WriteBatch(&record.RecordBatch{Columns: cols, Rows: rows}); err != nil {
		t.Fatal(err)
	}
	if err := sk.Close(); err != nil {
		t.Fatal(err)
	}

	src, err := source.New("avro", map[string]interface{}{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if v := batch.Value(0, "n"); v != int64(42) {
		t.Errorf("n = %v (%T), want int64(42)", v, v)
	}
	if v := batch.Value(0, "s"); v != "99" {
		t.Errorf("s = %v (%T), want \"99\"", v, v)
	}
}
