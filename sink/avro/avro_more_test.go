package avro_test

import (
	"io"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// TestAvroMissingPath verifies both the source and sink require a path.
func TestAvroMissingPath(t *testing.T) {
	if _, err := source.New("avro", map[string]interface{}{}); err == nil {
		t.Error("avro source: expected an error when path is missing")
	}
	if _, err := sink.New("avro", map[string]interface{}{}); err == nil {
		t.Error("avro sink: expected an error when path is missing")
	}
}

// TestAvroSourceSkip verifies checkpoint-resume Skip advances the OCF decoder by
// exactly n rows.
func TestAvroSourceSkip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "skip.avro")
	cols := []record.Column{{Name: "id", Type: record.TypeInt}}
	rows := make([][]interface{}, 10)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
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

	if err := src.Skip(4); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	b, err := src.ReadBatch(100)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if b.Len() != 6 {
		t.Fatalf("after Skip(4) read %d rows, want 6", b.Len())
	}
	if got := b.Value(0, "id"); got != int64(4) {
		t.Errorf("first id after skip = %v, want 4", got)
	}
}

// TestAvroSourceLogicalTypes reads a hand-written OCF file with date and decimal
// logical types, exercising the source's logical-type column mapping and value
// normalisation (which the sink-written round-trip files do not cover).
func TestAvroSourceLogicalTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logical.avro")
	schema := `{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"int","logicalType":"date"}},
		{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}}
	]}`

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := ocf.NewEncoder(schema, f)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	amt := big.NewRat(12345, 100) // 123.45
	if err := enc.Encode(map[string]interface{}{"d": day, "amt": amt}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	src, err := source.New("avro", map[string]interface{}{"path": path})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	types := map[string]record.ColumnType{}
	for _, c := range src.Schema() {
		types[c.Name] = c.Type
	}
	if types["d"] != record.TypeDate {
		t.Errorf("column d type = %q, want date", types["d"])
	}
	if types["amt"] != record.TypeDecimal {
		t.Errorf("column amt type = %q, want decimal", types["amt"])
	}

	b, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if b.Len() != 1 {
		t.Fatalf("rows = %d, want 1", b.Len())
	}
	// date -> RFC3339 string; decimal -> typed record.Decimal at the schema scale.
	if s, ok := b.Value(0, "d").(string); !ok || s == "" {
		t.Errorf("d value = %v (%T), want a non-empty string", b.Value(0, "d"), b.Value(0, "d"))
	}
	amtVal, ok := b.Value(0, "amt").(record.Decimal)
	if !ok {
		t.Fatalf("amt value = %v (%T), want record.Decimal", b.Value(0, "amt"), b.Value(0, "amt"))
	}
	want, _ := record.ParseDecimal("123.45")
	if amtVal.Cmp(want) != 0 {
		t.Errorf("amt = %s, want 123.45", amtVal.String())
	}
}
