package avro

import (
	"io"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// specialSchema exercises every avroColumnType branch that the avro sink cannot
// itself produce: bytes, timestamp/date logical types, a decimal logical type
// and a nested record.
const specialSchema = `{
  "type":"record","name":"Row","fields":[
    {"name":"id","type":"long"},
    {"name":"data","type":"bytes"},
    {"name":"when","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"day","type":{"type":"int","logicalType":"date"}},
    {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
    {"name":"child","type":{"type":"record","name":"Child","fields":[{"name":"x","type":"long"}]}}
  ]
}`

func writeAvro(t *testing.T, path, schema string, rows []map[string]interface{}) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := ocf.NewEncoder(schema, f)
	if err != nil {
		_ = f.Close()
		t.Fatalf("ocf.NewEncoder: %v", err)
	}
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("encoder close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func specialRows() []map[string]interface{} {
	when := time.Unix(1_700_000_000, 0).UTC()
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	return []map[string]interface{}{
		{"id": int64(1), "data": []byte("hello"), "when": when, "day": day, "amount": big.NewRat(1234, 100), "child": map[string]interface{}{"x": int64(7)}},
		{"id": int64(2), "data": []byte("x"), "when": when, "day": day, "amount": big.NewRat(5, 100), "child": map[string]interface{}{"x": int64(8)}},
		{"id": int64(3), "data": []byte("z"), "when": when, "day": day, "amount": big.NewRat(-50, 100), "child": map[string]interface{}{"x": int64(9)}},
	}
}

func TestAvroSourceSpecialTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "special.avro")
	writeAvro(t, path, specialSchema, specialRows())

	src, err := newAvroSource(map[string]interface{}{"path": path})
	if err != nil {
		t.Fatalf("newAvroSource: %v", err)
	}
	defer src.Close()

	wantTypes := map[string]record.ColumnType{
		"id": record.TypeInt, "data": record.TypeAny, "when": record.TypeDate,
		"day": record.TypeDate, "amount": record.TypeDecimal, "child": record.TypeAny,
	}
	for _, c := range src.Schema() {
		if wantTypes[c.Name] != c.Type {
			t.Errorf("col %s type = %v, want %v", c.Name, c.Type, wantTypes[c.Name])
		}
	}

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if batch.Len() != 3 {
		t.Fatalf("read %d rows, want 3", batch.Len())
	}
	if v := batch.Value(0, "id"); v != int64(1) {
		t.Errorf("id = %v, want 1", v)
	}
	if v := batch.Value(0, "data"); v != "hello" { // []byte normalised to string
		t.Errorf("data = %v (%T), want \"hello\"", v, v)
	}
	if v, ok := batch.Value(0, "amount").(record.Decimal); !ok {
		t.Errorf("amount = %v (%T), want record.Decimal", batch.Value(0, "amount"), batch.Value(0, "amount"))
	} else if want, _ := record.ParseDecimal("12.34"); v.Cmp(want) != 0 {
		t.Errorf("amount = %s, want 12.34", v.String())
	}
	if v := batch.Value(0, "when"); v == nil { // time.Time normalised to a string
		t.Error("when is nil")
	}
	if v := batch.Value(0, "child"); v == nil { // nested record kept as-is
		t.Error("child is nil")
	}
}

func TestAvroSourceSkip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "skip.avro")
	writeAvro(t, path, specialSchema, specialRows())

	src, err := newAvroSource(map[string]interface{}{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if err := src.Skip(2); err != nil {
		t.Fatalf("Skip: %v", err)
	}
	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch after Skip: %v", err)
	}
	if batch.Len() != 1 || batch.Value(0, "id") != int64(3) {
		t.Fatalf("after Skip(2) first id = %v, want 3 (len %d)", batch.Value(0, "id"), batch.Len())
	}
}

func TestAvroSourceErrors(t *testing.T) {
	if _, err := newAvroSource(map[string]interface{}{}); err == nil {
		t.Error("empty path expected error")
	}
	if _, err := newAvroSource(map[string]interface{}{"path": filepath.Join(t.TempDir(), "missing.avro")}); err == nil {
		t.Error("nonexistent file expected error")
	}
	// A file that is not a valid OCF container fails at header parse.
	bad := filepath.Join(t.TempDir(), "garbage.avro")
	if err := os.WriteFile(bad, []byte("this is not avro"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := newAvroSource(map[string]interface{}{"path": bad}); err == nil {
		t.Error("non-OCF file expected error")
	}
}

func TestColumnsFromSchemaNonRecord(t *testing.T) {
	if _, _, err := columnsFromSchema(avro.MustParse(`"string"`)); err == nil {
		t.Fatal("non-record top schema expected error")
	}
}

func TestAvroColumnType(t *testing.T) {
	cases := []struct {
		schema    string
		wantType  record.ColumnType
		wantScale int
	}{
		{`"long"`, record.TypeInt, 0},
		{`"int"`, record.TypeInt, 0},
		{`"string"`, record.TypeString, 0},
		{`"double"`, record.TypeFloat, 0},
		{`"float"`, record.TypeFloat, 0},
		{`"boolean"`, record.TypeBool, 0},
		{`"bytes"`, record.TypeAny, 0},
		{`["null","long"]`, record.TypeInt, 0},
		{`{"type":"int","logicalType":"date"}`, record.TypeDate, 0},
		{`{"type":"long","logicalType":"timestamp-millis"}`, record.TypeDate, 0},
		{`{"type":"int","logicalType":"time-millis"}`, record.TypeDate, 0},
		{`{"type":"bytes","logicalType":"decimal","precision":9,"scale":3}`, record.TypeDecimal, 3},
	}
	for _, c := range cases {
		t.Run(c.schema, func(t *testing.T) {
			ty, scale := avroColumnType(avro.MustParse(c.schema))
			if ty != c.wantType || scale != c.wantScale {
				t.Errorf("avroColumnType(%s) = %v/%d, want %v/%d", c.schema, ty, scale, c.wantType, c.wantScale)
			}
		})
	}
}

func TestNormalizeAvroValue(t *testing.T) {
	if v := normalizeAvroValue(nil); v != nil {
		t.Errorf("nil -> %v", v)
	}
	if v := normalizeAvroValue([]byte("abc")); v != "abc" {
		t.Errorf("[]byte -> %v, want abc", v)
	}
	if v := normalizeAvroValue(big.NewRat(1, 2)); v != "0.5000000000" {
		t.Errorf("*big.Rat -> %v, want 0.5000000000", v)
	}
	tm := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if v := normalizeAvroValue(tm); v != "2024-01-02T03:04:05Z" {
		t.Errorf("time.Time -> %v", v)
	}
	if v := normalizeAvroValue("plain"); v != "plain" {
		t.Errorf("string -> %v", v)
	}
	if v := normalizeAvroValue(int64(5)); v != int64(5) {
		t.Errorf("int64 -> %v", v)
	}
}

func TestAvroDecimalValueFallback(t *testing.T) {
	// A non-*big.Rat value falls back to the generic normaliser.
	if v := avroDecimalValue([]byte("raw"), 2); v != "raw" {
		t.Errorf("avroDecimalValue([]byte) = %v, want raw", v)
	}
	if v := avroDecimalValue(nil, 2); v != nil {
		t.Errorf("avroDecimalValue(nil) = %v, want nil", v)
	}
}
