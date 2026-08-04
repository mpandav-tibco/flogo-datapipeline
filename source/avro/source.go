// Package avro registers an Avro Object Container File (OCF) source.
// Each .avro file embeds its own writer schema in the header; that schema drives
// the columns (schema-on-read). The file is streamed block-by-block via the
// standard hamba/avro OCF reader, so memory stays O(batchSize) regardless of
// file size.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/avro"
//
// Config:
//
//	{
//	  "type": "avro",
//	  "path": "/data/events.avro"
//	}
//
// Column types are inferred from the embedded schema: nullable unions
// ["null", T] are unwrapped to T, logical types (date, timestamp, decimal) map
// to the corresponding record column type, and everything else maps by its
// primitive avro type. Nested records/arrays/maps degrade to TypeAny.
package avro

import (
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("avro", newAvroSource)
}

type avroSource struct {
	file    *os.File
	decoder *ocf.Decoder
	schema  []record.Column
	fields  []string // ordered field names aligned with schema
	closed  bool
}

func newAvroSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("avro source: 'path' is required")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("avro source: open %q: %w", path, err)
	}

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("avro source: read OCF header %q: %w", path, err)
	}

	cols, fields, err := columnsFromSchema(dec.Schema())
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("avro source: %q: %w", path, err)
	}

	return &avroSource{
		file:    f,
		decoder: dec,
		schema:  cols,
		fields:  fields,
	}, nil
}

func (s *avroSource) Schema() []record.Column { return s.schema }

func (s *avroSource) Skip(n int64) error {
	for i := int64(0); i < n; i++ {
		if !s.decoder.HasNext() {
			return s.decoder.Error()
		}
		scratch := make(map[string]interface{})
		if err := s.decoder.Decode(&scratch); err != nil {
			return fmt.Errorf("avro source: skip decode: %w", err)
		}
	}
	return nil
}

func (s *avroSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, 0, size)

	for i := 0; i < size; i++ {
		if !s.decoder.HasNext() {
			if err := s.decoder.Error(); err != nil {
				return batch, fmt.Errorf("avro source: decode block: %w", err)
			}
			if len(batch.Rows) == 0 {
				record.Put(batch)
				return nil, io.EOF
			}
			return batch, io.EOF
		}

		obj := make(map[string]interface{}, len(s.fields))
		if err := s.decoder.Decode(&obj); err != nil {
			return batch, fmt.Errorf("avro source: decode record: %w", err)
		}

		row := make([]interface{}, len(s.fields))
		for j, name := range s.fields {
			if s.schema[j].Type == record.TypeDecimal {
				row[j] = avroDecimalValue(obj[name], s.schema[j].Scale)
			} else {
				row[j] = normalizeAvroValue(obj[name])
			}
		}
		batch.Rows = append(batch.Rows, row)
	}

	return batch, nil
}

func (s *avroSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

// columnsFromSchema derives the ordered record columns from an OCF writer
// schema. The top-level schema of an OCF file is always a record; each field
// becomes a column whose type is inferred from the (possibly nullable-union,
// possibly logical) avro field type.
func columnsFromSchema(s avro.Schema) ([]record.Column, []string, error) {
	rec, ok := s.(*avro.RecordSchema)
	if !ok {
		return nil, nil, fmt.Errorf("expected a top-level record schema, got %s", s.Type())
	}
	flds := rec.Fields()
	cols := make([]record.Column, len(flds))
	names := make([]string, len(flds))
	for i, f := range flds {
		names[i] = f.Name()
		t, scale := avroColumnType(f.Type())
		cols[i] = record.Column{Name: f.Name(), Type: t, Scale: scale}
	}
	return cols, names, nil
}

// avroColumnType maps an avro field schema to a record column type (and, for a
// decimal logical type, its scale), unwrapping a nullable union ["null", T] to T
// and honouring logical types.
func avroColumnType(s avro.Schema) (record.ColumnType, int) {
	if u, ok := s.(*avro.UnionSchema); ok {
		for _, t := range u.Types() {
			if t.Type() != avro.Null {
				return avroColumnType(t)
			}
		}
		return record.TypeAny, 0
	}

	if ls, ok := s.(avro.LogicalTypeSchema); ok {
		if lg := ls.Logical(); lg != nil {
			if dec, ok := lg.(*avro.DecimalLogicalSchema); ok {
				return record.TypeDecimal, dec.Scale()
			}
			lt := string(lg.Type())
			if lt == string(avro.Date) || strings.Contains(lt, "timestamp") || strings.Contains(lt, "time") {
				return record.TypeDate, 0
			}
		}
	}

	switch s.Type() {
	case avro.String:
		return record.TypeString, 0
	case avro.Int, avro.Long:
		return record.TypeInt, 0
	case avro.Float, avro.Double:
		return record.TypeFloat, 0
	case avro.Boolean:
		return record.TypeBool, 0
	default: // bytes, fixed, enum, record, array, map, null
		return record.TypeAny, 0
	}
}

// normalizeAvroValue converts hamba's native decoded values into the plain Go
// types the pipeline expects downstream. hamba decodes avro bytes/fixed as
// []byte and date/timestamp logical types as time.Time; these are rendered to
// strings so generic transforms and text sinks handle them uniformly. Decimal
// logical values are handled by avroDecimalValue (typed record.Decimal), not
// here. All other native types (string, int, int64, float32, float64, bool)
// pass through unchanged; a stray *big.Rat (decimal outside a decimal column)
// degrades to a 10-dp string.
func normalizeAvroValue(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(t)
	case *big.Rat:
		return t.FloatString(10)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

// avroDecimalValue converts a decoded decimal-logical value (hamba yields a
// *big.Rat) into an exact record.Decimal at the column scale, so a TypeDecimal
// column carries a Decimal value rather than a stringified approximation. A nil
// stays nil; a non-Rat value falls back to the generic normaliser.
func avroDecimalValue(v interface{}, scale int) interface{} {
	r, ok := v.(*big.Rat)
	if !ok {
		return normalizeAvroValue(v)
	}
	if d, ok := record.ParseDecimal(r.FloatString(scale)); ok {
		return d
	}
	return r.FloatString(scale)
}
