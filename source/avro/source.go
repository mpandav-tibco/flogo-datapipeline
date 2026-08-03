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
			row[j] = normalizeAvroValue(obj[name])
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
		cols[i] = record.Column{Name: f.Name(), Type: columnTypeFor(f.Type())}
	}
	return cols, names, nil
}

// columnTypeFor maps an avro field schema to a record column type, unwrapping a
// nullable union ["null", T] to T and honouring logical types.
func columnTypeFor(s avro.Schema) record.ColumnType {
	if u, ok := s.(*avro.UnionSchema); ok {
		for _, t := range u.Types() {
			if t.Type() != avro.Null {
				return columnTypeFor(t)
			}
		}
		return record.TypeAny
	}

	if ls, ok := s.(avro.LogicalTypeSchema); ok {
		if lg := ls.Logical(); lg != nil {
			lt := string(lg.Type())
			switch {
			case lt == string(avro.Decimal):
				return record.TypeDecimal
			case lt == string(avro.Date) || strings.Contains(lt, "timestamp") || strings.Contains(lt, "time"):
				return record.TypeDate
			}
		}
	}

	switch s.Type() {
	case avro.String:
		return record.TypeString
	case avro.Int, avro.Long:
		return record.TypeInt
	case avro.Float, avro.Double:
		return record.TypeFloat
	case avro.Boolean:
		return record.TypeBool
	default: // bytes, fixed, enum, record, array, map, null
		return record.TypeAny
	}
}

// normalizeAvroValue converts hamba's native decoded values into the plain Go
// types the pipeline expects downstream. hamba decodes avro bytes/fixed as
// []byte, decimal logical types as *big.Rat, and date/timestamp logical types
// as time.Time; these are rendered to strings so generic transforms and text
// sinks handle them uniformly. All other native types (string, int, int64,
// float32, float64, bool) pass through unchanged.
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
