// Package avro registers an Avro Object Container File (OCF) sink.
// RecordBatches are written to a .avro file whose embedded schema is derived
// from the batch columns, using the standard hamba/avro OCF writer. Every field
// is written as a nullable union ["null", T] so any cell may be null, mirroring
// the optional-column model of the parquet sink.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/avro"
//
// Config:
//
//	{
//	  "type": "avro",
//	  "path": "/output/transactions.avro",
//	  "compression": "snappy",   // snappy (default) | deflate | zstandard | null
//	  "recordName": "Row"        // optional Avro record name
//	}
//
// Avro field names must be valid identifiers ([A-Za-z_][A-Za-z0-9_]*); any other
// character in a column name is replaced with '_' in the written schema.
package avro

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2/ocf"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("avro", newAvroSink)
}

type avroSink struct {
	path        string
	compression ocf.CodecName
	recordName  string
	file        *os.File
	encoder     *ocf.Encoder
	fields      []string            // sanitised avro field names (also map keys)
	types       []record.ColumnType // aligned with fields
	initialized bool
}

func newAvroSink(cfg map[string]interface{}) (sink.Sink, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("avro sink: 'path' is required")
	}

	recordName, _ := cfg["recordName"].(string)
	if recordName == "" {
		recordName = "Row"
	}

	compression, _ := cfg["compression"].(string)

	return &avroSink{
		path:        path,
		compression: resolveCodec(compression),
		recordName:  recordName,
	}, nil
}

// resolveCodec maps a compression name to the hamba OCF codec, defaulting to
// snappy (the same default as the parquet sink) for empty/unrecognised values.
func resolveCodec(name string) ocf.CodecName {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "null", "none", "uncompressed":
		return ocf.Null
	case "deflate", "gzip":
		return ocf.Deflate
	case "zstd", "zstandard":
		return ocf.ZStandard
	default: // "snappy" or unrecognised
		return ocf.Snappy
	}
}

func (s *avroSink) initEncoder(columns []record.Column) error {
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("avro sink: create output directory %q: %w", dir, err)
		}
	}
	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("avro sink: create %q: %w", s.path, err)
	}

	s.fields = make([]string, len(columns))
	s.types = make([]record.ColumnType, len(columns))
	for i, c := range columns {
		s.fields[i] = sanitizeName(c.Name)
		s.types[i] = c.Type
	}

	enc, err := ocf.NewEncoder(buildAvroSchema(s.recordName, s.fields, s.types), f, ocf.WithCodec(s.compression))
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("avro sink: init encoder: %w", err)
	}

	s.file = f
	s.encoder = enc
	s.initialized = true
	return nil
}

func (s *avroSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil {
		return nil
	}
	if !s.initialized {
		if err := s.initEncoder(batch.Columns); err != nil {
			return err
		}
	}

	for i := range batch.Rows {
		row := batch.Rows[i]
		m := make(map[string]interface{}, len(s.fields))
		for j := range s.fields {
			var cell interface{}
			if j < len(row) {
				cell = row[j]
			}
			m[s.fields[j]] = coerceForAvro(s.types[j], cell)
		}
		if err := s.encoder.Encode(m); err != nil {
			return fmt.Errorf("avro sink: encode row %d: %w", i, err)
		}
	}
	return nil
}

func (s *avroSink) Flush() error {
	if s.encoder == nil {
		return nil
	}
	return s.encoder.Flush()
}

func (s *avroSink) Close() error {
	var encErr, fErr error
	if s.encoder != nil {
		encErr = s.encoder.Close() // flushes the final block
	}
	if s.file != nil {
		fErr = s.file.Close()
	}
	return errors.Join(encErr, fErr)
}

// buildAvroSchema renders a record schema JSON string from the sanitised field
// names and their record column types. Each field is a nullable union so any
// value may be null.
func buildAvroSchema(recordName string, fields []string, types []record.ColumnType) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":`)
	b.WriteString(strconv.Quote(sanitizeName(recordName)))
	b.WriteString(`,"fields":[`)
	for i := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"name":`)
		b.WriteString(strconv.Quote(fields[i]))
		b.WriteString(`,"type":["null",`)
		b.WriteString(strconv.Quote(avroTypeFor(types[i])))
		b.WriteString(`],"default":null}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// avroTypeFor maps a record column type to the avro primitive used in the
// written schema. Dates, decimals and untyped values are written as strings to
// preserve their value without depending on avro logical-type support in the
// consumer.
func avroTypeFor(t record.ColumnType) string {
	switch t {
	case record.TypeInt:
		return "long"
	case record.TypeFloat:
		return "double"
	case record.TypeBool:
		return "boolean"
	default: // string, date, decimal, any
		return "string"
	}
}

// coerceForAvro converts a value to the exact Go type the avro schema branch
// expects (long->int64, double->float64, boolean->bool, string->string), so the
// nullable-union encoder resolves the branch unambiguously. A value that cannot
// be coerced becomes nil (the null branch) rather than a bogus zero.
func coerceForAvro(t record.ColumnType, v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch t {
	case record.TypeInt:
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case int32:
			return int64(n)
		case float64:
			return int64(n)
		case float32:
			return int64(n)
		default:
			if i, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprintf("%v", v)), 10, 64); err == nil {
				return i
			}
			return nil
		}
	case record.TypeFloat:
		switch n := v.(type) {
		case float64:
			return n
		case float32:
			return float64(n)
		case int64:
			return float64(n)
		case int:
			return float64(n)
		default:
			if f, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprintf("%v", v)), 64); err == nil {
				return f
			}
			return nil
		}
	case record.TypeBool:
		switch b := v.(type) {
		case bool:
			return b
		case string:
			return strings.EqualFold(strings.TrimSpace(b), "true")
		default:
			return nil
		}
	default: // string / date / decimal / any -> stringify
		if str, ok := v.(string); ok {
			return str
		}
		return fmt.Sprintf("%v", v)
	}
}

// sanitizeName coerces an arbitrary column name into a valid avro name
// ([A-Za-z_][A-Za-z0-9_]*). Invalid characters become '_'; a leading digit is
// prefixed with '_'. An empty name becomes "_".
func sanitizeName(name string) string {
	if name == "" {
		return "_"
	}
	var b strings.Builder
	b.Grow(len(name))
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
