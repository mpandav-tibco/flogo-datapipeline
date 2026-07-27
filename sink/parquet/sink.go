// Package parquet registers a Parquet sink that writes RecordBatches as
// row-group-aligned Parquet files. Each Flush() call closes the current row
// group, ensuring files are always valid and readable even if interrupted.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet"
//
// Config:
//
//	{
//	  "type": "parquet",
//	  "path": "/output/transactions.parquet",
//	  "compression": "snappy",
//	  "rowGroupSize": 100000
//	}
package parquet

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

func init() {
	sink.Register("parquet", newParquetSink)
}

type parquetSink struct {
	path         string
	file         *os.File
	writer       *parquetgo.GenericWriter[map[string]interface{}]
	compression  compress.Codec
	rowGroupSize int
	rowsWritten  int
	schema       []record.Column
	initialized  bool
}

// resolveCodec maps a compression name string to the appropriate compress.Codec.
// Returns nil for "uncompressed" / "none", which tells parquet-go to write without compression.
func resolveCodec(name string) compress.Codec {
	switch strings.ToLower(name) {
	case "gzip":
		return &gzip.Codec{}
	case "zstd":
		return &zstd.Codec{}
	case "uncompressed", "none":
		return nil
	default: // "snappy" or any unrecognised value
		return &snappy.Codec{}
	}
}

func newParquetSink(cfg map[string]interface{}) (sink.Sink, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("parquet sink: 'path' is required")
	}

	compressionName, _ := cfg["compression"].(string)
	if compressionName == "" {
		compressionName = "snappy"
	}

	rowGroupSize := 100000
	if rgs, ok := cfg["rowGroupSize"].(int); ok && rgs > 0 {
		rowGroupSize = rgs
	}

	return &parquetSink{
		path:         path,
		compression:  resolveCodec(compressionName),
		rowGroupSize: rowGroupSize,
	}, nil
}

func (s *parquetSink) initWriter(columns []record.Column) error {
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("parquet sink: create output directory %q: %w", dir, err)
		}
	}
	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("parquet sink: create %q: %w", s.path, err)
	}
	s.file = f
	s.schema = columns

	// Build a dynamic parquet schema from column definitions.
	// parquetgo.Field is an interface, not a struct — use parquetgo.Group
	// (map[string]Node) to describe the schema dynamically, then wrap it
	// in a *parquetgo.Schema and pass it as a WriterOption.
	//
	// Each column's parquet node type is derived from its record.ColumnType so
	// numeric/boolean columns keep their type instead of being stringified (which
	// also previously caused a panic when an int64 value met a BYTE_ARRAY node).
	group := make(parquetgo.Group, len(columns))
	for _, col := range columns {
		group[col.Name] = parquetgo.Optional(parquetNodeFor(col.Type))
	}
	schema := parquetgo.NewSchema("schema", group)

	// Collect WriterOptions; only add Compression when a codec is set.
	opts := []parquetgo.WriterOption{
		schema,
		parquetgo.MaxRowsPerRowGroup(int64(s.rowGroupSize)),
	}
	if s.compression != nil {
		opts = append(opts, parquetgo.Compression(s.compression))
	}

	s.writer = parquetgo.NewGenericWriter[map[string]interface{}](f, opts...)
	s.initialized = true
	return nil
}

func (s *parquetSink) WriteBatch(batch *record.RecordBatch) error {
	if !s.initialized {
		if err := s.initWriter(batch.Columns); err != nil {
			return err
		}
	}

	rows := make([]map[string]interface{}, batch.Len())
	for i := range batch.Rows {
		m := make(map[string]interface{}, len(batch.Columns))
		for j, col := range batch.Columns {
			if j < len(batch.Rows[i]) {
				m[col.Name] = coerceForParquet(col.Type, batch.Rows[i][j])
			}
		}
		rows[i] = m
	}

	_, err := s.writer.Write(rows)
	if err != nil {
		return fmt.Errorf("parquet sink: write: %w", err)
	}
	s.rowsWritten += batch.Len()
	return nil
}

// Parquet DECIMAL columns carry a single precision/scale for the whole column.
// We use DECIMAL(38,10) backed by a 16-byte FIXED_LEN_BYTE_ARRAY (128-bit, holds
// 38 digits) — matching the JDBC DDL — so values keep full precision. Each value's
// scale is normalised to 10 fractional digits; values with more fractional digits
// are rounded (half-away-from-zero) and values beyond 38 digits overflow.
const (
	parquetDecimalPrecision = 38
	parquetDecimalScale     = 10
	parquetDecimalBytes     = 16
)

// parquetNodeFor maps a record column type to the appropriate parquet leaf node.
func parquetNodeFor(t record.ColumnType) parquetgo.Node {
	switch t {
	case record.TypeInt:
		return parquetgo.Int(64)
	case record.TypeDecimal:
		return parquetgo.Decimal(parquetDecimalScale, parquetDecimalPrecision, parquetgo.FixedLenByteArrayType(parquetDecimalBytes))
	case record.TypeFloat:
		return parquetgo.Leaf(parquetgo.DoubleType)
	case record.TypeBool:
		return parquetgo.Leaf(parquetgo.BooleanType)
	default: // string, date, any, and unknowns
		return parquetgo.String()
	}
}

// decimalToParquetBytes encodes an exact decimal as its DECIMAL(38,10) unscaled
// coefficient in 16-byte big-endian two's-complement form (the physical layout of
// a FIXED_LEN_BYTE_ARRAY parquet decimal). It returns nil when the value does not
// fit a signed 128-bit coefficient (beyond DECIMAL(38,10) range) so the caller
// writes null rather than a silently corrupted (sign-flipped or truncated) value.
func decimalToParquetBytes(d record.Decimal) []byte {
	coef := d.Coef(parquetDecimalScale)
	if coef.BitLen() > parquetDecimalBytes*8-1 {
		return nil
	}
	return bigIntToTwosComplement(coef, parquetDecimalBytes)
}

// bigIntToTwosComplement renders bi as a fixed-size big-endian two's-complement
// byte slice. Values that do not fit are truncated to the low `size` bytes.
func bigIntToTwosComplement(bi *big.Int, size int) []byte {
	out := make([]byte, size)
	if bi.Sign() >= 0 {
		b := bi.Bytes()
		if len(b) > size {
			b = b[len(b)-size:]
		}
		copy(out[size-len(b):], b)
		return out
	}
	// Negative: 2^(8·size) + bi yields the two's-complement magnitude.
	mod := new(big.Int).Lsh(big.NewInt(1), uint(8*size))
	tc := new(big.Int).Add(mod, bi)
	b := tc.Bytes()
	if len(b) > size {
		b = b[len(b)-size:]
	}
	copy(out[size-len(b):], b)
	return out
}

// coerceForParquet converts a value to the Go type the parquet schema node
// expects, so a mismatch (e.g. a string "42" for an Int column, or an int for a
// String column) never panics inside the writer. nil is passed through (the
// column is Optional).
func coerceForParquet(t record.ColumnType, v interface{}) interface{} {
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
			return int64(0)
		}
	case record.TypeDecimal:
		d, ok := record.ToDecimal(v)
		if !ok {
			return nil
		}
		if b := decimalToParquetBytes(d); b != nil {
			return b
		}
		return nil // out-of-range → null (never a corrupt value)
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
			return float64(0)
		}
	case record.TypeBool:
		switch b := v.(type) {
		case bool:
			return b
		case string:
			return strings.EqualFold(strings.TrimSpace(b), "true")
		default:
			return false
		}
	default: // string / date / any → stringify
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
}

func (s *parquetSink) Flush() error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Flush()
}

func (s *parquetSink) Close() error {
	var wErr, fErr error
	if s.writer != nil {
		wErr = s.writer.Close()
	}
	if s.file != nil {
		fErr = s.file.Close()
	}
	return errors.Join(wErr, fErr)
}
