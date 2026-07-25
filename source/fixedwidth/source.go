// Package fixedwidth registers a fixed-width flat-file source.
// Handles mainframe output (EBCDIC or ASCII), positional column layouts,
// and implied-decimal fields common in banking and insurance data.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/fixedwidth"
//
// Config:
//
//	{
//	  "type": "fixedwidth",
//	  "path": "/data/transactions.dat",
//	  "encoding": "ebcdic-037",
//	  "skipLines": 1,
//	  "recordLength": 120,
//	  "columns": [
//	    { "name": "accountId", "start": 1, "length": 10, "type": "string", "trim": true },
//	    { "name": "amount",    "start": 11, "length": 12, "type": "decimal", "impliedDecimal": 2 },
//	    { "name": "postDate",  "start": 23, "length": 8,  "type": "date", "format": "20060102" }
//	  ]
//	}
package fixedwidth

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("fixedwidth", newFixedWidthSource)
	source.Register("flatfile", newFixedWidthSource)
}

// fwColumn extends record.Column with positional metadata.
type fwColumn struct {
	record.Column
	start          int // 1-based byte start position
	length         int // byte length
	trim           bool
	impliedDecimal int // e.g. 2 means divide the integer value by 100
	format         string
}

type fixedWidthSource struct {
	file         *os.File
	scanner      *bufio.Scanner
	schema       []record.Column
	columns      []fwColumn
	encoding     string
	recordLength int
	rowsRead     int64
	closed       bool
}

func newFixedWidthSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("fixedwidth source: 'path' is required")
	}

	encoding, _ := cfg["encoding"].(string)
	if encoding == "" {
		encoding = "utf-8"
	}

	skipLines := 0
	if sl, ok := cfg["skipLines"].(int); ok {
		skipLines = sl
	}

	recordLength := 0
	if rl, ok := cfg["recordLength"].(int); ok {
		recordLength = rl
	}

	cols, err := parseFixedWidthColumns(cfg)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fixedwidth source: open %q: %w", path, err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	// Skip header/trailer lines.
	for i := 0; i < skipLines; i++ {
		if !scanner.Scan() {
			break
		}
	}

	schema := make([]record.Column, len(cols))
	for i, c := range cols {
		schema[i] = c.Column
	}

	return &fixedWidthSource{
		file:         f,
		scanner:      scanner,
		schema:       schema,
		columns:      cols,
		encoding:     encoding,
		recordLength: recordLength,
	}, nil
}

func (s *fixedWidthSource) Schema() []record.Column { return s.schema }

func (s *fixedWidthSource) Skip(n int64) error {
	for i := int64(0); i < n; i++ {
		if !s.scanner.Scan() {
			return nil
		}
	}
	s.rowsRead = n
	return nil
}

func (s *fixedWidthSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, 0, size)

	for i := 0; i < size; i++ {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return batch, err
			}
			if len(batch.Rows) == 0 {
				record.Put(batch)
				return nil, io.EOF
			}
			return batch, io.EOF
		}
		line := s.scanner.Bytes()
		if s.encoding == "ebcdic-037" {
			line = ebcdicToASCII(line)
		}
		row, err := s.parseLine(string(line))
		if err != nil {
			// Log and skip malformed lines rather than aborting.
			continue
		}
		batch.Rows = append(batch.Rows, row)
		s.rowsRead++
	}

	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	return batch, nil
}

func (s *fixedWidthSource) parseLine(line string) ([]interface{}, error) {
	// Pad line to expected record length if shorter.
	if s.recordLength > 0 && len(line) < s.recordLength {
		line = line + strings.Repeat(" ", s.recordLength-len(line))
	}

	row := make([]interface{}, len(s.columns))
	for i, col := range s.columns {
		start := col.start - 1 // convert to 0-based
		end := start + col.length
		if start < 0 || start >= len(line) {
			row[i] = nil
			continue
		}
		if end > len(line) {
			end = len(line)
		}
		raw := line[start:end]
		if col.trim {
			raw = strings.TrimSpace(raw)
		}
		row[i] = coerceFixed(raw, col)
	}
	return row, nil
}

func (s *fixedWidthSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

func coerceFixed(raw string, col fwColumn) interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	switch col.Type {
	case record.TypeInt:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	case record.TypeDecimal, record.TypeFloat:
		if col.impliedDecimal > 0 {
			// Implied decimal: treat as integer, divide by 10^scale.
			if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
				divisor := int64(1)
				for i := 0; i < col.impliedDecimal; i++ {
					divisor *= 10
				}
				return float64(v) / float64(divisor)
			}
		}
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			return v
		}
	case record.TypeDate:
		format := col.format
		if format == "" {
			format = "20060102"
		}
		if t, err := time.Parse(format, raw); err == nil {
			return t
		}
	case record.TypeBool:
		switch strings.ToUpper(raw) {
		case "Y", "1", "T", "TRUE":
			return true
		case "N", "0", "F", "FALSE":
			return false
		}
	}
	return raw
}

func parseFixedWidthColumns(cfg map[string]interface{}) ([]fwColumn, error) {
	raw, ok := cfg["columns"]
	if !ok {
		return nil, fmt.Errorf("fixedwidth source: 'columns' is required")
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("fixedwidth source: 'columns' must be an array")
	}
	cols := make([]fwColumn, 0, len(items))
	for i, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("fixedwidth source: column[%d] must be an object", i)
		}
		col := fwColumn{}
		if name, ok := m["name"].(string); ok {
			col.Name = name
		}
		if typ, ok := m["type"].(string); ok {
			col.Type = record.ColumnType(typ)
		} else {
			col.Type = record.TypeString
		}
		if start, ok := m["start"].(int); ok {
			col.start = start
		} else if start, ok := m["start"].(float64); ok {
			col.start = int(start)
		}
		if length, ok := m["length"].(int); ok {
			col.length = length
		} else if length, ok := m["length"].(float64); ok {
			col.length = int(length)
		}
		if trim, ok := m["trim"].(bool); ok {
			col.trim = trim
		}
		if id, ok := m["impliedDecimal"].(int); ok {
			col.impliedDecimal = id
		} else if id, ok := m["impliedDecimal"].(float64); ok {
			col.impliedDecimal = int(id)
		}
		if format, ok := m["format"].(string); ok {
			col.format = format
		}
		// Positional fields are 1-based with a positive width. Validate here (a
		// config boundary) so a typo surfaces as a clear error instead of a
		// per-row slice-bounds panic (negative length) or silent nulls (start<1).
		if col.start < 1 {
			return nil, fmt.Errorf("fixedwidth source: column[%d] %q: 'start' must be >= 1 (1-based byte position), got %d", i, col.Name, col.start)
		}
		if col.length < 1 {
			return nil, fmt.Errorf("fixedwidth source: column[%d] %q: 'length' must be >= 1, got %d", i, col.Name, col.length)
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// ebcdicToASCII converts EBCDIC-037 bytes to ASCII.
// This table covers the standard IBM EBCDIC code page 037 used in North
// American mainframe environments (COBOL COMP-3, packed decimal output).
func ebcdicToASCII(in []byte) []byte {
	out := make([]byte, len(in))
	for i, b := range in {
		if int(b) < len(ebcdic037) {
			out[i] = ebcdic037[b]
		} else {
			out[i] = '?'
		}
	}
	return out
}

// ebcdic037 is the EBCDIC code page 037 to ASCII translation table.
var ebcdic037 = [256]byte{
	0x00, 0x01, 0x02, 0x03, 0x9C, 0x09, 0x86, 0x7F, 0x97, 0x8D, 0x8E, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
	0x10, 0x11, 0x12, 0x13, 0x9D, 0x0A, 0x08, 0x87, 0x18, 0x19, 0x92, 0x8F, 0x1C, 0x1D, 0x1E, 0x1F,
	0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x17, 0x1B, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x05, 0x06, 0x07,
	0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04, 0x98, 0x99, 0x9A, 0x9B, 0x14, 0x15, 0x9E, 0x1A,
	0x20, 0xA0, 0xE2, 0xE4, 0xE0, 0xE1, 0xE3, 0xE5, 0xE7, 0xF1, 0xA2, 0x2E, 0x3C, 0x28, 0x2B, 0x7C,
	0x26, 0xE9, 0xEA, 0xEB, 0xE8, 0xED, 0xEE, 0xEF, 0xEC, 0xDF, 0x21, 0x24, 0x2A, 0x29, 0x3B, 0x5E,
	0x2D, 0x2F, 0xC2, 0xC4, 0xC0, 0xC1, 0xC3, 0xC5, 0xC7, 0xD1, 0xA6, 0x2C, 0x25, 0x5F, 0x3E, 0x3F,
	0xF8, 0xC9, 0xCA, 0xCB, 0xC8, 0xCD, 0xCE, 0xCF, 0xCC, 0x60, 0x3A, 0x23, 0x40, 0x27, 0x3D, 0x22,
	0xD8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0xAB, 0xBB, 0xF0, 0xFD, 0xFE, 0xB1,
	0xB0, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0xAA, 0xBA, 0xE6, 0xB8, 0xC6, 0xA4,
	0xB5, 0x7E, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0xA1, 0xBF, 0xD0, 0x5B, 0xDE, 0xAE,
	0xAC, 0xA3, 0xA5, 0xB7, 0xA9, 0xA7, 0xB6, 0xBC, 0xBD, 0xBE, 0xDD, 0xA8, 0xAF, 0x5D, 0xB4, 0xD7,
	0x7B, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0xAD, 0xF4, 0xF6, 0xF2, 0xF3, 0xF5,
	0x7D, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0xB9, 0xFB, 0xFC, 0xF9, 0xFA, 0xFF,
	0x5C, 0xF7, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0xB2, 0xD4, 0xD6, 0xD2, 0xD3, 0xD5,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0xB3, 0xDB, 0xDC, 0xD9, 0xDA, 0x9F,
}
