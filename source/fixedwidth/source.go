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

	"golang.org/x/text/encoding/charmap"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("fixedwidth", newFixedWidthSource)
	source.Register("flatfile", newFixedWidthSource)
}

// encodingCharmaps maps a config encoding name to a golang.org/x/text character
// map. Adding a new code page is a single line here — there are no hand-maintained
// byte tables to keep in sync. Names are matched case-insensitively.
var encodingCharmaps = map[string]*charmap.Charmap{
	"ebcdic":       charmap.CodePage037, // bare "ebcdic" defaults to US/Canada (037)
	"ebcdic-037":   charmap.CodePage037,
	"cp037":        charmap.CodePage037,
	"ebcdic-1047":  charmap.CodePage1047, // Open Systems / Latin-1
	"cp1047":       charmap.CodePage1047,
	"ebcdic-1140":  charmap.CodePage1140, // US/Canada + Euro
	"cp1140":       charmap.CodePage1140,
	"latin-1":      charmap.ISO8859_1,
	"iso-8859-1":   charmap.ISO8859_1,
	"latin-9":      charmap.ISO8859_15,
	"iso-8859-15":  charmap.ISO8859_15,
	"windows-1252": charmap.Windows1252,
	"cp1252":       charmap.Windows1252,
}

// resolveEncoding returns the character map for the named encoding, or nil when
// the bytes are already UTF-8/ASCII (no decoding needed). Unknown names error.
func resolveEncoding(name string) (*charmap.Charmap, error) {
	key := strings.ToLower(strings.TrimSpace(name))
	switch key {
	case "", "utf-8", "utf8", "ascii", "us-ascii":
		return nil, nil
	}
	if cm, ok := encodingCharmaps[key]; ok {
		return cm, nil
	}
	return nil, fmt.Errorf("fixedwidth source: unsupported encoding %q (supported: utf-8, ascii, ebcdic-037, ebcdic-1047, ebcdic-1140, latin-1, latin-9, windows-1252)", name)
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
	cmap         *charmap.Charmap // resolved decoder for non-UTF-8 encodings; nil = UTF-8/ASCII passthrough
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
	cmap, err := resolveEncoding(encoding)
	if err != nil {
		return nil, err
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
		cmap:         cmap,
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
		row, err := s.parseLine(s.scanner.Bytes())
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

// parseLine slices each column from the raw record bytes by BYTE position, then
// decodes that field from the configured encoding. Slicing before decoding keeps
// fixed positions correct even when one source byte decodes to a multi-byte UTF-8
// rune (accented EBCDIC/Latin characters).
func (s *fixedWidthSource) parseLine(line []byte) ([]interface{}, error) {
	// Pad a short record to recordLength so trailing columns still resolve. Pad
	// with the encoding's own space byte (e.g. 0x40 in EBCDIC) so it decodes to ' '.
	if s.recordLength > 0 && len(line) < s.recordLength {
		pad := byte(' ')
		if s.cmap != nil {
			if b, ok := s.cmap.EncodeRune(' '); ok {
				pad = b
			}
		}
		padded := make([]byte, s.recordLength)
		copy(padded, line)
		for i := len(line); i < len(padded); i++ {
			padded[i] = pad
		}
		line = padded
	}

	row := make([]interface{}, len(s.columns))
	for i, col := range s.columns {
		start := col.start - 1 // convert to 0-based byte offset
		end := start + col.length
		if start < 0 || start >= len(line) {
			row[i] = nil
			continue
		}
		if end > len(line) {
			end = len(line)
		}
		raw := s.decodeField(line[start:end])
		if col.trim {
			raw = strings.TrimSpace(raw)
		}
		row[i] = coerceFixed(raw, col)
	}
	return row, nil
}

// decodeField converts a field's raw bytes to a UTF-8 string using the source's
// character map. A nil map (utf-8/ascii) passes the bytes through unchanged.
func (s *fixedWidthSource) decodeField(b []byte) string {
	if s.cmap == nil {
		return string(b)
	}
	var sb strings.Builder
	sb.Grow(len(b))
	for _, c := range b {
		sb.WriteRune(s.cmap.DecodeByte(c))
	}
	return sb.String()
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
	case record.TypeDecimal:
		// Exact fixed-point. Implied-decimal (COBOL-style) columns scale an integer
		// field by 10^-impliedDecimal with no float rounding.
		if col.impliedDecimal > 0 {
			if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
				return record.NewDecimal(v, int32(col.impliedDecimal))
			}
		}
		if d, ok := record.ParseDecimal(raw); ok {
			return d
		}
	case record.TypeFloat:
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
