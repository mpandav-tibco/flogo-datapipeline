// Package csv registers a CSV/TSV source with the source registry.
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
package csv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/fileutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("csv", newCSVSource)
	source.Register("tsv", func(cfg map[string]interface{}) (source.Source, error) {
		if cfg == nil {
			cfg = map[string]interface{}{}
		}
		cfg["delimiter"] = "\t"
		return newCSVSource(cfg)
	})
}

// csvSource reads CSV files in batches using encoding/csv.
// It streams line-by-line so memory usage is O(batchSize), not O(file size).
type csvSource struct {
	file      io.Closer
	reader    *csv.Reader
	schema    []record.Column
	hasHeader bool
	rowsRead  int64
	closed    bool
}

func newCSVSource(cfg map[string]interface{}) (source.Source, error) {
	delimiter := ","
	if d, ok := cfg["delimiter"].(string); ok && d != "" {
		delimiter = d
	}

	hasHeader := true
	if h, ok := cfg["hasHeader"].(bool); ok {
		hasHeader = h
	}

	f, err := fileutil.OpenInput(cfg, "csv source")
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20)) // 1MB buffer
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	if len([]rune(delimiter)) == 1 {
		r.Comma = []rune(delimiter)[0]
	}

	src := &csvSource{
		file:      f,
		reader:    r,
		hasHeader: hasHeader,
	}

	// Parse column definitions from config.
	cols := parseColumnDefs(cfg)

	if hasHeader {
		headerRow, err := r.Read()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("csv source: read header: %w", err)
		}
		if len(cols) == 0 {
			// Infer schema from header — all columns default to string.
			cols = make([]record.Column, len(headerRow))
			for i, name := range headerRow {
				cols[i] = record.Column{Name: strings.TrimSpace(name), Type: record.TypeString}
			}
		}
	}
	if len(cols) == 0 {
		f.Close()
		return nil, fmt.Errorf("csv source: no column definitions and no header row — provide 'columns' in config")
	}
	src.schema = cols
	return src, nil
}

func (s *csvSource) Schema() []record.Column { return s.schema }

// Skip advances the reader by n rows without emitting them.
// For CSV this is a sequential scan — there is no cheap random seek.
func (s *csvSource) Skip(n int64) error {
	for i := int64(0); i < n; i++ {
		if _, err := s.reader.Read(); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
	s.rowsRead = n
	return nil
}

// ReadBatch reads up to size rows and returns them as a RecordBatch.
func (s *csvSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	batch := record.Get()
	batch.Columns = s.schema
	if cap(batch.Rows) >= size {
		batch.Rows = batch.Rows[:0]
	} else {
		batch.Rows = make([][]interface{}, 0, size)
	}

	for i := 0; i < size; i++ {
		row, err := s.reader.Read()
		if err == io.EOF {
			if len(batch.Rows) == 0 {
				record.Put(batch)
				return nil, io.EOF
			}
			return batch, io.EOF
		}
		if err != nil {
			return batch, err
		}

		parsed := make([]interface{}, len(s.schema))
		for j, col := range s.schema {
			var raw string
			if j < len(row) {
				raw = row[j]
			}
			parsed[j] = coerce(raw, col)
		}
		batch.Rows = append(batch.Rows, parsed)
		s.rowsRead++
	}
	return batch, nil
}

func (s *csvSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

// coerce converts a raw CSV string value to the declared column type.
func coerce(raw string, col record.Column) interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "NULL" || raw == "null" {
		return nil
	}
	switch col.Type {
	case record.TypeInt:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	case record.TypeDecimal:
		// Exact fixed-point so monetary/NUMERIC values keep full precision.
		if d, ok := record.ParseDecimal(raw); ok {
			return d
		}
	case record.TypeFloat:
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			return v
		}
	case record.TypeBool:
		switch strings.ToLower(raw) {
		case "true", "1", "yes", "y":
			return true
		case "false", "0", "no", "n":
			return false
		}
	case record.TypeDate:
		format := col.Format
		if format == "" {
			format = "2006-01-02"
		}
		if t, err := time.Parse(format, raw); err == nil {
			return t
		}
	}
	return raw
}

// parseColumnDefs extracts []record.Column from the config map.
func parseColumnDefs(cfg map[string]interface{}) []record.Column {
	raw, ok := cfg["columns"]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []record.Column:
		return v
	case []interface{}:
		cols := make([]record.Column, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				col := record.Column{}
				if name, ok := m["name"].(string); ok {
					col.Name = name
				}
				if typ, ok := m["type"].(string); ok {
					col.Type = record.ColumnType(typ)
				}
				if col.Type == "" {
					col.Type = record.TypeString
				}
				if format, ok := m["format"].(string); ok {
					col.Format = format
				}
				// JSON unmarshals numbers as float64; accept both for robustness.
				if scale, ok := m["scale"].(int); ok {
					col.Scale = scale
				} else if scale, ok := m["scale"].(float64); ok {
					col.Scale = int(scale)
				}
				cols = append(cols, col)
			}
		}
		return cols
	}
	return nil
}
