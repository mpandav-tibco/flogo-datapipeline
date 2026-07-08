// Package jsonlines registers a JSON-lines (NDJSON) source.
// Each line in the file is a self-contained JSON object. Streams line-by-line
// so memory stays O(batchSize) regardless of file size.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines"
//
// Config:
//
//	{
//	  "type": "jsonlines",
//	  "path": "/data/events.ndjson",
//	  "fields": ["id", "event", "ts", "amount"]
//	}
//
// If "fields" is empty, all keys from the first record are used as columns
// (schema-on-read). Subsequent records may have additional or missing keys —
// missing keys produce nil values.
package jsonlines

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("jsonlines", newJSONLinesSource)
	source.Register("ndjson", newJSONLinesSource)
}

type jsonLinesSource struct {
	file     *os.File
	scanner  *bufio.Scanner
	schema   []record.Column
	fields   []string // ordered field names; nil = infer from first record
	rowsRead int64
	closed   bool
}

func newJSONLinesSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("jsonlines source: 'path' is required")
	}

	var fields []string
	if f, ok := cfg["fields"].([]interface{}); ok {
		for _, v := range f {
			if s, ok := v.(string); ok {
				fields = append(fields, s)
			}
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("jsonlines source: open %q: %w", path, err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4<<20), 4<<20) // 4MB line buffer (JSON records can be large)

	src := &jsonLinesSource{
		file:    f,
		scanner: scanner,
		fields:  fields,
	}

	// If fields declared, build schema immediately.
	if len(fields) > 0 {
		src.schema = make([]record.Column, len(fields))
		for i, name := range fields {
			src.schema[i] = record.Column{Name: name, Type: record.TypeAny}
		}
	}

	return src, nil
}

func (s *jsonLinesSource) Schema() []record.Column { return s.schema }

func (s *jsonLinesSource) Skip(n int64) error {
	for i := int64(0); i < n; i++ {
		if !s.scanner.Scan() {
			return nil
		}
	}
	s.rowsRead = n
	return nil
}

func (s *jsonLinesSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	batch := record.Get()
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
			batch.Columns = s.schema
			return batch, io.EOF
		}

		line := s.scanner.Bytes()
		if len(line) == 0 {
			i-- // skip blank lines
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			// Skip malformed lines.
			continue
		}

		// Infer schema from first record if not declared.
		if s.schema == nil {
			s.inferSchema(obj)
		}

		row := s.extractRow(obj)
		batch.Rows = append(batch.Rows, row)
		s.rowsRead++
	}

	batch.Columns = s.schema
	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	return batch, nil
}

func (s *jsonLinesSource) inferSchema(obj map[string]interface{}) {
	// Stable field order: use insertion order (Go 1.12+ map iteration is random,
	// so we sort keys for determinism).
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	// Simple sort for determinism.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	s.fields = keys
	s.schema = make([]record.Column, len(keys))
	for i, k := range keys {
		s.schema[i] = record.Column{Name: k, Type: record.TypeAny}
	}
}

func (s *jsonLinesSource) extractRow(obj map[string]interface{}) []interface{} {
	row := make([]interface{}, len(s.fields))
	for i, field := range s.fields {
		row[i] = obj[field]
	}
	return row
}

func (s *jsonLinesSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
