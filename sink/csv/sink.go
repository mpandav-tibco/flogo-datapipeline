// Package csv registers a delimited-text (CSV/TSV) file sink.
// The header row is written from the first batch's column names; every batch
// must share the same schema.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/csv"
//
// Used directly (write CSV to a local path) and as the inner sink of the
// object-store sink (format: "csv").
package csv

import (
	encodingcsv "encoding/csv"
	"fmt"
	"os"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("csv", newCSVSink)
	sink.Register("tsv", func(cfg map[string]interface{}) (sink.Sink, error) {
		if cfg == nil {
			cfg = map[string]interface{}{}
		}
		cfg["delimiter"] = "\t"
		return newCSVSink(cfg)
	})
}

type csvSink struct {
	f         *os.File
	w         *encodingcsv.Writer
	cols      []string
	wroteHead bool
	writeHead bool
}

func newCSVSink(cfg map[string]interface{}) (sink.Sink, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("csv sink: 'path' is required")
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("csv sink: create %q: %w", path, err)
	}
	w := encodingcsv.NewWriter(f)
	if d, ok := cfg["delimiter"].(string); ok && d != "" {
		w.Comma = []rune(d)[0]
	}
	writeHead := true
	if h, ok := cfg["writeHeader"].(bool); ok {
		writeHead = h
	}
	return &csvSink{f: f, w: w, writeHead: writeHead}, nil
}

func (s *csvSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	if s.cols == nil {
		s.cols = make([]string, len(batch.Columns))
		for i, c := range batch.Columns {
			s.cols[i] = c.Name
		}
		if s.writeHead && !s.wroteHead {
			if err := s.w.Write(s.cols); err != nil {
				return fmt.Errorf("csv sink: write header: %w", err)
			}
			s.wroteHead = true
		}
	}
	rec := make([]string, len(s.cols))
	for i := 0; i < batch.Len(); i++ {
		for j, name := range s.cols {
			rec[j] = cellString(batch.Value(i, name))
		}
		if err := s.w.Write(rec); err != nil {
			return fmt.Errorf("csv sink: write row %d: %w", i, err)
		}
	}
	return nil
}

func (s *csvSink) Flush() error {
	s.w.Flush()
	return s.w.Error()
}

func (s *csvSink) Close() error {
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}

func cellString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
