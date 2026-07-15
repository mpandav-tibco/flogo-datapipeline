// Package jsonlines registers a JSON-lines (NDJSON) file sink.
// Each record is written as one compact JSON object per line.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
//
// This sink is used both directly (write NDJSON to a local path) and as the
// inner sink of the object-store sink (format: "jsonlines"), which streams the
// bytes to S3/GCS/ADLS via an io.Pipe.
package jsonlines

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("jsonlines", newJSONLinesSink)
	sink.Register("ndjson", newJSONLinesSink)
}

type jsonlinesSink struct {
	f   *os.File
	w   *bufio.Writer
	enc *json.Encoder
}

func newJSONLinesSink(cfg map[string]interface{}) (sink.Sink, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("jsonlines sink: 'path' is required")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("jsonlines sink: create output directory %q: %w", dir, err)
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("jsonlines sink: create %q: %w", path, err)
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	// json.Encoder.Encode writes a trailing newline after each value, which is
	// exactly the NDJSON framing we want.
	return &jsonlinesSink{f: f, w: w, enc: enc}, nil
}

func (s *jsonlinesSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil {
		return nil
	}
	for i := 0; i < batch.Len(); i++ {
		if err := s.enc.Encode(batch.RowMap(i)); err != nil {
			return fmt.Errorf("jsonlines sink: encode row %d: %w", i, err)
		}
	}
	return nil
}

func (s *jsonlinesSink) Flush() error {
	return s.w.Flush()
}

func (s *jsonlinesSink) Close() error {
	if err := s.w.Flush(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}
