// Package sectioned registers a generic multi-section text source: a file made
// of a key/value HEADER block followed by a delimited BODY table. It is a
// building block for legacy / equipment / report file-format conversions
// (semiconductor tool output, mainframe reports, instrument logs, …) where the
// document carries file-level attributes in a header and rows in a table.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/sectioned"
//
// It is deliberately format-agnostic — the DaVinci overlay-metrology file is
// just one shape it handles. Config:
//
//	{
//	  "type": "sectioned",
//	  "path": "in/measurement.txt",
//	  "headerKVRegex": "^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$", // key <2+ spaces> value
//	  "bodyColumnMarker": "^MEASUREMENT\\s",              // line that names the columns
//	  "bodyDelimiter": "\\s+",                            // regex splitting body rows
//	  "broadcastHeader": true                             // add header KV to every row
//	}
//
// Each emitted row carries the body columns plus (when broadcastHeader) every
// header attribute, so a downstream template/mapper sees file-level context.
package sectioned

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("sectioned", newSectionedSource)
}

const defaultHeaderKV = `^(\S[\S ]*?)\s{2,}(.*?)\s*$`

type sectionedSource struct {
	schema []record.Column
	rows   [][]interface{}
	idx    int
}

func str(cfg map[string]interface{}, key, def string) string {
	if v, ok := cfg[key].(string); ok && v != "" {
		return v
	}
	return def
}

func newSectionedSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("sectioned source: 'path' is required")
	}
	colMarker := str(cfg, "bodyColumnMarker", "")
	if colMarker == "" {
		return nil, fmt.Errorf("sectioned source: 'bodyColumnMarker' is required (regex identifying the column-name line)")
	}
	kvRe, err := regexp.Compile(str(cfg, "headerKVRegex", defaultHeaderKV))
	if err != nil {
		return nil, fmt.Errorf("sectioned source: bad headerKVRegex: %w", err)
	}
	markerRe, err := regexp.Compile(colMarker)
	if err != nil {
		return nil, fmt.Errorf("sectioned source: bad bodyColumnMarker: %w", err)
	}
	delimRe, err := regexp.Compile(str(cfg, "bodyDelimiter", `\s+`))
	if err != nil {
		return nil, fmt.Errorf("sectioned source: bad bodyDelimiter: %w", err)
	}
	broadcast := true
	if b, ok := cfg["broadcastHeader"].(bool); ok {
		broadcast = b
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sectioned source: open %q: %w", path, err)
	}
	defer f.Close()

	var headerKeys []string
	header := map[string]string{}
	var bodyCols []string
	var bodyRows [][]string
	inBody := false

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !inBody {
			if markerRe.MatchString(line) {
				// The marker line names the body columns.
				bodyCols = splitFields(delimRe, line)
				inBody = true
				continue
			}
			if m := kvRe.FindStringSubmatch(line); m != nil {
				key := sanitize(m[1])
				if key == "" {
					continue
				}
				if _, seen := header[key]; !seen {
					headerKeys = append(headerKeys, key)
				}
				header[key] = strings.TrimSpace(m[2])
			}
			continue
		}
		// Body: keep only rows whose field count matches the column header.
		fields := splitFields(delimRe, line)
		if len(fields) == len(bodyCols) {
			bodyRows = append(bodyRows, fields)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("sectioned source: read %q: %w", path, err)
	}
	if len(bodyCols) == 0 {
		return nil, fmt.Errorf("sectioned source: bodyColumnMarker %q matched no line in %q", colMarker, path)
	}

	// Build the output schema: body columns first, then broadcast header keys.
	schema := make([]record.Column, 0, len(bodyCols)+len(headerKeys))
	for _, c := range bodyCols {
		schema = append(schema, record.Column{Name: c, Type: record.TypeString})
	}
	if broadcast {
		for _, k := range headerKeys {
			schema = append(schema, record.Column{Name: k, Type: record.TypeString})
		}
	}

	rows := make([][]interface{}, len(bodyRows))
	for i, fr := range bodyRows {
		row := make([]interface{}, 0, len(schema))
		for _, v := range fr {
			row = append(row, v)
		}
		if broadcast {
			for _, k := range headerKeys {
				row = append(row, header[k])
			}
		}
		rows[i] = row
	}

	return &sectionedSource{schema: schema, rows: rows}, nil
}

// splitFields trims the line and splits it on the delimiter, dropping the empty
// leading field that a leading-whitespace row would otherwise produce.
func splitFields(re *regexp.Regexp, line string) []string {
	parts := re.Split(strings.TrimSpace(line), -1)
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// sanitize turns a header key into a clean column name (spaces/hyphens → _).
func sanitize(s string) string {
	s = strings.TrimSpace(s)
	s = strings.NewReplacer(" ", "_", "-", "_", "\t", "_").Replace(s)
	return s
}

func (s *sectionedSource) Schema() []record.Column { return s.schema }

func (s *sectionedSource) Skip(n int64) error {
	s.idx += int(n)
	if s.idx > len(s.rows) {
		s.idx = len(s.rows)
	}
	return nil
}

func (s *sectionedSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.idx >= len(s.rows) {
		return nil, io.EOF
	}
	end := s.idx + size
	if end > len(s.rows) {
		end = len(s.rows)
	}
	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = append(batch.Rows[:0], s.rows[s.idx:end]...)
	s.idx = end
	if s.idx >= len(s.rows) {
		return batch, io.EOF
	}
	return batch, nil
}

func (s *sectionedSource) Close() error { return nil }
