// Package template registers a generic text-template sink. It renders buffered
// rows through a Go text/template, producing arbitrary custom text output —
// fixed-width reports, legacy instrument formats (e.g. semiconductor MC300/KLM),
// EDI-like layouts — that columnar/JDBC/Kafka sinks cannot express.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/template"
//
// Config:
//
//	{
//	  "type": "template",
//	  "path": "out/{{index .Attr \"WaferID\"}}.mc300", // output path (templated on header attrs)
//	  "template": "<Go text/template string>",         // or "templatePath": "layout.tmpl"
//	}
//
// The template is executed once with:
//
//	.Rows    []map[string]interface{}  every row as a name→value map
//	.Count   int                       len(.Rows) — handy for header row counts
//	.Columns []string                  column names in order
//	.Attr    map[string]interface{}    the first row (file-level/header attributes)
//
// Helper funcs available in the template: rpad, lpad, padc, sci, upper, lower,
// trim, add, seq — enough to reproduce fixed-width + scientific-notation layouts.
package template

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	texttemplate "text/template"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("template", newTemplateSink)
}

var helpers = texttemplate.FuncMap{
	"rpad":  func(w int, v interface{}) string { return pad(toStr(v), w, ' ', false) },
	"lpad":  func(w int, v interface{}) string { return pad(toStr(v), w, ' ', true) },
	"padc":  func(w int, c, v interface{}) string { return pad(toStr(v), w, firstRune(toStr(c)), true) },
	"sci":   func(v interface{}) string { return sci(v) },
	"upper": func(v interface{}) string { return strings.ToUpper(toStr(v)) },
	"lower": func(v interface{}) string { return strings.ToLower(toStr(v)) },
	"trim":  func(v interface{}) string { return strings.TrimSpace(toStr(v)) },
	"add":   func(a, b int) int { return a + b },
	"seq": func(n int) []int {
		s := make([]int, n)
		for i := range s {
			s[i] = i
		}
		return s
	},
}

type templateSink struct {
	pathTmpl *texttemplate.Template
	bodyTmpl *texttemplate.Template
	columns  []string
	rows     []map[string]interface{}
	done     bool
}

func newTemplateSink(cfg map[string]interface{}) (sink.Sink, error) {
	pathSpec, _ := cfg["path"].(string)
	if pathSpec == "" {
		return nil, fmt.Errorf("template sink: 'path' is required")
	}
	body, _ := cfg["template"].(string)
	if tp, ok := cfg["templatePath"].(string); ok && tp != "" {
		b, err := os.ReadFile(tp)
		if err != nil {
			return nil, fmt.Errorf("template sink: read templatePath: %w", err)
		}
		body = string(b)
	}
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("template sink: 'template' (or 'templatePath') is required")
	}
	bodyTmpl, err := texttemplate.New("body").Funcs(helpers).Parse(body)
	if err != nil {
		return nil, fmt.Errorf("template sink: parse template: %w", err)
	}
	pathTmpl, err := texttemplate.New("path").Funcs(helpers).Parse(pathSpec)
	if err != nil {
		return nil, fmt.Errorf("template sink: parse path template: %w", err)
	}
	return &templateSink{pathTmpl: pathTmpl, bodyTmpl: bodyTmpl}, nil
}

func (s *templateSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	if s.columns == nil {
		s.columns = make([]string, len(batch.Columns))
		for i, c := range batch.Columns {
			s.columns[i] = c.Name
		}
	}
	for i := range batch.Rows {
		s.rows = append(s.rows, batch.RowMap(i))
	}
	return nil
}

// Flush is a no-op; the template needs all rows (e.g. for header counts), so
// rendering happens once at Close.
func (s *templateSink) Flush() error { return nil }

func (s *templateSink) Close() error {
	if s.done {
		return nil
	}
	s.done = true

	data := map[string]interface{}{
		"Rows":    s.rows,
		"Count":   len(s.rows),
		"Columns": s.columns,
		"Attr":    map[string]interface{}{},
	}
	if len(s.rows) > 0 {
		data["Attr"] = s.rows[0]
	}

	var pathBuf bytes.Buffer
	if err := s.pathTmpl.Execute(&pathBuf, data); err != nil {
		return fmt.Errorf("template sink: render path: %w", err)
	}
	outPath := strings.TrimSpace(pathBuf.String())
	if dir := filepath.Dir(outPath); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}

	var buf bytes.Buffer
	if err := s.bodyTmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("template sink: render template: %w", err)
	}
	if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("template sink: write %q: %w", outPath, err)
	}
	return nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func toStr(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func pad(s string, w int, c rune, left bool) string {
	if len(s) >= w {
		return s
	}
	fill := strings.Repeat(string(c), w-len(s))
	if left {
		return fill + s
	}
	return s + fill
}

func firstRune(s string) rune {
	for _, r := range s {
		return r
	}
	return ' '
}

// sci formats a numeric value as signed 6-digit scientific notation
// (e.g. +1.332668E+04), matching common instrument/report layouts.
func sci(v interface{}) string {
	f, err := strconv.ParseFloat(strings.TrimSpace(toStr(v)), 64)
	if err != nil {
		return toStr(v)
	}
	return fmt.Sprintf("%+.6E", f)
}
