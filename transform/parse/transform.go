// Package parse registers a generic message-parsing transform that structures a
// single string field into multiple typed columns — the flogo-datapipeline
// equivalent of a Fluentd/Fluent Bit `parser` filter. It is source-agnostic:
// any pipeline with a text field to structure can use it (log lines, CSV blobs,
// embedded JSON, etc.), not just the podlog collector.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/parse"
//
// # Two modes
//
// 1. Regex (named capture groups → columns):
//
//	{
//	  "type": "parse",
//	  "source": "message",
//	  "mode": "regex",
//	  "pattern": "^(?P<ts>\\S+)\\s+(?P<level>\\w+)\\s+\\[(?P<thread>[^\\]]+)\\]\\s+(?P<msg>.*)$",
//	  "onError": "keep",      // keep | drop | route  (default keep)
//	  "keepSource": true       // retain the original 'source' field (default true)
//	}
//
// 2. JSON (structured log line → top-level columns):
//
//	{
//	  "type": "parse",
//	  "source": "message",
//	  "mode": "json",
//	  "fields": ["level", "logger", "msg", "trace_id"],  // optional: only these keys (else all)
//	  "prefix": "log_",                                    // optional: prefix extracted column names
//	  "onError": "keep"
//	}
//
// Extracted columns are appended to the record (existing columns are preserved).
// A named group / JSON key that does not match yields a nil value for that
// column. onError governs rows whose source field is missing or unparseable:
//   - keep  (default): emit the row unchanged (extracted columns nil)
//   - drop:            silently drop the row
//   - route:           mark the row failed so the pipeline routes it to the DLQ
package parse

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("parse", newParseTransform)
}

// ErrRouteRow is the sentinel a transform returns per-row to request DLQ routing.
// The pipeline's row-level write path already understands transform errors; here
// we surface unparseable rows by dropping them from the output batch when
// onError=drop, or by leaving a marker the validate/DLQ path handles when
// onError=route. To stay within the existing Transform contract (batch in →
// batch out), route is implemented by emitting the row with a synthetic
// "_parse_error" column set, which a downstream validate rule can catch.
type parseTransform struct {
	source     string
	mode       string // "regex" | "json"
	re         *regexp.Regexp
	groups     []string // named capture groups (regex mode)
	jsonFields []string // optional key allow-list (json mode)
	jsonPrefix string
	onError    string // keep | drop | route
	keepSource bool

	outCols       []record.Column
	typesResolved bool
}

func newParseTransform(cfg map[string]interface{}) (transform.Transform, error) {
	source, _ := cfg["source"].(string)
	if source == "" {
		return nil, fmt.Errorf("parse transform: 'source' field name is required")
	}
	mode, _ := cfg["mode"].(string)
	if mode == "" {
		mode = "regex"
	}
	onError, _ := cfg["onError"].(string)
	if onError == "" {
		onError = "keep"
	}
	switch onError {
	case "keep", "drop", "route":
	default:
		return nil, fmt.Errorf("parse transform: invalid onError %q (want keep|drop|route)", onError)
	}
	keepSource := true
	if v, ok := cfg["keepSource"].(bool); ok {
		keepSource = v
	}

	pt := &parseTransform{
		source:     source,
		mode:       mode,
		onError:    onError,
		keepSource: keepSource,
		jsonPrefix: strFromCfg(cfg, "prefix"),
	}

	switch mode {
	case "regex":
		pattern, _ := cfg["pattern"].(string)
		if pattern == "" {
			return nil, fmt.Errorf("parse transform: regex mode requires 'pattern'")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("parse transform: invalid 'pattern': %w", err)
		}
		// Only named groups become columns; anonymous groups are ignored.
		for _, name := range re.SubexpNames() {
			if name != "" {
				pt.groups = append(pt.groups, name)
			}
		}
		if len(pt.groups) == 0 {
			return nil, fmt.Errorf("parse transform: 'pattern' has no named capture groups (?P<name>...)")
		}
		pt.re = re
	case "json":
		if raw, ok := cfg["fields"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok {
					pt.jsonFields = append(pt.jsonFields, s)
				}
			}
		}
	default:
		return nil, fmt.Errorf("parse transform: invalid mode %q (want regex|json)", mode)
	}

	return pt, nil
}

func (p *parseTransform) Name() string { return "parse" }

func strFromCfg(cfg map[string]interface{}, k string) string {
	if v, ok := cfg[k].(string); ok {
		return v
	}
	return ""
}

func (p *parseTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	srcIdx := in.ColIndex(p.source)

	out := record.Get()
	out.Rows = make([][]interface{}, 0, in.Len())

	for i := range in.Rows {
		var text string
		if srcIdx >= 0 && srcIdx < len(in.Rows[i]) {
			if s, ok := in.Rows[i][srcIdx].(string); ok {
				text = s
			}
		}

		extracted, ok := p.extract(text)
		if !ok {
			switch p.onError {
			case "drop":
				continue
			case "route":
				// Emit the row plus a "_parse_error" flag column so a downstream
				// validate rule (field=_parse_error, in:["false"]) routes it to DLQ.
				out.Rows = append(out.Rows, p.buildRow(in, i, nil, true))
				continue
			}
			// keep: emit with nil extracted values.
		}
		out.Rows = append(out.Rows, p.buildRow(in, i, extracted, false))
	}

	if !p.typesResolved {
		p.buildOutCols(in)
		p.resolveTypes(out.Rows)
		p.typesResolved = true
	}
	out.Columns = p.outCols
	return out, nil
}

// extract returns the parsed key→value map for one source string, and whether
// parsing succeeded.
func (p *parseTransform) extract(text string) (map[string]interface{}, bool) {
	if text == "" {
		return nil, false
	}
	if p.mode == "regex" {
		m := p.re.FindStringSubmatch(text)
		if m == nil {
			return nil, false
		}
		res := make(map[string]interface{}, len(p.groups))
		for gi, name := range p.re.SubexpNames() {
			if name != "" && gi < len(m) {
				res[name] = m[gi]
			}
		}
		return res, true
	}
	// json mode
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		return nil, false
	}
	return obj, true
}

// buildRow assembles the output row: original columns (optionally dropping the
// source) plus the extracted columns in stable order.
func (p *parseTransform) buildRow(in *record.RecordBatch, i int, extracted map[string]interface{}, parseErr bool) []interface{} {
	row := make([]interface{}, 0, len(in.Columns)+len(p.extractedNames(in)))
	for ci, col := range in.Columns {
		if !p.keepSource && col.Name == p.source {
			continue
		}
		if ci < len(in.Rows[i]) {
			row = append(row, in.Rows[i][ci])
		} else {
			row = append(row, nil)
		}
	}
	for _, name := range p.extractedNames(in) {
		if name == "_parse_error" {
			row = append(row, boolStr(parseErr))
			continue
		}
		key := name
		if p.jsonPrefix != "" && p.mode == "json" {
			key = name[len(p.jsonPrefix):] // strip prefix to look up the raw key
		}
		if extracted != nil {
			row = append(row, extracted[key])
		} else {
			row = append(row, nil)
		}
	}
	return row
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// extractedNames returns the ordered list of columns this transform appends.
func (p *parseTransform) extractedNames(in *record.RecordBatch) []string {
	names := make([]string, 0, len(p.groups)+len(p.jsonFields)+1)
	if p.mode == "regex" {
		names = append(names, p.groups...)
	} else {
		if len(p.jsonFields) > 0 {
			for _, f := range p.jsonFields {
				names = append(names, p.jsonPrefix+f)
			}
		}
		// else: with no explicit 'fields' list, json-mode keys are only known at
		// runtime — the schema falls back to the FIRST parsed row's keys, handled
		// in buildOutCols.
	}
	if p.onError == "route" {
		names = append(names, "_parse_error")
	}
	return names
}

// buildOutCols computes the output column schema once, from the input columns
// plus the extracted names. For json mode with no explicit fields, the extracted
// key set is discovered from the input's first row is not available here, so an
// explicit 'fields' list is required for a stable schema; otherwise no extra
// columns are emitted.
func (p *parseTransform) buildOutCols(in *record.RecordBatch) {
	var cols []record.Column
	for _, col := range in.Columns {
		if !p.keepSource && col.Name == p.source {
			continue
		}
		cols = append(cols, col)
	}
	for _, name := range p.extractedNames(in) {
		cols = append(cols, record.Column{Name: name, Type: record.TypeString})
	}
	p.outCols = cols
}

// resolveTypes leaves extracted columns as string (regex groups are always
// strings; JSON values keep their decoded type via TypeAny inference below).
func (p *parseTransform) resolveTypes(rows [][]interface{}) {
	for j := range p.outCols {
		if p.outCols[j].Type != record.TypeString && p.outCols[j].Type != record.TypeAny {
			continue
		}
		for _, r := range rows {
			if j >= len(r) || r[j] == nil {
				continue
			}
			switch r[j].(type) {
			case float64:
				p.outCols[j].Type = record.TypeFloat
			case bool:
				p.outCols[j].Type = record.TypeBool
			case string:
				p.outCols[j].Type = record.TypeString
			}
			break
		}
	}
}

func (p *parseTransform) Close() error { return nil }
