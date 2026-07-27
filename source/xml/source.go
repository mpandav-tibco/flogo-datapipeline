// Package xml registers an XML streaming source using SAX-style token reading.
// Never loads the entire file into memory — processes one record element at a time.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/xml"
//
// Config:
//
//	{
//	  "type": "xml",
//	  "path": "/data/transactions.xml",
//	  "recordElement": "transaction",
//	  "fields": [
//	    { "name": "id",     "xpath": "id" },
//	    { "name": "amount", "xpath": "amount/@value", "type": "float" },
//	    { "name": "status", "xpath": "@status" }
//	  ]
//	}
//
// xpath support: element paths ("parent/child"), attribute paths ("element/@attr"),
// and direct element text ("element"). No full XPath — simple path expressions only.
package xml

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("xml", newXMLSource)
}

type xmlField struct {
	name    string
	path    []string // element path segments
	attr    string   // attribute name (empty = element text)
	colType record.ColumnType
}

type xmlSource struct {
	file          *os.File
	decoder       *xml.Decoder
	schema        []record.Column
	fields        []xmlField
	recordElement string
	rowsRead      int64
	closed        bool
	eof           bool
}

func newXMLSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("xml source: 'path' is required")
	}
	recordElement, _ := cfg["recordElement"].(string)
	if recordElement == "" {
		return nil, fmt.Errorf("xml source: 'recordElement' is required")
	}

	fields, schema, err := parseXMLFields(cfg)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("xml source: open %q: %w", path, err)
	}

	decoder := xml.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	decoder.Strict = false
	decoder.AutoClose = xml.HTMLAutoClose
	decoder.Entity = xml.HTMLEntity

	return &xmlSource{
		file:          f,
		decoder:       decoder,
		schema:        schema,
		fields:        fields,
		recordElement: recordElement,
	}, nil
}

func (s *xmlSource) Schema() []record.Column { return s.schema }

func (s *xmlSource) Skip(n int64) error {
	for i := int64(0); i < n; i++ {
		if _, err := s.readNextRecord(); err != nil {
			return nil // reached EOF during skip
		}
	}
	s.rowsRead = n
	return nil
}

func (s *xmlSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed || s.eof {
		return nil, io.EOF
	}

	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, 0, size)

	for i := 0; i < size; i++ {
		rec, err := s.readNextRecord()
		if err == io.EOF {
			s.eof = true
			if len(batch.Rows) == 0 {
				record.Put(batch)
				return nil, io.EOF
			}
			return batch, io.EOF
		}
		if err != nil {
			return batch, err
		}
		batch.Rows = append(batch.Rows, rec)
		s.rowsRead++
	}
	return batch, nil
}

// readNextRecord scans the XML token stream until it finds the next
// recordElement, captures its sub-elements and attributes, and extracts
// the declared fields.
func (s *xmlSource) readNextRecord() ([]interface{}, error) {
	// Scan to the opening tag of the record element.
	for {
		tok, err := s.decoder.Token()
		if err != nil {
			return nil, err // io.EOF or parse error
		}
		start, ok := tok.(xml.StartElement)
		if ok && start.Name.Local == s.recordElement {
			return s.extractRecord(start)
		}
	}
}

// extractRecord reads all tokens until the closing record element and
// extracts field values according to the declared xpath paths.
func (s *xmlSource) extractRecord(start xml.StartElement) ([]interface{}, error) {
	// Collect attributes from the root record element.
	rootAttrs := make(map[string]string)
	for _, a := range start.Attr {
		rootAttrs[a.Name.Local] = a.Value
	}

	// Walk child elements, collecting a flat map of path → text value.
	// Paths: "child", "parent/child". Attributes: stored as "element/@attr".
	values := make(map[string]string)
	for k, v := range rootAttrs {
		values["@"+k] = v
	}

	depth := 1
	var pathStack []string
	var curText strings.Builder

	for depth > 0 {
		tok, err := s.decoder.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			pathStack = append(pathStack, t.Name.Local)
			curText.Reset()
			for _, a := range t.Attr {
				attrKey := strings.Join(pathStack, "/") + "/@" + a.Name.Local
				values[attrKey] = a.Value
			}
			depth++
		case xml.EndElement:
			if depth > 1 && len(pathStack) > 0 {
				key := strings.Join(pathStack, "/")
				values[key] = strings.TrimSpace(curText.String())
				pathStack = pathStack[:len(pathStack)-1]
				curText.Reset()
			}
			depth--
		case xml.CharData:
			curText.Write(t)
		}
	}

	// Map extracted values to declared fields.
	row := make([]interface{}, len(s.fields))
	for i, f := range s.fields {
		xpath := strings.Join(f.path, "/")
		if f.attr != "" {
			xpath = xpath + "/@" + f.attr
		}
		raw := values[xpath]
		row[i] = coerceXML(raw, f.colType)
	}
	return row, nil
}

func (s *xmlSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

func parseXMLFields(cfg map[string]interface{}) ([]xmlField, []record.Column, error) {
	raw, ok := cfg["fields"]
	if !ok {
		return nil, nil, fmt.Errorf("xml source: 'fields' is required")
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("xml source: 'fields' must be an array")
	}
	fields := make([]xmlField, 0, len(items))
	schema := make([]record.Column, 0, len(items))
	for i, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("xml source: field[%d] must be an object", i)
		}
		name, _ := m["name"].(string)
		xpath, _ := m["xpath"].(string)
		typ := record.TypeString
		if t, ok := m["type"].(string); ok {
			typ = record.ColumnType(t)
		}

		// Parse xpath: "parent/child/@attr" → path=["parent","child"], attr="attr"
		var path []string
		var attr string
		parts := strings.Split(xpath, "/")
		for _, p := range parts {
			if strings.HasPrefix(p, "@") {
				attr = strings.TrimPrefix(p, "@")
			} else if p != "" {
				path = append(path, p)
			}
		}

		fields = append(fields, xmlField{name: name, path: path, attr: attr, colType: typ})
		schema = append(schema, record.Column{Name: name, Type: typ})
	}
	return fields, schema, nil
}

func coerceXML(raw string, typ record.ColumnType) interface{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	switch typ {
	case record.TypeInt:
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	case record.TypeDecimal:
		if d, ok := record.ParseDecimal(raw); ok {
			return d
		}
	case record.TypeFloat:
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			return v
		}
	case record.TypeBool:
		switch strings.ToLower(raw) {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}
	return raw
}
