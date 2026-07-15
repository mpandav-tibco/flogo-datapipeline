package api

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// handleIntrospectXML handles POST /v1/introspect/xml.
// Body: { "path": "data/products.xml" }
// Samples the file to detect the repeating record element and its child fields
// so the user doesn't have to hand-write recordElement + the field xpaths.
// Returns { "recordElement": "product", "fields": [{name, xpath, type}, ...],
//
//	"columns": [{name, type}, ...] }.
func (s *Server) handleIntrospectXML(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if body.Path == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "path is required")
		return
	}
	res, err := introspectXML(body.Path)
	if err != nil {
		writeError(w, http.StatusBadGateway, "introspect_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// XMLField is a discovered XML field mapped to the xml source config shape.
type XMLField struct {
	Name  string `json:"name"`
	XPath string `json:"xpath"`
	Type  string `json:"type"`
}

// XMLIntrospectResult is the response of the XML introspection endpoint.
type XMLIntrospectResult struct {
	RecordElement string       `json:"recordElement"`
	Fields        []XMLField   `json:"fields"`
	Columns       []ColumnInfo `json:"columns"`
}

// introspectXML samples an XML file, detects the repeating record element, and
// collects its child element/attribute fields with inferred types.
func introspectXML(path string) (*XMLIntrospectResult, error) {
	recordElement, err := detectRecordElement(path)
	if err != nil {
		return nil, err
	}
	if recordElement == "" {
		return nil, fmt.Errorf("could not detect a repeating record element — declare recordElement manually")
	}
	fields, err := collectXMLFields(path, recordElement)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("record element %q has no child fields", recordElement)
	}
	cols := make([]ColumnInfo, len(fields))
	for i, f := range fields {
		cols[i] = ColumnInfo{Name: f.Name, Type: f.Type, DBType: "xml"}
	}
	return &XMLIntrospectResult{RecordElement: recordElement, Fields: fields, Columns: cols}, nil
}

// detectRecordElement finds the most likely repeating record element: the
// element at depth 2 (a direct child of the root) whose local name occurs most
// often. Ties break toward the first seen.
func detectRecordElement(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	dec := xml.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	dec.Strict = false
	dec.AutoClose = xml.HTMLAutoClose
	dec.Entity = xml.HTMLEntity

	depth := 0
	counts := map[string]int{}
	order := []string{}
	const maxTokens = 200000
	for i := 0; i < maxTokens; i++ {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("parse xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 { // direct child of root
				name := t.Name.Local
				if _, seen := counts[name]; !seen {
					order = append(order, name)
				}
				counts[name]++
			}
		case xml.EndElement:
			depth--
		}
	}

	best := ""
	bestCount := 0
	for _, name := range order {
		if counts[name] > bestCount {
			best, bestCount = name, counts[name]
		}
	}
	return best, nil
}

// collectXMLFields captures the child element/attribute fields of the first few
// record elements and infers each field's type from sampled values.
func collectXMLFields(path, recordElement string) ([]XMLField, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	dec := xml.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	dec.Strict = false
	dec.AutoClose = xml.HTMLAutoClose
	dec.Entity = xml.HTMLEntity

	type sample struct {
		values []string
	}
	fieldOrder := []string{}        // xpath order as discovered
	samples := map[string]*sample{} // xpath -> sampled string values
	addSample := func(xpath, val string) {
		s, ok := samples[xpath]
		if !ok {
			s = &sample{}
			samples[xpath] = s
			fieldOrder = append(fieldOrder, xpath)
		}
		if len(s.values) < 20 {
			s.values = append(s.values, val)
		}
	}

	inRecord := false
	recordsSeen := 0
	const maxRecords = 50
	// pathStack holds element local names relative to (and including) the record element.
	var pathStack []string
	var textBuf strings.Builder

	relPath := func() string {
		// path segments below the record element (exclude the record element itself).
		if len(pathStack) <= 1 {
			return ""
		}
		return strings.Join(pathStack[1:], "/")
	}

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if !inRecord {
				if t.Name.Local == recordElement {
					inRecord = true
					pathStack = []string{recordElement}
					// record-level attributes → @attr
					for _, a := range t.Attr {
						if a.Name.Local == "" {
							continue
						}
						addSample("@"+a.Name.Local, a.Value)
					}
				}
				continue
			}
			pathStack = append(pathStack, t.Name.Local)
			textBuf.Reset()
			// element attributes → path/@attr
			for _, a := range t.Attr {
				if a.Name.Local == "" || strings.HasPrefix(a.Name.Local, "xmlns") {
					continue
				}
				rel := relPath()
				if rel == "" {
					addSample("@"+a.Name.Local, a.Value)
				} else {
					addSample(rel+"/@"+a.Name.Local, a.Value)
				}
			}
		case xml.CharData:
			if inRecord {
				textBuf.WriteString(string(t))
			}
		case xml.EndElement:
			if !inRecord {
				continue
			}
			if t.Name.Local == recordElement && len(pathStack) == 1 {
				inRecord = false
				pathStack = nil
				recordsSeen++
				if recordsSeen >= maxRecords {
					// stop early once we have enough samples
					goto done
				}
				continue
			}
			// Leaf element text → its relative path.
			rel := relPath()
			text := strings.TrimSpace(textBuf.String())
			if rel != "" && text != "" {
				addSample(rel, text)
			}
			textBuf.Reset()
			if len(pathStack) > 0 {
				pathStack = pathStack[:len(pathStack)-1]
			}
		}
	}
done:

	fields := make([]XMLField, 0, len(fieldOrder))
	for _, xpath := range fieldOrder {
		name := fieldNameFromXPath(xpath)
		typ := inferXMLType(samples[xpath].values)
		fields = append(fields, XMLField{Name: name, XPath: xpath, Type: typ})
	}
	return fields, nil
}

// fieldNameFromXPath derives a friendly column name from an xpath like
// "stock/@quantity" -> "quantity", "amount/value" -> "value", "@status" -> "status".
func fieldNameFromXPath(xpath string) string {
	s := xpath
	if i := strings.LastIndex(s, "/@"); i >= 0 {
		return s[i+2:]
	}
	s = strings.TrimPrefix(s, "@")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// inferXMLType guesses a pipeline column type from sampled string values.
func inferXMLType(values []string) string {
	if len(values) == 0 {
		return "string"
	}
	allInt, allFloat, allBool := true, true, true
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			allFloat = false
		}
		lv := strings.ToLower(v)
		if lv != "true" && lv != "false" {
			allBool = false
		}
	}
	switch {
	case allBool:
		return "bool"
	case allInt:
		return "int"
	case allFloat:
		return "float"
	default:
		return "string"
	}
}
