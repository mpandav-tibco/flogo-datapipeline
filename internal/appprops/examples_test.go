package appprops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// This file validates that EVERY scalar config value in EVERY shipped example
// pipeline can be externalised through a Flogo app property. For each example it
// replaces every string/number/boolean leaf in the pipeline config with a
// $property[pN] reference (declaring a matching property whose default is the
// original value), runs Resolve, and asserts the resolved pipeline data is
// identical to the original. A mismatch means a field is NOT round-trippable
// through app properties — i.e. a resolver gap.

func deepCopyJSON(t *testing.T, v interface{}) interface{} {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// externalizer walks a config subtree, replacing each scalar leaf with a
// $property[pN] reference and recording the matching property declaration.
type externalizer struct {
	props   []map[string]interface{}
	n       int
	skipped int // empty strings (resolver treats an empty default as "unset")
}

func (e *externalizer) walk(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			t[k] = e.walk(val)
		}
		return t
	case []interface{}:
		for i, val := range t {
			t[i] = e.walk(val)
		}
		return t
	case string:
		// An empty default is treated as "unset" by the resolver (flogo-agents
		// parity), so it would not round-trip without an env var — skip it.
		// Skip values that are already $property refs to avoid double-processing.
		if t == "" || strings.Contains(t, "$property[") {
			if t == "" {
				e.skipped++
			}
			return t
		}
		return e.ref("string", t)
	case float64:
		return e.ref("number", strconv.FormatFloat(t, 'f', -1, 64))
	case bool:
		return e.ref("boolean", strconv.FormatBool(t))
	default:
		return v // null, etc.
	}
}

func (e *externalizer) ref(typ, value string) string {
	name := "p" + strconv.Itoa(e.n)
	e.n++
	e.props = append(e.props, map[string]interface{}{"name": name, "type": typ, "value": value})
	return "$property[" + name + "]"
}

func jdump(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	s := string(b)
	if len(s) > 1600 {
		s = s[:1600] + "\n…(truncated)"
	}
	return s
}

func TestResolve_AllExamplesRoundTrip(t *testing.T) {
	root := filepath.Join("..", "..", "examples")
	var files []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".datapipeline") {
			files = append(files, p)
		}
		return nil
	})
	if len(files) == 0 {
		t.Skip("no examples found under ../../examples")
	}
	t.Logf("validating %d example pipelines", len(files))

	for _, f := range files {
		f := f
		name := strings.TrimPrefix(f, root+string(os.PathSeparator))
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var app map[string]interface{}
			if err := json.Unmarshal(raw, &app); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			res, _ := app["resources"].([]interface{})
			if app["type"] != "flogo:app" || len(res) == 0 {
				t.Skip("not a flogo:app with resources")
			}
			r0, _ := res[0].(map[string]interface{})
			data, _ := r0["data"].(map[string]interface{})
			if data == nil {
				t.Skip("no pipeline data")
			}

			original := deepCopyJSON(t, data)

			// Examples that natively parameterise config with $property[...] refs
			// cannot be round-tripped by this synthetic externalizer: it replaces the
			// app's property declarations with generated ones, so the example's own
			// refs are left unresolved by design (Resolve leaves undeclared refs
			// untouched). Skip them here — real resolution is covered elsewhere.
			if ob, _ := json.Marshal(original); strings.Contains(string(ob), "$property[") {
				t.Skip("example natively uses $property refs; not round-trippable by the synthetic externalizer")
			}

			ext := &externalizer{}
			ext.walk(data) // mutate the data node in place with $property refs
			if len(ext.props) == 0 {
				t.Skip("no externalisable scalar fields")
			}
			app["properties"] = ext.props

			modified, err := json.Marshal(app)
			if err != nil {
				t.Fatal(err)
			}
			resolved := Resolve(string(modified))

			var out map[string]interface{}
			if err := json.Unmarshal([]byte(resolved), &out); err != nil {
				t.Fatalf("resolved output is not valid JSON after externalising %d fields: %v", len(ext.props), err)
			}
			if strings.Contains(resolved, "$property[") {
				t.Fatalf("resolved output still contains $property refs (unresolved) after externalising %d fields", len(ext.props))
			}
			ores, _ := out["resources"].([]interface{})
			or0, _ := ores[0].(map[string]interface{})
			gotData, _ := or0["data"].(map[string]interface{})

			if !reflect.DeepEqual(gotData, original) {
				t.Errorf("round-trip MISMATCH after externalising %d fields (%d empty skipped)\n--- original ---\n%s\n--- resolved ---\n%s",
					len(ext.props), ext.skipped, jdump(original), jdump(gotData))
				return
			}
			t.Logf("OK — %d fields externalised & resolved identically (%d empty skipped)", len(ext.props), ext.skipped)
		})
	}
}
