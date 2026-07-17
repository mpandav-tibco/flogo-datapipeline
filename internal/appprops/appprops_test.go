package appprops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// dig navigates parsed JSON (maps keyed by string, arrays indexed by int) and
// returns the map at the end of the path.
func dig(t *testing.T, v interface{}, path ...interface{}) map[string]interface{} {
	t.Helper()
	cur := v
	for _, p := range path {
		switch key := p.(type) {
		case string:
			m, ok := cur.(map[string]interface{})
			if !ok {
				t.Fatalf("dig: expected map at %q, got %T", key, cur)
			}
			cur = m[key]
		case int:
			a, ok := cur.([]interface{})
			if !ok {
				t.Fatalf("dig: expected array at %d, got %T", key, cur)
			}
			cur = a[key]
		}
	}
	m, ok := cur.(map[string]interface{})
	if !ok {
		t.Fatalf("dig: final value is %T, not a map", cur)
	}
	return m
}

func mustResolve(t *testing.T, appJSON string) map[string]interface{} {
	t.Helper()
	out := Resolve(appJSON)
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("resolved output is not valid JSON: %v\n%s", err, out)
	}
	return m
}

// appDSN is a minimal app declaring one string property referenced by the sink.
const appDSN = `{"name":"t","properties":[{"name":"db.dsn","type":"string","value":"DEFAULT"}],` +
	`"resources":[{"id":"pipeline:p","data":{"sink":{"type":"jdbc","options":{"dsn":"$property[db.dsn]"}}}}]}`

func dsnOf(t *testing.T, appJSON string) interface{} {
	t.Helper()
	m := mustResolve(t, appJSON)
	return dig(t, m, "resources", 0, "data", "sink", "options")["dsn"]
}

func TestResolve_DefaultValue(t *testing.T) {
	if got := dsnOf(t, appDSN); got != "DEFAULT" {
		t.Errorf("default: got %v want DEFAULT", got)
	}
}

func TestResolve_DirectEnvWins(t *testing.T) {
	t.Setenv("db.dsn", "ENV")
	t.Setenv("FLOGO_APP_PROPS_OVERRIDE", "db.dsn=OVERRIDE") // env must beat override
	if got := dsnOf(t, appDSN); got != "ENV" {
		t.Errorf("direct env: got %v want ENV", got)
	}
}

func TestResolve_Override(t *testing.T) {
	t.Setenv("FLOGO_APP_PROPS_OVERRIDE", "other=x,db.dsn=OVERRIDE")
	if got := dsnOf(t, appDSN); got != "OVERRIDE" {
		t.Errorf("override: got %v want OVERRIDE", got)
	}
}

func TestResolve_EnvAutoMapping(t *testing.T) {
	t.Setenv("FLOGO_APP_PROPS_ENV", "auto")
	t.Setenv("db_dsn", "AUTO") // auto maps dots -> underscores
	if got := dsnOf(t, appDSN); got != "AUTO" {
		t.Errorf("env auto: got %v want AUTO", got)
	}
}

func TestResolve_EnvMappings(t *testing.T) {
	t.Setenv("FLOGO_APP_PROPS_ENV", `{"mappings":{"db.dsn":"DB_CONN"}}`)
	t.Setenv("DB_CONN", "MAPPED")
	if got := dsnOf(t, appDSN); got != "MAPPED" {
		t.Errorf("env mappings: got %v want MAPPED", got)
	}
}

func TestResolve_JSONFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "props.json")
	if err := os.WriteFile(f, []byte(`{"db.dsn":"FROMJSON"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLOGO_APP_PROPS_JSON", f)
	if got := dsnOf(t, appDSN); got != "FROMJSON" {
		t.Errorf("json file: got %v want FROMJSON", got)
	}
}

func TestResolve_QuotedForm(t *testing.T) {
	app := `{"properties":[{"name":"db.dsn","type":"string","value":"Q"}],` +
		`"resources":[{"data":{"sink":{"options":{"dsn":"$property[\"db.dsn\"]"}}}}]}`
	m := mustResolve(t, app)
	if got := dig(t, m, "resources", 0, "data", "sink", "options")["dsn"]; got != "Q" {
		t.Errorf("quoted form: got %v want Q", got)
	}
}

func TestResolve_NumberInjectedRaw(t *testing.T) {
	app := `{"properties":[{"name":"batch.size","type":"number","value":"500"}],` +
		`"resources":[{"data":{"sink":{"options":{"batchSize":"$property[batch.size]"}}}}]}`
	m := mustResolve(t, app)
	got := dig(t, m, "resources", 0, "data", "sink", "options")["batchSize"]
	n, ok := got.(float64)
	if !ok || n != 500 {
		t.Errorf("number: got %v (%T) want JSON number 500", got, got)
	}
}

func TestResolve_BooleanInjectedRaw(t *testing.T) {
	app := `{"properties":[{"name":"dq.on","type":"boolean","value":"true"}],` +
		`"resources":[{"data":{"sink":{"options":{"flag":"$property[dq.on]"}}}}]}`
	m := mustResolve(t, app)
	got := dig(t, m, "resources", 0, "data", "sink", "options")["flag"]
	b, ok := got.(bool)
	if !ok || b != true {
		t.Errorf("boolean: got %v (%T) want JSON true", got, got)
	}
}

func TestResolve_UndeclaredRefUntouched(t *testing.T) {
	app := `{"properties":[{"name":"a","type":"string","value":"X"}],` +
		`"resources":[{"data":{"sink":{"options":{"dsn":"$property[a]","other":"$property[nope]"}}}}]}`
	opts := dig(t, mustResolve(t, app), "resources", 0, "data", "sink", "options")
	if opts["dsn"] != "X" {
		t.Errorf("declared ref: got %v want X", opts["dsn"])
	}
	if opts["other"] != "$property[nope]" {
		t.Errorf("undeclared ref should be left as-is, got %v", opts["other"])
	}
}

func TestResolve_StringEscaping(t *testing.T) {
	// A value containing a double-quote must be JSON-escaped so the output stays
	// valid JSON (mustResolve fails to parse otherwise).
	app := `{"properties":[{"name":"p","type":"string","value":"he said \"hi\""}],` +
		`"resources":[{"data":{"sink":{"options":{"x":"$property[p]"}}}}]}`
	got := dig(t, mustResolve(t, app), "resources", 0, "data", "sink", "options")["x"]
	if got != `he said "hi"` {
		t.Errorf("escaping: got %v want %q", got, `he said "hi"`)
	}
}

func TestResolve_StripsPropertiesArray(t *testing.T) {
	m := mustResolve(t, appDSN)
	if _, ok := m["properties"]; ok {
		t.Error("properties[] should be stripped before engine load")
	}
}

func TestResolve_NoPropertiesUnchanged(t *testing.T) {
	app := `{"name":"t","resources":[]}`
	if got := Resolve(app); got != app {
		t.Errorf("no properties: expected unchanged\n got %q\nwant %q", got, app)
	}
}

func TestResolve_InvalidJSONUnchanged(t *testing.T) {
	app := `{not json`
	if got := Resolve(app); got != app {
		t.Errorf("invalid json: expected unchanged, got %q", got)
	}
}

func TestResolve_SamePropertyReusedAcrossFields(t *testing.T) {
	// One property referenced in multiple places all resolve.
	app := `{"properties":[{"name":"db.dsn","type":"string","value":"SHARED"}],` +
		`"resources":[{"data":{` +
		`"source":{"options":{"dsn":"$property[db.dsn]"}},` +
		`"sink":{"options":{"dsn":"$property[db.dsn]"}}}}]}`
	m := mustResolve(t, app)
	if got := dig(t, m, "resources", 0, "data", "source", "options")["dsn"]; got != "SHARED" {
		t.Errorf("source dsn: got %v want SHARED", got)
	}
	if got := dig(t, m, "resources", 0, "data", "sink", "options")["dsn"]; got != "SHARED" {
		t.Errorf("sink dsn: got %v want SHARED", got)
	}
}

// TestResolve_AppDistributedFile resolves the real shipped example and asserts
// no $property[...] reference survives and the output is still valid JSON.
func TestResolve_AppDistributedFile(t *testing.T) {
	p := filepath.Join("..", "..", "cmd", "flogo-datapipeline", "app-distributed.json")
	data, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("app-distributed.json not found: %v", err)
	}
	out := Resolve(string(data))
	if !json.Valid([]byte(out)) {
		t.Fatalf("resolved app-distributed.json is not valid JSON")
	}
	if strings.Contains(out, "$property[") {
		t.Errorf("unresolved $property refs remain after Resolve")
	}
	if strings.Contains(out, `"properties"`) {
		t.Errorf("properties[] should be stripped from the resolved app")
	}
}
