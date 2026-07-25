// Package appprops resolves Flogo application-property references in a
// datapipeline app (.datapipeline) JSON before the engine loads it, mirroring
// the flogo-agents / Flogo 2.26.x app-property model so datapipeline apps
// externalise configuration the same way flows and agents do.
//
// A pipeline declares typed properties at the app top level:
//
//	"properties": [ { "name": "db.dsn", "type": "string", "value": "host=localhost ..." } ]
//
// and references them in any config value:
//
//	"dsn": "$property[db.dsn]"          // legacy unquoted form
//	"dsn": "$property[\"db.dsn\"]"      // Flogo-style quoted form
//
// Resolve substitutes every reference with the property's effective value,
// resolved in this priority order (highest first):
//
//  1. FLOGO_APP_PROPS_OVERRIDE entries (K=V,K2=V2) — explicit, always on
//  2. FLOGO_APP_PROPS_ENV ("auto" | {"mappings":{"prop":"ENV"}}) — OPT-IN env
//     lookup, matching project-flogo/core: the environment is read only when
//     this is set (auto tries NAME, then NAME with dots→underscores, then the
//     UPPERCASE form).
//  3. FLOGO_APP_PROPS_JSON (comma-separated flat JSON files; later files win)
//  4. Default value from properties[] in the file
//
// Env is opt-in exactly as in Flogo core (nothing is read from the environment
// unless FLOGO_APP_PROPS_ENV is set); FLOGO_APP_PROPS_OVERRIDE is a documented
// datapipeline/agents extension that acts as the explicit escape hatch.
//
// Number/boolean/json-typed properties whose value is a valid literal are
// spliced as RAW (unquoted) JSON so typed config fields unmarshal correctly;
// string/password properties are JSON-string-escaped. The properties[] array is
// stripped before engine load: Flogo core rejects Studio type names such as
// "number"/"password"/"url" with "unknown type" and cannot coerce empty typed
// properties, and every reference has already been inlined here.
package appprops

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"strings"

	corelog "github.com/project-flogo/core/support/log"
)

var log = corelog.ChildLogger(corelog.RootLogger(), "appprops")

// Resolve substitutes $property[NAME] references throughout the raw app JSON and
// returns the transformed JSON. On any parse error, or when the app declares no
// properties, the input is returned unchanged. Only properties declared in the
// top-level "properties" array are resolved; a $property[X] reference with no
// matching declaration is left as-is.
func Resolve(appJSON string) string {
	var app map[string]interface{}
	if err := json.Unmarshal([]byte(appJSON), &app); err != nil {
		return appJSON
	}

	propsRaw, _ := app["properties"].([]interface{})
	if len(propsRaw) == 0 {
		return appJSON
	}

	// 2. FLOGO_APP_PROPS_OVERRIDE (format: K=V,K2=V2).
	overrides := map[string]string{}
	if override := os.Getenv("FLOGO_APP_PROPS_OVERRIDE"); override != "" {
		for _, kv := range strings.Split(override, ",") {
			parts := strings.SplitN(strings.TrimSpace(kv), "=", 2)
			if len(parts) == 2 {
				overrides[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	// 3-4. Flogo-core-compatible external property sources (env + json resolvers).
	envAuto, envMappings := parseCorePropsEnv()
	jsonProps := loadCorePropsJSON()

	// Build the resolved value map (value + declared type, for typed injection).
	type resolvedProp struct{ value, typ string }
	resolved := make(map[string]resolvedProp, len(propsRaw))
	for _, p := range propsRaw {
		prop, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := prop["name"].(string)
		if name == "" {
			continue
		}
		typ, _ := prop["type"].(string)
		// Priority (highest first): FLOGO_APP_PROPS_OVERRIDE > FLOGO_APP_PROPS_ENV
		// (opt-in) > FLOGO_APP_PROPS_JSON > default value in file. Environment
		// variables are read ONLY through FLOGO_APP_PROPS_ENV, matching
		// project-flogo/core — there is no unconditional per-name env lookup
		// (which risked colliding with OS vars such as HOME/PATH).
		var val string
		var have bool
		if v, ok := overrides[name]; ok {
			val, have = v, true
		} else if v, ok := coreEnvPropValue(name, envAuto, envMappings); ok {
			val, have = v, true
		} else if v, ok := jsonProps[name]; ok {
			if s := stringifyProp(v); s != "" {
				val, have = s, true
			}
		} else if v, _ := prop["value"].(string); v != "" {
			val, have = v, true
		}
		if have {
			resolved[name] = resolvedProp{value: val, typ: typ}
		}
	}

	// Drop properties[] from the runtime config (see package doc) and re-marshal
	// (HTML escaping off) before splicing values in. Downstream re-parses, so the
	// compacted form is fine.
	delete(app, "properties")
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(app); err != nil {
		return appJSON
	}
	result := strings.TrimRight(buf.String(), "\n")

	// Replace every $property["NAME"] (Flogo-style, quoted — inner quotes escaped
	// in the JSON text as $property[\"NAME\"]) and the legacy $property[NAME] form.
	// For numeric/boolean/json properties whose value is a valid literal, the
	// WHOLE-quoted form "$property[...]" (surrounding JSON quotes included) is
	// replaced with the raw value so a field like "batchSize": "$property[...]"
	// unmarshals as a number, not a string. That whole-quoted pass runs first so
	// standalone typed bindings win over the inner-token pass. Strings (and any
	// reference embedded in a larger string) use the inner-token replacement,
	// which preserves the surrounding quotes and JSON-escapes the value.
	for name, rp := range resolved {
		value := rp.value
		if isRawInjectable(rp.typ, value) {
			result = strings.ReplaceAll(result, `"$property[\"`+name+`\"]"`, value)
			result = strings.ReplaceAll(result, `"$property[`+name+`]"`, value)
		}
		inner := jsonStringInner(value)
		result = strings.ReplaceAll(result, `$property[\"`+name+`\"]`, inner)
		result = strings.ReplaceAll(result, "$property["+name+"]", inner)
	}

	if len(resolved) > 0 {
		log.Infof("resolved %d app properties", len(resolved))
	}
	return result
}

// parseCorePropsEnv interprets FLOGO_APP_PROPS_ENV like project-flogo/core's env
// resolver: "auto" enables name-based auto-mapping to environment variables;
// otherwise the value is a JSON document {"mappings":{"prop":"ENV_VAR"}}. A
// malformed value is warned and ignored.
func parseCorePropsEnv() (auto bool, mappings map[string]string) {
	cfg := strings.TrimSpace(os.Getenv("FLOGO_APP_PROPS_ENV"))
	if cfg == "" {
		return false, nil
	}
	if strings.EqualFold(cfg, "auto") {
		return true, nil
	}
	var pm struct {
		Mappings map[string]string `json:"mappings"`
	}
	if err := json.Unmarshal([]byte(cfg), &pm); err != nil {
		log.Warnf("FLOGO_APP_PROPS_ENV: cannot parse %q: %v (ignored)", cfg, err)
		return false, nil
	}
	return false, pm.Mappings
}

// coreEnvPropValue resolves one property via the FLOGO_APP_PROPS_ENV strategy.
// In auto mode it tries the property name as-is, then with dots replaced by
// underscores, then the upper-cased form. Only non-empty values are accepted.
func coreEnvPropValue(name string, autoMapping bool, mappings map[string]string) (string, bool) {
	try := func(k string) (string, bool) {
		if v, ok := os.LookupEnv(k); ok && v != "" {
			return v, true
		}
		return "", false
	}
	if autoMapping {
		if v, ok := try(name); ok {
			return v, true
		}
		alt := strings.ReplaceAll(name, ".", "_")
		if v, ok := try(alt); ok {
			return v, true
		}
		return try(strings.ToUpper(alt))
	}
	if envName, ok := mappings[name]; ok {
		return try(envName)
	}
	return "", false
}

// loadCorePropsJSON preloads and merges the flat JSON files listed
// (comma-separated) in FLOGO_APP_PROPS_JSON; later files win on key collisions.
// An unreadable/invalid file is warned and skipped.
func loadCorePropsJSON() map[string]interface{} {
	paths := strings.TrimSpace(os.Getenv("FLOGO_APP_PROPS_JSON"))
	if paths == "" {
		return nil
	}
	merged := map[string]interface{}{}
	for _, p := range strings.Split(paths, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		data, err := os.ReadFile(p)
		if err != nil {
			log.Warnf("FLOGO_APP_PROPS_JSON: cannot read %s: %v (skipped)", p, err)
			continue
		}
		var props map[string]interface{}
		if err := json.Unmarshal(data, &props); err != nil {
			log.Warnf("FLOGO_APP_PROPS_JSON: cannot parse %s: %v (skipped)", p, err)
			continue
		}
		for k, v := range props {
			merged[k] = v
		}
	}
	return merged
}

// stringifyProp renders a JSON-decoded property value as the string the
// substitution machinery expects: strings pass through; numbers/bools are
// formatted unquoted; objects/arrays are re-marshalled to compact JSON.
func stringifyProp(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// isRawInjectable reports whether a resolved property should be spliced as a raw
// (unquoted) JSON literal instead of a quoted string, so numeric/boolean/json
// properties can externalise typed config fields. Only well-formed literals
// qualify; anything else falls back to safe quoted-string replacement.
func isRawInjectable(typ, value string) bool {
	v := strings.TrimSpace(value)
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "number", "integer", "int", "float", "long", "double":
		// Validate as a genuine JSON number, not merely strconv.ParseFloat-parseable:
		// ParseFloat also accepts ".5", "1.", "+5", "5_0" and hex floats ("0x1p4"),
		// none of which are valid JSON — raw-splicing them would corrupt the app
		// JSON at load. json.Unmarshal into a float rejects those (and Inf/NaN).
		var n float64
		return json.Unmarshal([]byte(v), &n) == nil
	case "boolean", "bool":
		return v == "true" || v == "false"
	case "json", "object", "array":
		return json.Valid([]byte(v))
	}
	return false
}

// jsonStringInner returns s escaped for embedding inside a JSON string literal
// (json.Marshal with the surrounding quotes stripped), so a value containing "
// or \ cannot break the surrounding JSON. HTML escaping is disabled.
func jsonStringInner(s string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return s
	}
	out := strings.TrimRight(buf.String(), "\n")
	if len(out) >= 2 {
		return out[1 : len(out)-1]
	}
	return s
}
