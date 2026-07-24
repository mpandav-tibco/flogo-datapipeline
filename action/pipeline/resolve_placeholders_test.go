package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/project-flogo/core/data/property"
)

// TestResolvePlaceholders verifies that ${ENV} and $property[name] are resolved
// across every config section, that unset/reserved placeholders survive, and
// that Flogo expressions ("=…") keep their $property references for the runtime
// expression engine.
func TestResolvePlaceholders(t *testing.T) {
	prev := property.DefaultManager()
	defer property.SetDefaultManager(prev)
	property.SetDefaultManager(property.NewManager(map[string]interface{}{
		"db.dsn":     "host=db port=5432",
		"redis.addr": "redis:6379",
	}))
	t.Setenv("BUCKET_ENV", "my-bucket")

	in := []byte(`{
		"source": { "type": "csv", "path": "${BUCKET_ENV}/in.csv" },
		"sink": { "type": "s3-sink", "bucket": "$property[db.dsn]", "endpoint": "${MISSING_ENV}" },
		"checkpoint": { "type": "redis", "addr": "$property[redis.addr]" },
		"scaling": { "coordinator": { "dsn": "$property[db.dsn]" } },
		"transforms": [ { "type": "map", "fields": { "keep": "=$property[db.dsn]" } } ],
		"reserved": "/data/${date}/x.parquet"
	}`)

	out, err := resolvePlaceholders(in)
	if err != nil {
		t.Fatalf("resolvePlaceholders: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	str := func(path ...string) string {
		var cur interface{} = got
		for _, p := range path {
			cur = cur.(map[string]interface{})[p]
		}
		s, _ := cur.(string)
		return s
	}

	if v := str("source", "path"); v != "my-bucket/in.csv" {
		t.Errorf("source.path = %q; want ${ENV} resolved", v)
	}
	if v := str("sink", "bucket"); v != "host=db port=5432" {
		t.Errorf("sink.bucket = %q; want $property resolved", v)
	}
	if v := str("sink", "endpoint"); v != "${MISSING_ENV}" {
		t.Errorf("sink.endpoint = %q; want unset ${ENV} left literal", v)
	}
	if v := str("checkpoint", "addr"); v != "redis:6379" {
		t.Errorf("checkpoint.addr = %q; want $property resolved in all sections", v)
	}
	if v := str("scaling", "coordinator", "dsn"); v != "host=db port=5432" {
		t.Errorf("scaling.coordinator.dsn = %q; want $property resolved (nested)", v)
	}
	if v, _ := got["reserved"].(string); v != "/data/${date}/x.parquet" {
		t.Errorf("reserved = %q; want ${date} template preserved", v)
	}
	field := got["transforms"].([]interface{})[0].(map[string]interface{})["fields"].(map[string]interface{})["keep"]
	if field != "=$property[db.dsn]" {
		t.Errorf("map expression = %q; want $property left for the expression engine", field)
	}
}
