package parse

import (
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func mkBatch(msgs ...string) *record.RecordBatch {
	b := &record.RecordBatch{
		Columns: []record.Column{
			{Name: "node", Type: record.TypeString},
			{Name: "message", Type: record.TypeString},
		},
	}
	for _, m := range msgs {
		b.Rows = append(b.Rows, []interface{}{"node-1", m})
	}
	return b
}

func colIdx(cols []record.Column, name string) int {
	for i, c := range cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

func TestRegexParse(t *testing.T) {
	tr, err := transform.New("parse", map[string]interface{}{
		"source":  "message",
		"mode":    "regex",
		"pattern": `^(?P<ts>\S+)\s+(?P<level>\w+)\s+\[(?P<thread>[^\]]+)\]\s+(?P<msg>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}
	in := mkBatch("2026-07-14T10:00:00Z ERROR [worker-3] boom happened")
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	// Original columns preserved + 4 named groups appended.
	for _, want := range []string{"node", "message", "ts", "level", "thread", "msg"} {
		if colIdx(out.Columns, want) < 0 {
			t.Errorf("missing output column %q (cols=%v)", want, out.Columns)
		}
	}
	get := func(name string) interface{} { return out.Rows[0][colIdx(out.Columns, name)] }
	if get("level") != "ERROR" {
		t.Errorf("level = %v, want ERROR", get("level"))
	}
	if get("thread") != "worker-3" {
		t.Errorf("thread = %v, want worker-3", get("thread"))
	}
	if get("msg") != "boom happened" {
		t.Errorf("msg = %v, want 'boom happened'", get("msg"))
	}
}

func TestRegexNoMatchKeep(t *testing.T) {
	tr, _ := transform.New("parse", map[string]interface{}{
		"source":  "message",
		"mode":    "regex",
		"pattern": `^(?P<level>ERROR|WARN)\b.*$`,
		"onError": "keep",
	})
	in := mkBatch("this line has no level prefix")
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 1 {
		t.Fatalf("keep mode should retain the row, got %d rows", out.Len())
	}
	if v := out.Rows[0][colIdx(out.Columns, "level")]; v != nil {
		t.Errorf("unmatched row should have nil level, got %v", v)
	}
}

func TestRegexNoMatchDrop(t *testing.T) {
	tr, _ := transform.New("parse", map[string]interface{}{
		"source":  "message",
		"mode":    "regex",
		"pattern": `^(?P<level>ERROR)\b.*$`,
		"onError": "drop",
	})
	in := mkBatch("ERROR real error", "INFO not an error", "ERROR another")
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Len() != 2 {
		t.Fatalf("drop mode should keep only the 2 matching rows, got %d", out.Len())
	}
}

func TestRegexRouteAddsErrorFlag(t *testing.T) {
	tr, _ := transform.New("parse", map[string]interface{}{
		"source":  "message",
		"mode":    "regex",
		"pattern": `^(?P<level>ERROR)\b.*$`,
		"onError": "route",
	})
	in := mkBatch("ERROR ok", "garbage")
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	ei := colIdx(out.Columns, "_parse_error")
	if ei < 0 {
		t.Fatal("route mode must add a _parse_error column")
	}
	if out.Rows[0][ei] != "false" || out.Rows[1][ei] != "true" {
		t.Errorf("_parse_error flags = %v/%v, want false/true", out.Rows[0][ei], out.Rows[1][ei])
	}
}

func TestJSONParse(t *testing.T) {
	tr, err := transform.New("parse", map[string]interface{}{
		"source": "message",
		"mode":   "json",
		"fields": []interface{}{"level", "logger", "trace_id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := mkBatch(`{"level":"ERROR","logger":"com.foo.Bar","trace_id":"abc123","extra":"ignored"}`)
	out, err := tr.Apply(in)
	if err != nil {
		t.Fatal(err)
	}
	get := func(name string) interface{} { return out.Rows[0][colIdx(out.Columns, name)] }
	if get("level") != "ERROR" || get("logger") != "com.foo.Bar" || get("trace_id") != "abc123" {
		t.Errorf("json extract = %v/%v/%v", get("level"), get("logger"), get("trace_id"))
	}
	// Un-listed key must not become a column.
	if colIdx(out.Columns, "extra") >= 0 {
		t.Error("json field allow-list should exclude 'extra'")
	}
}

func TestJSONParseWithPrefix(t *testing.T) {
	tr, _ := transform.New("parse", map[string]interface{}{
		"source": "message",
		"mode":   "json",
		"fields": []interface{}{"level"},
		"prefix": "log_",
	})
	in := mkBatch(`{"level":"WARN"}`)
	out, _ := tr.Apply(in)
	if colIdx(out.Columns, "log_level") < 0 {
		t.Fatal("prefix should produce column log_level")
	}
	if v := out.Rows[0][colIdx(out.Columns, "log_level")]; v != "WARN" {
		t.Errorf("log_level = %v, want WARN", v)
	}
}

func TestDropSourceColumn(t *testing.T) {
	tr, _ := transform.New("parse", map[string]interface{}{
		"source":     "message",
		"mode":       "regex",
		"pattern":    `^(?P<level>\w+).*$`,
		"keepSource": false,
	})
	in := mkBatch("INFO hello")
	out, _ := tr.Apply(in)
	if colIdx(out.Columns, "message") >= 0 {
		t.Error("keepSource=false should drop the 'message' column")
	}
	if colIdx(out.Columns, "level") < 0 {
		t.Error("extracted 'level' column should be present")
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := transform.New("parse", map[string]interface{}{"mode": "regex", "pattern": "x"}); err == nil {
		t.Error("missing 'source' should error")
	}
	if _, err := transform.New("parse", map[string]interface{}{"source": "m", "mode": "regex", "pattern": "no groups here"}); err == nil {
		t.Error("pattern without named groups should error")
	}
	if _, err := transform.New("parse", map[string]interface{}{"source": "m", "mode": "regex", "pattern": "([unclosed"}); err == nil {
		t.Error("invalid regex should error")
	}
}
