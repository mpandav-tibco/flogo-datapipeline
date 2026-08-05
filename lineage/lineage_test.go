package lineage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// captureSink records the raw bytes sent, for white-box event assertions.
type captureSink struct{ events [][]byte }

func (c *captureSink) send(b []byte) error {
	cp := make([]byte, len(b))
	copy(cp, b)
	c.events = append(c.events, cp)
	return nil
}
func (c *captureSink) Close() error { return nil }

func decode(t *testing.T, b []byte) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("event is not valid JSON: %v\n%s", err, b)
	}
	return m
}

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// TestUUIDFromStringStable: same input → same valid UUID; different → different.
func TestUUIDFromStringStable(t *testing.T) {
	a := uuidFromString("run-123")
	b := uuidFromString("run-123")
	c := uuidFromString("run-999")
	if a != b {
		t.Errorf("uuidFromString not stable: %q != %q", a, b)
	}
	if a == c {
		t.Errorf("distinct inputs produced same UUID: %q", a)
	}
	if !uuidRe.MatchString(a) {
		t.Errorf("not an RFC-4122 UUID: %q", a)
	}
	if a[14] != '5' {
		t.Errorf("expected version 5 UUID, got %q", a)
	}
}

// TestBuildStartEvent verifies the START RunEvent structure, run-id mapping,
// dataset naming, and the schema facet — with no outputStatistics on START.
func TestBuildStartEvent(t *testing.T) {
	cs := &captureSink{}
	e := &emitter{namespace: "ns1", sink: cs}

	e.Emit(Event{
		Type: Start, RunID: "abc", Job: "job1",
		Inputs:   []Dataset{{Namespace: "csv", Name: "/data/in.csv", Fields: []Field{{"id", "int"}, {"name", "string"}}}},
		Outputs:  []Dataset{{Namespace: "null", Name: "null"}},
		RowCount: -1,
	})
	if len(cs.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(cs.events))
	}
	m := decode(t, cs.events[0])
	if m["eventType"] != "START" {
		t.Errorf("eventType = %v", m["eventType"])
	}
	if m["producer"] != producer {
		t.Errorf("producer = %v", m["producer"])
	}
	run := m["run"].(map[string]interface{})
	if !uuidRe.MatchString(run["runId"].(string)) {
		t.Errorf("run.runId not a UUID: %v", run["runId"])
	}
	job := m["job"].(map[string]interface{})
	if job["namespace"] != "ns1" || job["name"] != "job1" {
		t.Errorf("job = %v", job)
	}
	inputs := m["inputs"].([]interface{})
	in0 := inputs[0].(map[string]interface{})
	if in0["namespace"] != "csv" || in0["name"] != "/data/in.csv" {
		t.Errorf("input dataset = %v", in0)
	}
	facets := in0["facets"].(map[string]interface{})
	schema := facets["schema"].(map[string]interface{})
	fields := schema["fields"].([]interface{})
	if len(fields) != 2 {
		t.Fatalf("schema fields = %d, want 2", len(fields))
	}
	f0 := fields[0].(map[string]interface{})
	if f0["name"] != "id" || f0["type"] != "int" {
		t.Errorf("field[0] = %v", f0)
	}
	// START must not carry outputStatistics.
	out0 := m["outputs"].([]interface{})[0].(map[string]interface{})
	if of, ok := out0["facets"].(map[string]interface{}); ok {
		if _, has := of["outputStatistics"]; has {
			t.Errorf("START output must not have outputStatistics")
		}
	}
}

// TestBuildCompleteEventStats verifies COMPLETE carries the output row count and
// that the START/COMPLETE run ids match (same run entity).
func TestBuildCompleteEventStats(t *testing.T) {
	cs := &captureSink{}
	e := &emitter{namespace: "ns", sink: cs}
	e.Emit(Event{Type: Start, RunID: "r", Job: "j", Outputs: []Dataset{{Namespace: "jdbc", Name: "orders"}}, RowCount: -1})
	e.Emit(Event{Type: Complete, RunID: "r", Job: "j", Outputs: []Dataset{{Namespace: "jdbc", Name: "orders"}}, RowCount: 42})

	start := decode(t, cs.events[0])
	done := decode(t, cs.events[1])
	if start["run"].(map[string]interface{})["runId"] != done["run"].(map[string]interface{})["runId"] {
		t.Errorf("START and COMPLETE run ids differ")
	}
	if done["eventType"] != "COMPLETE" {
		t.Errorf("eventType = %v", done["eventType"])
	}
	out0 := done["outputs"].([]interface{})[0].(map[string]interface{})
	stats := out0["facets"].(map[string]interface{})["outputStatistics"].(map[string]interface{})
	if rc, _ := stats["rowCount"].(float64); rc != 42 {
		t.Errorf("rowCount = %v, want 42", stats["rowCount"])
	}
}

// TestBuildCompleteEventCdcStats verifies the custom CDC-stats facet is
// attached when RowsDeleted >= 0 (a delete-capable sink reporting a real
// count), and absent entirely for a plain pipeline (RowsDeleted omitted,
// defaulting to -1) — the facet must not clutter every lineage event with an
// always-zero "rowsDeleted" for sinks with no delete concept.
func TestBuildCompleteEventCdcStats(t *testing.T) {
	cs := &captureSink{}
	e := &emitter{namespace: "ns", sink: cs}

	// A delete-capable run: 900 written, 100 of which were deletes.
	e.Emit(Event{Type: Complete, RunID: "r1", Job: "j", Outputs: []Dataset{{Namespace: "jdbc", Name: "orders"}}, RowCount: 900, RowsDeleted: 100})
	done := decode(t, cs.events[0])
	out0 := done["outputs"].([]interface{})[0].(map[string]interface{})
	facets := out0["facets"].(map[string]interface{})
	cdcStats, ok := facets["datapipeline_cdcStats"].(map[string]interface{})
	if !ok {
		t.Fatalf("datapipeline_cdcStats facet missing: %+v", facets)
	}
	if rd, _ := cdcStats["rowsDeleted"].(float64); rd != 100 {
		t.Errorf("rowsDeleted = %v, want 100", cdcStats["rowsDeleted"])
	}

	// A plain (non-delete-capable) run: RowsDeleted omitted (-1) — no facet.
	cs2 := &captureSink{}
	e2 := &emitter{namespace: "ns", sink: cs2}
	e2.Emit(Event{Type: Complete, RunID: "r2", Job: "j", Outputs: []Dataset{{Namespace: "jdbc", Name: "orders"}}, RowCount: 500, RowsDeleted: -1})
	done2 := decode(t, cs2.events[0])
	out0b := done2["outputs"].([]interface{})[0].(map[string]interface{})
	if facets2, ok := out0b["facets"].(map[string]interface{}); ok {
		if _, has := facets2["datapipeline_cdcStats"]; has {
			t.Error("datapipeline_cdcStats facet must be absent when RowsDeleted is omitted (-1)")
		}
	}
}

// TestFailEventType maps a failed run to a FAIL event via the emitter build.
func TestFailEventType(t *testing.T) {
	cs := &captureSink{}
	e := &emitter{namespace: "ns", sink: cs}
	e.Emit(Event{Type: Fail, RunID: "r", Job: "j", RowCount: -1})
	if decode(t, cs.events[0])["eventType"] != "FAIL" {
		t.Errorf("want FAIL")
	}
}

// TestNewDisabledReturnsNoop: lineage is off by default.
func TestNewDisabledReturnsNoop(t *testing.T) {
	e, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(Noop); !ok {
		t.Fatalf("disabled lineage should be Noop, got %T", e)
	}
}

// TestNewFileTransportEndToEnd writes real events to a file and reads them back.
func TestNewFileTransportEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lineage.jsonl")
	e, err := New(Config{Enabled: true, Transport: "file", File: path, Namespace: "ns"})
	if err != nil {
		t.Fatal(err)
	}
	e.Emit(Event{Type: Start, RunID: "r", Job: "j", Inputs: []Dataset{{Namespace: "csv", Name: "in.csv"}}, RowCount: -1})
	e.Emit(Event{Type: Complete, RunID: "r", Job: "j", Outputs: []Dataset{{Namespace: "null", Name: "null"}}, RowCount: 3})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	f, _ := os.Open(path)
	defer f.Close()
	var events []map[string]interface{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		events = append(events, decode(t, sc.Bytes()))
	}
	if len(events) != 2 {
		t.Fatalf("file events = %d, want 2", len(events))
	}
	if events[0]["eventType"] != "START" || events[1]["eventType"] != "COMPLETE" {
		t.Errorf("event types = %v / %v", events[0]["eventType"], events[1]["eventType"])
	}
}

// TestNewEnvEnable: DP_LINEAGE and OPENLINEAGE_URL enable emission.
func TestNewEnvEnable(t *testing.T) {
	t.Setenv("DP_LINEAGE", "on")
	e, err := New(Config{}) // console transport (default)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(Noop); ok {
		t.Fatal("DP_LINEAGE=on should enable lineage")
	}
	_ = e.Close()

	t.Setenv("DP_LINEAGE", "")
	t.Setenv("OPENLINEAGE_URL", "http://collector.example/api/v1/lineage")
	e2, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e2.(Noop); ok {
		t.Fatal("OPENLINEAGE_URL should enable lineage (http transport)")
	}
	if _, ok := e2.(*emitter).sink.(*httpSink); !ok {
		t.Fatalf("OPENLINEAGE_URL should select the http transport, got %T", e2.(*emitter).sink)
	}
	_ = e2.Close()
}

// TestNewTransportErrors: misconfiguration surfaces at construction.
func TestNewTransportErrors(t *testing.T) {
	if _, err := New(Config{Enabled: true, Transport: "http"}); err == nil {
		t.Error("http transport without url should error")
	}
	if _, err := New(Config{Enabled: true, Transport: "file"}); err == nil {
		t.Error("file transport without file should error")
	}
	if _, err := New(Config{Enabled: true, Transport: "carrier-pigeon"}); err == nil {
		t.Error("unknown transport should error")
	}
}
