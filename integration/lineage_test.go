package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	"github.com/mpandav-tibco/flogo-datapipeline/lineage"
)

func readJSONL(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []map[string]interface{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad JSONL %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

// TestPipeline_Lineage_EndToEnd drives a real CSV → filter → null pipeline through
// the action with OpenLineage emission (file transport) and asserts the START /
// COMPLETE events, dataset naming, the shared run id, and the output row count.
func TestPipeline_Lineage_EndToEnd(t *testing.T) {
	csvPath, cleanup := writeTempCSV(t, csvData)
	defer cleanup()
	linPath := filepath.Join(t.TempDir(), "lineage.jsonl")

	def := &pipeline.Definition{
		Name:   "lineage-e2e",
		Source: pipeline.SourceDef{Type: "csv", Path: csvPath, Options: map[string]interface{}{"hasHeader": true}},
		Sink:   pipeline.SinkDef{Type: "null"},
		Transforms: []pipeline.TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
		},
		Observability: pipeline.ObservabilityDef{
			Lineage: lineage.Config{Enabled: true, Transport: "file", File: linPath, Namespace: "test-ns"},
		},
	}
	def.Defaults()

	act, err := pipeline.NewInline(def, "test://lineage-e2e")
	if err != nil {
		t.Fatalf("NewInline: %v", err)
	}
	defer act.Shutdown(context.Background())

	out, err := act.RunDirect(context.Background(), &pipeline.Input{})
	if err != nil {
		t.Fatalf("RunDirect: %v", err)
	}
	if out.Status != "completed" {
		t.Fatalf("run status = %q, want completed", out.Status)
	}

	events := readJSONL(t, linPath)
	if len(events) != 2 {
		t.Fatalf("lineage events = %d, want 2 (START + COMPLETE): %v", len(events), events)
	}
	start, done := events[0], events[1]

	if start["eventType"] != "START" || done["eventType"] != "COMPLETE" {
		t.Fatalf("event types = %v / %v, want START / COMPLETE", start["eventType"], done["eventType"])
	}
	startRun := start["run"].(map[string]interface{})["runId"]
	doneRun := done["run"].(map[string]interface{})["runId"]
	if startRun != doneRun {
		t.Errorf("START and COMPLETE run ids differ: %v / %v", startRun, doneRun)
	}
	job := start["job"].(map[string]interface{})
	if job["namespace"] != "test-ns" || job["name"] != "test://lineage-e2e" {
		t.Errorf("job = %v", job)
	}
	in0 := start["inputs"].([]interface{})[0].(map[string]interface{})
	if in0["namespace"] != "csv" || in0["name"] != csvPath {
		t.Errorf("input dataset = %v, want namespace=csv name=%s", in0, csvPath)
	}
	out0 := start["outputs"].([]interface{})[0].(map[string]interface{})
	if out0["namespace"] != "null" || out0["name"] != "null" {
		t.Errorf("output dataset = %v, want namespace=null name=null", out0)
	}
	// COMPLETE carries the output row count (3 rows have amount > 0).
	doneOut := done["outputs"].([]interface{})[0].(map[string]interface{})
	stats := doneOut["facets"].(map[string]interface{})["outputStatistics"].(map[string]interface{})
	if rc, _ := stats["rowCount"].(float64); rc != 3 {
		t.Errorf("outputStatistics.rowCount = %v, want 3", stats["rowCount"])
	}

	// Governance: no secret-bearing substrings ever appear in emitted events.
	raw, _ := os.ReadFile(linPath)
	for _, bad := range [][]byte{[]byte("password"), []byte("dsn"), []byte("secret"), []byte("token")} {
		if bytes.Contains(bytes.ToLower(raw), bad) {
			t.Errorf("lineage output leaked sensitive substring %q", bad)
		}
	}
}
