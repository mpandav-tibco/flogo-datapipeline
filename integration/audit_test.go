package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
	"github.com/mpandav-tibco/flogo-datapipeline/audit"
)

func readAuditRecords(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open audit file %s: %v", path, err)
	}
	defer f.Close()
	var recs []map[string]interface{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad audit JSONL %q: %v", sc.Text(), err)
		}
		recs = append(recs, m)
	}
	return recs
}

// TestPipeline_AuditLog_EndToEnd drives a real CSV → filter → null pipeline
// through the DataPipelineAction with the audit log enabled, and asserts that a
// run.started and run.finished record are written with the expected (non-secret)
// fields.
func TestPipeline_AuditLog_EndToEnd(t *testing.T) {
	csvPath, cleanup := writeTempCSV(t, csvData)
	defer cleanup()
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")

	def := &pipeline.Definition{
		Name:   "audit-e2e",
		Source: pipeline.SourceDef{Type: "csv", Path: csvPath, Options: map[string]interface{}{"hasHeader": true}},
		Sink:   pipeline.SinkDef{Type: "null"},
		Transforms: []pipeline.TransformDef{
			{Type: "filter", Extra: map[string]interface{}{"type": "filter", "expr": "amount > 0"}},
		},
		Observability: pipeline.ObservabilityDef{
			Audit: audit.Config{Enabled: true, File: auditPath},
		},
	}
	def.Defaults() // fill performance/observability defaults (as the JSON loader + SDK do)

	act, err := pipeline.NewInline(def, "test://audit-e2e")
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

	recs := readAuditRecords(t, auditPath)
	if len(recs) != 2 {
		t.Fatalf("audit records = %d, want 2 (started + finished): %v", len(recs), recs)
	}
	started, finished := recs[0], recs[1]

	if started["event"] != "pipeline.run.started" {
		t.Errorf("first event = %v, want pipeline.run.started", started["event"])
	}
	if started["source"] != "csv" || started["sink"] != "null" {
		t.Errorf("start source/sink = %v/%v, want csv/null", started["source"], started["sink"])
	}
	if started["pipeline"] != "test://audit-e2e" {
		t.Errorf("pipeline = %v", started["pipeline"])
	}
	if tfs, ok := started["transforms"].([]interface{}); !ok || len(tfs) != 1 || tfs[0] != "filter" {
		t.Errorf("transforms = %v, want [filter]", started["transforms"])
	}

	if finished["event"] != "pipeline.run.finished" {
		t.Errorf("second event = %v, want pipeline.run.finished", finished["event"])
	}
	if finished["status"] != "completed" {
		t.Errorf("finished status = %v, want completed", finished["status"])
	}
	// rowsWritten should be present (3 rows with amount > 0).
	if rw, ok := finished["rowsWritten"].(float64); !ok || rw != 3 {
		t.Errorf("rowsWritten = %v, want 3", finished["rowsWritten"])
	}

	// Governance guarantee: no secret-bearing keys ever appear in the trail.
	for _, r := range recs {
		for _, k := range []string{"password", "passwordEnv", "dsn", "secret", "token", "options"} {
			if _, present := r[k]; present {
				t.Errorf("audit record leaked sensitive key %q: %v", k, r)
			}
		}
	}
}
