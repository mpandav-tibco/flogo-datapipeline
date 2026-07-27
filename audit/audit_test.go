package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func readRecords(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var recs []map[string]interface{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("invalid JSONL line %q: %v", line, err)
		}
		recs = append(recs, m)
	}
	return recs
}

// TestNewDisabledReturnsNoop: auditing is off by default.
func TestNewDisabledReturnsNoop(t *testing.T) {
	a, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.(Noop); !ok {
		t.Fatalf("disabled audit should be Noop, got %T", a)
	}
}

// TestNewEnabledViaEnv: DP_AUDIT enables auditing even when config says disabled.
func TestNewEnabledViaEnv(t *testing.T) {
	t.Setenv("DP_AUDIT", "on")
	a, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, ok := a.(Noop); ok {
		t.Fatal("DP_AUDIT=on should enable auditing")
	}
}

// TestFileAuditorWritesJSONL: records are one JSON object per line with event+ts.
func TestFileAuditorWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	a, err := New(Config{Enabled: true, File: path})
	if err != nil {
		t.Fatal(err)
	}
	a.Log("pipeline.run.started", map[string]interface{}{"runId": "r1", "pipeline": "p1"})
	a.Log("pipeline.run.finished", map[string]interface{}{"runId": "r1", "status": "completed", "rowsWritten": int64(5)})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	recs := readRecords(t, path)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0]["event"] != "pipeline.run.started" {
		t.Errorf("event = %v", recs[0]["event"])
	}
	if recs[0]["runId"] != "r1" || recs[0]["pipeline"] != "p1" {
		t.Errorf("start fields: %v", recs[0])
	}
	if _, ok := recs[0]["ts"].(string); !ok {
		t.Errorf("record missing ts: %v", recs[0])
	}
	if recs[1]["status"] != "completed" {
		t.Errorf("status = %v", recs[1]["status"])
	}
	if rw, ok := recs[1]["rowsWritten"].(float64); !ok || rw != 5 {
		t.Errorf("rowsWritten = %v", recs[1]["rowsWritten"])
	}
}

// TestFileAuditorAppendsAcrossOpens: reopening the same file appends (append-only trail).
func TestFileAuditorAppendsAcrossOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	a1, err := New(Config{Enabled: true, File: path})
	if err != nil {
		t.Fatal(err)
	}
	a1.Log("e1", nil)
	_ = a1.Close()

	a2, err := New(Config{Enabled: true, File: path})
	if err != nil {
		t.Fatal(err)
	}
	a2.Log("e2", nil)
	_ = a2.Close()

	if recs := readRecords(t, path); len(recs) != 2 {
		t.Fatalf("append across opens: got %d records, want 2", len(recs))
	}
}

// TestEnvFileOverride: DP_AUDIT + DP_AUDIT_FILE enable and redirect output.
func TestEnvFileOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env.jsonl")
	t.Setenv("DP_AUDIT", "true")
	t.Setenv("DP_AUDIT_FILE", path)
	a, err := New(Config{}) // config disabled + no file; env supplies both
	if err != nil {
		t.Fatal(err)
	}
	a.Log("e", nil)
	_ = a.Close()
	if recs := readRecords(t, path); len(recs) != 1 {
		t.Fatalf("env file override: got %d records, want 1", len(recs))
	}
}

// TestEventTsNotShadowable: caller fields cannot override the authoritative event/ts.
func TestEventTsNotShadowable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.jsonl")
	a, err := New(Config{Enabled: true, File: path})
	if err != nil {
		t.Fatal(err)
	}
	a.Log("real.event", map[string]interface{}{"event": "fake", "ts": "fake"})
	_ = a.Close()
	recs := readRecords(t, path)
	if recs[0]["event"] != "real.event" {
		t.Errorf("event must not be shadowable, got %v", recs[0]["event"])
	}
	if recs[0]["ts"] == "fake" {
		t.Errorf("ts must not be shadowable")
	}
}
