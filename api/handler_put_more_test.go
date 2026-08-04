package api

import (
	"net/http"
	"testing"
)

// TestPutPipeline_ObservabilityChanged exercises the Factory.Replace rebuild
// branch: when the observability config differs from the previous definition,
// fresh recorders are built and the old action is shut down (rather than the
// recorder-reuse fast path). The seeded pipeline has no observability; the PUT
// sets metrics to "none", which differs and forces the rebuild path.
func TestPutPipeline_ObservabilityChanged(t *testing.T) {
	s := newTestServerWithPipeline(t)
	body := `{"source":{"type":"csv","options":{"path":"/tmp/x.csv"}},"sink":{"type":"null"},"observability":{"metrics":"none"}}`
	w := doPUT(t, s, "demo", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	act := s.findAction("demo")
	if act == nil {
		t.Fatal("pipeline demo missing after observability-changed PUT")
	}
	if act.SourceType() != "csv" || act.SinkType() != "null" {
		t.Errorf("after rebuild source/sink = %s/%s, want csv/null", act.SourceType(), act.SinkType())
	}
}
