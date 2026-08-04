package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
)

// newTxServer seeds a csv → filter → mapper → null pipeline so the
// preview/sampleinput/teststage endpoints exercise real transform stages.
func newTxServer(t *testing.T) (*Server, string) {
	t.Helper()
	csvPath := filepath.Join(t.TempDir(), "in.csv")
	if err := os.WriteFile(csvPath, []byte("id,name,amount\n1,alice,100\n2,bob,0\n3,carol,300\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := &pipeline.Factory{}
	defJSON := fmt.Sprintf(`{
		"source":{"type":"csv","path":%q,
			"columns":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"amount","type":"float"}],
			"options":{"hasHeader":true}},
		"transforms":[
			{"type":"filter","expr":"amount > 0"},
			{"type":"mapper","fields":{"id":"=$id","upper_name":"=upper($name)"}}
		],
		"sink":{"type":"null"}}`, csvPath)
	def := mustDef(t, defJSON)
	if _, err := f.Replace("res://pipeline:tx-demo", def); err != nil {
		t.Fatalf("seed tx pipeline: %v", err)
	}
	return NewServer(f, ":0"), "tx-demo"
}

func TestPreviewWithTransforms(t *testing.T) {
	s, ref := newTxServer(t)
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/preview?limit=10", "")
	if w.Code != http.StatusOK {
		t.Fatalf("preview = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestSampleInput(t *testing.T) {
	s, ref := newTxServer(t)
	// Rows entering stage 1 (mapper) = source run through the filter.
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/sampleinput", `{"stage":1,"limit":10}`)
	if w.Code != http.StatusOK {
		t.Fatalf("sampleinput = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// Unknown ref → 404.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/nope/sampleinput", `{"stage":0}`); w.Code != http.StatusNotFound {
		t.Errorf("sampleinput unknown ref = %d, want 404", w.Code)
	}
}

func TestTestStage(t *testing.T) {
	s, ref := newTxServer(t)

	// Stage 0 is the filter (amount > 0): one row kept, one dropped.
	body := `{"stage":0,"columns":["id","name","amount"],"rows":[` +
		`{"id":1,"name":"a","amount":50},{"id":2,"name":"b","amount":0}]}`
	w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/teststage", body)
	if w.Code != http.StatusOK {
		t.Fatalf("teststage = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var res pipeline.StageTestResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.Kept != 1 || res.Dropped != 1 {
		t.Errorf("teststage filter kept=%d dropped=%d, want 1/1", res.Kept, res.Dropped)
	}

	// Stage 1 is the mapper: both rows kept, upper_name derived.
	body2 := `{"stage":1,"columns":["id","name"],"rows":[{"id":1,"name":"alice"}]}`
	w2 := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/teststage", body2)
	if w2.Code != http.StatusOK {
		t.Fatalf("teststage mapper = %d, want 200; body=%s", w2.Code, w2.Body.String())
	}

	// Stage out of range → 400.
	if w := do(t, s, http.MethodPost, "/v1/pipelines/"+ref+"/teststage",
		`{"stage":9,"rows":[{"a":1}]}`); w.Code != http.StatusBadRequest {
		t.Errorf("teststage out-of-range = %d, want 400", w.Code)
	}
}
