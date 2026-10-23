package pipeline

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
)

// failingCheckpoint accepts Load/Delete but every Save fails, modelling an
// unreachable checkpoint backend.
type failingCheckpoint struct{ saves *int64 }

var failingSaves int64

func init() {
	checkpoint.Register("failtest", func(map[string]interface{}) (checkpoint.Checkpoint, error) {
		return failingCheckpoint{saves: &failingSaves}, nil
	})
}

func (f failingCheckpoint) Save(string, *checkpoint.Token) error {
	atomic.AddInt64(f.saves, 1)
	return errors.New("backend unavailable")
}
func (failingCheckpoint) Load(string) (*checkpoint.Token, error) { return nil, nil }
func (failingCheckpoint) Delete(string) error                    { return nil }
func (failingCheckpoint) Close() error                           { return nil }

func checkpointPolicyDef(t *testing.T, policy string) *Definition {
	var sb strings.Builder
	sb.WriteString("id,name,amount\n")
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&sb, "%d,n%d,%d\n", i, i, i)
	}
	return &Definition{
		Source:      csvSource(writeTemp(t, "in.csv", sb.String())),
		Sink:        jsonlSink(filepath.Join(t.TempDir(), "out.jsonl")),
		Performance: PerformanceDef{BatchSize: 10},
		Checkpoint:  CheckpointDef{Type: "failtest", IntervalRows: 20, OnSaveError: policy},
	}
}

func TestCheckpointOnSaveError_ContinueByDefault(t *testing.T) {
	atomic.StoreInt64(&failingSaves, 0)
	out, err := runInline(t, checkpointPolicyDef(t, ""))
	if err != nil || out.Status != "completed" || out.RowsWritten != 50 {
		t.Fatalf("default policy must continue: err=%v status=%q written=%d", err, out.Status, out.RowsWritten)
	}
	if atomic.LoadInt64(&failingSaves) < 2 {
		t.Fatalf("expected interval + final save attempts, got %d", failingSaves)
	}
}

func TestCheckpointOnSaveError_Fail(t *testing.T) {
	atomic.StoreInt64(&failingSaves, 0)
	out, err := runInline(t, checkpointPolicyDef(t, "fail"))
	if err == nil && out.Status != "failed" {
		t.Fatalf("onSaveError=fail must stop the run: err=%v status=%q", err, out.Status)
	}
	msg := out.Error
	if err != nil {
		msg = err.Error()
	}
	if !strings.Contains(msg, "onSaveError=fail") {
		t.Fatalf("error should name the policy, got %q", msg)
	}
	if out.RowsWritten >= 50 {
		t.Fatalf("run kept going after the first failed checkpoint (written=%d)", out.RowsWritten)
	}
}

func TestCheckpointOnSaveError_Validation(t *testing.T) {
	d := checkpointPolicyDef(t, "explode")
	d.Defaults()
	if err := d.Validate(); err == nil || !strings.Contains(err.Error(), "onSaveError") {
		t.Fatalf("want validation error, got %v", err)
	}
}
