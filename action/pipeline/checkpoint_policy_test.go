package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/checkpoint"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
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

// cancelOnApply cancels the run from inside Apply and fails the batch, like a
// lookup aborted by cancellation.
type cancelOnApply struct{ cancel func() }

func (c *cancelOnApply) Name() string { return "cancelonapply" }
func (c *cancelOnApply) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	c.cancel()
	return nil, errors.New("aborted by cancellation")
}
func (c *cancelOnApply) Close() error { return nil }

var cancelHook func()

func init() {
	transform.Register("cancelonapply", func(map[string]interface{}) (transform.Transform, error) {
		return &cancelOnApply{cancel: func() { cancelHook() }}, nil
	})
}

type countingCheckpoint struct{ saves *int64 }

var countedSaves int64

func init() {
	checkpoint.Register("counttest", func(map[string]interface{}) (checkpoint.Checkpoint, error) {
		return countingCheckpoint{saves: &countedSaves}, nil
	})
}
func (c countingCheckpoint) Save(string, *checkpoint.Token) error {
	atomic.AddInt64(c.saves, 1)
	return nil
}
func (countingCheckpoint) Load(string) (*checkpoint.Token, error) { return nil, nil }
func (countingCheckpoint) Delete(string) error                    { return nil }
func (countingCheckpoint) Close() error                           { return nil }

// A transform failure caused by cancellation must stop the run before a due
// checkpoint is saved — otherwise the checkpoint (and source offsets) would
// advance past rows that were never written.
func TestCancelledTransformNeverCheckpoints(t *testing.T) {
	atomic.StoreInt64(&countedSaves, 0)
	def := checkpointPolicyDef(t, "")
	def.Checkpoint = CheckpointDef{Type: "counttest", IntervalRows: 1}
	def.Transforms = []TransformDef{{Type: "cancelonapply", Extra: map[string]interface{}{"type": "cancelonapply"}}}
	def.Defaults()
	act, err := NewInline(def, "sdk://test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelHook = cancel
	out, _ := act.RunDirect(ctx, &Input{RunID: "cancel-run"})
	if n := atomic.LoadInt64(&countedSaves); n != 0 {
		t.Fatalf("checkpoint saved %d time(s) after cancellation", n)
	}
	if out.Status == "completed" {
		t.Fatalf("cancelled run reported completed")
	}
}
