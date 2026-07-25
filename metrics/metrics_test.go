package metrics

import (
	"context"
	"testing"
	"time"
)

func TestNoopRecorder_AllMethods(t *testing.T) {
	var r Recorder = NoopRecorder{}
	// None of these should panic or allocate observable state.
	r.BatchRead("p", "csv", 10, time.Millisecond)
	r.BatchTransformed("p", 10, 8, time.Millisecond)
	r.BatchWritten("p", "null", 8, time.Millisecond)
	r.BatchFailed("p", 2)
	r.CheckpointSaved("p")
	r.PipelineComplete("p", "ok", 10, 8, 2, time.Second)
	r.Stall("p", "read", time.Second)
	r.Counter("c", "help", map[string]string{"k": "v"}, 1)
	r.Gauge("g", "help", nil, 3.14)
	if err := r.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestNew_NoneAndEmpty(t *testing.T) {
	for _, name := range []string{"", "none"} {
		r, err := New(name, nil)
		if err != nil {
			t.Fatalf("New(%q): %v", name, err)
		}
		if _, ok := r.(NoopRecorder); !ok {
			t.Errorf("New(%q) = %T, want NoopRecorder", name, r)
		}
	}
}

func TestNew_UnknownBackend(t *testing.T) {
	if _, err := New("does-not-exist", nil); err == nil {
		t.Error("unknown backend should error")
	}
}

func TestRegisterAndBackends(t *testing.T) {
	name := "testbackend"
	Register(name, func(cfg map[string]interface{}) (Recorder, error) {
		return NoopRecorder{}, nil
	})
	found := false
	for _, b := range Backends() {
		if b == name {
			found = true
		}
	}
	if !found {
		t.Errorf("Backends() missing %q: %v", name, Backends())
	}
	// New should now resolve it.
	if _, err := New(name, nil); err != nil {
		t.Errorf("New(%q): %v", name, err)
	}
}

func TestRegister_DuplicatePanics(t *testing.T) {
	name := "dupbackend"
	Register(name, func(cfg map[string]interface{}) (Recorder, error) { return NoopRecorder{}, nil })
	defer func() {
		if recover() == nil {
			t.Error("duplicate Register should panic")
		}
	}()
	Register(name, func(cfg map[string]interface{}) (Recorder, error) { return NoopRecorder{}, nil })
}
