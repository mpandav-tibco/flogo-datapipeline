package transform

import (
	"errors"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

type passT struct{}

func (passT) Name() string                                              { return "passT" }
func (passT) Apply(in *record.RecordBatch) (*record.RecordBatch, error) { return in, nil }
func (passT) Close() error                                              { return nil }

type distinctT struct{}

func (distinctT) Name() string { return "distinctT" }
func (distinctT) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	out := record.Get()
	out.Columns = in.Columns
	out.Rows = append(out.Rows[:0], in.Rows...)
	return out, nil
}
func (distinctT) Close() error { return nil }

type dropT struct{}

func (dropT) Name() string                                           { return "dropT" }
func (dropT) Apply(*record.RecordBatch) (*record.RecordBatch, error) { return nil, nil }
func (dropT) Close() error                                           { return nil }

type errApplyT struct{}

func (errApplyT) Name() string { return "errApplyT" }
func (errApplyT) Apply(*record.RecordBatch) (*record.RecordBatch, error) {
	return nil, errors.New("apply boom")
}
func (errApplyT) Close() error { return nil }

type errCloseT struct{}

func (errCloseT) Name() string                                              { return "errCloseT" }
func (errCloseT) Apply(in *record.RecordBatch) (*record.RecordBatch, error) { return in, nil }
func (errCloseT) Close() error                                              { return errors.New("close boom") }

func mkBatch(rows ...[]interface{}) *record.RecordBatch {
	b := record.Get()
	b.Columns = []record.Column{{Name: "v", Type: record.TypeInt}}
	b.Rows = append(b.Rows[:0], rows...)
	return b
}

func TestTransformRegistry(t *testing.T) {
	Register("t-reg-dummy", func(map[string]interface{}) (Transform, error) { return passT{}, nil })
	if _, err := New("t-reg-dummy", nil); err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := New("t-reg-unknown", nil); err == nil {
		t.Fatal("New(unknown) expected error")
	}
}

func TestTransformRegisterDuplicatePanics(t *testing.T) {
	Register("t-reg-dup", func(map[string]interface{}) (Transform, error) { return passT{}, nil })
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate Register")
		}
	}()
	Register("t-reg-dup", func(map[string]interface{}) (Transform, error) { return passT{}, nil })
}

func TestBuild(t *testing.T) {
	Register("t-build-dummy", func(map[string]interface{}) (Transform, error) { return passT{}, nil })
	p, err := Build([]map[string]interface{}{{"type": "t-build-dummy"}, {"type": "t-build-dummy"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Stages()) != 2 {
		t.Errorf("stages = %d, want 2", len(p.Stages()))
	}
	if _, err := Build([]map[string]interface{}{{}}); err == nil {
		t.Error("Build with missing type expected error")
	}
	if _, err := Build([]map[string]interface{}{{"type": "t-nope-xyz"}}); err == nil {
		t.Error("Build with unknown type expected error")
	}
}

func TestPipelineApplyPassthroughAndDistinct(t *testing.T) {
	p := NewPipeline([]Transform{distinctT{}, passT{}})
	out, err := p.Apply(mkBatch([]interface{}{1}, []interface{}{2}, []interface{}{3}))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out == nil || out.Len() != 3 {
		t.Fatalf("out = %v, want 3 rows", out)
	}
	stats := p.StageStats()
	if len(stats) != 2 || stats[0].RowsIn != 3 || stats[1].RowsOut != 3 {
		t.Errorf("stats = %+v", stats)
	}
}

func TestPipelineApplyDropShortCircuits(t *testing.T) {
	p := NewPipeline([]Transform{distinctT{}, dropT{}, passT{}})
	out, err := p.Apply(mkBatch([]interface{}{1}))
	if err != nil || out != nil {
		t.Fatalf("Apply drop = %v, %v; want nil, nil", out, err)
	}
}

func TestPipelineApplyEmptyInput(t *testing.T) {
	p := NewPipeline([]Transform{passT{}})
	out, err := p.Apply(mkBatch())
	if err != nil || out != nil {
		t.Fatalf("Apply empty = %v, %v; want nil, nil", out, err)
	}
}

func TestPipelineApplyError(t *testing.T) {
	p := NewPipeline([]Transform{errApplyT{}})
	if _, err := p.Apply(mkBatch([]interface{}{1})); err == nil {
		t.Fatal("expected apply error")
	}
}

func TestPipelineCloseAccumulatesErrors(t *testing.T) {
	p := NewPipeline([]Transform{errCloseT{}, passT{}})
	err := p.Close()
	if err == nil || !strings.Contains(err.Error(), "close boom") {
		t.Fatalf("Close err = %v, want to contain 'close boom'", err)
	}
}
