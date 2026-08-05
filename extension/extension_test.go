package extension_test

import (
	"io"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/extension"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// A plugin registered through the facade must be resolvable through the engine's
// own registry — proving extension.Register* writes to the SAME registry the
// engine reads, so an author using extension.* is wired identically to a built-in.
func TestFacadeSharesEngineRegistry(t *testing.T) {
	extension.RegisterSource("ext_facade_src", func(map[string]interface{}) (extension.Source, error) {
		return &oneRowSource{}, nil
	})
	extension.RegisterTransform("ext_facade_xf", func(map[string]interface{}) (extension.Transform, error) {
		return passthrough{}, nil
	})
	extension.RegisterSink("ext_facade_snk", func(map[string]interface{}) (extension.Sink, error) {
		return &countSink{}, nil
	})

	src, err := source.New("ext_facade_src", nil)
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	xf, err := transform.New("ext_facade_xf", nil)
	if err != nil {
		t.Fatalf("transform.New: %v", err)
	}
	snk, err := sink.New("ext_facade_snk", nil)
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}

	batch, err := src.ReadBatch(10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	out, err := xf.Apply(batch)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := snk.WriteBatch(out); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if got := snk.(*countSink).rows; got != 1 {
		t.Fatalf("sink saw %d rows, want 1", got)
	}
}

type oneRowSource struct{ done bool }

func (s *oneRowSource) Schema() []extension.Column {
	return []extension.Column{{Name: "id", Type: extension.TypeInt}}
}
func (s *oneRowSource) ReadBatch(int) (*extension.RecordBatch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	return &extension.RecordBatch{Columns: s.Schema(), Rows: [][]interface{}{{int64(1)}}}, nil
}
func (s *oneRowSource) Skip(int64) error { return nil }
func (s *oneRowSource) Close() error     { return nil }

type passthrough struct{}

func (passthrough) Name() string                                                    { return "passthrough" }
func (passthrough) Apply(in *extension.RecordBatch) (*extension.RecordBatch, error) { return in, nil }
func (passthrough) Close() error                                                    { return nil }

type countSink struct{ rows int }

func (s *countSink) WriteBatch(b *extension.RecordBatch) error { s.rows += b.Len(); return nil }
func (s *countSink) Flush() error                              { return nil }
func (s *countSink) Close() error                              { return nil }
