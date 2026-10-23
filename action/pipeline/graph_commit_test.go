package pipeline

import (
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// committingSource emits 20 rows in batches of 4 and records every Commit, like
// a Kafka source in commitMode "checkpoint".
type committingSource struct {
	mu      sync.Mutex
	sent    int
	commits []int64
}

var lastCommitting *committingSource

func init() {
	source.Register("committest", func(map[string]interface{}) (source.Source, error) {
		lastCommitting = &committingSource{}
		return lastCommitting, nil
	})
}

func (s *committingSource) Schema() []record.Column {
	return []record.Column{{Name: "amount", Type: record.TypeFloat}}
}
func (s *committingSource) ReadBatch(int) (*record.RecordBatch, error) {
	if s.sent >= 20 {
		return nil, io.EOF
	}
	b := record.Get()
	b.Columns = s.Schema()
	for i := 0; i < 4; i++ {
		b.Rows = append(b.Rows, []interface{}{float64(s.sent * 50)})
		s.sent++
	}
	return b, nil
}
func (s *committingSource) Skip(int64) error { return nil }
func (s *committingSource) Close() error     { return nil }
func (s *committingSource) Commit(n int64) error {
	s.mu.Lock()
	s.commits = append(s.commits, n)
	s.mu.Unlock()
	return nil
}

// The graph runner must acknowledge a committing source at each durable
// checkpoint barrier, as the linear engine does; otherwise Kafka offsets in
// commitMode "checkpoint" are never committed on graph pipelines.
func TestGraphRun_CheckpointCommitsSource(t *testing.T) {
	big := filepath.Join(t.TempDir(), "big.jsonl")
	rest := filepath.Join(t.TempDir(), "rest.jsonl")
	def := &Definition{
		Source: SourceDef{Type: "committest", Options: map[string]interface{}{}},
		Sink:   SinkDef{Type: "null"},
		Checkpoint: CheckpointDef{
			Type: "file", Path: t.TempDir(), Key: "graph-commit", IntervalRows: 8,
		},
		Graph: &GraphDef{
			Nodes: []GraphNodeDef{
				{ID: "source", Kind: "source", Type: "committest"},
				{ID: "big", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": big}}},
				{ID: "rest", Kind: "sink", Type: "jsonlines", Extra: map[string]interface{}{"config": map[string]interface{}{"path": rest}}},
			},
			Edges: []GraphEdge{
				{From: "source:rows", To: "big:in", Type: "expression", Value: "amount > 400"},
				{From: "source:rows", To: "rest:in", Type: "otherwise"},
			},
		},
	}
	out, err := runInline(t, def)
	if err != nil || out.RowsWritten != 20 {
		t.Fatalf("run: err=%v written=%d", err, out.RowsWritten)
	}
	s := lastCommitting
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.commits) < 2 || s.commits[0] != 8 || s.commits[1] != 16 {
		t.Fatalf("commits=%v; want checkpoint-aligned commits starting [8 16 ...]", s.commits)
	}
}
