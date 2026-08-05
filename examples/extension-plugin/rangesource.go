package acmeplugins

import (
	"fmt"
	"io"

	ext "github.com/mpandav-tibco/flogo-datapipeline/extension"
)

// rangesource emits `count` rows with columns {n:int, label:"item-<n>"}.
// A minimal custom Source built entirely on the public extension contract.
func init() { ext.RegisterSource("rangesource", newRangeSource) }

type rangeSource struct {
	count int
	next  int
}

func newRangeSource(cfg map[string]interface{}) (ext.Source, error) {
	n, ok := toInt(cfg["count"])
	if !ok || n < 0 {
		return nil, fmt.Errorf("rangesource: 'count' must be a non-negative integer")
	}
	return &rangeSource{count: n}, nil
}

func (s *rangeSource) Schema() []ext.Column {
	return []ext.Column{
		{Name: "n", Type: ext.TypeInt},
		{Name: "label", Type: ext.TypeString},
	}
}

func (s *rangeSource) ReadBatch(size int) (*ext.RecordBatch, error) {
	if s.next >= s.count {
		return nil, io.EOF
	}
	b := &ext.RecordBatch{Columns: s.Schema()}
	for i := 0; i < size && s.next < s.count; i++ {
		b.Rows = append(b.Rows, []interface{}{int64(s.next), fmt.Sprintf("item-%d", s.next)})
		s.next++
	}
	return b, nil
}

func (s *rangeSource) Skip(n int64) error { s.next += int(n); return nil }
func (s *rangeSource) Close() error       { return nil }

func toInt(v interface{}) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64: // JSON numbers decode as float64
		return int(x), true
	default:
		return 0, false
	}
}
