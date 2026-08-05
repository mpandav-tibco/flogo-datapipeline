package acmeplugins

import (
	"sync"

	ext "github.com/mpandav-tibco/flogo-datapipeline/extension"
)

func init() { ext.RegisterSink("capture", newCaptureSink) }

// captureSink collects every written row into memory — handy for tests and dry
// runs. A real sink would write to a file, database, or network destination.
type captureSink struct {
	mu   sync.Mutex
	Rows [][]interface{}
}

func newCaptureSink(map[string]interface{}) (ext.Sink, error) { return &captureSink{}, nil }

func (s *captureSink) WriteBatch(b *ext.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range b.Rows {
		s.Rows = append(s.Rows, append([]interface{}(nil), row...))
	}
	return nil
}

func (s *captureSink) Flush() error { return nil }
func (s *captureSink) Close() error { return nil }
