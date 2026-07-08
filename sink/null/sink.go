// Package null registers a discard sink useful for testing and dead-letter routing.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/null"
package null

import (
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("null", func(_ map[string]interface{}) (sink.Sink, error) {
		return &nullSink{}, nil
	})
	sink.Register("discard", func(_ map[string]interface{}) (sink.Sink, error) {
		return &nullSink{}, nil
	})
}

type nullSink struct{}

func (n *nullSink) WriteBatch(_ *record.RecordBatch) error { return nil }
func (n *nullSink) Flush() error                           { return nil }
func (n *nullSink) Close() error                           { return nil }
