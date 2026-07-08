// Package dlq registers a Dead-Letter Queue sink that wraps an inner sink and
// annotates each routed row with a JSON error-envelope column before forwarding
// to the inner destination.
//
// The DLQ sink is designed for write-time error routing: when a row fails at
// the primary sink, it is forwarded here with metadata explaining the failure.
// This is distinct from the validate transform's errorSink, which handles rows
// that violate declared business rules before the write stage.
//
// Import to activate:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/dlq"
//
// Example pipeline definition snippet:
//
//	"dlq": {
//	  "type":            "jdbc",
//	  "options":         { "dsn": "...", "table": "dlq_errors" },
//	  "maxRowsPerBatch": 500,
//	  "onFull":          "drop"
//	}
//
// The outer sink type is always "dlq". The inner destination is selected by the
// "innerType" and "innerOptions" keys inside the options map. Example config map
// passed to the dlq factory:
//
//	{
//	  "innerType":       "jdbc",
//	  "innerOptions":    { "dsn": "postgres://...", "table": "my_dlq" },
//	  "envelopeColumn":  "_dlq_error",   // optional, default "_dlq_error"
//	  "maxRowsPerBatch": 500,             // optional, 0 = unlimited
//	  "onFull":          "drop"           // optional, "block" (default) | "drop"
//	}
package dlq

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

var dlog = corelog.ChildLogger(corelog.RootLogger(), "dlq")

func init() {
	sink.Register("dlq", newDLQSink)
}

// ErrorEnvelope is the JSON structure written into the envelope column for
// each row that is routed to the DLQ.  All fields are exported so that
// encoding/json can marshal them without a custom marshaller.
type ErrorEnvelope struct {
	Pipeline     string      `json:"pipeline"`
	RunID        string      `json:"runId"`
	BatchSeq     int64       `json:"batchSeq"`
	RowIndex     int         `json:"rowIndex"`
	ErrorMessage string      `json:"errorMessage"`
	OriginalRow  interface{} `json:"originalRow"`
	RoutedAt     string      `json:"routedAt"`
}

// dlqSink wraps an inner Sink and prepends an error-envelope column to every
// row before forwarding to the inner sink.
//
// The inner sink is opened lazily on the first WriteBatch call so that pipelines
// with zero bad rows pay no connection overhead.
type dlqSink struct {
	// configuration
	innerType      string
	innerOptions   map[string]interface{}
	envelopeColumn string
	maxRowsPerBatch int  // 0 = unlimited
	onFull         string // "block" | "drop"

	// runtime state — guarded by mu for concurrent safety
	mu       sync.Mutex
	inner    sink.Sink // nil until first write
	batchSeq int64     // monotonically increasing batch counter

	// context injected by the action before the pipeline loop begins
	pipelineRef string
	runID       string
}

func newDLQSink(cfg map[string]interface{}) (sink.Sink, error) {
	innerType, _ := cfg["innerType"].(string)
	if innerType == "" {
		return nil, fmt.Errorf("dlq sink: 'innerType' is required")
	}

	var innerOptions map[string]interface{}
	if opts, ok := cfg["innerOptions"].(map[string]interface{}); ok {
		innerOptions = opts
	} else {
		innerOptions = map[string]interface{}{}
	}

	envelopeColumn, _ := cfg["envelopeColumn"].(string)
	if envelopeColumn == "" {
		envelopeColumn = "_dlq_error"
	}

	maxRowsPerBatch := 0
	if v, ok := cfg["maxRowsPerBatch"].(int); ok && v > 0 {
		maxRowsPerBatch = v
	} else if v, ok := cfg["maxRowsPerBatch"].(float64); ok && v > 0 {
		maxRowsPerBatch = int(v)
	}

	onFull, _ := cfg["onFull"].(string)
	if onFull == "" {
		onFull = "block"
	}
	if onFull != "block" && onFull != "drop" {
		return nil, fmt.Errorf("dlq sink: 'onFull' must be 'block' or 'drop', got %q", onFull)
	}

	return &dlqSink{
		innerType:       innerType,
		innerOptions:    innerOptions,
		envelopeColumn:  envelopeColumn,
		maxRowsPerBatch: maxRowsPerBatch,
		onFull:          onFull,
	}, nil
}

// InjectContext sets the pipeline reference and run ID that appear in every
// error envelope.  Called by the action's openDLQ() before the pipeline loop.
func (d *dlqSink) InjectContext(pipelineRef, runID string) {
	d.mu.Lock()
	d.pipelineRef = pipelineRef
	d.runID = runID
	d.mu.Unlock()
}

// WriteBatch accepts a batch of rows that have already been tagged with error
// context.  Each row is enriched with an ErrorEnvelope JSON column and written
// to the inner sink.
//
// batch.Columns must not include the envelope column — dlqSink adds it.
// The caller is responsible for passing a batch whose rows map 1:1 with the
// error messages provided via the parallel errors slice.
//
// This overload is the internal path used by rowLevelWrite in action.go.
// The public sink.Sink interface WriteBatch is also implemented so dlqSink can
// be used as a drop-in destination in other pipeline configurations.
func (d *dlqSink) WriteBatch(batch *record.RecordBatch) error {
	return d.writeBatchWithErrors(batch, nil)
}

// WriteBatchErrors is the extended write path called by rowLevelWrite.
// errors[i] is the error message for batch.Rows[i].  If errors is nil or
// shorter than batch.Rows, rows without a matching error entry get an empty
// errorMessage.
func (d *dlqSink) WriteBatchErrors(batch *record.RecordBatch, errors []string) error {
	return d.writeBatchWithErrors(batch, errors)
}

func (d *dlqSink) writeBatchWithErrors(batch *record.RecordBatch, errors []string) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Lazy open: connect to the inner sink only when the first bad row arrives.
	if d.inner == nil {
		inner, err := sink.New(d.innerType, d.innerOptions)
		if err != nil {
			return fmt.Errorf("dlq sink: open inner sink %q: %w", d.innerType, err)
		}
		d.inner = inner
		dlog.Infof("dlq sink: opened inner sink type=%q", d.innerType)
	}

	d.batchSeq++
	batchSeq := d.batchSeq

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Build the output schema: original columns + envelope column.
	outCols := make([]record.Column, len(batch.Columns)+1)
	copy(outCols, batch.Columns)
	outCols[len(batch.Columns)] = record.Column{
		Name: d.envelopeColumn,
		Type: record.TypeString,
	}

	outBatch := record.Get()
	outBatch.Columns = outCols

	rows := batch.Rows
	if d.maxRowsPerBatch > 0 && len(rows) > d.maxRowsPerBatch {
		switch d.onFull {
		case "drop":
			dlog.Warnf("dlq sink: maxRowsPerBatch=%d exceeded (batch has %d rows) — dropping overflow rows",
				d.maxRowsPerBatch, len(rows))
			rows = rows[:d.maxRowsPerBatch]
		default: // "block" — accept all rows regardless; no blocking needed here because
			// we are already in the write path. "block" semantics mean don't drop.
		}
	}

	for i, row := range rows {
		// Build the original row as a map for the envelope JSON.
		rowMap := make(map[string]interface{}, len(batch.Columns))
		for j, col := range batch.Columns {
			if j < len(row) {
				rowMap[col.Name] = row[j]
			}
		}

		errMsg := ""
		if errors != nil && i < len(errors) {
			errMsg = errors[i]
		}

		env := ErrorEnvelope{
			Pipeline:     d.pipelineRef,
			RunID:        d.runID,
			BatchSeq:     batchSeq,
			RowIndex:     i,
			ErrorMessage: errMsg,
			OriginalRow:  rowMap,
			RoutedAt:     now,
		}
		envJSON, err := json.Marshal(env)
		if err != nil {
			// If marshalling fails (extremely unlikely), write a minimal envelope.
			envJSON = []byte(fmt.Sprintf(`{"errorMessage":%q,"marshalError":"envelope marshal failed"}`, errMsg))
		}

		outRow := make([]interface{}, len(outCols))
		for j := range batch.Columns {
			if j < len(row) {
				outRow[j] = row[j]
			}
		}
		outRow[len(batch.Columns)] = string(envJSON)

		outBatch.Rows = append(outBatch.Rows, outRow)
	}

	writeErr := d.inner.WriteBatch(outBatch)
	record.Put(outBatch)
	if writeErr != nil {
		return fmt.Errorf("dlq sink: inner write: %w", writeErr)
	}
	return nil
}

// Flush flushes the inner sink.  Safe to call when no rows have been routed
// (inner is nil in that case).
func (d *dlqSink) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return nil
	}
	return d.inner.Flush()
}

// Close flushes and closes the inner sink.  A nil inner (no rows ever routed)
// is a no-op.
func (d *dlqSink) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return nil
	}
	return d.inner.Close()
}
