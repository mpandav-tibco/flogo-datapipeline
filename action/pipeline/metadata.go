package pipeline

import (
	"github.com/project-flogo/core/action"
	"github.com/project-flogo/core/data"
	"github.com/project-flogo/core/data/metadata"
)

// Input defines the action input schema.
type Input struct {
	InputPath string                 `md:"inputPath"`
	RunID     string                 `md:"runId"`
	Params    map[string]interface{} `md:"params"`
}

func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"inputPath": i.InputPath,
		"runId":     i.RunID,
		"params":    i.Params,
	}
}

func (i *Input) FromMap(m map[string]interface{}) error {
	if v, ok := m["inputPath"].(string); ok {
		i.InputPath = v
	}
	if v, ok := m["runId"].(string); ok {
		i.RunID = v
	}
	if v, ok := m["params"].(map[string]interface{}); ok {
		i.Params = v
	}
	return nil
}

// Output defines the action output schema.
type Output struct {
	RowsProcessed int64 `md:"rowsProcessed"`
	RowsWritten   int64 `md:"rowsWritten"`
	RowsFailed    int64 `md:"rowsFailed"`
	// RowsDropped counts rows that were intentionally discarded by a transform
	// (e.g. schemadrift policy=drop, or a filter that eliminated all rows in a
	// batch). Dropped rows are distinct from RowsFailed: a failure indicates a
	// processing error; a drop is a deliberate policy decision.
	RowsDropped int64  `md:"rowsDropped"`
	DurationMs  int64  `md:"durationMs"`
	Status      string `md:"status"`
	Error       string `md:"error"`
	// Metadata carries optional enrichment data contributed by specialised pipeline
	// stages. Currently populated keys:
	//   "dqReport" — *dq.DQReport from the last DQ gate stage (when present).
	// Downstream Flogo flows access this via $activity[pipeline].output.metadata.
	Metadata map[string]interface{} `md:"metadata"`
}

func (o *Output) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"rowsProcessed": o.RowsProcessed,
		"rowsWritten":   o.RowsWritten,
		"rowsFailed":    o.RowsFailed,
		"rowsDropped":   o.RowsDropped,
		"durationMs":    o.DurationMs,
		"status":        o.Status,
		"error":         o.Error,
	}
	if len(o.Metadata) > 0 {
		m["metadata"] = o.Metadata
	}
	return m
}

// actionMd is the action metadata derived from the Settings, Input, and Output structs.
var actionMd = action.ToMetadata(&Settings{}, &Input{}, &Output{})

// ioMetadata builds the IOMetadata for an action instance.
// Both Input and Output schemas are static — they don't vary per pipeline.
func buildIOMetadata() *metadata.IOMetadata {
	return &metadata.IOMetadata{
		Input: map[string]data.TypedValue{
			"inputPath": data.NewTypedValue(data.TypeString, ""),
			"runId":     data.NewTypedValue(data.TypeString, ""),
			"params":    data.NewTypedValue(data.TypeAny, nil),
		},
		Output: map[string]data.TypedValue{
			"rowsProcessed": data.NewTypedValue(data.TypeInt, int64(0)),
			"rowsWritten":   data.NewTypedValue(data.TypeInt, int64(0)),
			"rowsFailed":    data.NewTypedValue(data.TypeInt, int64(0)),
			"rowsDropped":   data.NewTypedValue(data.TypeInt, int64(0)),
			"durationMs":    data.NewTypedValue(data.TypeInt, int64(0)),
			"status":        data.NewTypedValue(data.TypeString, ""),
			"error":         data.NewTypedValue(data.TypeString, ""),
			"metadata":      data.NewTypedValue(data.TypeAny, nil),
		},
	}
}
