package pipeline

import (
	"context"
	"fmt"
	"io"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

// PreviewResult is the outcome of a bounded dry-run preview: sample rows read
// from the source and passed through the transform chain, with no sink writes.
// It powers the Studio "Dry-run Preview" panel so users can validate a pipeline
// against live data before committing to a full run.
type PreviewResult struct {
	// Columns is the ordered column list of the transformed rows.
	Columns []string `json:"columns"`
	// Rows are up to `limit` sample rows after the transform chain.
	Rows []map[string]interface{} `json:"rows"`
	// RowsRead is the number of source rows consumed to produce the sample.
	RowsRead int64 `json:"rowsRead"`
	// RowsDropped counts rows deliberately discarded by a transform (filter, drift-drop).
	RowsDropped int64 `json:"rowsDropped"`
	// RowsFailed counts rows the transform chain could not process.
	RowsFailed int64 `json:"rowsFailed"`
	// Truncated is true when the source had more rows than the preview limit.
	Truncated bool `json:"truncated"`
}

// DefinitionSummary returns a JSON-safe snapshot of the parsed pipeline
// definition for the Studio Inspector / verification. Transform configs are
// returned verbatim (the raw map the factory received).
func (a *DataPipelineAction) DefinitionSummary() map[string]interface{} {
	def := a.def
	transforms := make([]map[string]interface{}, 0, len(def.Transforms))
	for _, td := range def.Transforms {
		transforms = append(transforms, td.Extra)
	}
	cols := make([]map[string]interface{}, 0, len(def.Source.Columns))
	for _, c := range def.Source.Columns {
		cols = append(cols, map[string]interface{}{"name": c.Name, "type": c.Type})
	}
	return map[string]interface{}{
		"name":        def.Name,
		"description": def.Description,
		"source": map[string]interface{}{
			"type":    def.Source.Type,
			"path":    def.Source.Path,
			"columns": cols,
		},
		"transforms": transforms,
		"sink": map[string]interface{}{
			"type": def.Sink.Type,
		},
		"schemaDrift":   def.SchemaDrift.Policy,
		"incremental":   def.Incremental.Mode,
		"observability": def.Observability.Metrics,
		"dlq":           def.DLQ.Type,
	}
}

// Preview executes a bounded, side-effect-free dry run: it reads up to `limit`
// rows from the source, applies the full transform chain (including an
// auto-prepended schemadrift stage when configured), and returns the resulting
// sample rows. No sink, DLQ, checkpoint, or watermark I/O occurs — this is a
// pure read+transform sample intended for the Studio preview panel.
func (a *DataPipelineAction) Preview(ctx context.Context, limit int) (*PreviewResult, error) {
	return a.previewInternal(ctx, limit, -1)
}

// SampleInput reads a bounded sample from the source and applies the transform
// chain UP TO (but not including) the user transform at stageIndex, returning
// the rows that would ENTER that stage. This seeds the Studio "atomic stage
// tester" with realistic upstream data. stageIndex is an index into the
// user-defined transforms (def.Transforms); the auto-prepended schemadrift
// stage is always applied.
func (a *DataPipelineAction) SampleInput(ctx context.Context, limit, stageIndex int) (*PreviewResult, error) {
	if stageIndex < 0 {
		stageIndex = 0
	}
	return a.previewInternal(ctx, limit, stageIndex)
}

// previewInternal runs the source through the schemadrift + first `stopAfter`
// user transforms. stopAfter < 0 applies the full user chain.
func (a *DataPipelineAction) previewInternal(ctx context.Context, limit, stopAfter int) (*PreviewResult, error) {
	if limit <= 0 {
		limit = 100
	}
	def := a.def
	res := &PreviewResult{Rows: make([]map[string]interface{}, 0, limit)}

	// --- source ---
	srcCfg := a.buildSourceConfig(&Input{})
	src, err := source.New(def.Source.Type, srcCfg)
	if err != nil {
		return res, fmt.Errorf("source open: %w", err)
	}
	defer src.Close()

	// --- transforms (mirror executePipeline: auto-prepend schemadrift) ---
	userCfgs := make([]map[string]interface{}, 0, len(def.Transforms))
	for _, td := range def.Transforms {
		userCfgs = append(userCfgs, td.Extra)
	}
	if stopAfter >= 0 && stopAfter < len(userCfgs) {
		userCfgs = userCfgs[:stopAfter]
	}
	transformCfgs := make([]map[string]interface{}, 0, len(userCfgs)+1)
	if def.SchemaDrift.Policy != "" {
		transformCfgs = append(transformCfgs, map[string]interface{}{
			"type":    "schemadrift",
			"policy":  def.SchemaDrift.Policy,
			"onDrift": def.SchemaDrift.OnDrift,
		})
	}
	transformCfgs = append(transformCfgs, userCfgs...)
	txPipeline, err := transform.Build(transformCfgs)
	if err != nil {
		return res, fmt.Errorf("transforms build: %w", err)
	}
	defer txPipeline.Close()

	batchSize := def.Performance.BatchSize
	if batchSize <= 0 || batchSize > limit {
		batchSize = limit
	}

	for len(res.Rows) < limit {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		batch, readErr := src.ReadBatch(batchSize)
		if readErr != nil && readErr != io.EOF {
			return res, fmt.Errorf("source read: %w", readErr)
		}
		done := readErr == io.EOF

		if batch != nil && batch.Len() > 0 {
			batchLen := int64(batch.Len())
			res.RowsRead += batchLen

			transformed, txErr := txPipeline.Apply(batch)
			// A pass-through/identity chain (e.g. an empty transform list when
			// sampling stage 0) returns the SAME batch. Only recycle the input
			// batch when the transform produced a distinct one, otherwise we
			// would clear the very rows we are about to read.
			if transformed != batch {
				record.Put(batch)
			}

			if txErr != nil {
				res.RowsFailed += batchLen
			} else if transformed == nil {
				res.RowsDropped += batchLen
			} else if transformed.Len() > 0 {
				if len(res.Columns) == 0 {
					for _, c := range transformed.Columns {
						res.Columns = append(res.Columns, c.Name)
					}
				}
				for i := 0; i < transformed.Len() && len(res.Rows) < limit; i++ {
					res.Rows = append(res.Rows, transformed.RowMap(i))
				}
				record.Put(transformed)
			}
		}

		if done {
			break
		}
		if len(res.Rows) >= limit {
			res.Truncated = true
		}
	}
	return res, nil
}

// StageTestRow is the per-row outcome of an atomic single-stage test.
type StageTestRow struct {
	// Status is "kept", "dropped", or "failed".
	Status string `json:"status"`
	// Input is the row as supplied to the stage.
	Input map[string]interface{} `json:"input"`
	// Output is the transformed row when Status == "kept".
	Output map[string]interface{} `json:"output,omitempty"`
	// Error is the failure reason when Status == "failed".
	Error string `json:"error,omitempty"`
}

// StageTestResult is the outcome of testing one transform in isolation against
// a user-supplied input sample.
type StageTestResult struct {
	// Type is the transform type that was exercised (filter, map, validate, …).
	Type string `json:"type"`
	// Columns is the ordered output column list (union across kept rows).
	Columns []string `json:"columns"`
	// Rows carries the per-row input → output/status detail.
	Rows    []StageTestRow `json:"rows"`
	RowsIn  int            `json:"rowsIn"`
	Kept    int            `json:"kept"`
	Dropped int            `json:"dropped"`
	Failed  int            `json:"failed"`
}

// TestStage runs a single user transform (by its index in def.Transforms)
// against the supplied input rows, in isolation, and returns per-row
// kept/dropped/failed outcomes. It is side-effect-free (no source/sink I/O) and
// powers the Studio "atomic stage tester", where users edit a sample and see
// exactly how one activity transforms it.
//
// Each row is applied as a single-row batch through one transform instance so
// the tester can attribute a precise outcome to every input row. Stateful
// transforms (hwm, dq, schemadrift) still update their state across the rows in
// order, but are best validated with a full run.
func (a *DataPipelineAction) TestStage(
	ctx context.Context,
	stageIndex int,
	columns []string,
	rows []map[string]interface{},
) (*StageTestResult, error) {
	def := a.def
	if stageIndex < 0 || stageIndex >= len(def.Transforms) {
		return nil, fmt.Errorf("stage index %d out of range (0..%d)", stageIndex, len(def.Transforms)-1)
	}
	cfg := def.Transforms[stageIndex].Extra
	typ, _ := cfg["type"].(string)
	if typ == "" {
		return nil, fmt.Errorf("stage %d has no transform type", stageIndex)
	}

	// Column order: use the caller-provided order, else derive from the rows.
	if len(columns) == 0 {
		seen := map[string]bool{}
		for _, row := range rows {
			for k := range row {
				if !seen[k] {
					seen[k] = true
					columns = append(columns, k)
				}
			}
		}
	}
	cols := make([]record.Column, len(columns))
	for i, name := range columns {
		cols[i] = record.Column{Name: name, Type: record.TypeAny}
	}

	res := &StageTestResult{Type: typ, Rows: make([]StageTestRow, 0, len(rows))}

	t, err := transform.New(typ, cfg)
	if err != nil {
		return nil, fmt.Errorf("build transform %q: %w", typ, err)
	}
	defer t.Close()

	outColsSeen := map[string]bool{}
	var outColsOrder []string

	for _, row := range rows {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		res.RowsIn++
		vals := make([]interface{}, len(columns))
		for i, name := range columns {
			vals[i] = row[name]
		}
		in := &record.RecordBatch{Columns: cols, Rows: [][]interface{}{vals}}
		out, aerr := t.Apply(in)

		tr := StageTestRow{Input: row}
		switch {
		case aerr != nil:
			tr.Status = "failed"
			tr.Error = aerr.Error()
			res.Failed++
		case out == nil || out.Len() == 0:
			tr.Status = "dropped"
			res.Dropped++
		default:
			tr.Status = "kept"
			tr.Output = out.RowMap(0)
			for _, c := range out.Columns {
				if !outColsSeen[c.Name] {
					outColsSeen[c.Name] = true
					outColsOrder = append(outColsOrder, c.Name)
				}
			}
			res.Kept++
		}
		res.Rows = append(res.Rows, tr)
	}
	if len(outColsOrder) == 0 {
		outColsOrder = columns
	}
	res.Columns = outColsOrder
	return res, nil
}
