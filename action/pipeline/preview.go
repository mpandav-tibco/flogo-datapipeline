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
	transformCfgs := make([]map[string]interface{}, 0, len(def.Transforms)+1)
	if def.SchemaDrift.Policy != "" {
		transformCfgs = append(transformCfgs, map[string]interface{}{
			"type":    "schemadrift",
			"policy":  def.SchemaDrift.Policy,
			"onDrift": def.SchemaDrift.OnDrift,
		})
	}
	for _, td := range def.Transforms {
		transformCfgs = append(transformCfgs, td.Extra)
	}
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
			record.Put(batch)

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
