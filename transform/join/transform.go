// Package join registers a keyed JOIN transform that enriches the primary
// (left) stream by matching each row against a second data source (the right
// side) loaded once into an in-memory keyed table. It is the streaming
// equivalent of a broadcast/stream-table join: the right side is a bounded
// reference dataset (CSV, JDBC, Parquet, a Kafka batch drain, …) and the left
// side is the pipeline's streaming source.
//
// Compose it with the `window` transform for a *windowed* keyed join — join to
// enrich, then window+aggregate the joined stream:
//
//	transforms: [ { "type": "join", ... }, { "type": "window", ... } ]
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/join"
//
// Config:
//
//	{
//	  "type": "join",
//	  "right":   { "type": "csv", "path": "customers.csv", "hasHeader": true },
//	  "leftKey": "customerId",   // left column to match on
//	  "rightKey": "id",          // right column to match on (default: leftKey)
//	  "joinType": "inner",       // "inner" (default) | "left"
//	  "rightColumns": ["name","tier"], // right columns to add (default: all but rightKey)
//	  "prefix": "cust_",         // optional prefix for added right columns
//	  "maxRows": 1000000         // cap on right rows loaded (default 1,000,000)
//	}
//
// Semantics: inner drops left rows with no right match; left keeps them with
// null right columns. A left row matching N right rows yields N output rows
// (many-to-many). The right dataset is held in memory — size it accordingly; it
// MUST be bounded (a Kafka right source is auto-drained to its current end), and
// loading more than maxRows rows is a hard error. Keys are matched by string
// form, so align key column types across the two sides (e.g. both int or both
// string) to avoid surprises.
//
// Graph-wired right side: set "right" to the string "graph" instead of an
// inline source config to wire the right side from a graph edge (a node's
// "right"-port input) rather than an embedded config — e.g. authored in
// Studio as two source nodes feeding a join's "left"/"right" ports. In this
// mode the transform is constructed empty and expects its caller (the graph
// runner) to call LoadRight for every right-side batch, then CloseRight
// exactly once before the first ApplyLeft call — see LoadRight/CloseRight/
// ApplyLeft below, which together satisfy graphexec.MultiInputOp (structural
// typing; this package does not import graphexec). Omitting "right" entirely
// (neither an inline config nor the "graph" sentinel) is still a hard
// construction error, so a genuinely missing config keeps failing loudly.
package join

import (
	"fmt"
	"io"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("join", newJoinTransform)
}

type joinTransform struct {
	leftKey       string
	rightKey      string
	joinType      string // "inner" | "left"
	prefix        string
	wantCols      []string // configured rightColumns, if any
	maxRows       int64
	rightCols     []string // resolved once the first right batch/row is seen
	rightColNames []string // output names for the added right columns (prefixed) — set by CloseRight (graph-wired) or the constructor (inline)
	table         map[string][][]interface{}
	loaded        int64
	graphWired    bool // true when constructed via the "right":"graph" sentinel
	rightClosed   bool // true once CloseRight has run (graph-wired mode only)
}

func newJoinTransform(cfg map[string]interface{}) (transform.Transform, error) {
	leftKey, _ := cfg["leftKey"].(string)
	if leftKey == "" {
		leftKey, _ = cfg["key"].(string)
	}
	if leftKey == "" {
		return nil, fmt.Errorf("join transform: 'leftKey' (or 'key') is required")
	}
	rightKey, _ := cfg["rightKey"].(string)
	if rightKey == "" {
		rightKey = leftKey
	}

	joinType, _ := cfg["joinType"].(string)
	if joinType == "" {
		joinType = "inner"
	}
	if joinType != "inner" && joinType != "left" {
		return nil, fmt.Errorf("join transform: joinType must be 'inner' or 'left', got %q", joinType)
	}

	prefix, _ := cfg["prefix"].(string)

	var wantCols []string
	if rc, ok := cfg["rightColumns"].([]interface{}); ok {
		for _, c := range rc {
			if s, ok := c.(string); ok {
				wantCols = append(wantCols, s)
			}
		}
	}

	// maxRows caps the in-memory reference table (guards OOM and catches a source
	// that never ends). Default 1,000,000; override with 'maxRows'.
	maxRows := int64(1_000_000)
	switch v := cfg["maxRows"].(type) {
	case float64:
		if v > 0 {
			maxRows = int64(v)
		}
	case int:
		if v > 0 {
			maxRows = int64(v)
		}
	case int64:
		if v > 0 {
			maxRows = v
		}
	}

	j := &joinTransform{
		leftKey:  leftKey,
		rightKey: rightKey,
		joinType: joinType,
		prefix:   prefix,
		wantCols: wantCols,
		maxRows:  maxRows,
		table:    make(map[string][][]interface{}),
	}
	// When rightColumns is configured, the output schema is fixed up front so
	// it does not depend on whether the reference side ever produces a row —
	// an empty right side must still add the configured columns (as nulls for
	// a left join).
	if len(wantCols) > 0 {
		j.rightCols = wantCols
	}

	// Graph-wired mode: "right" is the sentinel string "graph" — the caller
	// (the graph runner) supplies right-side batches via LoadRight/CloseRight
	// instead of an inline source config. Leave the table empty; do NOT touch
	// rightColNames until CloseRight (see the ApplyLeft guard below).
	if rstr, ok := cfg["right"].(string); ok && rstr == "graph" {
		j.graphWired = true
		return j, nil
	}

	rmap, ok := cfg["right"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("join transform: 'right' source config is required")
	}
	rtype, _ := rmap["type"].(string)
	if rtype == "" {
		return nil, fmt.Errorf("join transform: right.type is required")
	}

	// The right side is read fully into memory (a broadcast / stream-table join),
	// so it MUST be bounded. A Kafka right source defaults to a batch drain-to-
	// current-end so it terminates instead of blocking forever on a live topic.
	if rtype == "kafka" {
		if _, ok := rmap["mode"]; !ok {
			rmap["mode"] = "batch"
		}
		_, hasEnd := rmap["stopAtCurrentEnd"]
		_, hasOff := rmap["stopOffset"]
		_, hasTS := rmap["stopTimestamp"]
		if !hasEnd && !hasOff && !hasTS {
			rmap["stopAtCurrentEnd"] = true
		}
	}

	// Load the right side fully into a keyed multimap.
	rs, err := source.New(rtype, rmap)
	if err != nil {
		return nil, fmt.Errorf("join transform: open right source: %w", err)
	}
	defer rs.Close()

	for {
		b, rErr := rs.ReadBatch(1000)
		if b != nil && b.Len() > 0 {
			if lerr := j.loadRightBatch(b); lerr != nil {
				return nil, lerr
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return nil, fmt.Errorf("join transform: read right source: %w", rErr)
		}
	}
	j.finalizeRightColumns()

	return j, nil
}

// resolveRightCols picks the right columns to emit: the configured subset, or
// every right column except the join key.
func resolveRightCols(cols []record.Column, want []string, rightKey string) []string {
	if len(want) > 0 {
		return want
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if c.Name != rightKey {
			out = append(out, c.Name)
		}
	}
	return out
}

// loadRightBatch is the shared core of the constructor's inline load loop and
// the graph-wired LoadRight: enforce maxRows, resolve rightCols from the FIRST
// batch seen, then key every row into the table.
func (j *joinTransform) loadRightBatch(b *record.RecordBatch) error {
	// Enforce the cap BEFORE growing the table so the in-memory footprint is
	// bounded at maxRows exactly (never maxRows + one batch).
	if j.loaded+int64(b.Len()) > j.maxRows {
		return fmt.Errorf("join transform: right source exceeded maxRows=%d — it must be a bounded reference dataset (raise 'maxRows' or use a bounded/batch source)", j.maxRows)
	}
	if j.rightCols == nil {
		j.rightCols = resolveRightCols(b.Columns, j.wantCols, j.rightKey)
	}
	// pos[0] is the key column, pos[1:] the carried columns (-1 = absent → nil).
	pos := record.IndexOf(b.Columns, append([]string{j.rightKey}, j.rightCols...)...)
	cellAt := func(row []interface{}, p int) interface{} {
		if p >= 0 && p < len(row) {
			return row[p]
		}
		return nil
	}
	for _, row := range b.Rows {
		key := record.FormatValue(cellAt(row, pos[0]))
		vals := make([]interface{}, len(j.rightCols))
		for k := range j.rightCols {
			vals[k] = cellAt(row, pos[k+1])
		}
		j.table[key] = append(j.table[key], vals)
	}
	j.loaded += int64(b.Len())
	return nil
}

// finalizeRightColumns computes the prefixed output column names from
// whatever rightCols were resolved (possibly still nil if zero right rows were
// ever loaded — an empty right side is valid for a left join).
func (j *joinTransform) finalizeRightColumns() {
	names := make([]string, len(j.rightCols))
	for i, c := range j.rightCols {
		names[i] = j.prefix + c
	}
	j.rightColNames = names
}

// LoadRight ingests one batch of the graph-wired right side. Satisfies
// graphexec.MultiInputOp structurally (this package imports no graphexec type).
func (j *joinTransform) LoadRight(b *record.RecordBatch) error {
	if b == nil {
		return nil
	}
	return j.loadRightBatch(b)
}

// CloseRight finalizes the right-side table once every right-port producer has
// finished. Must be called before the first ApplyLeft.
func (j *joinTransform) CloseRight() error {
	j.finalizeRightColumns()
	j.rightClosed = true
	return nil
}

// ApplyLeft processes one batch of the graph-wired left (primary) side. Errors
// if called before CloseRight, rather than silently joining against a
// partially-loaded table.
func (j *joinTransform) ApplyLeft(in *record.RecordBatch) (*record.RecordBatch, error) {
	if !j.rightClosed {
		return nil, fmt.Errorf("join transform: ApplyLeft called before CloseRight — the graph runner must fully drain the right port first")
	}
	return j.applyJoin(in)
}

func (j *joinTransform) Name() string { return "join" }

func (j *joinTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	return j.applyJoin(in)
}

// applyJoin is the shared join logic used by both the linear-model Apply and
// the graph-wired ApplyLeft.
func (j *joinTransform) applyJoin(in *record.RecordBatch) (*record.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	outCols := make([]record.Column, len(in.Columns)+len(j.rightColNames))
	copy(outCols, in.Columns)
	for i, name := range j.rightColNames {
		outCols[len(in.Columns)+i] = record.Column{Name: name, Type: record.TypeAny}
	}

	out := record.Get()
	out.Columns = outCols
	out.Rows = make([][]interface{}, 0, len(in.Rows))

	leftIdx := in.ColIndex(j.leftKey)
	for i := range in.Rows {
		// A missing column or short row is nil — exactly what RowMap would yield.
		var keyVal interface{}
		if leftIdx >= 0 && leftIdx < len(in.Rows[i]) {
			keyVal = in.Rows[i][leftIdx]
		}
		matches := j.table[record.FormatValue(keyVal)]
		if len(matches) == 0 {
			if j.joinType == "left" {
				row := make([]interface{}, len(outCols))
				copy(row, in.Rows[i])
				out.Rows = append(out.Rows, row) // right columns left nil
			}
			continue
		}
		for _, rv := range matches {
			row := make([]interface{}, len(outCols))
			copy(row, in.Rows[i])
			copy(row[len(in.Columns):], rv)
			out.Rows = append(out.Rows, row)
		}
	}
	return out, nil
}

func (j *joinTransform) Close() error { return nil }
