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
//	  "prefix": "cust_"          // optional prefix for added right columns
//	}
//
// Semantics: inner drops left rows with no right match; left keeps them with
// null right columns. A left row matching N right rows yields N output rows
// (many-to-many). The right dataset is held in memory — size it accordingly.
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
	joinType      string   // "inner" | "left"
	rightColNames []string // output names for the added right columns (prefixed)
	table         map[string][][]interface{}
}

func newJoinTransform(cfg map[string]interface{}) (transform.Transform, error) {
	rmap, ok := cfg["right"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("join transform: 'right' source config is required")
	}
	rtype, _ := rmap["type"].(string)
	if rtype == "" {
		return nil, fmt.Errorf("join transform: right.type is required")
	}

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

	// Load the right side fully into a keyed multimap.
	rs, err := source.New(rtype, rmap)
	if err != nil {
		return nil, fmt.Errorf("join transform: open right source: %w", err)
	}
	defer rs.Close()

	table := make(map[string][][]interface{})
	var rightCols []string
	first := true
	for {
		b, rErr := rs.ReadBatch(1000)
		if b != nil && b.Len() > 0 {
			if first {
				rightCols = resolveRightCols(b.Columns, wantCols, rightKey)
				first = false
			}
			for i := 0; i < b.Len(); i++ {
				m := b.RowMap(i)
				key := fmt.Sprintf("%v", m[rightKey])
				vals := make([]interface{}, len(rightCols))
				for j, c := range rightCols {
					vals[j] = m[c]
				}
				table[key] = append(table[key], vals)
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return nil, fmt.Errorf("join transform: read right source: %w", rErr)
		}
	}

	names := make([]string, len(rightCols))
	for i, c := range rightCols {
		names[i] = prefix + c
	}

	return &joinTransform{
		leftKey:       leftKey,
		joinType:      joinType,
		rightColNames: names,
		table:         table,
	}, nil
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

func (j *joinTransform) Name() string { return "join" }

func (j *joinTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
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
		var keyVal interface{}
		if leftIdx >= 0 && leftIdx < len(in.Rows[i]) {
			keyVal = in.Rows[i][leftIdx]
		} else {
			keyVal = in.RowMap(i)[j.leftKey]
		}
		matches := j.table[fmt.Sprintf("%v", keyVal)]
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
