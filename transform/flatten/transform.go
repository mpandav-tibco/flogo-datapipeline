// Package flatten registers a transform that flattens nested-object columns into
// dotted flat columns. A column whose value is a nested map (e.g. from a JSON/XML
// source, carried as record.TypeAny) is replaced by one column per leaf, named
// "<column><sep><key>...". Arrays are left as values — use the explode transform
// to expand them. Pure map-side, per-row; no shuffle.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/flatten"
//
// Config:
//
//	{
//	  "type": "flatten",
//	  "columns": ["address", "meta"], // columns to flatten (default: every map-valued column)
//	  "separator": ".",                // key separator (default ".")
//	  "depth": 0                        // max nesting depth to flatten (0 = unlimited)
//	}
//
// The output schema is the union of leaf keys across the batch (rows missing a
// key get null), in deterministic sorted order. A row where a flattened column is
// not a map contributes nulls for that column's leaves.
package flatten

import (
	"fmt"
	"sort"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("flatten", newFlattenTransform)
}

type flattenTransform struct {
	columns  map[string]bool // nil = flatten every map-valued column
	sep      string
	maxDepth int
}

func newFlattenTransform(cfg map[string]interface{}) (transform.Transform, error) {
	sep, _ := cfg["separator"].(string)
	if sep == "" {
		sep = "."
	}
	depth := 0
	switch d := cfg["depth"].(type) {
	case float64:
		depth = int(d)
	case int:
		depth = d
	}
	if depth < 0 {
		return nil, fmt.Errorf("flatten transform: 'depth' must be >= 0")
	}

	var cols map[string]bool
	if raw, ok := cfg["columns"]; ok {
		cols = map[string]bool{}
		switch v := raw.(type) {
		case []interface{}:
			for _, c := range v {
				if s, ok := c.(string); ok {
					cols[s] = true
				}
			}
		case []string:
			for _, s := range v {
				cols[s] = true
			}
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("flatten transform: 'columns' was provided but empty")
		}
	}
	return &flattenTransform{columns: cols, sep: sep, maxDepth: depth}, nil
}

func (f *flattenTransform) Name() string { return "flatten" }

// Apply flattens the configured nested-object columns into dotted leaf columns.
func (f *flattenTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	n := len(in.Columns)

	// Pass 1: flatten each selected map-valued cell once, collecting per-column
	// leaf maps and the union of leaf keys.
	flatten := make([]bool, n)
	keySets := make([]map[string]struct{}, n)
	rowLeaves := make([]map[int]map[string]interface{}, len(in.Rows))
	for r, row := range in.Rows {
		for i, col := range in.Columns {
			if f.columns != nil && !f.columns[col.Name] {
				continue
			}
			if i >= len(row) {
				continue
			}
			m, ok := row[i].(map[string]interface{})
			if !ok {
				continue
			}
			leaves := map[string]interface{}{}
			flattenMap(m, col.Name, f.sep, 1, f.maxDepth, leaves)
			if rowLeaves[r] == nil {
				rowLeaves[r] = map[int]map[string]interface{}{}
			}
			rowLeaves[r][i] = leaves
			if keySets[i] == nil {
				keySets[i] = map[string]struct{}{}
			}
			for k := range leaves {
				keySets[i][k] = struct{}{}
			}
			flatten[i] = true
		}
	}

	// Deterministic sorted leaf key order per flattened column.
	keys := make([][]string, n)
	for i := 0; i < n; i++ {
		if !flatten[i] {
			continue
		}
		ks := make([]string, 0, len(keySets[i]))
		for k := range keySets[i] {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		keys[i] = ks
	}

	// Pass 2: build the output schema and rows.
	outCols := make([]record.Column, 0, n)
	for i, col := range in.Columns {
		if flatten[i] {
			for _, k := range keys[i] {
				outCols = append(outCols, record.Column{Name: k, Type: record.TypeAny})
			}
		} else {
			outCols = append(outCols, col)
		}
	}

	out := record.Get()
	out.Columns = outCols
	out.Rows = make([][]interface{}, len(in.Rows))
	for r, row := range in.Rows {
		dst := make([]interface{}, 0, len(outCols))
		for i, col := range in.Columns {
			_ = col
			if flatten[i] {
				var leaves map[string]interface{}
				if rowLeaves[r] != nil {
					leaves = rowLeaves[r][i]
				}
				for _, k := range keys[i] {
					if leaves != nil {
						dst = append(dst, leaves[k])
					} else {
						dst = append(dst, nil)
					}
				}
			} else {
				var v interface{}
				if i < len(row) {
					v = row[i]
				}
				dst = append(dst, v)
			}
		}
		out.Rows[r] = dst
	}
	return out, nil
}

func (f *flattenTransform) Close() error { return nil }

// flattenMap recursively writes leaf values of m into out under dotted keys
// prefixed by prefix. Nested maps recurse until maxDepth (0 = unlimited); all
// other values (including arrays) are stored as leaves.
func flattenMap(m map[string]interface{}, prefix, sep string, depth, maxDepth int, out map[string]interface{}) {
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + sep + k
		}
		if sub, ok := v.(map[string]interface{}); ok && (maxDepth == 0 || depth < maxDepth) {
			flattenMap(sub, key, sep, depth+1, maxDepth, out)
			continue
		}
		out[key] = v
	}
}
