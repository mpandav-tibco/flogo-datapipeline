// Package explode registers a transform that expands an array-valued column into
// one output row per element (the other columns are duplicated). It is the
// map-side way to normalise arrays-of-objects from JSON/XML/API sources into flat
// rows — no shuffle, no cross-row state.
//
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/explode"
//
// Config:
//
//	{
//	  "type": "explode",
//	  "column": "items",     // the array-valued column to expand (required)
//	  "into": "item",         // output column name for each element (default: column)
//	  "keepEmpty": false       // when true, a null/empty array still emits one row (element = null)
//	}
//
// Semantics: an array of N elements yields N rows; an empty array or null yields 0
// rows (or 1 null row when keepEmpty); a non-array scalar yields a single row
// (element = the value) so "sometimes an array, sometimes a scalar" JSON is
// handled gracefully. A missing column passes the batch through unchanged.
package explode

import (
	"fmt"
	"reflect"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
)

func init() {
	transform.Register("explode", newExplodeTransform)
}

type explodeTransform struct {
	column    string
	into      string
	keepEmpty bool
}

func newExplodeTransform(cfg map[string]interface{}) (transform.Transform, error) {
	column, _ := cfg["column"].(string)
	if column == "" {
		return nil, fmt.Errorf("explode transform: 'column' is required")
	}
	into, _ := cfg["into"].(string)
	if into == "" {
		into = column
	}
	keepEmpty, _ := cfg["keepEmpty"].(bool)
	return &explodeTransform{column: column, into: into, keepEmpty: keepEmpty}, nil
}

func (e *explodeTransform) Name() string { return "explode" }

// Apply expands the configured array column, emitting one row per element.
func (e *explodeTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	idx := in.ColIndex(e.column)
	if idx < 0 {
		return in, nil // column absent — pass through unchanged
	}

	outCols := make([]record.Column, len(in.Columns))
	copy(outCols, in.Columns)
	outCols[idx].Name = e.into
	outCols[idx].Type = record.TypeAny

	out := record.Get()
	out.Columns = outCols
	out.Rows = make([][]interface{}, 0, len(in.Rows))
	for _, row := range in.Rows {
		var val interface{}
		if idx < len(row) {
			val = row[idx]
		}
		if arr, ok := asSlice(val); ok {
			if len(arr) == 0 {
				if e.keepEmpty {
					out.Rows = append(out.Rows, e.emit(row, idx, nil))
				}
				continue
			}
			for _, el := range arr {
				out.Rows = append(out.Rows, e.emit(row, idx, el))
			}
			continue
		}
		if val == nil {
			if e.keepEmpty {
				out.Rows = append(out.Rows, e.emit(row, idx, nil))
			}
			continue
		}
		// A non-array scalar explodes to a single row (element = the value).
		out.Rows = append(out.Rows, e.emit(row, idx, val))
	}

	if len(out.Rows) == 0 {
		record.Put(out)
		return nil, nil
	}
	return out, nil
}

// emit builds an output row: a copy of row with the exploded column set to elem.
func (e *explodeTransform) emit(row []interface{}, idx int, elem interface{}) []interface{} {
	dst := make([]interface{}, len(row))
	copy(dst, row)
	if idx < len(dst) {
		dst[idx] = elem
	}
	return dst
}

func (e *explodeTransform) Close() error { return nil }

// asSlice returns v as a []interface{} when it is an array/slice (JSON arrays
// arrive as []interface{}; other slice kinds are handled reflectively). A []byte
// is treated as a scalar, not a slice of bytes.
func asSlice(v interface{}) ([]interface{}, bool) {
	if s, ok := v.([]interface{}); ok {
		return s, true
	}
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() != reflect.Uint8 {
		out := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = rv.Index(i).Interface()
		}
		return out, true
	}
	return nil, false
}
