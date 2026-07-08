// Package schema provides schema comparison and projection utilities for the
// data pipeline. It is a pure-function package — no state, no init(), no
// registration. The schemadrift transform package imports this package and
// applies the logic to each incoming RecordBatch.
package schema

import (
	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// DriftPolicy controls what happens when the source schema changes at runtime.
type DriftPolicy string

const (
	// DriftPolicyAdditive propagates new columns downstream (baseline grows).
	// Dropped source columns emit nil in their original column positions.
	// Type changes are recorded in the baseline; downstream sees the new type.
	DriftPolicyAdditive DriftPolicy = "additive"

	// DriftPolicyStrict aborts the pipeline with an error on any schema change.
	// Suitable for production sinks that cannot tolerate schema changes.
	DriftPolicyStrict DriftPolicy = "strict"

	// DriftPolicyDrop silently discards any batch whose schema deviates from
	// the baseline. New columns are ignored; missing columns produce nil.
	// Rows are not individually filtered — the entire batch is dropped.
	DriftPolicyDrop DriftPolicy = "drop"

	// DriftPolicyProject passes only the declared (baseline) schema columns
	// downstream; new columns are ignored, missing columns emit nil.
	// This is the safest policy for fixed-schema sinks (JDBC, Parquet).
	DriftPolicyProject DriftPolicy = "project"
)

// ColumnChange describes a single detected schema change between two schema
// snapshots. Kind is one of "added", "dropped", or "retyped".
type ColumnChange struct {
	// Kind is one of "added", "dropped", or "retyped".
	Kind string
	// Column is the name of the column that changed.
	Column string
	// OldType is the column type in the baseline; zero value when Kind=="added".
	OldType record.ColumnType
	// NewType is the column type in the incoming batch; zero value when Kind=="dropped".
	NewType record.ColumnType
}

// Diff computes the schema changes between baseline and current column lists.
// It detects:
//   - "added"   — a column present in current but not in baseline
//   - "dropped" — a column present in baseline but not in current
//   - "retyped" — a column present in both but with a different ColumnType
//
// Column order within each list is not considered; comparison is by name.
// Returns nil (not an empty slice) when no changes are detected.
func Diff(baseline, current []record.Column) []ColumnChange {
	baseMap := colMap(baseline)
	currMap := colMap(current)

	// Estimate capacity: at most len(baseline)+len(current) changes possible,
	// though in practice the number is small.
	var changes []ColumnChange

	// Detect added and retyped columns (iterate current).
	for _, col := range current {
		if base, ok := baseMap[col.Name]; !ok {
			changes = append(changes, ColumnChange{
				Kind:    "added",
				Column:  col.Name,
				NewType: col.Type,
			})
		} else if base.Type != col.Type {
			changes = append(changes, ColumnChange{
				Kind:    "retyped",
				Column:  col.Name,
				OldType: base.Type,
				NewType: col.Type,
			})
		}
	}

	// Detect dropped columns (iterate baseline).
	for _, col := range baseline {
		if _, ok := currMap[col.Name]; !ok {
			changes = append(changes, ColumnChange{
				Kind:    "dropped",
				Column:  col.Name,
				OldType: col.Type,
			})
		}
	}

	return changes
}

// Project reshapes a batch so its schema matches targetSchema exactly.
// For each row in batch:
//   - columns present in targetSchema and the source batch are copied by position.
//   - columns present in targetSchema but absent in the source batch emit nil.
//   - columns present in the source batch but absent in targetSchema are dropped.
//
// Project allocates a new RecordBatch from the pool and a new row slice per row.
// The caller is responsible for calling record.Put on the returned batch when done.
// The input batch is not modified and is not released by this function.
//
// Performance note: the source-to-target index mapping (srcIdx) is built once
// per Project call and reused across all rows. For repeated calls with a stable
// schema pair, callers should pre-compute and cache the index mapping externally.
func Project(batch *record.RecordBatch, targetSchema []record.Column) *record.RecordBatch {
	out := record.Get()
	// Assign a copy of targetSchema so the output batch owns its column slice
	// independently of the caller's slice (which may be mutated by mergeBaseline).
	outCols := make([]record.Column, len(targetSchema))
	copy(outCols, targetSchema)
	out.Columns = outCols
	out.Rows = make([][]interface{}, 0, batch.Len())

	// Build source column index by name once; reused for every row.
	srcIdx := make(map[string]int, len(batch.Columns))
	for i, col := range batch.Columns {
		srcIdx[col.Name] = i
	}

	for _, srcRow := range batch.Rows {
		dstRow := make([]interface{}, len(targetSchema))
		for j, col := range targetSchema {
			if idx, ok := srcIdx[col.Name]; ok && idx < len(srcRow) {
				dstRow[j] = srcRow[idx]
			}
			// else: nil — column missing in source or row is shorter than schema
		}
		out.Rows = append(out.Rows, dstRow)
	}
	return out
}

// colMap builds a name→Column lookup table for fast schema comparison.
func colMap(cols []record.Column) map[string]record.Column {
	m := make(map[string]record.Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
