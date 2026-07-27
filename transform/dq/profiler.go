// Package dq implements the dataset-level Data Quality / Expectation Engine.
// This file defines BatchProfile and ColumnStat — the statistical backbone
// that every Expectation is evaluated against. Profile.Compute runs in a single
// O(rows × cols) pass so large batches remain efficient even with many expectations.
package dq

import (
	"fmt"
	"math"
	"strconv"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// ColumnStat accumulates per-column statistics for one RecordBatch.
// All fields are populated in a single forward pass by BatchProfile.Compute.
type ColumnStat struct {
	// NullCount is the number of rows where the column value is nil.
	NullCount int
	// NonNullCount is the number of rows where the column value is non-nil.
	NonNullCount int
	// DistinctValues is the set of all distinct non-nil values.
	// Values are stored as their canonical interface{} representation so that
	// numeric types are unified via toFloat64 before insertion.
	DistinctValues map[interface{}]struct{}
	// Min is the smallest observed numeric value, or nil when no numeric rows exist.
	Min interface{}
	// Max is the largest observed numeric value, or nil when no numeric rows exist.
	Max interface{}
	// Sum is the sum of all numeric values (used for mean computation).
	Sum float64
	// SumSq is the sum of squares of numeric values (used for stddev computation).
	SumSq float64
	// numericCount counts the rows that contributed to Sum/SumSq/Min/Max.
	numericCount int
}

// Mean returns the arithmetic mean of numeric values in this column,
// or NaN if no numeric values were observed.
func (cs *ColumnStat) Mean() float64 {
	if cs.numericCount == 0 {
		return math.NaN()
	}
	return cs.Sum / float64(cs.numericCount)
}

// Stddev returns the population standard deviation of numeric values,
// or NaN if fewer than two numeric values were observed.
func (cs *ColumnStat) Stddev() float64 {
	if cs.numericCount < 2 {
		return math.NaN()
	}
	n := float64(cs.numericCount)
	variance := cs.SumSq/n - (cs.Sum/n)*(cs.Sum/n)
	if variance < 0 {
		variance = 0 // guard against floating-point rounding below zero
	}
	return math.Sqrt(variance)
}

// MinFloat64 returns Min as a float64, or NaN if Min is nil.
func (cs *ColumnStat) MinFloat64() float64 {
	if cs.Min == nil {
		return math.NaN()
	}
	if f, ok := cs.Min.(float64); ok {
		return f
	}
	return math.NaN()
}

// MaxFloat64 returns Max as a float64, or NaN if Max is nil.
func (cs *ColumnStat) MaxFloat64() float64 {
	if cs.Max == nil {
		return math.NaN()
	}
	if f, ok := cs.Max.(float64); ok {
		return f
	}
	return math.NaN()
}

// DistinctCount returns the number of distinct non-nil values.
func (cs *ColumnStat) DistinctCount() int {
	return len(cs.DistinctValues)
}

// BatchProfile holds aggregate statistics for all columns in one RecordBatch.
// It is computed once per batch and reused by all Expectations.
type BatchProfile struct {
	// RowCount is the total number of rows in the batch.
	RowCount int
	// ColumnStats maps column name → ColumnStat.
	ColumnStats map[string]*ColumnStat
}

// Compute populates p from batch in a single O(rows × cols) pass.
// Compute reinitialises p on each call — safe to call multiple times with
// different batches (e.g. in tests) without constructing a new BatchProfile.
func (p *BatchProfile) Compute(batch *record.RecordBatch) {
	p.RowCount = batch.Len()
	p.ColumnStats = make(map[string]*ColumnStat, len(batch.Columns))

	// Initialise a ColumnStat for every declared column up-front so that
	// expectations can safely look up any column even when no rows exist.
	for _, col := range batch.Columns {
		p.ColumnStats[col.Name] = &ColumnStat{
			DistinctValues: make(map[interface{}]struct{}),
		}
	}

	for _, row := range batch.Rows {
		for colIdx, col := range batch.Columns {
			cs := p.ColumnStats[col.Name]
			if colIdx >= len(row) || row[colIdx] == nil {
				cs.NullCount++
				continue
			}
			cs.NonNullCount++

			raw := row[colIdx]

			// Store in DistinctValues using a comparable key. Numeric values are
			// canonicalised to float64 so that int(5) and float64(5) count as the
			// same distinct value.
			distinctKey := canonicalKey(raw)
			cs.DistinctValues[distinctKey] = struct{}{}

			// Numeric accumulation.
			if f, ok := toFloat64(raw); ok {
				cs.numericCount++
				cs.Sum += f
				cs.SumSq += f * f
				if cs.Min == nil {
					cs.Min = f
					cs.Max = f
				} else {
					if f < cs.Min.(float64) {
						cs.Min = f
					}
					if f > cs.Max.(float64) {
						cs.Max = f
					}
				}
			}
		}
	}
}

// canonicalKey returns a comparable representation of v suitable for use as a
// map key. Numeric types are unified to float64 so that numeric equality is
// type-agnostic (int64(42) == float64(42)). Other types are stored as-is when
// they are already comparable, or as their fmt.Sprintf representation otherwise.
func canonicalKey(v interface{}) interface{} {
	if f, ok := toFloat64(v); ok {
		return f
	}
	// Booleans, strings, and most primitive types are already comparable.
	switch v.(type) {
	case bool, string:
		return v
	default:
		// Fall back to string representation for complex or non-comparable types.
		return fmt.Sprintf("%v", v)
	}
}

// toFloat64 converts common numeric interface{} values to float64.
// Returns (0, false) when v is not numeric.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case record.Decimal:
		return n.Float64(), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}
