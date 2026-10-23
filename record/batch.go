// Package record defines the internal pipeline currency: RecordBatch.
// All sources, transforms, and sinks operate on batches, never individual rows.
// Batch processing is critical for columnar formats (Parquet) and for
// amortising the overhead of database round-trips in JDBC sinks.
package record

import "sync"

// ColumnType enumerates the supported column value types.
type ColumnType string

const (
	TypeString  ColumnType = "string"
	TypeInt     ColumnType = "int"
	TypeFloat   ColumnType = "float"
	TypeBool    ColumnType = "bool"
	TypeDate    ColumnType = "date"
	TypeDecimal ColumnType = "decimal"
	TypeAny     ColumnType = "any"
)

// Column describes a single field in a RecordBatch schema.
type Column struct {
	Name   string     `json:"name"`
	Type   ColumnType `json:"type"`
	Format string     `json:"format,omitempty"` // date/time parse layout for TypeDate, e.g. "2006-01-02T15:04:05"
	Scale  int        `json:"scale,omitempty"`  // decimal places for TypeDecimal
}

// RecordBatch is the unit of work flowing through the pipeline.
// Rows[i][j] is the value of column j in row i.
// All values are untyped interface{}; type coercion happens at source read time
// according to the column schema.
type RecordBatch struct {
	Columns []Column
	Rows    [][]interface{}
}

// Len returns the number of rows in this batch.
func (b *RecordBatch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.Rows)
}

// ColIndex returns the index of a column by name, or -1 if not found.
func (b *RecordBatch) ColIndex(name string) int {
	for i, c := range b.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// Value returns the value of the named column in row i, or nil.
func (b *RecordBatch) Value(row int, col string) interface{} {
	idx := b.ColIndex(col)
	if idx < 0 || row >= len(b.Rows) || idx >= len(b.Rows[row]) {
		return nil
	}
	return b.Rows[row][idx]
}

// RowMap returns row i as a map[string]interface{} for expression evaluation.
// Out-of-range i yields an empty map (never panics), matching the defensive
// contract of Value.
func (b *RecordBatch) RowMap(i int) map[string]interface{} {
	m := make(map[string]interface{}, len(b.Columns))
	if i < 0 || i >= len(b.Rows) {
		return m
	}
	for j, col := range b.Columns {
		if j < len(b.Rows[i]) {
			m[col.Name] = b.Rows[i][j]
		}
	}
	return m
}

// RowMapInto is the allocation-free variant of RowMap for hot loops: it clears m
// and refills it with row i (allocating m only when nil), yielding exactly the
// same contents RowMap(i) would. The returned map is overwritten by the next
// call, so callers must not retain it beyond evaluating the current row.
func (b *RecordBatch) RowMapInto(i int, m map[string]interface{}) map[string]interface{} {
	if m == nil {
		m = make(map[string]interface{}, len(b.Columns))
	} else {
		clear(m)
	}
	if i < 0 || i >= len(b.Rows) {
		return m
	}
	row := b.Rows[i]
	for j, col := range b.Columns {
		if j < len(row) {
			m[col.Name] = row[j]
		}
	}
	return m
}

// Clone returns a shallow copy with an independent Rows slice but shared column
// definitions. Used by transforms that need to produce a filtered subset.
func (b *RecordBatch) Clone(capacity int) *RecordBatch {
	clone := pool.Get().(*RecordBatch)
	clone.Columns = b.Columns
	if cap(clone.Rows) >= capacity {
		clone.Rows = clone.Rows[:0]
	} else {
		clone.Rows = make([][]interface{}, 0, capacity)
	}
	return clone
}

// pool reuses RecordBatch allocations to reduce GC pressure when processing
// billions of rows. Sources acquire via Get(), callers release via Put() after
// the batch has been fully consumed by the next stage.
var pool = sync.Pool{
	New: func() interface{} {
		return &RecordBatch{}
	},
}

// Get acquires a RecordBatch from the pool.
func Get() *RecordBatch {
	return pool.Get().(*RecordBatch)
}

// Put releases a RecordBatch back to the pool. The batch must not be used
// after this call.
func Put(b *RecordBatch) {
	if b == nil {
		return
	}
	b.Columns = nil
	// Drop row references so a pooled batch doesn't keep the previous batch's
	// rows (and every value they box) reachable until its slots are overwritten.
	clear(b.Rows[:cap(b.Rows)])
	b.Rows = b.Rows[:0]
	pool.Put(b)
}
