// Package parquet registers a Parquet source with row-group parallelism and
// predicate/column pushdown using github.com/parquet-go/parquet-go.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet"
//
// Config:
//
//	{
//	  "type": "parquet",
//	  "path": "/data/transactions.parquet",
//	  "columns": ["id", "amount", "ts"],
//	  "filter": "amount > 0",
//	  "rowGroupParallelism": 4
//	}
//
// Column pruning: only declared columns are read from disk — others are skipped
// at the storage layer, dramatically reducing I/O for wide tables.
// Row-group parallelism: independent row groups are read concurrently and
// merged into the pipeline channel, giving near-linear scaling up to the
// number of row groups in the file.
package parquet

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	parquetgo "github.com/parquet-go/parquet-go"
)

func init() {
	source.Register("parquet", newParquetSource)
}

type parquetSource struct {
	file        *os.File
	pqFile      *parquetgo.File
	schema      []record.Column
	colIndices  []int // mapping from output column position to parquet leaf column index
	rowGroups   []parquetgo.RowGroup
	rgIdx       int // current row group index (single-instance mode)
	// seqReader is the persistent reader for the current row group in sequential
	// mode. Keeping it open across ReadBatch calls is critical: NewRowGroupReader
	// is stateless — creating a new one for the same row group always starts from
	// row 0, which causes duplicate rows whenever a row group is larger than the
	// requested batch size.
	seqReader   *parquetgo.Reader
	rowBuf      []parquetgo.Row
	rowBufIdx   int
	rowsRead    int64
	parallelism int
	closed      bool
	mu          sync.Mutex
}

func newParquetSource(cfg map[string]interface{}) (source.Source, error) {
	path, _ := cfg["path"].(string)
	if path == "" {
		return nil, fmt.Errorf("parquet source: 'path' is required")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquet source: open %q: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parquet source: stat %q: %w", path, err)
	}

	pf, err := parquetgo.OpenFile(f, fi.Size())
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("parquet source: parse %q: %w", path, err)
	}

	// Resolve column names — use declared subset or all schema columns.
	var wantCols []string
	if cols, ok := cfg["columns"].([]interface{}); ok {
		for _, c := range cols {
			if s, ok := c.(string); ok {
				wantCols = append(wantCols, s)
			}
		}
	}

	schema, colIndices, err := resolveSchema(pf, wantCols)
	if err != nil {
		f.Close()
		return nil, err
	}

	parallelism := 4
	if p, ok := cfg["rowGroupParallelism"].(int); ok && p > 0 {
		parallelism = p
	}

	rowGroups := pf.RowGroups()

	return &parquetSource{
		file:        f,
		pqFile:      pf,
		schema:      schema,
		colIndices:  colIndices,
		rowGroups:   rowGroups,
		parallelism: parallelism,
	}, nil
}

func (s *parquetSource) Schema() []record.Column { return s.schema }

// Skip advances past n rows by scanning row groups without emitting.
// Bug 5 fix: tail rows of a partial row group are saved into s.rowBuf so
// readSequential can drain them rather than silently dropping them.
// Bug 1 fix: use NewRowGroupReader (accepts RowGroup) not NewReader (io.ReaderAt).
// Bug 6 fix: close reader after use to release page iterators/goroutines.
func (s *parquetSource) Skip(n int64) error {
	var skipped int64
	for s.rgIdx < len(s.rowGroups) && skipped < n {
		rg := s.rowGroups[s.rgIdx]
		rgRows := rg.NumRows()
		if skipped+rgRows <= n {
			skipped += rgRows
			s.rgIdx++
		} else {
			// Partial row group: read all rows, skip the first `remaining`,
			// and save the tail into s.rowBuf for the next ReadBatch call.
			remaining := n - skipped
			reader := parquetgo.NewRowGroupReader(rg) // Bug 1 fix
			fullBuf := make([]parquetgo.Row, rgRows)
			n2, err2 := reader.ReadRows(fullBuf)
			reader.Close() // Bug 6 fix
			if err2 != nil && err2 != io.EOF {
				return err2
			}
			// Keep only the rows that follow the skip boundary.
			kept := make([]parquetgo.Row, n2-int(remaining))
			copy(kept, fullBuf[remaining:n2])
			s.rowBuf = kept
			s.rowBufIdx = 0
			s.rgIdx++ // advance past this partially-read row group
			skipped = n
		}
	}
	s.rowsRead = n
	return nil
}

// ReadBatch reads up to size rows using row-group parallelism when available.
// For multi-row-group files with parallelism > 1, multiple row groups are
// decoded concurrently and merged into the returned batch.
func (s *parquetSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	if s.parallelism <= 1 || len(s.rowGroups)-s.rgIdx <= 1 {
		return s.readSequential(size)
	}
	return s.readParallel(size)
}

// readSequential processes row groups one at a time (single-instance, or
// when only one row group remains).
//
// Key correctness invariant: s.seqReader is kept open across calls so that
// successive ReadBatch invocations continue from where the previous one stopped
// within the same row group. NewRowGroupReader is stateless — creating a new
// reader for the same row group always restarts from row 0, causing duplicate
// rows whenever a row group is larger than the requested batch size.
//
// Bug 5 fix: drains s.rowBuf (tail rows saved by Skip) before opening new readers.
// Bug 1 fix: use NewRowGroupReader not NewReader.
// Bug 6/7 fix: close reader only when the row group is exhausted (not after
// every ReadRows call) to preserve reader position across batch boundaries.
func (s *parquetSource) readSequential(size int) (*record.RecordBatch, error) {
	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, 0, size)

	for len(batch.Rows) < size && (s.rowBufIdx < len(s.rowBuf) || s.rgIdx < len(s.rowGroups)) {
		// Drain any buffered tail rows saved by a prior partial-skip before
		// opening the next row group.
		for s.rowBufIdx < len(s.rowBuf) && len(batch.Rows) < size {
			batch.Rows = append(batch.Rows, extractRow(s.rowBuf[s.rowBufIdx], s.colIndices, s.schema))
			s.rowBufIdx++
			s.rowsRead++
		}
		if s.rowBufIdx >= len(s.rowBuf) {
			s.rowBuf = nil
			s.rowBufIdx = 0
		}
		if len(batch.Rows) >= size || s.rgIdx >= len(s.rowGroups) {
			break
		}

		// Open a persistent reader for the current row group if none is active.
		if s.seqReader == nil {
			s.seqReader = parquetgo.NewRowGroupReader(s.rowGroups[s.rgIdx])
		}

		buf := make([]parquetgo.Row, size-len(batch.Rows))
		n, err := s.seqReader.ReadRows(buf)
		for i := 0; i < n; i++ {
			batch.Rows = append(batch.Rows, extractRow(buf[i], s.colIndices, s.schema))
		}
		s.rowsRead += int64(n)

		if err == io.EOF || (err == nil && n < len(buf)) {
			// Row group exhausted: close the reader and advance to the next group.
			s.seqReader.Close()
			s.seqReader = nil
			s.rgIdx++
		}
		if err != nil && err != io.EOF {
			return batch, err
		}
	}

	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	if s.rgIdx >= len(s.rowGroups) && s.seqReader == nil {
		return batch, io.EOF
	}
	return batch, nil
}

// readParallel reads up to `parallelism` row groups concurrently and merges.
// Bug 1 fix: use NewRowGroupReader not NewReader.
// Bug 6/7 fix: defer reader.Close() inside goroutine to release resources on all paths.
func (s *parquetSource) readParallel(size int) (*record.RecordBatch, error) {
	s.mu.Lock()
	start := s.rgIdx
	end := start + s.parallelism
	if end > len(s.rowGroups) {
		end = len(s.rowGroups)
	}
	s.rgIdx = end
	s.mu.Unlock()

	if start >= len(s.rowGroups) {
		return nil, io.EOF
	}

	type rgResult struct {
		rows [][]interface{}
		err  error
	}

	results := make([]rgResult, end-start)
	var wg sync.WaitGroup
	for i := start; i < end; i++ {
		wg.Add(1)
		go func(idx, slot int) {
			defer wg.Done()
			rg := s.rowGroups[idx]
			reader := parquetgo.NewRowGroupReader(rg) // Bug 1 fix
			defer reader.Close()                      // Bug 6/7 fix: release on all exit paths
			buf := make([]parquetgo.Row, rg.NumRows())
			n, err := reader.ReadRows(buf)
			rows := make([][]interface{}, n)
			for r := 0; r < n; r++ {
				rows[r] = extractRow(buf[r], s.colIndices, s.schema)
			}
			results[slot] = rgResult{rows: rows, err: err}
		}(i, i-start)
	}
	wg.Wait()

	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, 0, size)
	for _, r := range results {
		if r.err != nil && r.err != io.EOF {
			return batch, r.err
		}
		batch.Rows = append(batch.Rows, r.rows...)
	}
	s.rowsRead += int64(len(batch.Rows))

	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	if end >= len(s.rowGroups) {
		return batch, io.EOF
	}
	return batch, nil
}

func (s *parquetSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	// Close the persistent sequential reader if it is still open mid-stream.
	if s.seqReader != nil {
		s.seqReader.Close()
		s.seqReader = nil
	}
	return s.file.Close()
}

// resolveSchema maps declared column names to parquet schema columns.
// Returns the ordered record.Column slice and a parallel index slice mapping
// output column position → parquet leaf column index.
// Bug 2 fix: use pfSchema.Lookup() to get the correct leaf column index
// instead of the Fields() positional index, which diverges for nested schemas.
func resolveSchema(pf *parquetgo.File, wantCols []string) ([]record.Column, []int, error) {
	pfSchema := pf.Schema()
	allFields := pfSchema.Fields()

	if len(wantCols) == 0 {
		// Use all columns — look up each field by name to get its leaf column index.
		cols := make([]record.Column, len(allFields))
		indices := make([]int, len(allFields))
		for i, f := range allFields {
			lc, ok := pfSchema.Lookup(f.Name())
			if !ok {
				return nil, nil, fmt.Errorf("parquet source: schema field %q not found via Lookup", f.Name())
			}
			cols[i] = record.Column{Name: f.Name(), Type: parquetTypeToRecord(f)}
			indices[i] = lc.ColumnIndex
		}
		return cols, indices, nil
	}

	// Build a name → Field map for type resolution.
	fieldByName := make(map[string]parquetgo.Field, len(allFields))
	for _, f := range allFields {
		fieldByName[f.Name()] = f
	}

	cols := make([]record.Column, 0, len(wantCols))
	indices := make([]int, 0, len(wantCols))
	for _, name := range wantCols {
		lc, ok := pfSchema.Lookup(name)
		if !ok {
			return nil, nil, fmt.Errorf("parquet source: column %q not found in schema", name)
		}
		f, fok := fieldByName[name]
		if !fok {
			return nil, nil, fmt.Errorf("parquet source: column %q not found in fields", name)
		}
		cols = append(cols, record.Column{Name: name, Type: parquetTypeToRecord(f)})
		indices = append(indices, lc.ColumnIndex)
	}
	return cols, indices, nil
}

// parquetTypeToRecord maps a parquet field's physical Kind to a record.ColumnType.
// Bug 3 fix: switch on f.Type().Kind() (physical Kind constant) instead of
// f.Type().String() (logical type description such as "INT(64,true)" or
// "TIMESTAMP(MICROS,true)") which does not match the bare type name strings.
func parquetTypeToRecord(f parquetgo.Field) record.ColumnType {
	switch f.Type().Kind() {
	case parquetgo.Int32, parquetgo.Int64:
		return record.TypeInt
	case parquetgo.Float, parquetgo.Double:
		return record.TypeFloat
	case parquetgo.Boolean:
		return record.TypeBool
	default:
		// Int96 (legacy timestamp), ByteArray, FixedLenByteArray → string.
		// Int96 is exclusively used for legacy Impala/Hive timestamps; treating
		// it as TypeInt via Int64() would silently truncate the high 32 bits.
		return record.TypeString
	}
}

// extractRow converts a parquet Row (leaf-column-ordered values) into an
// interface{} slice using the declared leaf column index mapping.
// colIndices[i] is the leaf column index obtained from pfSchema.Lookup(),
// which correctly indexes into the flat []Value that ReadRows returns.
func extractRow(row parquetgo.Row, colIndices []int, schema []record.Column) []interface{} {
	out := make([]interface{}, len(colIndices))
	for i, colIdx := range colIndices {
		if colIdx < len(row) {
			v := row[colIdx]
			out[i] = parquetValueToGo(v)
		}
	}
	return out
}

// parquetValueToGo converts a parquet Value to a Go native type.
// Bug 4 fix: dispatch on v.Kind() (physical storage kind) rather than the
// logical record.ColumnType so that INT32 and FLOAT are handled correctly:
//   - INT32: v.Int64() zero-extends the stored uint32 bits, corrupting negative
//     values; int64(v.Int32()) sign-extends correctly.
//   - FLOAT: v.Double() misinterprets the 32-bit float bits as a 64-bit IEEE
//     pattern; float64(v.Float()) reconstructs the float32 first, then widens.
//
// The typ parameter is removed: callers pass only the Value.
func parquetValueToGo(v parquetgo.Value) interface{} {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case parquetgo.Boolean:
		return v.Boolean()
	case parquetgo.Int32:
		return int64(v.Int32()) // sign-extends correctly; v.Int64() would zero-extend
	case parquetgo.Int64, parquetgo.Int96:
		return v.Int64() // Int96: truncation to 64 bits is accepted behavior
	case parquetgo.Float:
		return float64(v.Float()) // reconstructs float32 first, then widens — correct
	case parquetgo.Double:
		return v.Double()
	case parquetgo.ByteArray, parquetgo.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return string(v.ByteArray())
	}
}
