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
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	parquetgo "github.com/parquet-go/parquet-go"
)

func init() {
	source.Register("parquet", newParquetSource)
}

type parquetSource struct {
	file       *os.File
	pqFile     *parquetgo.File
	schema     []record.Column
	colIndices []int   // output column position → parquet leaf column index
	decScales  []int32 // per output column: DECIMAL scale, or -1 when not a decimal
	rowGroups  []parquetgo.RowGroup
	rgIdx      int // next row group to decode

	// readLeaves is the set of leaf column indices actually read from storage —
	// the projected output columns plus the filter column when it is not itself
	// projected. Only these column chunks' pages are touched, so unselected
	// columns are never read from disk (true column pushdown). leafScale maps a
	// read leaf to its DECIMAL scale, or -1 when it is not a decimal.
	readLeaves []int
	leafScale  map[int]int32

	// filter, when non-nil, is a single-comparison predicate used to (1) skip
	// whole row groups whose column statistics prove no row can match
	// (predicate pushdown) and (2) drop non-matching rows exactly.
	filter *parquetPredicate

	// buf holds decoded, projected, filtered rows pending emission; served in
	// size-bounded batches so a slow sink applies backpressure instead of the
	// source materialising whole row groups downstream.
	buf    [][]interface{}
	bufIdx int

	rowsRead    int64
	parallelism int
	closed      bool
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

	schema, colIndices, decScales, err := resolveSchema(pf, wantCols)
	if err != nil {
		f.Close()
		return nil, err
	}

	parallelism := 4
	if p, ok := cfg["rowGroupParallelism"].(int); ok && p > 0 {
		parallelism = p
	} else if pf, ok := cfg["rowGroupParallelism"].(float64); ok && pf > 0 {
		// JSON-decoded pipeline configs deliver numbers as float64.
		parallelism = int(pf)
	}

	rowGroups := pf.RowGroups()

	// Build the set of leaf column indices to read (the projected outputs) and
	// their decimal scales. Only these column chunks are touched on disk.
	leafScale := make(map[int]int32, len(colIndices)+1)
	readLeaves := make([]int, 0, len(colIndices)+1)
	seen := make(map[int]bool, len(colIndices)+1)
	for k, leaf := range colIndices {
		if !seen[leaf] {
			seen[leaf] = true
			readLeaves = append(readLeaves, leaf)
			leafScale[leaf] = decScales[k]
		}
	}

	// Parse the optional predicate. The filter column is read even when it is not
	// part of the projection, so row-level filtering stays exact.
	var pred *parquetPredicate
	if fexpr, _ := cfg["filter"].(string); fexpr != "" {
		pred, err = parseParquetPredicate(fexpr, pf.Schema())
		if err != nil {
			f.Close()
			return nil, err
		}
		if !seen[pred.leafIndex] {
			seen[pred.leafIndex] = true
			readLeaves = append(readLeaves, pred.leafIndex)
			leafScale[pred.leafIndex] = pred.decScale
		}
	}

	return &parquetSource{
		file:        f,
		pqFile:      pf,
		schema:      schema,
		colIndices:  colIndices,
		decScales:   decScales,
		rowGroups:   rowGroups,
		readLeaves:  readLeaves,
		leafScale:   leafScale,
		filter:      pred,
		parallelism: parallelism,
	}, nil
}

func (s *parquetSource) Schema() []record.Column { return s.schema }

// Skip advances past n emitted (post-filter) rows so a checkpoint can resume.
// When no filter is set, whole row groups are skipped by their row count without
// decoding. With a filter, a partially-consumed row group is decoded once and
// its surviving tail buffered for the next ReadBatch.
func (s *parquetSource) Skip(n int64) error {
	var skipped int64
	for skipped < n && s.rgIdx < len(s.rowGroups) {
		rg := s.rowGroups[s.rgIdx]
		if s.filter == nil {
			rgRows := rg.NumRows()
			if skipped+rgRows <= n {
				skipped += rgRows
				s.rgIdx++
				continue
			}
		}
		rows, err := s.decodeRowGroup(rg)
		if err != nil {
			return err
		}
		s.rgIdx++
		emitted := int64(len(rows))
		if skipped+emitted <= n {
			skipped += emitted
			continue
		}
		// Partial row group: keep the tail that follows the skip boundary.
		keepFrom := n - skipped
		s.buf = append(s.buf[:0], rows[keepFrom:]...)
		s.bufIdx = 0
		skipped = n
	}
	s.rowsRead = n
	return nil
}

// ReadBatch returns up to size rows. Rows are decoded from row groups into an
// internal buffer (one row group in sequential mode, up to `parallelism`
// concurrently otherwise), projected to the selected columns and filtered by the
// optional predicate; the buffer is then served in size-bounded slices so a slow
// sink applies backpressure. Returns io.EOF once every row group is drained.
func (s *parquetSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	if s.bufIdx >= len(s.buf) {
		if err := s.fill(); err != nil {
			return nil, err
		}
	}
	if s.bufIdx >= len(s.buf) {
		return nil, io.EOF
	}

	avail := len(s.buf) - s.bufIdx
	n := size
	if avail < n {
		n = avail
	}
	batch := record.Get()
	batch.Columns = s.schema
	batch.Rows = make([][]interface{}, n)
	copy(batch.Rows, s.buf[s.bufIdx:s.bufIdx+n])
	s.bufIdx += n
	s.rowsRead += int64(n)

	if s.bufIdx >= len(s.buf) && s.rgIdx >= len(s.rowGroups) {
		return batch, io.EOF
	}
	return batch, nil
}

// fill decodes the next chunk of row groups into s.buf. In sequential mode one
// row group is decoded per call; otherwise up to `parallelism` groups are
// decoded concurrently and appended in row-group order. It loops so a run of
// entirely-pruned/empty row groups advances rather than returning empty.
func (s *parquetSource) fill() error {
	s.buf = s.buf[:0]
	s.bufIdx = 0
	for len(s.buf) == 0 && s.rgIdx < len(s.rowGroups) {
		start := s.rgIdx
		end := start + 1
		if s.parallelism > 1 {
			end = start + s.parallelism
		}
		if end > len(s.rowGroups) {
			end = len(s.rowGroups)
		}

		if end-start == 1 {
			rows, err := s.decodeRowGroup(s.rowGroups[start])
			if err != nil {
				return err
			}
			s.buf = append(s.buf, rows...)
		} else {
			results := make([][][]interface{}, end-start)
			errs := make([]error, end-start)
			var wg sync.WaitGroup
			for i := start; i < end; i++ {
				wg.Add(1)
				go func(idx, slot int) {
					defer wg.Done()
					results[slot], errs[slot] = s.decodeRowGroup(s.rowGroups[idx])
				}(i, i-start)
			}
			wg.Wait()
			for _, e := range errs {
				if e != nil {
					return e
				}
			}
			for _, r := range results {
				s.buf = append(s.buf, r...)
			}
		}
		s.rgIdx = end
	}
	return nil
}

// decodeRowGroup reads the selected column chunks of a single row group (only
// those columns' pages are touched — true column pushdown), applies the optional
// predicate (skipping the whole group when its statistics prove no row can
// match), and returns the surviving rows projected to the output schema.
func (s *parquetSource) decodeRowGroup(rg parquetgo.RowGroup) ([][]interface{}, error) {
	if s.filter != nil && !s.filter.rowGroupMayMatch(rg) {
		return nil, nil // pruned: no row in this group can match
	}

	numRows := int(rg.NumRows())
	chunks := rg.ColumnChunks()

	colData := make(map[int][]interface{}, len(s.readLeaves))
	for _, leaf := range s.readLeaves {
		if leaf < 0 || leaf >= len(chunks) {
			return nil, fmt.Errorf("parquet source: leaf column %d out of range (%d chunks)", leaf, len(chunks))
		}
		vals, err := readColumnGo(chunks[leaf], numRows, s.leafScale[leaf])
		if err != nil {
			return nil, err
		}
		colData[leaf] = vals
	}

	out := make([][]interface{}, 0, numRows)
	for r := 0; r < numRows; r++ {
		if s.filter != nil && !s.filter.matchGo(valueAt(colData[s.filter.leafIndex], r)) {
			continue
		}
		row := make([]interface{}, len(s.colIndices))
		for k, leaf := range s.colIndices {
			row[k] = valueAt(colData[leaf], r)
		}
		out = append(out, row)
	}
	return out, nil
}

// valueAt returns vals[i] or nil when i is out of range (defensive against a
// column chunk that yields fewer values than the row group's row count).
func valueAt(vals []interface{}, i int) interface{} {
	if i < 0 || i >= len(vals) {
		return nil
	}
	return vals[i]
}

// readColumnGo reads every value of a single column chunk (draining all its
// pages) and converts each to its Go representation, decoding DECIMAL columns
// (scale >= 0) into an exact record.Decimal. Only this column chunk's pages are
// read from storage, which is what makes column projection a genuine
// storage-level pushdown rather than a post-decode filter.
func readColumnGo(cc parquetgo.ColumnChunk, numRows int, scale int32) ([]interface{}, error) {
	out := make([]interface{}, 0, numRows)
	pages := cc.Pages()
	defer pages.Close()

	buf := make([]parquetgo.Value, 512)
	for {
		pg, err := pages.ReadPage()
		if pg != nil {
			vr := pg.Values()
			for {
				n, verr := vr.ReadValues(buf)
				// Convert within the page's lifetime (before Release) so the
				// byte-array-backed values are copied into Go strings/decimals.
				for i := 0; i < n; i++ {
					if scale >= 0 {
						out = append(out, parquetDecimalToGo(buf[i], scale))
					} else {
						out = append(out, parquetValueToGo(buf[i]))
					}
				}
				if verr == io.EOF {
					break
				}
				if verr != nil {
					parquetgo.Release(pg)
					return nil, verr
				}
			}
			parquetgo.Release(pg)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// parquetPredicate is a single-comparison filter (column OP literal) used for
// both row-group statistics pruning and exact row-level filtering. Compound
// predicates (AND/OR) are not supported here — use a `filter` transform for those.
type parquetPredicate struct {
	colName   string
	leafIndex int    // leaf column index in the parquet schema
	op        string // one of = != > >= < <=
	litIsStr  bool
	litStr    string
	litNum    float64
	decScale  int32 // filter column's DECIMAL scale, or -1
}

// parseParquetPredicate parses "column OP value" where OP is one of =,!=,>,>=,<,<=
// and value is a number or a single-quoted string, resolving the column to its
// leaf index in the file schema.
func parseParquetPredicate(expr string, schema *parquetgo.Schema) (*parquetPredicate, error) {
	col, op, lit, err := splitComparison(expr)
	if err != nil {
		return nil, err
	}
	lc, ok := schema.Lookup(col)
	if !ok {
		return nil, fmt.Errorf("parquet source: filter column %q not found in schema", col)
	}
	p := &parquetPredicate{colName: col, leafIndex: lc.ColumnIndex, op: op, decScale: -1}
	for _, f := range schema.Fields() {
		if f.Name() == col {
			p.decScale = parquetDecimalScaleOf(f)
			break
		}
	}
	if len(lit) >= 2 && lit[0] == '\'' && lit[len(lit)-1] == '\'' {
		p.litStr = lit[1 : len(lit)-1]
		p.litIsStr = true
	} else if num, e := strconv.ParseFloat(lit, 64); e == nil {
		p.litNum = num
	} else {
		return nil, fmt.Errorf("parquet source: filter literal %q must be a number or a single-quoted string", lit)
	}
	return p, nil
}

// splitComparison finds the comparison operator in expr and splits it into the
// column name and literal. Two-character operators are tested before their
// single-character prefixes so ">=", "<=" and "!=" are matched whole.
func splitComparison(expr string) (col, op, lit string, err error) {
	for _, o := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if i := strings.Index(expr, o); i >= 0 {
			col = strings.TrimSpace(expr[:i])
			lit = strings.TrimSpace(expr[i+len(o):])
			if col == "" || lit == "" {
				return "", "", "", fmt.Errorf("parquet source: malformed filter %q (want 'column OP value')", expr)
			}
			return col, o, lit, nil
		}
	}
	return "", "", "", fmt.Errorf("parquet source: filter %q must be a single comparison (column =|!=|>|>=|<|<= value)", expr)
}

// matchGo evaluates the predicate against a decoded Go value. A null value never
// satisfies a comparison.
func (p *parquetPredicate) matchGo(v interface{}) bool {
	if v == nil {
		return false
	}
	if p.litIsStr {
		return cmpOrdered(toStr(v), p.litStr, p.op)
	}
	f, ok := toFloat64(v)
	if !ok {
		return false
	}
	return cmpOrdered(f, p.litNum, p.op)
}

// rowGroupMayMatch reports whether any row in rg could satisfy the predicate,
// using the column's page-index min/max statistics. It is conservative: it
// returns true whenever statistics are missing or unusable, so pruning never
// drops a matching row. DECIMAL columns are never pruned (their scaled physical
// statistics are not directly comparable to a plain literal); row-level
// filtering still applies to them exactly.
func (p *parquetPredicate) rowGroupMayMatch(rg parquetgo.RowGroup) bool {
	if p.decScale >= 0 {
		return true
	}
	chunks := rg.ColumnChunks()
	if p.leafIndex < 0 || p.leafIndex >= len(chunks) {
		return true
	}
	ci, err := chunks[p.leafIndex].ColumnIndex()
	if err != nil {
		return true // no page index → cannot prune
	}
	have := false
	var minF, maxF float64
	var minS, maxS string
	for i := 0; i < ci.NumPages(); i++ {
		if ci.NullPage(i) {
			continue
		}
		mnv, mxv := ci.MinValue(i), ci.MaxValue(i)
		if mnv.IsNull() || mxv.IsNull() {
			continue
		}
		if p.litIsStr {
			mn, mx := parquetValueToStr(mnv), parquetValueToStr(mxv)
			if !have {
				minS, maxS, have = mn, mx, true
			} else {
				if mn < minS {
					minS = mn
				}
				if mx > maxS {
					maxS = mx
				}
			}
		} else {
			mn, ok1 := parquetValueToFloat(mnv)
			mx, ok2 := parquetValueToFloat(mxv)
			if !ok1 || !ok2 {
				return true
			}
			if !have {
				minF, maxF, have = mn, mx, true
			} else {
				if mn < minF {
					minF = mn
				}
				if mx > maxF {
					maxF = mx
				}
			}
		}
	}
	if !have {
		return true
	}
	if p.litIsStr {
		return rangeMayMatch(minS, maxS, p.litStr, p.op)
	}
	return rangeMayMatch(minF, maxF, p.litNum, p.op)
}

// cmpOrdered evaluates `a OP b` for any ordered type.
func cmpOrdered[T int64 | float64 | string](a, b T, op string) bool {
	switch op {
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case "=":
		return a == b
	case "!=":
		return a != b
	}
	return false
}

// rangeMayMatch reports whether the closed interval [min,max] could contain a
// value satisfying `x OP lit`.
func rangeMayMatch[T int64 | float64 | string](min, max, lit T, op string) bool {
	switch op {
	case ">":
		return max > lit
	case ">=":
		return max >= lit
	case "<":
		return min < lit
	case "<=":
		return min <= lit
	case "=":
		return min <= lit && lit <= max
	case "!=":
		return !(min == max && min == lit)
	}
	return true
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case record.Decimal:
		f, err := strconv.ParseFloat(n.String(), 64)
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func toStr(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func parquetValueToFloat(v parquetgo.Value) (float64, bool) {
	switch v.Kind() {
	case parquetgo.Int32:
		return float64(v.Int32()), true
	case parquetgo.Int64:
		return float64(v.Int64()), true
	case parquetgo.Float:
		return float64(v.Float()), true
	case parquetgo.Double:
		return v.Double(), true
	default:
		return 0, false
	}
}

func parquetValueToStr(v parquetgo.Value) string {
	if v.Kind() == parquetgo.ByteArray || v.Kind() == parquetgo.FixedLenByteArray {
		return string(v.ByteArray())
	}
	return v.String()
}

func (s *parquetSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

// resolveSchema maps declared column names to parquet schema columns.
// Returns the ordered record.Column slice and a parallel index slice mapping
// output column position → parquet leaf column index.
// Bug 2 fix: use pfSchema.Lookup() to get the correct leaf column index
// instead of the Fields() positional index, which diverges for nested schemas.
func resolveSchema(pf *parquetgo.File, wantCols []string) ([]record.Column, []int, []int32, error) {
	pfSchema := pf.Schema()
	allFields := pfSchema.Fields()

	if len(wantCols) == 0 {
		// Use all columns — look up each field by name to get its leaf column index.
		cols := make([]record.Column, len(allFields))
		indices := make([]int, len(allFields))
		decScales := make([]int32, len(allFields))
		for i, f := range allFields {
			lc, ok := pfSchema.Lookup(f.Name())
			if !ok {
				return nil, nil, nil, fmt.Errorf("parquet source: schema field %q not found via Lookup", f.Name())
			}
			cols[i] = record.Column{Name: f.Name(), Type: parquetTypeToRecord(f)}
			indices[i] = lc.ColumnIndex
			decScales[i] = parquetDecimalScaleOf(f)
		}
		return cols, indices, decScales, nil
	}

	// Build a name → Field map for type resolution.
	fieldByName := make(map[string]parquetgo.Field, len(allFields))
	for _, f := range allFields {
		fieldByName[f.Name()] = f
	}

	cols := make([]record.Column, 0, len(wantCols))
	indices := make([]int, 0, len(wantCols))
	decScales := make([]int32, 0, len(wantCols))
	for _, name := range wantCols {
		lc, ok := pfSchema.Lookup(name)
		if !ok {
			return nil, nil, nil, fmt.Errorf("parquet source: column %q not found in schema", name)
		}
		f, fok := fieldByName[name]
		if !fok {
			return nil, nil, nil, fmt.Errorf("parquet source: column %q not found in fields", name)
		}
		cols = append(cols, record.Column{Name: name, Type: parquetTypeToRecord(f)})
		indices = append(indices, lc.ColumnIndex)
		decScales = append(decScales, parquetDecimalScaleOf(f))
	}
	return cols, indices, decScales, nil
}

// parquetTypeToRecord maps a parquet field's physical Kind to a record.ColumnType.
// Bug 3 fix: switch on f.Type().Kind() (physical Kind constant) instead of
// f.Type().String() (logical type description such as "INT(64,true)" or
// "TIMESTAMP(MICROS,true)") which does not match the bare type name strings.
func parquetTypeToRecord(f parquetgo.Field) record.ColumnType {
	if lt := f.Type().LogicalType(); lt != nil && lt.Decimal != nil {
		return record.TypeDecimal
	}
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

// parquetDecimalScaleOf returns the DECIMAL logical-type scale of a field, or -1
// when the field is not a decimal.
func parquetDecimalScaleOf(f parquetgo.Field) int32 {
	if lt := f.Type().LogicalType(); lt != nil && lt.Decimal != nil {
		return lt.Decimal.Scale
	}
	return -1
}

// parquetDecimalToGo decodes a parquet DECIMAL value (stored as int32/int64 or a
// big-endian two's-complement byte array) into an exact record.Decimal at the
// column's scale.
func parquetDecimalToGo(v parquetgo.Value, scale int32) interface{} {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case parquetgo.Int32:
		return record.NewDecimal(int64(v.Int32()), scale)
	case parquetgo.Int64:
		return record.NewDecimal(v.Int64(), scale)
	case parquetgo.ByteArray, parquetgo.FixedLenByteArray:
		return record.NewDecimalFromBig(twosComplementToBigInt(v.ByteArray()), scale)
	default:
		return parquetValueToGo(v)
	}
}

// twosComplementToBigInt decodes a big-endian two's-complement byte slice (the
// physical layout of a fixed-len parquet decimal) into a signed big.Int.
func twosComplementToBigInt(b []byte) *big.Int {
	bi := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		bi.Sub(bi, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return bi
}
