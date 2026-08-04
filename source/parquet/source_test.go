package parquet

import (
	"bytes"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	parquetgo "github.com/parquet-go/parquet-go"
)

// These tests guard the reader-lifecycle behavior of the parquet source after
// the migration from the deprecated *parquetgo.Reader to
// *parquetgo.GenericReader[any]. The persistent sequential reader must keep its
// position across ReadBatch calls (no duplicated/dropped rows when a batch is
// smaller than a row group or a row group spans multiple pages), the parallel
// path must return every row exactly once, Skip must not drop tail rows, and
// DECIMAL columns must still decode to an exact record.Decimal.

// nodeForType mirrors the parquet sink's schema mapping so the files these tests
// write are read back through exactly the same code paths a real pipeline uses.
func nodeForType(t record.ColumnType) parquetgo.Node {
	switch t {
	case record.TypeInt:
		return parquetgo.Int(64)
	case record.TypeFloat:
		return parquetgo.Leaf(parquetgo.DoubleType)
	case record.TypeBool:
		return parquetgo.Leaf(parquetgo.BooleanType)
	case record.TypeDecimal:
		return parquetgo.Decimal(10, 38, parquetgo.FixedLenByteArrayType(16))
	default:
		return parquetgo.String()
	}
}

// encodeDecimal16 renders a decimal's scale-10 coefficient as a 16-byte
// big-endian two's-complement value — the physical layout the parquet sink
// writes for DECIMAL(38,10) and the source decodes via twosComplementToBigInt.
func encodeDecimal16(d record.Decimal) []byte {
	bi := d.Coef(10)
	out := make([]byte, 16)
	if bi.Sign() >= 0 {
		b := bi.Bytes()
		copy(out[16-len(b):], b)
		return out
	}
	tc := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 128), bi)
	b := tc.Bytes()
	copy(out[16-len(b):], b)
	return out
}

// writeTestParquet writes rows into a parquet file, sealing a new row group
// every rowsPerGroup rows (via Flush) and, when pageBufferSize > 0, using small
// data pages so a single row group spans multiple pages.
func writeTestParquet(t *testing.T, path string, columns []record.Column, rows [][]interface{}, rowsPerGroup, pageBufferSize int) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	group := make(parquetgo.Group, len(columns))
	for _, c := range columns {
		group[c.Name] = parquetgo.Optional(nodeForType(c.Type))
	}
	opts := []parquetgo.WriterOption{
		parquetgo.NewSchema("test", group),
	}
	if pageBufferSize > 0 {
		opts = append(opts, parquetgo.PageBufferSize(pageBufferSize))
	}
	w := parquetgo.NewGenericWriter[map[string]interface{}](f, opts...)
	rowMaps := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		m := make(map[string]interface{}, len(columns))
		for j, c := range columns {
			if c.Type == record.TypeDecimal {
				m[c.Name] = encodeDecimal16(r[j].(record.Decimal))
			} else {
				m[c.Name] = r[j]
			}
		}
		rowMaps[i] = m
	}
	// Seal a row group every rowsPerGroup rows with Flush(). The writer's default
	// MaxRowsPerRowGroup is MaxInt64 and its internal boundary chunking panics when
	// a single Write spans a configured boundary, so we control grouping explicitly
	// with Flush() instead — deterministic and panic-free.
	gsize := rowsPerGroup
	if gsize <= 0 || gsize > len(rowMaps) {
		gsize = len(rowMaps)
	}
	for start := 0; start < len(rowMaps); start += gsize {
		end := start + gsize
		if end > len(rowMaps) {
			end = len(rowMaps)
		}
		if _, err := w.Write(rowMaps[start:end]); err != nil {
			t.Fatalf("write rows: %v", err)
		}
		if end < len(rowMaps) {
			// Seal this row group; Close() seals the final one.
			if err := w.Flush(); err != nil {
				t.Fatalf("flush row group: %v", err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close: %v", err)
	}
}

// readAllRows drains a source via repeated ReadBatch(batchSize) until io.EOF.
func readAllRows(t *testing.T, src source.Source, batchSize int) [][]interface{} {
	t.Helper()
	var all [][]interface{}
	for {
		b, err := src.ReadBatch(batchSize)
		if b != nil {
			all = append(all, b.Rows...)
		}
		if err == io.EOF {
			return all
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
	}
}

func newSourceForTest(t *testing.T, path string, parallelism int) *parquetSource {
	t.Helper()
	src, err := newParquetSource(map[string]interface{}{
		"path":                path,
		"rowGroupParallelism": parallelism,
	})
	if err != nil {
		t.Fatalf("newParquetSource: %v", err)
	}
	ps, ok := src.(*parquetSource)
	if !ok {
		t.Fatalf("newParquetSource returned %T, want *parquetSource", src)
	}
	return ps
}

// TestParquetSource_FromReaderAt exercises the injected-io.ReaderAt construction
// path (used by the object-store source for remote range-GET pushdown): a
// bytes.Reader over the file bytes stands in for a range-backed reader. The rows
// read back must match a normal path-based read, and omitting size must error.
func TestParquetSource_FromReaderAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ra.parquet")
	columns := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "name", Type: record.TypeString},
	}
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	}
	writeTestParquet(t, path, columns, rows, 2, 0) // two row groups

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	// bytes.Reader is an io.ReaderAt — inject it exactly as the objstore source
	// injects a range-GET-backed reader.
	src, err := newParquetSource(map[string]interface{}{
		"readerAt": bytes.NewReader(data),
		"size":     int64(len(data)),
	})
	if err != nil {
		t.Fatalf("newParquetSource(readerAt): %v", err)
	}
	defer src.Close()

	got := readAllRows(t, src, 2)
	if len(got) != 3 {
		t.Fatalf("readerAt path read %d rows, want 3", len(got))
	}
	for i, want := range []int64{1, 2, 3} {
		if got[i][0] != want {
			t.Fatalf("row %d id = %v, want %d", i, got[i][0], want)
		}
	}

	// A readerAt without a positive size must be rejected.
	if _, err := newParquetSource(map[string]interface{}{"readerAt": bytes.NewReader(data)}); err == nil {
		t.Fatal("expected an error when 'size' is missing alongside 'readerAt'")
	}
}

// TestSequentialPreservesPositionAcrossBatches is the core regression: with a
// batch smaller than the (single) row group and small pages forcing short reads,
// the persistent GenericReader must continue where it left off — every row must
// appear exactly once, in order.
func TestSequentialPreservesPositionAcrossBatches(t *testing.T) {
	const n = 300
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}}
	rows := make([][]interface{}, n)
	for i := 0; i < n; i++ {
		rows[i] = []interface{}{int64(i), "row-" + itoa(i)}
	}
	path := filepath.Join(t.TempDir(), "seq.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000 /*single row group*/, 512 /*small pages*/)

	src := newSourceForTest(t, path, 1) // parallelism 1 → sequential path
	defer src.Close()
	if len(src.rowGroups) != 1 {
		t.Fatalf("expected 1 row group, got %d", len(src.rowGroups))
	}

	got := readAllRows(t, src, 64) // batch < row group
	if len(got) != n {
		t.Fatalf("read %d rows, want %d (duplicated or dropped rows)", len(got), n)
	}
	for i := 0; i < n; i++ {
		if id, _ := got[i][0].(int64); id != int64(i) {
			t.Fatalf("row %d: id = %v, want %d (out-of-order or duplicated)", i, got[i][0], i)
		}
		if name, _ := got[i][1].(string); name != "row-"+itoa(i) {
			t.Fatalf("row %d: name = %q, want %q", i, name, "row-"+itoa(i))
		}
	}
}

// TestParallelReturnsEveryRowOnce verifies the parallel path across many row
// groups yields every row exactly once (order across groups is not guaranteed).
func TestParallelReturnsEveryRowOnce(t *testing.T) {
	const n = 500
	cols := []record.Column{{Name: "id", Type: record.TypeInt}}
	rows := make([][]interface{}, n)
	for i := 0; i < n; i++ {
		rows[i] = []interface{}{int64(i)}
	}
	path := filepath.Join(t.TempDir(), "par.parquet")
	writeTestParquet(t, path, cols, rows, 50 /*→ 10 row groups*/, 0)

	src := newSourceForTest(t, path, 4) // parallelism 4 → parallel path
	defer src.Close()
	if len(src.rowGroups) < 2 {
		t.Fatalf("expected multiple row groups, got %d", len(src.rowGroups))
	}

	got := readAllRows(t, src, 64)
	seen := make(map[int64]int, n)
	for _, r := range got {
		id, _ := r[0].(int64)
		seen[id]++
	}
	if len(got) != n {
		t.Fatalf("read %d rows, want %d", len(got), n)
	}
	for i := int64(0); i < n; i++ {
		if seen[i] != 1 {
			t.Fatalf("id %d seen %d times, want exactly 1", i, seen[i])
		}
	}
}

// TestSkipDoesNotDropTailRows verifies Skip advances exactly n rows and the tail
// of a partially-skipped row group is drained (not silently dropped).
func TestSkipDoesNotDropTailRows(t *testing.T) {
	const n = 100
	cols := []record.Column{{Name: "id", Type: record.TypeInt}}
	rows := make([][]interface{}, n)
	for i := 0; i < n; i++ {
		rows[i] = []interface{}{int64(i)}
	}
	path := filepath.Join(t.TempDir(), "skip.parquet")
	writeTestParquet(t, path, cols, rows, 40 /*row groups 40,40,20*/, 0)

	src := newSourceForTest(t, path, 1)
	defer src.Close()
	if err := src.Skip(55); err != nil { // skip into the middle of the 2nd row group
		t.Fatalf("Skip: %v", err)
	}
	got := readAllRows(t, src, 16)
	if len(got) != n-55 {
		t.Fatalf("after Skip(55) read %d rows, want %d", len(got), n-55)
	}
	for i := 0; i < len(got); i++ {
		want := int64(55 + i)
		if id, _ := got[i][0].(int64); id != want {
			t.Fatalf("row %d: id = %v, want %d", i, got[i][0], want)
		}
	}
}

// TestDecimalRoundTrip verifies DECIMAL columns decode to an exact record.Decimal
// after the reader migration.
func TestDecimalRoundTrip(t *testing.T) {
	cols := []record.Column{{Name: "amt", Type: record.TypeDecimal}}
	vals := []string{"0", "123.45", "-67.89", "999999.999999", "-0.0000000001"}
	rows := make([][]interface{}, len(vals))
	want := make([]record.Decimal, len(vals))
	for i, s := range vals {
		d, ok := record.ParseDecimal(s)
		if !ok {
			t.Fatalf("ParseDecimal(%q) failed", s)
		}
		rows[i] = []interface{}{d}
		want[i] = d
	}
	path := filepath.Join(t.TempDir(), "dec.parquet")
	writeTestParquet(t, path, cols, rows, 1_000_000, 0)

	src := newSourceForTest(t, path, 1)
	defer src.Close()
	if got := src.Schema()[0].Type; got != record.TypeDecimal {
		t.Fatalf("schema type = %q, want decimal", got)
	}

	got := readAllRows(t, src, 8)
	if len(got) != len(vals) {
		t.Fatalf("read %d rows, want %d", len(got), len(vals))
	}
	for i := range vals {
		d, ok := got[i][0].(record.Decimal)
		if !ok {
			t.Fatalf("row %d: value %v is %T, want record.Decimal", i, got[i][0], got[i][0])
		}
		if d.Cmp(want[i]) != 0 {
			t.Fatalf("row %d: decoded %s, want %s", i, d.String(), want[i].String())
		}
	}
}

// itoa is a tiny dependency-free int→string helper for row labels.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
