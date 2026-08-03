package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	parquetgo "github.com/parquet-go/parquet-go"
)

// TestVariableBatchRowGroups reproduces the graph-runner fan-out panic
// condition: many variable-size batches whose cumulative row counts repeatedly
// straddle the configured row-group boundary. parquet-go's MaxRowsPerRowGroup
// mis-tracked the writer's row count on such writes and panicked with an
// inverted slice bound ("slice bounds out of range [i:j:]" with i>j) inside
// writeRows. The sink now closes row groups itself via Flush() instead of using
// that option, so this must complete and read back the exact row count across
// multiple row groups.
func TestVariableBatchRowGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vb.parquet")
	s, err := newParquetSink(map[string]interface{}{
		"path": path, "compression": "snappy", "rowGroupSize": 1000,
	})
	if err != nil {
		t.Fatalf("newParquetSink: %v", err)
	}
	cols := []record.Column{
		{Name: "id", Type: record.TypeInt},
		{Name: "region", Type: record.TypeString},
		{Name: "price", Type: record.TypeFloat},
	}
	// Sizes that do not evenly divide the 1000-row group and vary widely, so
	// writes straddle row-group boundaries — the exact trigger for the old panic.
	sizes := []int{37, 500, 1, 999, 250, 1000, 3, 763}
	total, id := 0, 0
	for round := 0; round < 20; round++ {
		n := sizes[round%len(sizes)]
		rows := make([][]interface{}, n)
		for i := 0; i < n; i++ {
			id++
			rows[i] = []interface{}{id, "R", 1.5}
		}
		if err := s.WriteBatch(&record.RecordBatch{Columns: cols, Rows: rows}); err != nil {
			t.Fatalf("WriteBatch round %d (n=%d): %v", round, n, err)
		}
		total += n
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	pf, err := parquetgo.OpenFile(f, info.Size())
	if err != nil {
		t.Fatalf("OpenFile (invalid footer?): %v", err)
	}
	if got := pf.NumRows(); got != int64(total) {
		t.Errorf("read-back NumRows = %d; want %d", got, total)
	}
	if got := len(pf.RowGroups()); got < 2 {
		t.Errorf("expected multiple row groups for %d rows at rowGroupSize=1000, got %d", total, got)
	}
}
