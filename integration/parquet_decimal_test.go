package integration

import (
	"io"
	"os"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet"
)

// TestPipeline_ParquetDecimalRoundTrip proves TypeDecimal values survive a
// CSV → Parquet(write) → Parquet(read) round trip exactly, using the native
// Parquet DECIMAL logical type (not a lossy double). Values are compared by
// numeric equality because Parquet normalises every value to the column scale.
func TestPipeline_ParquetDecimalRoundTrip(t *testing.T) {
	csv := "id,price\n1,12345678901234567.89\n2,0.1\n3,100.00\n4,-5.25\n"
	in, cleanup := writeTempCSV(t, csv)
	defer cleanup()

	pq := in + ".parquet"
	defer os.Remove(pq)

	src, err := source.New("csv", map[string]interface{}{
		"path":      in,
		"hasHeader": true,
		"columns": []interface{}{
			map[string]interface{}{"name": "id", "type": "int"},
			map[string]interface{}{"name": "price", "type": "decimal"},
		},
	})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	snk, err := sink.New("parquet", map[string]interface{}{"path": pq})
	if err != nil {
		t.Fatalf("sink.New(parquet): %v", err)
	}
	for {
		b, rerr := src.ReadBatch(10)
		if b != nil && b.Len() > 0 {
			if werr := snk.WriteBatch(b); werr != nil {
				t.Fatalf("WriteBatch: %v", werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("ReadBatch(csv): %v", rerr)
		}
	}
	src.Close()
	if err := snk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read the Parquet file back through the source.
	psrc, err := source.New("parquet", map[string]interface{}{"path": pq})
	if err != nil {
		t.Fatalf("source.New(parquet): %v", err)
	}
	defer psrc.Close()

	priceIdx := -1
	for i, c := range psrc.Schema() {
		if c.Name == "price" {
			priceIdx = i
			if c.Type != record.TypeDecimal {
				t.Fatalf("price column type = %v; want TypeDecimal", c.Type)
			}
		}
	}
	if priceIdx < 0 {
		t.Fatal("price column missing from parquet schema")
	}

	want := map[string]bool{
		"12345678901234567.89": false, // 19 significant digits — float64 would drift
		"0.1":                  false,
		"100.00":               false,
		"-5.25":                false, // negative → two's-complement encoding
	}
	for {
		b, rerr := psrc.ReadBatch(10)
		if b != nil {
			for _, row := range b.Rows {
				d, ok := row[priceIdx].(record.Decimal)
				if !ok {
					t.Fatalf("price value = %T; want record.Decimal", row[priceIdx])
				}
				for w := range want {
					exp, _ := record.ParseDecimal(w)
					if d.Cmp(exp) == 0 {
						want[w] = true
					}
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("ReadBatch(parquet): %v", rerr)
		}
	}
	for w, seen := range want {
		if !seen {
			t.Errorf("decimal %q did not round-trip exactly through Parquet", w)
		}
	}
}
