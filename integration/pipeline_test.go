// Package integration wires source/csv → transform/filter → transform/mapper → sink/null
// using real registry calls (blank imports inside this file). No external services
// or Docker dependencies are required. The CSV data is read from a temp file
// backed by a strings.NewReader equivalent, so the test runs offline.
package integration

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"

	// Activate registries via blank imports.
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/null"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/filter"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
)

// csvData is the inline CSV content used throughout the integration tests.
const csvData = `id,name,amount,currency
1,Alice,100,USD
2,Bob,-50,EUR
3,Carol,200,USD
4,Dave,0,GBP
5,Eve,300,USD
`

// writeTempCSV writes csvData to a temp file and returns its path + cleanup fn.
func writeTempCSV(t *testing.T, content string) (path string, cleanup func()) {
	t.Helper()
	f, err := os.CreateTemp("", "integration_test_*.csv")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := io.WriteString(f, content); err != nil {
		t.Fatalf("write CSV: %v", err)
	}
	f.Close()
	return f.Name(), func() { os.Remove(f.Name()) }
}

// ─── Full pipeline: CSV → filter → mapper → null sink ────────────────────────

func TestPipeline_CSV_Filter_Mapper_NullSink(t *testing.T) {
	path, cleanup := writeTempCSV(t, csvData)
	defer cleanup()

	// 1. Create source.
	src, err := source.New("csv", map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("source.New(csv): %v", err)
	}
	defer src.Close()

	// 2. Build transform pipeline: keep rows with amount > 0, then rename fields.
	pipeline, err := transform.Build([]map[string]interface{}{
		{
			"type": "filter",
			"expr": "amount > 0",
		},
		{
			"type": "mapper",
			"fields": map[string]interface{}{
				"customerId": "=$id",
				"fullName":   "=$name",
				"amountUsd":  "=$amount",
			},
			"fieldOrder": []interface{}{"customerId", "fullName", "amountUsd"},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build: %v", err)
	}
	defer pipeline.Close()

	// 3. Create null sink.
	snk, err := sink.New("null", nil)
	if err != nil {
		t.Fatalf("sink.New(null): %v", err)
	}
	defer snk.Close()

	// 4. Run pipeline manually.
	var outputRows []*record.RecordBatch
	for {
		batch, readErr := src.ReadBatch(10)
		if readErr != nil && readErr != io.EOF {
			t.Fatalf("ReadBatch: %v", readErr)
		}
		if batch != nil && batch.Len() > 0 {
			out, err := pipeline.Apply(batch)
			if err != nil {
				t.Fatalf("pipeline.Apply: %v", err)
			}
			if out != nil && out.Len() > 0 {
				outputRows = append(outputRows, out)
				if err := snk.WriteBatch(out); err != nil {
					t.Fatalf("WriteBatch: %v", err)
				}
			}
		}
		if readErr == io.EOF {
			break
		}
	}

	if err := snk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// 5. Assert results.
	// Input has 5 rows; filter keeps amount > 0 → Alice(100), Carol(200), Eve(300).
	totalRows := 0
	for _, b := range outputRows {
		totalRows += b.Len()
	}
	if totalRows != 3 {
		t.Errorf("output row count = %d, want 3 (amount > 0)", totalRows)
	}

	// Verify column names from mapper.
	if len(outputRows) > 0 {
		first := outputRows[0]
		wantCols := []string{"customerId", "fullName", "amountUsd"}
		if len(first.Columns) != len(wantCols) {
			t.Fatalf("output columns = %v, want %v", first.Columns, wantCols)
		}
		for i, col := range first.Columns {
			if col.Name != wantCols[i] {
				t.Errorf("column[%d] = %q, want %q", i, col.Name, wantCols[i])
			}
		}

		// First surviving row: Alice with amount=100.
		if got := first.Value(0, "fullName"); got != "Alice" {
			t.Errorf("first output row fullName = %v, want Alice", got)
		}
		if got := first.Value(0, "customerId"); got != "1" {
			t.Errorf("first output row customerId = %v, want '1' (string from CSV)", got)
		}
	}
}

// ─── Regression: source.Formats() contains "csv" ─────────────────────────────

func TestPipeline_RegistryGuard_SourceFormats(t *testing.T) {
	formats := source.Formats()
	found := false
	for _, f := range formats {
		if f == "csv" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("source.Formats() does not contain 'csv': %v", formats)
	}
}

// ─── Regression: sink.Types() contains "null" ────────────────────────────────

func TestPipeline_RegistryGuard_SinkTypes(t *testing.T) {
	types := sink.Types()
	found := false
	for _, ty := range types {
		if ty == "null" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("sink.Types() does not contain 'null': %v", types)
	}
}

// ─── All rows filtered → pipeline handles nil gracefully ─────────────────────

func TestPipeline_AllRowsFiltered(t *testing.T) {
	// CSV data where all rows have amount <= 0.
	path, cleanup := writeTempCSV(t, "id,name,amount\n1,X,-1\n2,Y,0\n")
	defer cleanup()

	src, err := source.New("csv", map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	pipeline, err := transform.Build([]map[string]interface{}{
		{"type": "filter", "expr": "amount > 0"},
	})
	if err != nil {
		t.Fatalf("transform.Build: %v", err)
	}
	defer pipeline.Close()

	batch, _ := src.ReadBatch(10)
	if batch == nil {
		t.Fatal("expected a batch from source")
	}
	out, err := pipeline.Apply(batch)
	if err != nil {
		t.Fatalf("pipeline.Apply: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil output when all rows filtered, got %d rows", out.Len())
	}
}

// ─── strings.NewReader-based reader verification ──────────────────────────────
// This test confirms the pipeline logic works with inline string data by
// verifying the filter and mapper builtins behave correctly end-to-end.

func TestPipeline_InlineStringData(t *testing.T) {
	inlineCSV := strings.Join([]string{
		"product,price,qty",
		"Widget,50,10",
		"Gadget,0,5",
		"Doohickey,200,1",
	}, "\n") + "\n"

	path, cleanup := writeTempCSV(t, inlineCSV)
	defer cleanup()

	src, err := source.New("csv", map[string]interface{}{
		"path":      path,
		"hasHeader": true,
	})
	if err != nil {
		t.Fatalf("source.New: %v", err)
	}
	defer src.Close()

	pipeline, err := transform.Build([]map[string]interface{}{
		{"type": "filter", "expr": "price > 0"},
		{
			"type": "mapper",
			"fields": map[string]interface{}{
				"name":  "=$product",
				"total": "=multiply(=$price, =$qty)",
			},
			"fieldOrder": []interface{}{"name", "total"},
		},
	})
	if err != nil {
		t.Fatalf("transform.Build: %v", err)
	}
	defer pipeline.Close()

	batch, _ := src.ReadBatch(10)
	out, err := pipeline.Apply(batch)
	if err != nil {
		t.Fatalf("pipeline.Apply: %v", err)
	}
	if out == nil || out.Len() != 2 {
		rows := 0
		if out != nil {
			rows = out.Len()
		}
		t.Fatalf("expected 2 rows (price > 0), got %d", rows)
	}

	// Widget: 50 * 10 = 500; Doohickey: 200 * 1 = 200.
	// After lex order sort the mapper output is: name, total.
	if got := out.Value(0, "name"); got != "Widget" {
		t.Errorf("row 0 name = %v, want Widget", got)
	}
	if got, ok := out.Value(0, "total").(float64); !ok || got != 500.0 {
		t.Errorf("row 0 total = %v, want 500", out.Value(0, "total"))
	}
}
