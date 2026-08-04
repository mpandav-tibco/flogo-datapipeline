package integration

// Enterprise scenario: remote Parquet range-GET pushdown. Pipeline 1 writes a
// multi-column Parquet object to an S3-compatible bucket (objstore sink); Pipeline
// 2 reads it back projecting only two columns. Because the s3 store implements
// objstore.RangeReader, the parquet source is handed a range-GET-backed
// io.ReaderAt and fetches only the footer and the id/amount column chunks — the
// object is never downloaded whole to a temp file.
//
// Requires an S3 endpoint: set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000);
// bucket defaults to dp-test-bucket, creds to minioadmin.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3" // registers the s3 provider (+ RangeReader)
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet" // parquet writer for the objstore sink
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet" // ranged parquet reader under test
)

func TestEnterprise_S3_ParquetRangePushdown(t *testing.T) {
	endpoint := os.Getenv("DP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000) to run the S3 parquet pushdown scenario")
	}
	bucket := envOr("DP_TEST_S3_BUCKET", "dp-test-bucket")
	ak := envOr("DP_TEST_S3_ACCESS_KEY", "minioadmin")
	sk := envOr("DP_TEST_S3_SECRET_KEY", "minioadmin")

	prefix := fmt.Sprintf("dptest-parquet/%d/", time.Now().UnixNano())
	objKey := prefix + "orders.parquet"

	s3Conn := func(extra map[string]interface{}) map[string]interface{} {
		m := map[string]interface{}{
			"bucket":          bucket,
			"region":          "us-east-1",
			"endpoint":        endpoint,
			"forcePathStyle":  true,
			"accessKeyId":     ak,
			"secretAccessKey": sk,
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	// A 5-column CSV; only id/amount are projected on read so the other three
	// column chunks are never fetched.
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "orders.csv")
	var sb strings.Builder
	sb.WriteString("id,customer,amount,region,status\n")
	for i := 1; i <= 20; i++ {
		sb.WriteString(fmt.Sprintf("%d,cust%d,%d,r%d,S%d\n", i, i, i*10, i%3, i))
	}
	if err := os.WriteFile(csvPath, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	// --- Pipeline 1: CSV → S3 Parquet sink (one object) ---
	write := &pipeline.Definition{
		Name:   "csv-to-s3-parquet",
		Source: pipeline.SourceDef{Type: "csv", Path: csvPath, Options: map[string]interface{}{"hasHeader": true}},
		Sink: pipeline.SinkDef{Type: "s3-sink", Options: s3Conn(map[string]interface{}{
			"type": "s3-sink", "key": objKey, "format": "parquet",
		})},
	}
	write.Defaults()
	wOut := runInlinePipeline(t, write, "s3-parquet-write")
	if wOut.RowsWritten != 20 {
		t.Fatalf("s3 parquet write written=%d, want 20", wOut.RowsWritten)
	}

	// --- Pipeline 2: S3 Parquet source, projecting only id+amount → local JSONL ---
	outPath := filepath.Join(dir, "readback.jsonl")
	read := &pipeline.Definition{
		Name: "s3-parquet-read",
		Source: pipeline.SourceDef{Type: "s3", Options: s3Conn(map[string]interface{}{
			"type": "s3", "prefix": prefix, "format": "parquet",
			"columns": []interface{}{"id", "amount"},
		})},
		Sink: pipeline.SinkDef{Type: "jsonlines", Options: map[string]interface{}{"path": outPath}},
	}
	read.Defaults()
	rOut := runInlinePipeline(t, read, "s3-parquet-read")
	if rOut.RowsProcessed != 20 || rOut.RowsWritten != 20 {
		t.Fatalf("s3 parquet read processed=%d written=%d, want 20/20", rOut.RowsProcessed, rOut.RowsWritten)
	}

	// --- verify projection + values round-tripped over ranged reads ---
	seen := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(readFileString(t, outPath)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse readback line %q: %v", line, err)
		}
		if len(m) != 2 {
			t.Fatalf("row %v has %d fields, want 2 (id, amount) — column projection failed", m, len(m))
		}
		if _, ok := m["region"]; ok {
			t.Fatalf("unprojected column 'region' present in %v", m)
		}
		seen[fmt.Sprintf("%v", m["id"])] = fmt.Sprintf("%v", m["amount"])
	}
	if len(seen) != 20 {
		t.Fatalf("distinct ids = %d, want 20", len(seen))
	}
	for i := 1; i <= 20; i++ {
		id := strconv.Itoa(i)
		wantAmount := strconv.Itoa(i * 10)
		if seen[id] != wantAmount {
			t.Fatalf("id %s amount = %q, want %q", id, seen[id], wantAmount)
		}
	}
}
