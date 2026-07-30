package integration

// Enterprise scenario: an object-store round-trip through the real orchestration
// — Pipeline 1 writes a JSONL object to an S3-compatible bucket (objstore sink),
// Pipeline 2 reads it back by prefix (objstore source) to a local file. Proves
// the s3 sink and source interoperate against a live S3 API (MinIO).
//
// Shippable collateral for the "land curated data in a data lake" pattern.
// Requires an S3 endpoint: set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000)
// and DP_TEST_S3_BUCKET (default dp-test-bucket); creds default to minioadmin.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"

	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3" // registers the s3 provider
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines" // s3 source parses jsonlines objects via this
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/transform/mapper"
)

func TestEnterprise_S3_RoundTrip(t *testing.T) {
	endpoint := os.Getenv("DP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000) to run the enterprise S3 scenario")
	}
	bucket := os.Getenv("DP_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dp-test-bucket"
	}
	ak := envOr("DP_TEST_S3_ACCESS_KEY", "minioadmin")
	sk := envOr("DP_TEST_S3_SECRET_KEY", "minioadmin")

	// Unique key prefix per run so re-runs never collide.
	prefix := fmt.Sprintf("dptest/%d/", time.Now().UnixNano())
	objKey := prefix + "orders.jsonl"

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

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(csvPath, []byte("id,customer,amount\n1,Alice,100\n2,Bob,200\n3,Carol,300\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	// --- Pipeline 1: CSV → mapper → S3 sink (write one JSONL object) ---
	write := &pipeline.Definition{
		Name:   "csv-to-s3",
		Source: pipeline.SourceDef{Type: "csv", Path: csvPath, Options: map[string]interface{}{"hasHeader": true}},
		Transforms: []pipeline.TransformDef{
			{Type: "mapper", Extra: map[string]interface{}{
				"type": "mapper",
				"fields": map[string]interface{}{
					"orderId":  "=$id",
					"customer": "=$customer",
				},
			}},
		},
		Sink: pipeline.SinkDef{Type: "s3-sink", Options: s3Conn(map[string]interface{}{
			"type": "s3-sink", "key": objKey, "format": "jsonlines",
		})},
	}
	write.Defaults()
	wOut := runInlinePipeline(t, write, "s3-write")
	if wOut.RowsWritten != 3 {
		t.Fatalf("s3 write written=%d, want 3", wOut.RowsWritten)
	}

	// --- Pipeline 2: S3 source (read the prefix back) → local JSONL ---
	outPath := filepath.Join(dir, "readback.jsonl")
	read := &pipeline.Definition{
		Name: "s3-to-jsonl",
		Source: pipeline.SourceDef{Type: "s3", Options: s3Conn(map[string]interface{}{
			"type": "s3", "prefix": prefix, "format": "jsonlines",
		})},
		Sink: pipeline.SinkDef{Type: "jsonlines", Options: map[string]interface{}{"path": outPath}},
	}
	read.Defaults()
	rOut := runInlinePipeline(t, read, "s3-read")
	if rOut.RowsProcessed != 3 || rOut.RowsWritten != 3 {
		t.Fatalf("s3 read processed=%d written=%d, want 3/3", rOut.RowsProcessed, rOut.RowsWritten)
	}

	// --- verify the objects round-tripped ---
	seen := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(readFileString(t, outPath)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse readback line %q: %v", line, err)
		}
		seen[fmt.Sprintf("%v", m["orderId"])] = fmt.Sprintf("%v", m["customer"])
	}
	if len(seen) != 3 || seen["1"] != "Alice" || seen["3"] != "Carol" {
		t.Fatalf("round-tripped objects = %v, want {1:Alice,2:Bob,3:Carol}", seen)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
