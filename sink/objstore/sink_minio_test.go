package objstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3" // registers the s3 provider
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/jsonlines" // inner format
)

func s3Conn(t *testing.T, extra map[string]interface{}) map[string]interface{} {
	t.Helper()
	endpoint := os.Getenv("DP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000) to run the live objstore sink test")
	}
	bucket := os.Getenv("DP_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dp-test-bucket"
	}
	m := map[string]interface{}{
		"bucket":          bucket,
		"region":          "us-east-1",
		"endpoint":        endpoint,
		"forcePathStyle":  true,
		"accessKeyId":     "minioadmin",
		"secretAccessKey": "minioadmin",
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestObjstoreSink_RoundTripJSONLines(t *testing.T) {
	prefix := fmt.Sprintf("dptest/sink/%d/", time.Now().UnixNano())
	objKey := prefix + "out.jsonl"
	cfg := s3Conn(t, map[string]interface{}{"type": "s3-sink", "key": objKey, "format": "jsonlines"})

	sk, err := newObjstoreSink("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSink: %v", err)
	}
	batch := &record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "customer", Type: record.TypeString}},
		Rows:    [][]interface{}{{int64(1), "Alice"}, {int64(2), "Bob"}},
	}
	if err := sk.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := sk.Close(); err != nil {
		t.Fatalf("Close (upload): %v", err)
	}

	// Verify the uploaded object contains the rows.
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	t.Cleanup(func() { _ = store.Delete(context.Background(), objKey) })

	rc, err := store.GetReader(context.Background(), objKey)
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !strings.Contains(string(data), "Alice") || !strings.Contains(string(data), "Bob") {
		t.Errorf("uploaded object missing rows: %s", data)
	}
}

func TestObjstoreSink_MissingKey(t *testing.T) {
	if _, err := newObjstoreSink("s3", map[string]interface{}{}); err == nil {
		t.Error("missing 'key' expected an error")
	}
}
