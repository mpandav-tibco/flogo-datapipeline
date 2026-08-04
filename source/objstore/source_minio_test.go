package objstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"      // registers the s3 provider
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines" // inner format
)

func s3Conn(t *testing.T, extra map[string]interface{}) map[string]interface{} {
	t.Helper()
	endpoint := os.Getenv("DP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000) to run the live objstore source test")
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

func TestObjstoreSource_RoundTripJSONLines(t *testing.T) {
	prefix := fmt.Sprintf("dptest/src/%d/", time.Now().UnixNano())
	objKey := prefix + "in.jsonl"
	cfg := s3Conn(t, map[string]interface{}{"prefix": prefix, "format": "jsonlines"})

	// Seed one object via the raw store.
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	w, err := store.PutWriter(context.Background(), objKey)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := io.WriteString(w, "{\"id\":1,\"customer\":\"Alice\"}\n{\"id\":2,\"customer\":\"Bob\"}\n"); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), objKey); _ = store.Close() })

	src, err := newObjstoreSource("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSource: %v", err)
	}
	defer src.Close()

	batch, err := src.ReadBatch(10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if batch == nil || batch.Len() != 2 {
		t.Fatalf("read %v rows, want 2", batch)
	}
	found := false
	for i := 0; i < batch.Len(); i++ {
		if fmt.Sprintf("%v", batch.Value(i, "customer")) == "Alice" {
			found = true
		}
	}
	if !found {
		t.Error("expected to read the seeded 'Alice' row")
	}
}

func TestObjstoreSource_Errors(t *testing.T) {
	if _, err := newObjstoreSource("s3", map[string]interface{}{}); err == nil {
		t.Error("missing 'prefix' expected an error")
	}
	if _, err := newObjstoreSource("s3", map[string]interface{}{"prefix": "p/"}); err == nil {
		t.Error("missing 'format' expected an error")
	}
}
