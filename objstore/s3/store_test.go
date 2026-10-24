package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

// liveStore builds an s3Store against the S3-compatible endpoint in
// DP_TEST_S3_ENDPOINT (MinIO), skipping when it is unset.
func liveStore(t *testing.T) objstore.Store {
	t.Helper()
	endpoint := os.Getenv("DP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_S3_ENDPOINT (e.g. http://localhost:9000) to run the live S3 store test")
	}
	bucket := os.Getenv("DP_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dp-test-bucket"
	}
	ak := envOr("DP_TEST_S3_ACCESS_KEY", "minioadmin")
	sk := envOr("DP_TEST_S3_SECRET_KEY", "minioadmin")
	st, err := newS3Store(map[string]interface{}{
		"bucket":          bucket,
		"region":          "us-east-1",
		"endpoint":        endpoint,
		"forcePathStyle":  true,
		"accessKeyId":     ak,
		"secretAccessKey": sk,
	})
	if err != nil {
		t.Fatalf("newS3Store: %v", err)
	}
	return st
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func containsKey(objs []objstore.Object, key string) bool {
	for _, o := range objs {
		if o.Key == key {
			return true
		}
	}
	return false
}

func TestS3Store_RoundTrip(t *testing.T) {
	st := liveStore(t)
	defer st.Close()
	ctx := context.Background()
	prefix := fmt.Sprintf("dptest/unit/%d/", time.Now().UnixNano())
	key := prefix + "obj.txt"
	payload := []byte("hello world")

	w, err := st.PutWriter(ctx, key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("commit upload: %v", err)
	}
	t.Cleanup(func() { _ = st.Delete(ctx, key) })

	// GetReader streams the whole object back.
	rc, err := st.GetReader(ctx, key)
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("GetReader = %q, want %q", got, payload)
	}

	// List with a plain prefix and with a glob both find the object.
	objs, err := st.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List prefix: %v", err)
	}
	if !containsKey(objs, key) {
		t.Errorf("List(%q) missing %q: %+v", prefix, key, objs)
	}
	globbed, err := st.List(ctx, prefix+"*.txt")
	if err != nil {
		t.Fatalf("List glob: %v", err)
	}
	if !containsKey(globbed, key) {
		t.Errorf("List(%q) glob missing %q", prefix+"*.txt", key)
	}

	// GetRange (RangeReader) returns just the requested byte window.
	rr, ok := st.(objstore.RangeReader)
	if !ok {
		t.Fatal("s3 store does not implement objstore.RangeReader")
	}
	r2, err := rr.GetRange(ctx, key, 6, 5) // "world"
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	part, _ := io.ReadAll(r2)
	_ = r2.Close()
	if string(part) != "world" {
		t.Errorf("GetRange = %q, want world", part)
	}

	// Delete, then Delete again — the second is a no-op per the Store contract.
	if err := st.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Delete(ctx, key); err != nil {
		t.Errorf("Delete of missing key should be a no-op, got %v", err)
	}
}

func TestS3Store_GetReaderMissing(t *testing.T) {
	st := liveStore(t)
	defer st.Close()
	if _, err := st.GetReader(context.Background(), "dptest/definitely/missing.txt"); err == nil {
		t.Error("GetReader of a missing key expected an error")
	}
}

// TestNewS3StoreErrors covers the offline construction error paths (no network).
func TestNewS3StoreErrors(t *testing.T) {
	if _, err := newS3Store(map[string]interface{}{}); err == nil {
		t.Error("missing bucket expected an error")
	}
	// Only one of the pair set: a misconfiguration, reported.
	t.Setenv("HALF_SET_AK_ENV_VAR", "AKIA")
	if _, err := newS3Store(map[string]interface{}{
		"bucket":             "b",
		"accessKeyIdEnv":     "HALF_SET_AK_ENV_VAR",
		"secretAccessKeyEnv": "NO_SUCH_SK_ENV_VAR",
	}); err == nil {
		t.Error("a half-set credential pair expected an error")
	}
}

// Neither referenced variable set (IRSA / instance profile on AWS: the key
// secret is not mounted): fall back to the default chain instead of failing.
func TestNewS3StoreUnsetCredentialEnvUsesDefaultChain(t *testing.T) {
	if _, err := newS3Store(map[string]interface{}{
		"bucket":             "b",
		"region":             "eu-west-1",
		"accessKeyIdEnv":     "NO_SUCH_AK_ENV_VAR",
		"secretAccessKeyEnv": "NO_SUCH_SK_ENV_VAR",
	}); err != nil {
		t.Fatalf("unset credential env vars must fall back to the default chain: %v", err)
	}
}
