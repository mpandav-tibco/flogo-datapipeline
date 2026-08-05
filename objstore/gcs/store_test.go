package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

const gcsTestBucket = "dp-test-bucket"

// liveGCSStore builds a gcsStore against the fake-gcs-server emulator in
// DP_TEST_GCS_ENDPOINT, skipping when it is unset. It creates the test bucket
// first (the store itself does not create buckets).
func liveGCSStore(t *testing.T) objstore.Store {
	t.Helper()
	endpoint := os.Getenv("DP_TEST_GCS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DP_TEST_GCS_ENDPOINT (e.g. http://127.0.0.1:4443/storage/v1/) to run the live GCS store test")
	}
	ctx := context.Background()
	raw, err := storage.NewClient(ctx, option.WithEndpoint(endpoint), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("raw gcs client: %v", err)
	}
	_ = raw.Bucket(gcsTestBucket).Create(ctx, "test-project", nil) // ignore already-exists
	_ = raw.Close()

	st, err := newGCSStore(map[string]interface{}{"bucket": gcsTestBucket, "endpoint": endpoint})
	if err != nil {
		t.Fatalf("newGCSStore: %v", err)
	}
	return st
}

func containsKey(objs []objstore.Object, key string) bool {
	for _, o := range objs {
		if o.Key == key {
			return true
		}
	}
	return false
}

func TestGCSStore_RoundTrip(t *testing.T) {
	st := liveGCSStore(t)
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

	rc, err := st.GetReader(ctx, key)
	if err != nil {
		t.Fatalf("GetReader: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("GetReader = %q, want %q", got, payload)
	}

	objs, err := st.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List prefix: %v", err)
	}
	if !containsKey(objs, key) {
		t.Errorf("List(%q) missing %q", prefix, key)
	}
	globbed, err := st.List(ctx, prefix+"*.txt")
	if err != nil {
		t.Fatalf("List glob: %v", err)
	}
	if !containsKey(globbed, key) {
		t.Errorf("List glob missing %q", key)
	}

	if rr, ok := st.(objstore.RangeReader); ok {
		r2, err := rr.GetRange(ctx, key, 6, 5) // "world"
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		part, _ := io.ReadAll(r2)
		_ = r2.Close()
		if string(part) != "world" {
			t.Errorf("GetRange = %q, want world", part)
		}
	}

	if err := st.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := st.Delete(ctx, key); err != nil {
		t.Errorf("Delete of missing key should be a no-op, got %v", err)
	}
}

func TestGCSStore_GetReaderMissing(t *testing.T) {
	st := liveGCSStore(t)
	defer st.Close()
	if _, err := st.GetReader(context.Background(), "dptest/definitely/missing.txt"); err == nil {
		t.Error("GetReader of a missing key expected an error")
	}
}

// TestNewGCSStoreErrors covers the offline construction error paths.
func TestNewGCSStoreErrors(t *testing.T) {
	if _, err := newGCSStore(map[string]interface{}{}); err == nil {
		t.Error("missing bucket expected an error")
	}
	if _, err := newGCSStore(map[string]interface{}{"bucket": "b", "credentialsFileEnv": "NO_SUCH_CRED_ENV_VAR"}); err == nil {
		t.Error("empty credentialsFileEnv-referenced credential expected an error")
	}
}
