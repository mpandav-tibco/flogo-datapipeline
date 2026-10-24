package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

func putS3(t *testing.T, st objstore.Store, key string, data []byte) {
	t.Helper()
	w, err := st.PutWriter(context.Background(), key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("commit upload: %v", err)
	}
}

// GetRangeIfMatch serves the range for the listed version and refuses with
// ErrObjectChanged once the object was overwritten.
func TestS3Store_GetRangeIfMatch(t *testing.T) {
	st := liveStore(t)
	defer st.Close()
	ctx := context.Background()
	key := fmt.Sprintf("dptest/unit/%d/versioned.txt", time.Now().UnixNano())
	putS3(t, st, key, []byte("0123456789"))
	t.Cleanup(func() { _ = st.Delete(ctx, key) })

	objs, err := st.List(ctx, key)
	if err != nil || len(objs) != 1 || objs[0].ETag == "" {
		t.Fatalf("List: %+v err=%v (need one object with an ETag)", objs, err)
	}
	vr, ok := st.(objstore.VersionedRangeReader)
	if !ok {
		t.Fatal("store does not implement VersionedRangeReader")
	}
	rc, err := vr.GetRangeIfMatch(ctx, key, objs[0].ETag, 3, 4)
	if err != nil {
		t.Fatalf("GetRangeIfMatch: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, []byte("3456")) {
		t.Fatalf("range = %q, want \"3456\"", got)
	}

	putS3(t, st, key, []byte("abcdefghij"))
	if rc, err := vr.GetRangeIfMatch(ctx, key, objs[0].ETag, 3, 4); !errors.Is(err, objstore.ErrObjectChanged) {
		if rc != nil {
			_ = rc.Close()
		}
		t.Fatalf("after overwrite: err=%v, want ErrObjectChanged", err)
	}
}
