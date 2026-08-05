package objstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

func listUnder(t *testing.T, cfg map[string]interface{}, prefix string) []objstore.Object {
	t.Helper()
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	objs, err := store.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return objs
}

func cleanupPrefix(t *testing.T, cfg map[string]interface{}, prefix string) {
	t.Helper()
	store, err := objstore.New("s3", cfg)
	if err != nil {
		return
	}
	defer store.Close()
	objs, _ := store.List(context.Background(), prefix)
	for _, o := range objs {
		_ = store.Delete(context.Background(), o.Key)
	}
}

// TestObjstoreSink_Rolling exercises the rolling sink: a small rollMaxBytes
// forces the writer to roll to a fresh -NNNN object (plus manifest sidecar) on
// every batch, covering openNext/shouldRoll/roll/Flush/Close.
func TestObjstoreSink_Rolling(t *testing.T) {
	prefix := fmt.Sprintf("dptest/roll/%d/", time.Now().UnixNano())
	cfg := s3Conn(t, map[string]interface{}{
		"type": "s3-sink", "key": prefix + "part.jsonl", "format": "jsonlines",
		"rollMaxBytes": 10,
	})
	t.Cleanup(func() { cleanupPrefix(t, cfg, prefix) })

	sk, err := newObjstoreSink("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSink: %v", err)
	}
	cols := []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}}
	for i := 0; i < 4; i++ {
		b := &record.RecordBatch{Columns: cols, Rows: [][]interface{}{{int64(i), "somename"}}}
		if err := sk.WriteBatch(b); err != nil {
			t.Fatalf("WriteBatch %d: %v", i, err)
		}
	}
	if err := sk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := sk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if objs := listUnder(t, cfg, prefix); len(objs) < 2 {
		t.Errorf("rolling produced %d objects, want >= 2 rolled parts", len(objs))
	}
}

// TestObjstoreSink_Partitioned exercises the partitioned sink: a {region}
// placeholder fans the batch out to one object per distinct region.
func TestObjstoreSink_Partitioned(t *testing.T) {
	prefix := fmt.Sprintf("dptest/pk/%d/", time.Now().UnixNano())
	cfg := s3Conn(t, map[string]interface{}{
		"type": "s3-sink", "key": prefix + "{region}/data.jsonl", "format": "jsonlines",
	})
	t.Cleanup(func() { cleanupPrefix(t, cfg, prefix) })

	sk, err := newObjstoreSink("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSink: %v", err)
	}
	cols := []record.Column{{Name: "region", Type: record.TypeString}, {Name: "id", Type: record.TypeInt}}
	b := &record.RecordBatch{Columns: cols, Rows: [][]interface{}{
		{"EU", int64(1)}, {"US", int64(2)}, {"EU", int64(3)},
	}}
	if err := sk.WriteBatch(b); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := sk.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := sk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if objs := listUnder(t, cfg, prefix); len(objs) < 2 {
		t.Errorf("partitioned produced %d objects, want >= 2 (EU, US)", len(objs))
	}
}

// TestObjstoreSink_PartitionedMissingFormat covers the partitioned-sink
// format-required error (no infra needed — fails before opening any store).
func TestObjstoreSink_PartitionedMissingFormat(t *testing.T) {
	if _, err := newObjstoreSink("s3", map[string]interface{}{"key": "{region}/x.jsonl"}); err == nil {
		t.Error("partitioned sink without 'format' expected an error")
	}
}
