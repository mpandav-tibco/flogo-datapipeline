package objstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	_ "github.com/mpandav-tibco/flogo-datapipeline/sink/parquet"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet"
)

func seedObj(t *testing.T, store objstore.Store, key, content string) {
	t.Helper()
	w, err := store.PutWriter(context.Background(), key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := io.WriteString(w, content); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func drainAll(t *testing.T, src source.Source) int {
	t.Helper()
	total := 0
	for {
		b, err := src.ReadBatch(10)
		if b != nil {
			total += b.Len()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
		if b == nil {
			break
		}
	}
	return total
}

func TestObjstoreSource_Partitions(t *testing.T) {
	prefix := fmt.Sprintf("dptest/part/%d/", time.Now().UnixNano())
	cfg := s3Conn(t, map[string]interface{}{"prefix": prefix, "format": "jsonlines"})
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	seedObj(t, store, prefix+"a.jsonl", "{\"id\":1}\n{\"id\":2}\n")
	seedObj(t, store, prefix+"b.jsonl", "{\"id\":3}\n")
	t.Cleanup(func() {
		_ = store.Delete(context.Background(), prefix+"a.jsonl")
		_ = store.Delete(context.Background(), prefix+"b.jsonl")
	})

	src, err := newObjstoreSource("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSource: %v", err)
	}
	defer src.Close()

	_ = src.Schema()
	if err := src.Skip(0); err != nil {
		t.Errorf("Skip: %v", err)
	}

	ps, ok := src.(source.PartitionableSource)
	if !ok {
		t.Fatal("objstore source does not implement PartitionableSource")
	}
	parts, err := ps.Partitions(2)
	if err != nil {
		t.Fatalf("Partitions: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("partitions = %d, want 2 (one per object)", len(parts))
	}

	total := 0
	for _, p := range parts {
		child, err := ps.OpenPartition(p)
		if err != nil {
			t.Fatalf("OpenPartition(%s): %v", p.ID, err)
		}
		_ = child.Schema()
		_ = child.Skip(0)
		total += drainAll(t, child)
		_ = child.Close()
	}
	if total != 3 {
		t.Errorf("partitioned read total = %d, want 3", total)
	}
}

func TestObjstoreSource_DownloadSpool(t *testing.T) {
	prefix := fmt.Sprintf("dptest/dl/%d/", time.Now().UnixNano())
	// download:true forces the whole object to be spooled to a temp file
	// (jsonlines is not range-reader capable).
	cfg := s3Conn(t, map[string]interface{}{"prefix": prefix, "format": "jsonlines", "download": true})
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	seedObj(t, store, prefix+"a.jsonl", "{\"id\":1}\n{\"id\":2}\n")
	t.Cleanup(func() { _ = store.Delete(context.Background(), prefix+"a.jsonl") })

	src, err := newObjstoreSource("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSource (download): %v", err)
	}
	defer src.Close()
	if got := drainAll(t, src); got != 2 {
		t.Errorf("download-spool read = %d, want 2", got)
	}
}

func TestObjstoreSource_Parquet(t *testing.T) {
	// Write a local parquet file, upload it, then read it back through the
	// objstore parquet source (range-GET reader-at path).
	pqPath := filepath.Join(t.TempDir(), "data.parquet")
	psink, err := sink.New("parquet", map[string]interface{}{"path": pqPath})
	if err != nil {
		t.Fatalf("parquet sink: %v", err)
	}
	if err := psink.WriteBatch(&record.RecordBatch{
		Columns: []record.Column{{Name: "id", Type: record.TypeInt}, {Name: "name", Type: record.TypeString}},
		Rows:    [][]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
	}); err != nil {
		t.Fatalf("parquet WriteBatch: %v", err)
	}
	if err := psink.Close(); err != nil {
		t.Fatalf("parquet Close: %v", err)
	}
	data, err := os.ReadFile(pqPath)
	if err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("dptest/pq/%d/", time.Now().UnixNano())
	key := prefix + "data.parquet"
	cfg := s3Conn(t, map[string]interface{}{"prefix": prefix, "format": "parquet"})
	store, err := objstore.New("s3", cfg)
	if err != nil {
		t.Fatalf("objstore.New: %v", err)
	}
	defer store.Close()
	w, err := store.PutWriter(context.Background(), key)
	if err != nil {
		t.Fatalf("PutWriter: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("upload parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("commit parquet: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), key) })

	src, err := newObjstoreSource("s3", cfg)
	if err != nil {
		t.Fatalf("newObjstoreSource (parquet): %v", err)
	}
	defer src.Close()
	if got := drainAll(t, src); got != 3 {
		t.Errorf("parquet read total = %d, want 3", got)
	}
}
