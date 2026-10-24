package objstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
)

// flakyStore is a file-backed store whose uploads fail while down is set.
type flakyStore struct {
	fileStore
	down *atomic.Bool
}

func (s *flakyStore) PutWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	if s.down.Load() {
		return nil, errors.New("store unavailable")
	}
	return s.fileStore.PutWriter(ctx, key)
}

var storeDown atomic.Bool

func init() {
	objstore.Register("testflaky", func(cfg map[string]interface{}) (objstore.Store, error) {
		root, _ := cfg["root"].(string)
		return &flakyStore{fileStore: fileStore{root: root}, down: &storeDown}, nil
	})
}

func rowsUnder(t *testing.T, root string) (rows int, objects int) {
	t.Helper()
	_ = filepath.Walk(root, func(p string, info os.FileInfo, _ error) error {
		if info == nil || info.IsDir() || strings.HasSuffix(p, ".manifest.json") {
			return nil
		}
		objects++
		f, _ := os.Open(p)
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			rows++
		}
		return nil
	})
	return rows, objects
}

func oneRow(i int) *record.RecordBatch {
	return &record.RecordBatch{Columns: []record.Column{{Name: "msg", Type: record.TypeString}},
		Rows: [][]interface{}{{fmt.Sprintf("row %d", i)}}}
}

// Flush is what a checkpoint relies on: after it returns nil, every row
// written so far must be in the object store, not only in a local temp file.
func TestRollingFlushUploads(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key": "logs/part.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.WriteBatch(oneRow(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if rows, _ := rowsUnder(t, root); rows != 3 {
		t.Fatalf("after Flush the store holds %d rows, want 3", rows)
	}
	_ = s.WriteBatch(oneRow(3))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, objs := rowsUnder(t, root); rows != 4 || objs != 2 {
		t.Fatalf("after Close: %d rows in %d objects, want 4 rows in 2 objects", rows, objs)
	}
}

// An upload that fails during a size roll must not lose the rolled rows nor
// fail the write (they are not lost); Flush fails until the store is back,
// then uploads them.
func TestRollingUploadFailureRetried(t *testing.T) {
	root := t.TempDir()
	storeDown.Store(true)
	defer storeDown.Store(false)
	s, err := newObjstoreSink("testflaky", map[string]interface{}{
		"key": "logs/part.ndjson", "format": "jsonlines", "root": root, "rollMaxBytes": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.WriteBatch(oneRow(i)); err != nil {
			t.Fatalf("WriteBatch %d returned %v; a failed upload must not reject rows that are kept", i, err)
		}
	}
	if err := s.Flush(); err == nil {
		t.Fatal("Flush must fail while uploads are failing (the checkpoint must not advance)")
	}
	storeDown.Store(false)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush after recovery: %v", err)
	}
	if rows, _ := rowsUnder(t, root); rows != 3 {
		t.Fatalf("store holds %d rows after recovery, want 3", rows)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Per-key children of a partitioned sink are released once flushed, so the
// map does not grow with every pod ever seen.
func TestPartitionedChildrenReleasedAfterFlush(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key": "{pod}/part.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
	})
	if err != nil {
		t.Fatal(err)
	}
	p := s.(*partitionedObjstoreSink)
	for i := 0; i < 5; i++ {
		b := &record.RecordBatch{Columns: []record.Column{{Name: "pod", Type: record.TypeString}},
			Rows: [][]interface{}{{fmt.Sprintf("pod-%d", i)}}}
		if err := s.WriteBatch(b); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.children) != 5 {
		t.Fatalf("children=%d before flush, want 5", len(p.children))
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(p.children) != 0 {
		t.Fatalf("children=%d after flush, want 0", len(p.children))
	}
	if rows, objs := rowsUnder(t, root); rows != 5 || objs != 5 {
		t.Fatalf("%d rows in %d objects, want 5/5", rows, objs)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Without {uuid} in the key, objects of successive flushes (each recreating
// the partition's child, whose seq restarts at 0) must not overwrite each other.
func TestPartitionedFlushesDoNotOverwrite(t *testing.T) {
	root := t.TempDir()
	s, err := newObjstoreSink("testfile", map[string]interface{}{
		"key": "{pod}/part.ndjson", "format": "jsonlines", "root": root, "rollMaxSeconds": 3600,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		b := &record.RecordBatch{Columns: []record.Column{{Name: "pod", Type: record.TypeString}}, Rows: [][]interface{}{{"pod-a"}}}
		if err := s.WriteBatch(b); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	if rows, objs := rowsUnder(t, root); rows != 3 || objs != 3 {
		t.Fatalf("%d rows in %d objects, want 3 rows in 3 distinct objects", rows, objs)
	}
}
