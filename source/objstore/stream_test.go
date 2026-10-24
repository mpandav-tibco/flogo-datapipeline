package objstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/source"

	_ "github.com/mpandav-tibco/flogo-datapipeline/source/csv"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/jsonlines"
)

// memStore is an in-memory objstore.Store that tracks open object streams.
type memStore struct {
	objs        map[string]string
	opened      int64
	closed      int64
	cancelledOK int64 // GetReader calls whose ctx was already cancelled
}

type trackedReader struct {
	io.Reader
	s *memStore
}

func (r *trackedReader) Close() error { atomic.AddInt64(&r.s.closed, 1); return nil }

func (m *memStore) List(context.Context, string) ([]objstore.Object, error) {
	var out []objstore.Object
	for k, v := range m.objs {
		out = append(out, objstore.Object{Key: k, Size: int64(len(v))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
func (m *memStore) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	if ctx.Err() != nil {
		atomic.AddInt64(&m.cancelledOK, 1)
		return nil, ctx.Err()
	}
	atomic.AddInt64(&m.opened, 1)
	return &trackedReader{Reader: bytes.NewReader([]byte(m.objs[key])), s: m}, nil
}
func (m *memStore) PutWriter(context.Context, string) (io.WriteCloser, error) { return nil, nil }
func (m *memStore) Delete(context.Context, string) error                      { return nil }
func (m *memStore) Close() error                                              { return nil }

var activeMem *memStore

func init() {
	objstore.Register("memtest", func(map[string]interface{}) (objstore.Store, error) { return activeMem, nil })
	source.Register("memtest", func(cfg map[string]interface{}) (source.Source, error) {
		return newObjstoreSource("memtest", cfg)
	})
}

func tempFiles(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

func TestObjstore_StreamsWithoutTempFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	activeMem = &memStore{objs: map[string]string{
		"a.csv": "id,name\n1,a\n2,b\n", "b.csv": "id,name\n3,c\n", "c.csv": "id,name\n4,d\n5,e\n",
	}}
	src, err := newObjstoreSource("memtest", map[string]interface{}{"prefix": "x", "format": "csv"})
	if err != nil {
		t.Fatal(err)
	}
	if got := drainAll(t, src); got != 5 {
		t.Fatalf("rows=%d want 5", got)
	}
	if n := tempFiles(t, dir); n != 0 {
		t.Fatalf("streaming created %d temp files", n)
	}
	_ = src.Close()
	if o, c := atomic.LoadInt64(&activeMem.opened), atomic.LoadInt64(&activeMem.closed); o != 3 || c != 3 {
		t.Fatalf("object streams opened=%d closed=%d; want 3/3", o, c)
	}
}

func TestObjstore_SpoolDeletesEachTempFileWhenConsumed(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	activeMem = &memStore{objs: map[string]string{
		"a.jsonl": "{\"id\":1}\n{\"id\":2}\n", "b.jsonl": "{\"id\":3}\n", "c.jsonl": "{\"id\":4}\n",
	}}
	src, err := newObjstoreSource("memtest", map[string]interface{}{"prefix": "x", "format": "jsonlines", "download": true})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for {
		b, err := src.ReadBatch(1)
		if b != nil {
			total += b.Len()
		}
		if n := tempFiles(t, dir); n > 1 {
			t.Fatalf("%d spooled objects on disk at once; want at most 1", n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 4 {
		t.Fatalf("rows=%d want 4", total)
	}
	_ = src.Close()
	if n := tempFiles(t, dir); n != 0 {
		t.Fatalf("%d temp files left after Close", n)
	}
}

func TestObjstore_PartitionStreamClosedWithChild(t *testing.T) {
	activeMem = &memStore{objs: map[string]string{"a.csv": "id\n1\n", "b.csv": "id\n2\n"}}
	src, err := newObjstoreSource("memtest", map[string]interface{}{"prefix": "x", "format": "csv"})
	if err != nil {
		t.Fatal(err)
	}
	ps := src.(source.PartitionableSource)
	parts, _ := ps.Partitions(2)
	total := 0
	for _, p := range parts {
		child, err := ps.OpenPartition(p)
		if err != nil {
			t.Fatal(err)
		}
		total += drainAll(t, child)
		_ = child.Close()
	}
	_ = src.Close()
	if total != 2 {
		t.Fatalf("rows=%d want 2", total)
	}
	if o, c := atomic.LoadInt64(&activeMem.opened), atomic.LoadInt64(&activeMem.closed); o != c {
		t.Fatalf("leaked object streams: opened=%d closed=%d", o, c)
	}
}
