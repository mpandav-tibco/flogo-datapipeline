package objstore

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	_ "github.com/mpandav-tibco/flogo-datapipeline/source/parquet" // register the inner parquet source
	parquetgo "github.com/parquet-go/parquet-go"
)

// countingRangeStore is a fake Store that also implements objstore.RangeReader,
// backed by a fixed byte slice. It counts whole-object GetReader calls versus
// ranged GetRange calls so a test can prove the parquet path used range GETs and
// never downloaded the whole object.
type countingRangeStore struct {
	data       []byte
	getReaders int64
	getRanges  int64
}

func (c *countingRangeStore) List(context.Context, string) ([]objstore.Object, error) {
	return []objstore.Object{{Key: "obj.parquet", Size: int64(len(c.data))}}, nil
}

func (c *countingRangeStore) GetReader(context.Context, string) (io.ReadCloser, error) {
	atomic.AddInt64(&c.getReaders, 1)
	return io.NopCloser(bytes.NewReader(c.data)), nil
}

func (c *countingRangeStore) GetRange(_ context.Context, _ string, off, length int64) (io.ReadCloser, error) {
	atomic.AddInt64(&c.getRanges, 1)
	end := off + length
	if end > int64(len(c.data)) {
		end = int64(len(c.data))
	}
	return io.NopCloser(bytes.NewReader(c.data[off:end])), nil
}

func (c *countingRangeStore) PutWriter(context.Context, string) (io.WriteCloser, error) {
	return nil, nil
}
func (c *countingRangeStore) Delete(context.Context, string) error { return nil }
func (c *countingRangeStore) Close() error                         { return nil }

func writeParquetBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	group := parquetgo.Group{
		"id":   parquetgo.Optional(parquetgo.Int(64)),
		"name": parquetgo.Optional(parquetgo.String()),
	}
	w := parquetgo.NewGenericWriter[map[string]interface{}](&buf, parquetgo.NewSchema("t", group))
	rows := []map[string]interface{}{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
		{"id": int64(3), "name": "c"},
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	return buf.Bytes()
}

// TestObjstore_ParquetUsesRangeReads proves the range-GET pushdown: reading a
// parquet object whose store supports ranged reads must issue GetRange calls and
// must NOT call GetReader (which would download the whole object to a temp file).
func TestObjstore_ParquetUsesRangeReads(t *testing.T) {
	fake := &countingRangeStore{data: writeParquetBytes(t)}
	objstore.Register("fakeranged", func(map[string]interface{}) (objstore.Store, error) {
		return fake, nil
	})

	src, err := newObjstoreSource("fakeranged", map[string]interface{}{
		"prefix": "*.parquet",
		"format": "parquet",
	})
	if err != nil {
		t.Fatalf("newObjstoreSource: %v", err)
	}
	defer src.Close()

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
	}
	if total != 3 {
		t.Fatalf("read %d rows, want 3", total)
	}
	if got := atomic.LoadInt64(&fake.getReaders); got != 0 {
		t.Fatalf("GetReader (whole-object download) called %d times; want 0 — parquet must read via range GETs", got)
	}
	if got := atomic.LoadInt64(&fake.getRanges); got == 0 {
		t.Fatal("GetRange was never called — the range-pushdown path was not taken")
	}
}
