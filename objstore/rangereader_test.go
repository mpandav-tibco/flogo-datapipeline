package objstore

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
)

// memRangeReader is an in-memory RangeReader over a fixed byte slice, recording
// how many ranged GETs were issued.
type memRangeReader struct {
	data  []byte
	mu    sync.Mutex
	calls int
}

func (m *memRangeReader) GetRange(_ context.Context, _ string, off, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	end := off + length
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	return io.NopCloser(bytes.NewReader(m.data[off:end])), nil
}

func TestRangeReaderAt_ReadAt(t *testing.T) {
	data := []byte("0123456789ABCDEF") // 16 bytes
	m := &memRangeReader{data: data}
	ra := NewRangeReaderAt(context.Background(), m, "k", int64(len(data)))

	// Full mid-object read.
	p := make([]byte, 4)
	if n, err := ra.ReadAt(p, 2); n != 4 || err != nil || string(p) != "2345" {
		t.Fatalf("mid read = %d,%v,%q; want 4,nil,2345", n, err, p)
	}

	// Read that ends exactly at the object boundary.
	p = make([]byte, 6)
	if n, err := ra.ReadAt(p, 10); n != 6 || err != nil || string(p) != "ABCDEF" {
		t.Fatalf("end-exact read = %d,%v,%q; want 6,nil,ABCDEF", n, err, p)
	}

	// Short read spanning past the end: n<len(p) must come with io.EOF.
	p = make([]byte, 10)
	if n, err := ra.ReadAt(p, 10); n != 6 || err != io.EOF || string(p[:n]) != "ABCDEF" {
		t.Fatalf("short read = %d,%v,%q; want 6,EOF,ABCDEF", n, err, string(p[:n]))
	}

	// Offset at/after size returns io.EOF with zero bytes.
	if n, err := ra.ReadAt(make([]byte, 4), int64(len(data))); n != 0 || err != io.EOF {
		t.Fatalf("past-end read = %d,%v; want 0,EOF", n, err)
	}

	// Zero-length read is a no-op and issues no GET.
	before := m.calls
	if n, err := ra.ReadAt(nil, 3); n != 0 || err != nil || m.calls != before {
		t.Fatalf("zero-len read = %d,%v (calls delta %d); want 0,nil,0", n, err, m.calls-before)
	}
}

// TestRangeReaderAt_Concurrent asserts the adapter is safe for the concurrent
// ReadAt calls parquet-go issues when decoding row groups in parallel.
func TestRangeReaderAt_Concurrent(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	ra := NewRangeReaderAt(context.Background(), &memRangeReader{data: data}, "k", int64(len(data)))

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			p := make([]byte, 128)
			n, err := ra.ReadAt(p, off)
			if err != nil && err != io.EOF {
				t.Errorf("ReadAt(%d): %v", off, err)
				return
			}
			for i := 0; i < n; i++ {
				if p[i] != byte(int(off)+i) {
					t.Errorf("ReadAt(%d) byte %d = %d, want %d", off, i, p[i], byte(int(off)+i))
					return
				}
			}
		}(int64(g * 256))
	}
	wg.Wait()
}
