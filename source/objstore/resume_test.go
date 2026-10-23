package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// flakyRange serves data but cuts every stream after `cut` bytes, alternating
// between a transport error and a premature clean EOF.
type flakyRange struct {
	data   []byte
	cut    int
	ranges int
	broken bool // every GetRange fails
}

type cutReader struct {
	r         io.Reader
	left      int
	cleanEOF  bool
	delivered bool
}

func (c *cutReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		if c.cleanEOF {
			return 0, io.EOF
		}
		return 0, errors.New("connection reset by peer")
	}
	if len(p) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= n
	return n, err
}
func (c *cutReader) Close() error { return nil }

func (f *flakyRange) open(off int64) io.ReadCloser {
	return &cutReader{r: bytes.NewReader(f.data[off:]), left: f.cut, cleanEOF: f.ranges%2 == 0}
}

func (f *flakyRange) GetRange(_ context.Context, _ string, off, length int64) (io.ReadCloser, error) {
	f.ranges++
	if f.broken {
		return nil, errors.New("service unavailable")
	}
	return f.open(off), nil
}

func fastResume(r *resumableReader) *resumableReader {
	r.baseDelay, r.maxDelay = time.Millisecond, 2*time.Millisecond
	return r
}

func TestResumableReader_ResumesAfterDrops(t *testing.T) {
	data := []byte(strings.Repeat("0123456789abcdef", 10000)) // 160 KB
	f := &flakyRange{data: data, cut: 7000}
	r := fastResume(newResumableReader(context.Background(), f, "k", int64(len(data)), f.open(0)))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content differs after resumption (%d vs %d bytes)", len(got), len(data))
	}
	if f.ranges < 20 {
		t.Fatalf("expected many resumptions, got %d", f.ranges)
	}
}

func TestResumableReader_GivesUp(t *testing.T) {
	data := []byte(strings.Repeat("x", 1000))
	f := &flakyRange{data: data, cut: 100, broken: true}
	r := fastResume(newResumableReader(context.Background(), f, "k", int64(len(data)), f.open(0)))
	_, err := io.ReadAll(r)
	if err == nil || !strings.Contains(err.Error(), "gave up") {
		t.Fatalf("want give-up error, got %v", err)
	}
}

func TestResumableReader_CancelStopsWaiting(t *testing.T) {
	data := []byte(strings.Repeat("x", 1000))
	f := &flakyRange{data: data, cut: 100, broken: true}
	ctx, cancel := context.WithCancel(context.Background())
	r := newResumableReader(ctx, f, "k", int64(len(data)), f.open(0))
	r.baseDelay, r.maxDelay = time.Hour, time.Hour
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	if _, err := io.ReadAll(r); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("cancellation did not interrupt the backoff")
	}
}
