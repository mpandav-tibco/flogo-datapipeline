package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

// flakyRange serves data but cuts every stream after `cut` bytes, alternating
// between a connection reset and a body cut short of its Content-Length.
type flakyRange struct {
	data    []byte
	cut     int
	ranges  int
	broken  bool   // every GetRange fails
	etag    string // current object version
	changed bool   // the object was overwritten after listing
}

type cutReader struct {
	r     io.Reader
	left  int
	reset bool
}

func (c *cutReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		if c.reset {
			return 0, errors.New("connection reset by peer")
		}
		return 0, io.ErrUnexpectedEOF
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
	return &cutReader{r: bytes.NewReader(f.data[off:]), left: f.cut, reset: f.ranges%2 == 0}
}

func (f *flakyRange) GetRangeIfMatch(_ context.Context, _, etag string, off, length int64) (io.ReadCloser, error) {
	f.ranges++
	if f.broken {
		return nil, errors.New("service unavailable")
	}
	if f.changed || etag != f.etag {
		return nil, objstore.ErrObjectChanged
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
	r := fastResume(newResumableReader(context.Background(), f, "k", "", int64(len(data)), f.open(0)))
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
	r := fastResume(newResumableReader(context.Background(), f, "k", "", int64(len(data)), f.open(0)))
	_, err := io.ReadAll(r)
	if err == nil || !strings.Contains(err.Error(), "gave up") {
		t.Fatalf("want give-up error, got %v", err)
	}
}

func TestResumableReader_CancelStopsWaiting(t *testing.T) {
	data := []byte(strings.Repeat("x", 1000))
	f := &flakyRange{data: data, cut: 100, broken: true}
	ctx, cancel := context.WithCancel(context.Background())
	r := newResumableReader(ctx, f, "k", "", int64(len(data)), f.open(0))
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

// An object overwritten after listing must fail the read, not splice the
// tail of the new version onto the head of the old one.
func TestResumableReader_ObjectChangedFails(t *testing.T) {
	data := []byte(strings.Repeat("x", 1000))
	f := &flakyRange{data: data, cut: 100, changed: true}
	r := fastResume(newResumableReader(context.Background(), f, "k", "", int64(len(data)), f.open(0)))
	got, err := io.ReadAll(r)
	if !errors.Is(err, objstore.ErrObjectChanged) {
		t.Fatalf("want ErrObjectChanged, got %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("delivered %d bytes, want only the first 100 of the original version", len(got))
	}
}

// A clean io.EOF ends the stream even if fewer bytes than the listed size
// arrived (e.g. a decompressively transcoded object) — never re-read it.
func TestResumableReader_CleanShortEOFIsEnd(t *testing.T) {
	f := &flakyRange{data: []byte("abc")}
	r := fastResume(newResumableReader(context.Background(), f, "k", "", 28, io.NopCloser(strings.NewReader("abc"))))
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "abc" || f.ranges != 0 {
		t.Fatalf("got %q err=%v ranges=%d, want \"abc\" with no resume", got, err, f.ranges)
	}
}

// Once the listed size has been delivered, a trailing error is not resumed
// (a resume at offset >= size would request a bogus range).
func TestResumableReader_NoResumePastSize(t *testing.T) {
	data := []byte(strings.Repeat("y", 100))
	f := &flakyRange{data: data, cut: 100}
	r := fastResume(newResumableReader(context.Background(), f, "k", "", int64(len(data)), &cutReader{r: bytes.NewReader(data), left: 100, reset: true}))
	got, err := io.ReadAll(r)
	if len(got) != 100 || f.ranges != 0 || err == nil {
		t.Fatalf("got %d bytes ranges=%d err=%v, want 100 bytes, no resume, the transport error", len(got), f.ranges, err)
	}
}
