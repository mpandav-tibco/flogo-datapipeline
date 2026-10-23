package podlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newDrainTailer(t *testing.T) (*tailer, *openLogFile, *os.File) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "0.log")
	w, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	tl := &tailer{fromBeginning: true, out: make(chan enrichedRecord, 16), open: map[string]*openLogFile{}}
	of := tl.openFileAt(path, podMeta{}, 0)
	if of == nil {
		t.Fatal("open failed")
	}
	t.Cleanup(func() { of.f.Close() })
	return tl, of, w
}

// A line written in several chunks (each drained before the newline arrives)
// must be reassembled whole, and the offset must cover every consumed byte.
func TestDrainReassemblesMultiChunkLine(t *testing.T) {
	tl, of, w := newDrainTailer(t)
	chunks := []string{"2026-07-14T10:00:00.1Z stdout F hel", "lo wor", "ld\n"}
	for _, c := range chunks {
		if _, err := w.WriteString(c); err != nil {
			t.Fatal(err)
		}
		if _, err := tl.drain(context.Background(), of); err != nil {
			t.Fatal(err)
		}
	}
	if rec, ok := of.st.flush(); ok {
		tl.out <- enrichedRecord{message: rec.message}
	}
	close(tl.out)
	var msgs []string
	for r := range tl.out {
		msgs = append(msgs, r.message)
	}
	if len(msgs) != 1 || msgs[0] != "hello world" {
		t.Fatalf("records=%q; want [\"hello world\"]", msgs)
	}
	if want := int64(len(strings.Join(chunks, ""))); of.offset != want {
		t.Fatalf("offset=%d; want %d (all consumed bytes)", of.offset, want)
	}
}

// An unterminated line larger than maxLineBytes is force-emitted instead of
// growing the partial buffer without bound.
func TestDrainBoundsPartialLine(t *testing.T) {
	tl, of, w := newDrainTailer(t)
	tl.maxLineBytes = 1024
	if _, err := w.WriteString("2026-07-14T10:00:00.1Z stdout F " + strings.Repeat("x", 4096)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := tl.drain(context.Background(), of); err != nil {
			t.Fatal(err)
		}
	}
	if len(of.partial) > 1024 {
		t.Fatalf("partial buffer grew to %d bytes; cap is 1024", len(of.partial))
	}
}

// A deleted file's pending record must be delivered even when the output
// channel is momentarily full (previously it was silently dropped).
func TestFlushAndCloseBlocksInsteadOfDropping(t *testing.T) {
	tl, of, w := newDrainTailer(t)
	tl.out = make(chan enrichedRecord, 1)
	tl.out <- enrichedRecord{message: "occupying"}
	// A CRI "P" (partial) line stays pending in the stitcher until flushed.
	if _, err := w.WriteString("2026-07-14T10:00:00.1Z stdout P tail-record\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := tl.drain(context.Background(), of); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { tl.flushAndClose(context.Background(), of); close(done) }()
	first, second := <-tl.out, <-tl.out
	<-done
	if first.message != "occupying" || second.message != "tail-record" {
		t.Fatalf("got %q then %q; want the pending record delivered", first.message, second.message)
	}
}
