package podlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// stdout and stderr interleave in one CRI file. A stack trace on stderr must
// stay one record of its own, and the file position must never be committed
// past a line another stream still buffers (a crash would lose it).
func TestInterleavedStreamsStitchedSeparately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.log")
	lines := []string{
		"2026-07-14T10:00:00.1Z stdout F 2026-07-14 10:00:00 INFO a",
		"2026-07-14T10:00:00.2Z stderr F 2026-07-14 10:00:00 ERROR boom",
		"2026-07-14T10:00:00.3Z stderr F \tat X",
		"2026-07-14T10:00:00.4Z stdout F 2026-07-14 10:00:00 INFO b",
		"2026-07-14T10:00:00.5Z stderr F \tat Y",
		"2026-07-14T10:00:00.6Z stdout F 2026-07-14 10:00:00 INFO c",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stderrStart := int64(len(lines[0]) + 1)
	tl := &tailer{fromBeginning: true, startPattern: mustMatch(t, `^\d{4}-\d{2}-\d{2}`),
		out: make(chan enrichedRecord, 16), open: map[string]*openLogFile{}}
	of := tl.openFileAt(path, podMeta{}, 0)
	defer of.f.Close()
	if _, err := tl.drain(context.Background(), of); err != nil {
		t.Fatal(err)
	}
	// Before the final flush: emitted records' commit offsets must not pass
	// the start of the still-buffered stderr record.
	var early []enrichedRecord
	for len(tl.out) > 0 {
		early = append(early, <-tl.out)
	}
	for _, r := range early {
		if r.commitOffset > stderrStart {
			t.Errorf("record %q commits offset %d past the buffered stderr record at %d", r.message, r.commitOffset, stderrStart)
		}
	}
	tl.finish(context.Background(), of)
	close(tl.out)
	msgs := map[string]string{}
	for _, r := range early {
		msgs[r.message] = r.stream
	}
	for r := range tl.out {
		msgs[r.message] = r.stream
	}
	want := map[string]string{
		"2026-07-14 10:00:00 INFO a":                     "stdout",
		"2026-07-14 10:00:00 INFO b":                     "stdout",
		"2026-07-14 10:00:00 INFO c":                     "stdout",
		"2026-07-14 10:00:00 ERROR boom\n\tat X\n\tat Y": "stderr",
	}
	if len(msgs) != len(want) {
		t.Fatalf("records = %q, want %q", msgs, want)
	}
	for m, st := range want {
		if msgs[m] != st {
			t.Errorf("missing or wrong-stream record %q (got %q); all: %q", m, msgs[m], msgs)
		}
	}
}

func newTestSource(t *testing.T, root, posFile string, fromBeginning bool) *podlogSource {
	t.Helper()
	s, err := newPodlogSource(map[string]interface{}{"root": root, "positionFile": posFile, "fromBeginning": fromBeginning, "pollMs": 20})
	if err != nil {
		t.Fatal(err)
	}
	return s.(*podlogSource)
}

func readN(t *testing.T, s *podlogSource, n int) []string {
	t.Helper()
	col := -1
	for i, c := range podlogColumns {
		if c.Name == "message" {
			col = i
		}
	}
	var out []string
	for i := 0; i < 200 && len(out) < n; i++ {
		b, err := s.ReadBatch(100)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range b.Rows {
			out = append(out, r[col].(string))
		}
	}
	return out
}

// fromBeginning=false skips only what existed at the collector's very first
// start; a pod created afterwards is read from its first line.
func TestFromBeginningFalseReadsNewPodsFromStart(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "ns_old_uid", "c")
	_ = os.MkdirAll(old, 0o755)
	appendLines(t, filepath.Join(old, "0.log"), 0, 3)
	posFile := filepath.Join(t.TempDir(), "positions.json")
	s := newTestSource(t, root, posFile, false)
	defer s.Close()
	time.Sleep(100 * time.Millisecond)
	newPod := filepath.Join(root, "ns_new_uid", "c")
	_ = os.MkdirAll(newPod, 0o755)
	appendLines(t, filepath.Join(newPod, "0.log"), 10, 13)
	got := readN(t, s, 3)
	if len(got) != 3 || !strings.Contains(got[0], "line 10 ") {
		t.Fatalf("new pod's lines = %q, want lines 10-12 from its first line (history of the old pod skipped)", got)
	}
}

// Positions of removed pods are dropped from the registry.
func TestRegistryDropsRemovedPods(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	appendLines(t, filepath.Join(dir, "0.log"), 0, 2)
	posFile := filepath.Join(t.TempDir(), "positions.json")
	s := newTestSource(t, root, posFile, true)
	defer s.Close()
	readN(t, s, 2)
	s.Cursor()
	if _, ok := s.positions.get(filepath.Join(dir, "0.log")); !ok {
		t.Fatal("position not recorded")
	}
	_ = os.RemoveAll(filepath.Join(root, "ns_pod_uid"))
	s.Cursor()
	if _, ok := s.positions.get(filepath.Join(dir, "0.log")); ok {
		t.Fatal("removed pod's position still in the registry")
	}
}

// A node tails hundreds of files at once; per-file memory must stay small
// (1 MiB read buffers OOMed the collector at 400 containers/node).
func TestManyOpenFilesBoundedMemory(t *testing.T) {
	dir := t.TempDir()
	const files = 400
	tl := &tailer{fromBeginning: true, out: make(chan enrichedRecord, files*2), open: map[string]*openLogFile{}}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	opened := make([]*openLogFile, 0, files)
	for i := 0; i < files; i++ {
		p := filepath.Join(dir, fmt.Sprintf("%d.log", i))
		if err := os.WriteFile(p, []byte("2026-07-14T10:00:00.1Z stdout F hello\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		of := tl.openFileAt(p, podMeta{}, 0)
		if _, err := tl.drain(context.Background(), of); err != nil {
			t.Fatal(err)
		}
		opened = append(opened, of)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	for _, of := range opened {
		of.f.Close()
	}
	if perFile := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / files; perFile > 128<<10 {
		t.Fatalf("%d bytes heap per open file, want <= 128 KiB", perFile)
	}
}
