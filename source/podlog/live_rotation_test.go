package podlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Kubelet rotates a running container's log by renaming 0.log to
// 0.log.<timestamp> and having containerd reopen a fresh 0.log. A collector
// that is tailing at that moment must (1) finish the tail of the renamed
// file and (2) switch to the new 0.log — losing neither side.
func TestLiveRenameRotation(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "0.log")
	line := func(i int) string {
		return fmt.Sprintf("2026-07-14T10:00:00.%06dZ stdout F line %d padded so the fingerprint window holds real content\n", i, i)
	}
	w, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, _ = w.WriteString(line(i))
	}
	tl := &tailer{root: root, fromBeginning: true, out: make(chan enrichedRecord, 1000), open: map[string]*openLogFile{},
		positions: &positionStore{m: map[string]position{}}}
	ctx := context.Background()
	pass := func() {
		tl.scan(ctx)
		for _, of := range tl.open {
			if _, err := tl.drain(ctx, of); err != nil {
				of.f.Close()
				delete(tl.open, of.key)
			}
		}
	}
	pass()
	// Lines written to the old file just before rotation (still unread).
	for i := 5; i < 8; i++ {
		_, _ = w.WriteString(line(i))
	}
	w.Close()
	if err := os.Rename(path, path+".20260714-100000"); err != nil {
		t.Fatal(err)
	}
	w2, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 8; i < 12; i++ {
		_, _ = w2.WriteString(line(i))
	}
	w2.Close()
	for i := 0; i < 3; i++ {
		pass()
	}
	close(tl.out)
	got := map[string]bool{}
	n := 0
	for r := range tl.out {
		got[r.message] = true
		n++
	}
	if n != 12 {
		t.Errorf("read %d records, want exactly 12 (no duplicates)", n)
	}
	for i := 0; i < 12; i++ {
		want := fmt.Sprintf("line %d padded so the fingerprint window holds real content", i)
		if !got[want] {
			t.Errorf("missing %q after live rotation (got %d records)", want, len(got))
		}
	}
}

func testCRILine(i int) string {
	return fmt.Sprintf("2026-07-14T10:00:00.%06dZ stdout F line %d padded so the fingerprint window holds real content\n", i, i)
}

func msgsOf(rows [][]interface{}) map[string]bool {
	col := -1
	for i, c := range podlogColumns {
		if c.Name == "message" {
			col = i
		}
	}
	m := map[string]bool{}
	for _, r := range rows {
		m[r[col].(string)] = true
	}
	return m
}

func wantLines(t *testing.T, got map[string]bool, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		if !got[fmt.Sprintf("line %d padded so the fingerprint window holds real content", i)] {
			t.Errorf("line %d lost", i)
		}
	}
}

// The collector was down while kubelet rotated the file: the unread tail of
// the rotated file (0.log.<ts>) must be read from the saved offset, then the
// new 0.log from its start.
func TestRotationWhileDownResumesRotatedFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	posFile := filepath.Join(t.TempDir(), "positions.json")
	w, _ := os.Create(path)
	for i := 0; i < 5; i++ {
		_, _ = w.WriteString(testCRILine(i))
	}
	if r := drainAll(t, root, posFile); len(r) != 5 {
		t.Fatalf("first run read %d, want 5", len(r))
	}
	// While the collector is down: more lines, rotation, lines in the new file.
	for i := 5; i < 9; i++ {
		_, _ = w.WriteString(testCRILine(i))
	}
	w.Close()
	if err := os.Rename(path, path+".20260714-100000"); err != nil {
		t.Fatal(err)
	}
	w2, _ := os.Create(path)
	for i := 9; i < 13; i++ {
		_, _ = w2.WriteString(testCRILine(i))
	}
	w2.Close()
	rows := drainAll(t, root, posFile)
	got := msgsOf(rows)
	wantLines(t, got, 5, 13)
	if len(rows) != 8 {
		t.Errorf("second run read %d records, want exactly 8 (no re-read of lines 0-4)", len(rows))
	}
}

// A file deleted (pod removed) after lines were written but before they were
// read must still be read to its end before the handle is closed.
func TestDeletedFileDrainedBeforeClose(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	w, _ := os.Create(path)
	_, _ = w.WriteString(testCRILine(0))
	tl := &tailer{root: root, fromBeginning: true, out: make(chan enrichedRecord, 100), open: map[string]*openLogFile{},
		positions: &positionStore{m: map[string]position{}}}
	ctx := context.Background()
	tl.scan(ctx)
	for _, of := range tl.open {
		_, _ = tl.drain(ctx, of)
	}
	_, _ = w.WriteString(testCRILine(1))
	_, _ = w.WriteString(testCRILine(2))
	w.Close()
	_ = os.RemoveAll(filepath.Join(root, "ns_pod_uid"))
	tl.scan(ctx) // notices the deletion
	close(tl.out)
	got := map[string]bool{}
	for r := range tl.out {
		got[r.message] = true
	}
	wantLines(t, got, 0, 3)
}

// Close must not commit offsets of records that were handed to the pipeline
// but never covered by a checkpoint (they may not have reached the sink).
func TestCloseDoesNotCommitUncheckpointedOffsets(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	w, _ := os.Create(filepath.Join(dir, "0.log"))
	for i := 0; i < 3; i++ {
		_, _ = w.WriteString(testCRILine(i))
	}
	w.Close()
	posFile := filepath.Join(t.TempDir(), "positions.json")
	src, err := newPodlogSource(map[string]interface{}{"root": root, "positionFile": posFile, "fromBeginning": true, "bounded": true, "pollMs": 50})
	if err != nil {
		t.Fatal(err)
	}
	for {
		b, err := src.ReadBatch(10)
		if b != nil && b.Len() > 0 || err != nil {
			break
		}
	}
	_ = src.Close() // no Cursor(): nothing was checkpointed
	if r := drainAll(t, root, posFile); len(r) != 3 {
		t.Fatalf("restart read %d records, want all 3 replayed (none were checkpointed)", len(r))
	}
}

// Between kubelet's rename and the runtime creating the new file the path is
// briefly absent. That must not close the handle (which would re-read the
// rotated file from the last checkpoint: duplicates) — the new file is picked
// up when it appears, and every line is read exactly once.
func TestRotationGapNoDuplicates(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	w, _ := os.Create(path)
	for i := 0; i < 4; i++ {
		_, _ = w.WriteString(testCRILine(i))
	}
	tl := &tailer{root: root, fromBeginning: true, out: make(chan enrichedRecord, 100), open: map[string]*openLogFile{},
		positions: &positionStore{m: map[string]position{}}}
	ctx := context.Background()
	pass := func() {
		tl.scan(ctx)
		for _, of := range tl.open {
			if _, err := tl.drain(ctx, of); err != nil {
				of.f.Close()
				delete(tl.open, of.key)
			}
		}
	}
	pass()
	_, _ = w.WriteString(testCRILine(4))
	w.Close()
	_ = os.Rename(path, path+".20260714-100000")
	pass() // path absent: rotation in progress
	if len(tl.open) != 1 {
		t.Fatalf("handle closed during the rotation gap (open=%d)", len(tl.open))
	}
	w2, _ := os.Create(path)
	for i := 5; i < 8; i++ {
		_, _ = w2.WriteString(testCRILine(i))
	}
	w2.Close()
	for i := 0; i < 3; i++ {
		pass()
	}
	close(tl.out)
	n := 0
	got := map[string]bool{}
	for r := range tl.out {
		got[r.message] = true
		n++
	}
	wantLines(t, got, 0, 8)
	if n != 8 {
		t.Errorf("read %d records, want exactly 8 (no duplicates)", n)
	}
}

// Two rotations while the collector is behind (back-pressured): the file in
// the middle (rotated again before it was opened) must not be skipped.
func TestDoubleRotationWhileBehind(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	write := func(p string, from, to int) {
		f, _ := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		for i := from; i < to; i++ {
			_, _ = f.WriteString(testCRILine(i))
		}
		f.Close()
	}
	write(path, 0, 3)
	tl := &tailer{root: root, fromBeginning: true, out: make(chan enrichedRecord, 100), open: map[string]*openLogFile{},
		positions: &positionStore{m: map[string]position{}}}
	ctx := context.Background()
	pass := func() {
		tl.scan(ctx)
		for _, of := range tl.open {
			if _, err := tl.drain(ctx, of); err != nil {
				of.f.Close()
				delete(tl.open, of.key)
			}
		}
	}
	pass()
	write(path, 3, 5) // unread tail of A
	_ = os.Rename(path, path+".20260714-100000")
	time.Sleep(20 * time.Millisecond) // distinct mtimes
	write(path, 5, 8)                 // B
	_ = os.Rename(path, path+".20260714-100010")
	time.Sleep(20 * time.Millisecond)
	write(path, 8, 10) // C (live)
	for i := 0; i < 6; i++ {
		pass()
	}
	close(tl.out)
	got, n := map[string]bool{}, 0
	for r := range tl.out {
		got[r.message] = true
		n++
	}
	wantLines(t, got, 0, 10)
	if n != 10 {
		t.Errorf("read %d records, want exactly 10", n)
	}
}
