package podlog

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// kubeletCompress mimics kubelet: write <p>.gz from <p>, then remove <p>.
func kubeletCompress(t *testing.T, p string, keepOriginal bool) {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(p + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	_, _ = zw.Write(data)
	_ = zw.Close()
	_ = f.Close()
	if !keepOriginal {
		_ = os.Remove(p)
	}
}

func appendLines(t *testing.T, p string, from, to int) {
	t.Helper()
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for i := from; i < to; i++ {
		_, _ = f.WriteString(testCRILine(i))
	}
	f.Close()
}

// The collector falls behind (e.g. S3 down) while kubelet rotates the log
// three times and gzips the older rotations: A (being read) is compressed
// and its original deleted, B is rotated and compressed, C is the newest
// rotation, D is live. Every line must be read exactly once.
func TestBehindAcrossCompressedRotations(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	appendLines(t, path, 0, 3)
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
	pass() // reads A[0:3]
	appendLines(t, path, 3, 5)
	a := path + ".20260714-100000"
	_ = os.Rename(path, a)
	appendLines(t, path, 5, 8) // B
	b := path + ".20260714-100100"
	_ = os.Rename(path, b)
	kubeletCompress(t, a, false)
	appendLines(t, path, 8, 11) // C
	c := path + ".20260714-100200"
	_ = os.Rename(path, c)
	kubeletCompress(t, b, false)
	appendLines(t, path, 11, 14) // D (live)
	for i := 0; i < 8; i++ {
		pass()
	}
	close(tl.out)
	got, n := map[string]bool{}, 0
	for r := range tl.out {
		got[r.message] = true
		n++
	}
	wantLines(t, got, 0, 14)
	if n != 14 {
		t.Errorf("read %d records, want exactly 14", n)
	}
}

// The collector was down while kubelet rotated and compressed the file it
// had been reading: a restart resumes inside the .gz at the saved offset,
// then reads the newer rotations and the live file.
func TestRestartResumesInsideCompressedRotation(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "0.log")
	posFile := filepath.Join(t.TempDir(), "positions.json")
	appendLines(t, path, 0, 4)
	if r := drainAll(t, root, posFile); len(r) != 4 {
		t.Fatalf("first run read %d, want 4", len(r))
	}
	appendLines(t, path, 4, 7)
	a := path + ".20260714-100000"
	_ = os.Rename(path, a)
	appendLines(t, path, 7, 9)
	_ = os.Rename(path, path+".20260714-100100")
	kubeletCompress(t, a, false)
	appendLines(t, path, 9, 12)
	rows := drainAll(t, root, posFile)
	wantLines(t, msgsOf(rows), 4, 12)
	if len(rows) != 8 {
		t.Errorf("second run read %d records, want exactly 8 (no re-read of lines 0-3)", len(rows))
	}
}

// While kubelet compresses, the original and its .gz exist side by side;
// the file must be listed once.
func TestSiblingsDedupDuringCompression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.log")
	appendLines(t, path+".20260714-100000", 0, 2)
	kubeletCompress(t, path+".20260714-100000", true)
	appendLines(t, path+".20260714-090000", 0, 1)
	kubeletCompress(t, path+".20260714-090000", false)
	got := rotatedSiblings(path)
	want := []string{path + ".20260714-090000.gz", path + ".20260714-100000"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("siblings = %v, want %v", got, want)
	}
}
