package podlog

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func TestParseCRILine(t *testing.T) {
	cases := []struct {
		raw     string
		ts      string
		stream  string
		partial bool
		msg     string
		valid   bool
	}{
		{"2026-07-14T10:00:00.123Z stdout F Started engine", "2026-07-14T10:00:00.123Z", "stdout", false, "Started engine", true},
		{"2026-07-14T10:00:00.200Z stderr P java.lang.RuntimeException", "2026-07-14T10:00:00.200Z", "stderr", true, "java.lang.RuntimeException", true},
		{"garbage line without cri", "", "", false, "garbage line without cri", false},
		{"2026-07-14T10:00:00Z weirdstream F msg", "2026-07-14T10:00:00Z", "", false, "2026-07-14T10:00:00Z weirdstream F msg", false},
	}
	for _, c := range cases {
		got := parseCRILine(c.raw)
		if got.valid != c.valid {
			t.Errorf("valid: raw=%q got %v want %v", c.raw, got.valid, c.valid)
		}
		if c.valid {
			if got.timestamp != c.ts || got.stream != c.stream || got.partial != c.partial || got.message != c.msg {
				t.Errorf("parse raw=%q got %+v", c.raw, got)
			}
		}
	}
}

func TestStitcherPartialReassembly(t *testing.T) {
	s := &stitcher{} // no startPattern → each full line is its own record
	// A CRI partial split across two physical lines rejoins into one message.
	if _, ok := s.push(criLine{timestamp: "t1", stream: "stdout", partial: true, message: "hello ", valid: true}, 10); ok {
		t.Fatal("partial line should not complete a record")
	}
	rec, ok := s.push(criLine{timestamp: "t1", stream: "stdout", partial: false, message: "world", valid: true}, 20)
	if !ok {
		t.Fatal("final line should complete the record")
	}
	if rec.message != "hello world" {
		t.Errorf("partial reassembly: got %q want %q", rec.message, "hello world")
	}
	if rec.endOffset != 20 {
		t.Errorf("partial reassembly endOffset: got %d want 20", rec.endOffset)
	}
}

func TestStitcherMultiline(t *testing.T) {
	s := &stitcher{startPattern: mustMatch(t, `^\d{4}-\d{2}-\d{2}`)}
	// First timestamped line starts a record; two continuation lines append.
	s.push(criLine{stream: "stderr", message: "2026-07-14 ERROR boom", valid: true}, 100)
	s.push(criLine{stream: "stderr", message: "\tat com.foo.Bar(Bar.java:1)", valid: true}, 130)
	s.push(criLine{stream: "stderr", message: "\tat com.foo.Baz(Baz.java:2)", valid: true}, 160)
	// A new timestamped line flushes the stitched stack trace.
	rec, ok := s.push(criLine{stream: "stderr", message: "2026-07-14 INFO next", valid: true}, 190)
	if !ok {
		t.Fatal("new record should flush the pending multiline record")
	}
	want := "2026-07-14 ERROR boom\n\tat com.foo.Bar(Bar.java:1)\n\tat com.foo.Baz(Baz.java:2)"
	if rec.message != want {
		t.Errorf("multiline stitch:\n got %q\nwant %q", rec.message, want)
	}
	// Critical: the stitched record's endOffset is the offset after ITS last
	// line (160), NOT the current read position (190) which is past the next
	// record's first line. Committing 190 would skip the "INFO next" record.
	if rec.endOffset != 160 {
		t.Errorf("multiline endOffset: got %d want 160 (offset must not run past the record's own last line)", rec.endOffset)
	}
	// The trailing record is released on flush.
	last, ok := s.flush()
	if !ok || last.message != "2026-07-14 INFO next" {
		t.Errorf("flush trailing: ok=%v msg=%q", ok, last.message)
	}
	if last.endOffset != 190 {
		t.Errorf("flushed record endOffset: got %d want 190", last.endOffset)
	}
}

func TestStitcherMaxLinesCap(t *testing.T) {
	// maxLines=3 caps a stitched record at three physical lines, so a stream
	// whose lines never match startPattern cannot grow one record unbounded.
	s := &stitcher{startPattern: mustMatch(t, `^\d{4}-\d{2}-\d{2}`), maxLines: 3}
	if _, ok := s.push(criLine{stream: "stderr", message: "2026-07-14 ERROR boom", valid: true}, 100); ok {
		t.Fatal("start line should not complete a record")
	}
	if _, ok := s.push(criLine{stream: "stderr", message: "line2", valid: true}, 110); ok {
		t.Fatal("2nd line (under cap) should not complete a record")
	}
	rec, ok := s.push(criLine{stream: "stderr", message: "line3", valid: true}, 120)
	if !ok {
		t.Fatal("3rd line should force-emit the capped record")
	}
	if want := "2026-07-14 ERROR boom\nline2\nline3"; rec.message != want {
		t.Errorf("capped record:\n got %q\nwant %q", rec.message, want)
	}
	// After the cap flush the next continuation line starts a fresh record.
	if _, ok := s.push(criLine{stream: "stderr", message: "line4", valid: true}, 130); ok {
		t.Fatal("post-cap line should start a fresh pending record, not complete one")
	}
	if last, ok := s.flush(); !ok || last.message != "line4" {
		t.Errorf("flush after cap: ok=%v msg=%q want line4", ok, last.message)
	}
}

// TestIntegrationBoundedDrain builds a synthetic /var/log/pods tree, runs the
// source in bounded mode, and asserts every logical record is emitted with
// correct enrichment and a stitched multiline stack trace.
func TestIntegrationBoundedDrain(t *testing.T) {
	root := t.TempDir()
	// <root>/<ns>_<pod>_<uid>/<container>/0.log
	dir := filepath.Join(root, "team-a_orders-7f9_abc123", "bwappnode")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "0.log")
	content := "" +
		"2026-07-14T10:00:00.100Z stdout F 2026-07-14 INFO started\n" +
		"2026-07-14T10:00:00.200Z stderr F 2026-07-14 ERROR boom\n" +
		"2026-07-14T10:00:00.201Z stderr F \tat com.foo.Bar(Bar.java:1)\n" +
		"2026-07-14T10:00:00.202Z stderr F \tat com.foo.Baz(Baz.java:2)\n" +
		"2026-07-14T10:00:00.300Z stdout F 2026-07-14 INFO done\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	posFile := filepath.Join(t.TempDir(), "positions.json")
	src, err := newPodlogSource(map[string]interface{}{
		"root":          root,
		"node":          "node-1",
		"positionFile":  posFile,
		"fromBeginning": true,
		"bounded":       true,
		"pollMs":        50,
		"multiline": map[string]interface{}{
			"startPattern": `^\d{4}-\d{2}-\d{2}`,
			"flushIdleMs":  100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	var rows [][]interface{}
	for {
		batch, err := src.ReadBatch(10)
		if batch != nil {
			rows = append(rows, batch.Rows...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	// 3 logical records: INFO started, ERROR+2 stack lines stitched, INFO done.
	if len(rows) != 3 {
		t.Fatalf("expected 3 stitched records, got %d: %v", len(rows), rows)
	}

	schema := src.Schema()
	col := func(name string) int {
		for i, c := range schema {
			if c.Name == name {
				return i
			}
		}
		return -1
	}
	if got := rows[0][col("namespace")]; got != "team-a" {
		t.Errorf("namespace enrichment: got %v", got)
	}
	if got := rows[0][col("pod")]; got != "orders-7f9" {
		t.Errorf("pod enrichment: got %v", got)
	}
	if got := rows[0][col("container")]; got != "bwappnode" {
		t.Errorf("container enrichment: got %v", got)
	}
	if got := rows[0][col("node")]; got != "node-1" {
		t.Errorf("node stamp: got %v", got)
	}
	// The stack trace lands as ONE record with newline-joined frames.
	stack, _ := rows[1][col("message")].(string)
	wantStack := "2026-07-14 ERROR boom\n\tat com.foo.Bar(Bar.java:1)\n\tat com.foo.Baz(Baz.java:2)"
	if stack != wantStack {
		t.Errorf("multiline record:\n got %q\nwant %q", stack, wantStack)
	}

	// Every logical record must carry a DISTINCT idempotency key. A regression
	// where multiline records inherited the file's current offset produced
	// colliding keys (and could skip an unwritten record on resume).
	seen := map[string]bool{}
	for i, r := range rows {
		k, _ := r[col("idempotency_key")].(string)
		if k == "" {
			t.Errorf("row %d has empty idempotency_key", i)
		}
		if seen[k] {
			t.Errorf("row %d has duplicate idempotency_key %s", i, k)
		}
		seen[k] = true
	}

	// Cursor() persists the position registry after (simulated) sink ack.
	cp, ok := src.(interface{ Cursor() json.RawMessage })
	if !ok {
		t.Fatal("source must implement CursorProvider")
	}
	snap := cp.Cursor()
	var reg map[string]position
	if err := json.Unmarshal(snap, &reg); err != nil {
		t.Fatal(err)
	}
	if p, ok := reg[logPath]; !ok || p.Offset == 0 {
		t.Errorf("position registry did not advance for %s: %+v", logPath, reg)
	}
	// The registry file was persisted atomically.
	if _, err := os.Stat(posFile); err != nil {
		t.Errorf("position file not written: %v", err)
	}
}

// TestResumeFromRegistry verifies a second source starting with a persisted
// registry skips already-delivered bytes (no re-emit of the same records).
func TestResumeFromRegistry(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "0.log")
	// The rotation fingerprint hashes the first 256 bytes, so the initial
	// content must exceed 256 bytes for the prefix (and thus the fingerprint) to
	// stay stable across an append — exactly the CRI real-world case.
	first := "" +
		"2026-07-14T10:00:00.10Z stdout F line one padded out to keep the prefix stable\n" +
		"2026-07-14T10:00:00.11Z stdout F line two padded out to keep the prefix stable\n" +
		"2026-07-14T10:00:00.12Z stdout F line three padded out to keep the prefix stable\n" +
		"2026-07-14T10:00:00.13Z stdout F line four padded out to keep the prefix stable\n"
	if err := os.WriteFile(logPath, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	posFile := filepath.Join(t.TempDir(), "positions.json")

	// First pass: drain all four lines and persist the registry.
	if firstRows := drainAll(t, root, posFile); len(firstRows) != 4 {
		t.Fatalf("first pass should read 4 lines, got %d", len(firstRows))
	}
	// Append two more lines, then resume: only the new lines should be read.
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("2026-07-14T10:00:00.30Z stdout F line five padded out to keep the prefix stable\n2026-07-14T10:00:00.40Z stdout F line six padded out to keep the prefix stable\n")
	f.Close()

	rows := drainAll(t, root, posFile)
	if len(rows) != 2 {
		t.Fatalf("resume should read only the 2 appended lines, got %d", len(rows))
	}
}

// TestRotationRestartsFromZero verifies containerd-style rotation (a fresh file
// at the same path, detected by a changed fingerprint) resets the read offset to
// 0 so no rotated-in data is skipped — Acceptance criterion #4.
func TestRotationRestartsFromZero(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "0.log")
	// First file content (>256 bytes so the fingerprint prefix is stable).
	original := "2026-07-14T10:00:00.10Z stdout F original line one padded to exceed the 256-byte fingerprint window for stable rotation detection here\n2026-07-14T10:00:00.20Z stdout F original line two padded to exceed the 256-byte fingerprint window for stable rotation detection\n"
	if err := os.WriteFile(logPath, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	posFile := filepath.Join(t.TempDir(), "positions.json")
	if r := drainAll(t, root, posFile); len(r) != 2 {
		t.Fatalf("first pass should read 2 lines, got %d", len(r))
	}

	// Rotate: replace the file at the same path with entirely different content
	// (containerd renames the old file and creates a fresh one). The new file has
	// a different fingerprint, so the collector must read it from offset 0.
	rotated := "2026-07-14T11:00:00.10Z stdout F ROTATED line one with completely different leading bytes so the fingerprint no longer matches the prior file at all here\n2026-07-14T11:00:00.20Z stdout F ROTATED line two with completely different leading bytes so the fingerprint no longer matches\n2026-07-14T11:00:00.30Z stdout F ROTATED line three\n"
	if err := os.WriteFile(logPath, []byte(rotated), 0o644); err != nil {
		t.Fatal(err)
	}

	rows := drainAll(t, root, posFile)
	if len(rows) != 3 {
		t.Fatalf("after rotation the collector must read all 3 new lines from offset 0, got %d", len(rows))
	}
	// Confirm the content is the rotated file's, not the original's.
	schema := podlogColumns
	msgCol := -1
	for i, c := range schema {
		if c.Name == "message" {
			msgCol = i
		}
	}
	if got, _ := rows[0][msgCol].(string); got != "ROTATED line one with completely different leading bytes so the fingerprint no longer matches the prior file at all here" {
		t.Errorf("expected rotated content, got %q", got)
	}
}

// TestTruncationRestartsFromZero verifies in-place truncation (size regression
// below the saved offset) resets the read to 0.
func TestTruncationRestartsFromZero(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "0.log")
	original := "2026-07-14T10:00:00.10Z stdout F line one padded to exceed the 256-byte fingerprint window for stable detection across the truncation boundary here now\n2026-07-14T10:00:00.20Z stdout F line two padded to exceed the 256-byte window\n"
	if err := os.WriteFile(logPath, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	posFile := filepath.Join(t.TempDir(), "positions.json")
	if r := drainAll(t, root, posFile); len(r) != 2 {
		t.Fatalf("first pass should read 2 lines, got %d", len(r))
	}

	// Truncate in place to a shorter content (size < saved offset).
	truncated := "2026-07-14T12:00:00.10Z stdout F fresh short line\n"
	if err := os.WriteFile(logPath, []byte(truncated), 0o644); err != nil {
		t.Fatal(err)
	}
	rows := drainAll(t, root, posFile)
	if len(rows) != 1 {
		t.Fatalf("after truncation the collector must read the 1 new line from offset 0, got %d", len(rows))
	}
}

func drainAll(t *testing.T, root, posFile string) [][]interface{} {
	t.Helper()
	src, err := newPodlogSource(map[string]interface{}{
		"root":          root,
		"positionFile":  posFile,
		"fromBeginning": true,
		"bounded":       true,
		"pollMs":        50,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var rows [][]interface{}
	for {
		batch, err := src.ReadBatch(10)
		if batch != nil {
			rows = append(rows, batch.Rows...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if cp, ok := src.(interface{ Cursor() json.RawMessage }); ok {
		cp.Cursor()
	}
	return rows
}

func mustMatch(t *testing.T, pattern string) matcher {
	t.Helper()
	m, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

var _ source.Source = (*podlogSource)(nil)
