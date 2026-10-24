package podlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var bwStart = `^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}`

func full(msg string) criLine { return criLine{stream: "stdout", message: msg, valid: true} }

// A container that never logs a start line (a sidecar, nginx, a JSON logger
// on the same node as the BW apps) gets one record per line — not 1,000-line
// blocks glued under the node-wide BW startPattern — and without latency.
func TestForeignContainerLinesStayOnePerRecord(t *testing.T) {
	s := &stitcher{startPattern: mustMatch(t, bwStart), maxLines: 1000}
	for i, l := range []string{`[2026-10-24T10:00:00.000Z] "GET /health HTTP/1.1" 200`, `{"level":"info","msg":"x"}`, `10.0.0.1 - - "POST /a" 201`} {
		rec, ok := s.push(full(l), int64(10*(i+1)))
		if !ok || rec.message != l {
			t.Fatalf("line %d: got (%q, %v), want it emitted on its own", i, rec.message, ok)
		}
	}
	if _, ok := s.flush(); ok {
		t.Fatal("nothing should be buffered")
	}
	// A BW record is still stitched with its trace.
	s.push(full("2026-10-24 10:00:00,000 ERROR [t] x - boom"), 100)
	s.push(full("com.tibco.bw.core.BWEngineException: failed"), 110)
	s.push(full("\tat a.b.C(C.java:1)"), 120)
	rec, ok := s.push(full("2026-10-24 10:00:01,000 INFO [t] x - next"), 130)
	if !ok || strings.Count(rec.message, "\n") != 2 {
		t.Fatalf("BW trace not stitched: %q", rec.message)
	}
}

// continuePattern stitches a stack trace with no timestamped header line
// (printStackTrace on stderr), while other foreign lines stay separate.
func TestContinuePatternStitchesHeaderlessTrace(t *testing.T) {
	s := &stitcher{startPattern: mustMatch(t, bwStart), continuePattern: mustMatch(t, `^(\s+at |\s*Caused by:|\s+\.\.\. \d+ more)`)}
	var got []string
	push := func(l string, end int64) {
		if rec, ok := s.push(full(l), end); ok {
			got = append(got, rec.message)
		}
	}
	push("java.lang.IllegalStateException: boom", 10)
	push("\tat a.B.c(B.java:1)", 20)
	push("Caused by: java.io.IOException: x", 30)
	push("\t... 3 more", 40)
	push("plain line", 50)
	if rec, ok := s.flush(); ok {
		got = append(got, rec.message)
	}
	want := []string{"java.lang.IllegalStateException: boom\n\tat a.B.c(B.java:1)\nCaused by: java.io.IOException: x\n\t... 3 more", "plain line"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

// A line that never ends (a multi-MB payload the runtime splits into 16 KiB
// partial chunks) is cut at maxBytes instead of growing without bound; every
// byte is still delivered, in order.
func TestPartialReassemblyIsBounded(t *testing.T) {
	s := &stitcher{startPattern: mustMatch(t, bwStart), maxBytes: 100}
	payload := "2026-10-24 10:00:00,000 INFO [t] x - " + strings.Repeat("p", 500)
	var recs []logRecord
	off := int64(0)
	for i := 0; i < len(payload); i += 40 {
		end := i + 40
		tag := true
		if end >= len(payload) {
			end, tag = len(payload), false
		}
		off += 50
		if rec, ok := s.push(criLine{stream: "stdout", partial: tag, message: payload[i:end], valid: true}, off); ok {
			recs = append(recs, rec)
		}
	}
	if rec, ok := s.flush(); ok {
		recs = append(recs, rec)
	}
	var all strings.Builder
	last := int64(-1)
	for _, r := range recs {
		if len(r.message) > 100+40 { // cap + one partial chunk
			t.Errorf("record of %d bytes exceeds ~maxBytes", len(r.message))
		}
		if r.endOffset <= last {
			t.Errorf("endOffsets not increasing: %d after %d", r.endOffset, last)
		}
		last = r.endOffset
		all.WriteString(strings.ReplaceAll(r.message, "\n", ""))
	}
	if all.String() != payload {
		t.Fatalf("payload not delivered intact: got %d bytes in %d records, want %d", all.Len(), len(recs), len(payload))
	}
	if len(recs) < 3 {
		t.Fatalf("payload not split: %d records", len(recs))
	}
}

// A record reassembled from CRI partial chunks starts at its FIRST chunk: the
// position committed for another stream's record must not pass it.
func TestCommitNeverPassesBufferedPartialChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.log")
	lines := []string{
		"2026-07-14T10:00:00.0Z stderr F 2026-07-14 10:00:00 ERROR first",
		"2026-07-14T10:00:00.1Z stderr P 2026-07-14 10:00:00 ERROR part-one ",
		"2026-07-14T10:00:00.2Z stderr F part-two",
		"2026-07-14T10:00:00.3Z stdout F 2026-07-14 10:00:00 INFO a",
		"2026-07-14T10:00:00.4Z stdout F 2026-07-14 10:00:01 INFO b",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl := &tailer{fromBeginning: true, startPattern: mustMatch(t, `^\d{4}-\d{2}-\d{2}`),
		out: make(chan enrichedRecord, 16), open: map[string]*openLogFile{}}
	of := tl.openFileAt(path, podMeta{}, 0)
	defer of.f.Close()
	if _, err := tl.drain(context.Background(), of); err != nil {
		t.Fatal(err)
	}
	// "first" completes when the partial record's F chunk arrives; the new
	// pending record starts at its P chunk, not at the F chunk.
	chunkStart := int64(len(lines[0]) + 1)
	for len(tl.out) > 0 {
		r := <-tl.out
		if r.stream == "stdout" && r.commitOffset > chunkStart {
			t.Errorf("record %q commits offset %d past the buffered stderr record starting at %d", r.message, r.commitOffset, chunkStart)
		}
	}
}

func TestByteBudget(t *testing.T) {
	b := newByteBudget(100)
	ctx := context.Background()
	if !b.acquire(ctx, 500) {
		t.Fatal("an oversized record must be admitted when nothing is queued")
	}
	done := make(chan bool)
	go func() { done <- b.acquire(ctx, 10) }()
	select {
	case <-done:
		t.Fatal("acquire did not block over budget")
	case <-time.After(50 * time.Millisecond):
	}
	b.release(500)
	if !<-done {
		t.Fatal("acquire failed after release")
	}
	cctx, cancel := context.WithCancel(ctx)
	b.acquire(ctx, 85) // 10 + 85 held
	go func() { done <- b.acquire(cctx, 50) }()
	cancel()
	if <-done {
		t.Fatal("acquire succeeded after cancel")
	}
}

// Big log records (multi-MB payload logging) are bounded in bytes between the
// tailer and the pipeline, not only in record count.
func TestReadBatchBoundedInBytes(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	var sb strings.Builder
	big := strings.Repeat("x", 700<<10)
	for i := 0; i < 40; i++ {
		sb.WriteString("2026-07-14T10:00:00.1Z stdout F " + big + "\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "0.log"), []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newTestSource(t, root, "", true)
	defer s.Close()
	total := 0
	for i := 0; i < 200 && total < 40; i++ {
		b, err := s.ReadBatch(1000)
		if err != nil {
			t.Fatal(err)
		}
		bytes := 0
		for _, r := range b.Rows {
			bytes += len(r[6].(string))
		}
		if bytes > maxBatchBytes+(1<<20) {
			t.Fatalf("batch of %d bytes, cap %d", bytes, maxBatchBytes)
		}
		total += len(b.Rows)
	}
	if total != 40 {
		t.Fatalf("read %d of 40 records", total)
	}
}

// A corrupt position registry must not crash-loop the collector: it is moved
// aside and every file is read from its start (duplicates, no loss).
func TestCorruptRegistryRecovers(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ns_pod_uid", "c")
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, "0.log"), []byte("2026-07-14T10:00:00.1Z stdout F hello\n"), 0o644)
	pos := filepath.Join(t.TempDir(), "positions.json")
	_ = os.WriteFile(pos, []byte(`{"truncated":`), 0o644)
	s := newTestSource(t, root, pos, false)
	defer s.Close()
	if got := readN(t, s, 1); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("got %q, want the file re-read from its start", got)
	}
	if m, _ := filepath.Glob(pos + ".corrupt-*"); len(m) != 1 {
		t.Fatalf("corrupt registry not kept aside: %v", m)
	}
}
