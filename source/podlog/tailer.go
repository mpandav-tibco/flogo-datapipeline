package podlog

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fpLen is the number of leading bytes hashed into a file fingerprint used to
// detect rotation (a fresh file at the same path has different first bytes).
const fpLen = 256

// readBufSize is the per-open-file read buffer. Memory scales with the number
// of tailed files (hundreds per node), and ReadBytes grows its result beyond
// the buffer anyway, so a large buffer only costs RSS (1 MiB × 400 files OOMed
// the collector in the 400-containers/node scale test).
const readBufSize = 32 << 10

// podMeta is the Kubernetes context derived from a CRI pod-log directory name.
// containerd/CRI-O lay logs out as:
//
//	<root>/<namespace>_<pod>_<uid>/<container>/<n>.log
//
// Namespaces, pod names and container names are all DNS-1123 labels and cannot
// contain "_", so the pod directory splits cleanly into three fields.
type podMeta struct {
	namespace string
	pod       string
	uid       string
	container string
}

// enrichedRecord is one logical log line ready to become a pipeline row,
// carrying both the log payload and the delivery bookkeeping (fileKey/endOffset)
// the source needs to advance the position store after the sink acks.
type enrichedRecord struct {
	meta      podMeta
	stream    string
	timestamp string
	message   string
	fileKey   string
	endOffset int64 // end of the record's last line: its identity in the file
	// commitOffset is how far the file's position may advance once this
	// record is delivered: every byte before it is in this or an earlier
	// record (<= endOffset when another stream still buffers earlier lines).
	commitOffset int64
	fp           string
}

// streamStitchers stitches each CRI stream of a file separately: stdout and
// stderr lines interleave in one file, and a stack trace on one stream must
// not be glued to a record of the other (nor CRI partial chunks mixed). It
// also tracks where each stream's buffered, not yet emitted lines begin, so
// the file position is never committed past a line still buffered.
type streamStitchers struct {
	startPattern matcher
	maxLines     int
	byStream     map[string]*stitcher
	start        map[string]int64
	order        []string
}

func (t *tailer) newStitchers() *streamStitchers {
	return &streamStitchers{startPattern: t.startPattern, maxLines: t.maxLines,
		byStream: map[string]*stitcher{}, start: map[string]int64{}}
}

func (ss *streamStitchers) push(line criLine, lineStart, lineEnd int64) (logRecord, bool) {
	st, ok := ss.byStream[line.stream]
	if !ok {
		st = &stitcher{startPattern: ss.startPattern, maxLines: ss.maxLines}
		ss.byStream[line.stream] = st
		ss.order = append(ss.order, line.stream)
	}
	if _, buffering := ss.start[line.stream]; !buffering {
		ss.start[line.stream] = lineStart
	}
	rec, done := st.push(line, lineEnd)
	if done {
		if st.buffering() {
			ss.start[line.stream] = lineStart // the current line opened a new record
		} else {
			delete(ss.start, line.stream)
		}
	}
	return rec, done
}

// flush returns every stream's buffered record, oldest first.
func (ss *streamStitchers) flush() []logRecord {
	var out []logRecord
	for _, stream := range ss.order {
		if rec, ok := ss.byStream[stream].flush(); ok {
			out = append(out, rec)
		}
		delete(ss.start, stream)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].endOffset < out[j].endOffset })
	return out
}

// safeOffset caps a position at the first byte still buffered in any stream.
func (ss *streamStitchers) safeOffset(end int64) int64 {
	for _, s := range ss.start {
		if s < end {
			end = s
		}
	}
	return end
}

// openLogFile is an actively tailed CRI log file.
type openLogFile struct {
	key    string // == path
	f      *os.File
	reader *bufio.Reader
	offset int64
	fp     string
	meta   podMeta
	st     *streamStitchers
	// partial accumulates bytes read past the last newline (an incomplete final
	// line), across as many drains as it takes for the newline to arrive. Its
	// length is NOT added to offset until the line is completed, so a crash
	// never checkpoints a half-written line.
	partial []byte
	// lastData is when this file last produced a complete line; used to flush a
	// pending multiline record once the file has been idle for flushIdle.
	lastData time.Time
	// rotatedSeen is when the path was first seen to name a different file
	// than this handle (kubelet renamed it and the runtime reopened a fresh
	// one). The handle keeps draining for rotationGrace after that, since the
	// runtime may still flush a last write into the renamed file.
	rotatedSeen time.Time
	// rotated is set when this handle is on a rotated-away file (resumed after
	// a restart, or a newer rotation reached while back-pressured): at its end
	// the tailer moves on without the rotation grace.
	rotated bool
	// gz marks a kubelet-compressed rotation read through gzip: its on-disk
	// size is not comparable with the (decompressed) offset.
	gz bool
	// missingSince is when the path was first seen absent while its container
	// directory still exists — normally the instant between kubelet's rename
	// and the runtime creating the new file.
	missingSince time.Time
}

// missingGrace is how long a path may be absent (container directory still
// present) before its handle is treated as deleted and closed.
const missingGrace = 30 * time.Second

// defaultRotationGrace is how long a rotated-away file keeps being drained
// before the tailer switches to the file now at its path.
const defaultRotationGrace = time.Second

// tailer discovers and tails every CRI log file under root, emitting stitched,
// enriched records onto out. It is the node-local collection engine that
// replaces a per-namespace Fluentd DaemonSet.
type tailer struct {
	root          string
	node          string
	fromBeginning bool
	pollMs        int
	startPattern  matcher
	includeNS     map[string]bool
	excludeNS     map[string]bool
	positions     *positionStore
	// maxLines caps physical lines stitched into one multiline record (0 =
	// unlimited); propagated to each file's stitcher.
	maxLines int
	// maxLineBytes bounds partial: an unterminated line reaching this size is
	// emitted as-is so a writer that never ends a line cannot grow memory
	// without limit (0 = unbounded).
	maxLineBytes int
	// flushIdle is how long a file may be idle before a pending stitched
	// (multiline) record is emitted so trailing records are not held forever.
	flushIdle time.Duration
	// bounded runs a single scan → drain → flush → close pass instead of an
	// unbounded poll loop. Used by tests and batch/drain scenarios so ReadBatch
	// terminates deterministically with io.EOF.
	bounded bool
	// initialScanDone is set after the first scan (see fromBeginning).
	initialScanDone bool
	// rotationGrace: see defaultRotationGrace.
	rotationGrace time.Duration
	// recon receives files-open and rotation counts for reconciliation metrics.
	// May be nil (metrics disabled path in tests).
	recon *reconCounters

	open map[string]*openLogFile
	out  chan enrichedRecord
	// delivered is the furthest position handed downstream per path in this
	// process. A handle reopened in the same process resumes from it rather
	// than from the (older) checkpointed position, so nothing is re-read; the
	// persisted registry still governs restarts (at-least-once).
	delivered map[string]position
}

func (t *tailer) run(ctx context.Context) {
	defer close(t.out)
	if t.open == nil {
		t.open = map[string]*openLogFile{}
	}
	if t.bounded {
		t.runBounded(ctx)
		return
	}
	poll := time.Duration(t.pollMs) * time.Millisecond
	for {
		if ctx.Err() != nil {
			t.closeAll()
			return
		}
		t.scan(ctx)
		read := false
		for _, of := range t.open {
			n, err := t.drain(ctx, of)
			if err != nil {
				// Drop the handle; scan() will reopen if the path still exists.
				of.f.Close()
				delete(t.open, of.key)
				continue
			}
			if n > 0 {
				read = true
			}
		}
		// Emit any multiline records whose file has been idle past flushIdle so
		// a trailing stack trace is not held back indefinitely.
		if t.flushIdle > 0 {
			for _, of := range t.open {
				if !of.lastData.IsZero() && time.Since(of.lastData) >= t.flushIdle {
					for _, rec := range of.st.flush() {
						if !t.emit(ctx, of, rec) {
							t.closeAll()
							return
						}
						read = true
					}
					of.lastData = time.Time{} // don't re-flush until new data arrives
				}
			}
		}
		if ctx.Err() != nil {
			t.flushAll(ctx)
			t.closeAll()
			return
		}
		if !read {
			select {
			case <-ctx.Done():
				t.flushAll(ctx)
				t.closeAll()
				return
			case <-time.After(poll):
			}
		}
	}
}

// runBounded performs a single scan → drain → flush → close pass so ReadBatch
// terminates deterministically. It keeps scanning+draining until a full pass
// produces no new records (all files caught up), then force-flushes every
// stitcher's pending record and returns (the deferred close(out) signals EOF).
func (t *tailer) runBounded(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			t.closeAll()
			return
		}
		t.scan(ctx)
		read := false
		for _, of := range t.open {
			n, err := t.drain(ctx, of)
			if err != nil {
				of.f.Close()
				delete(t.open, of.key)
				continue
			}
			if n > 0 {
				read = true
			}
		}
		if !read {
			break
		}
	}
	t.flushAll(ctx)
	t.closeAll()
}

// scan reconciles the set of open files with what is on disk: it opens new log
// files (respecting namespace filters), and closes handles whose path vanished.
func (t *tailer) scan(ctx context.Context) {
	seen := map[string]bool{}
	// <root>/<ns>_<pod>_<uid>/<container>/*.log
	matches, _ := filepath.Glob(filepath.Join(t.root, "*", "*", "*.log"))
	for _, path := range matches {
		meta, ok := parsePodPath(t.root, path)
		if !ok {
			continue
		}
		if !t.nsAllowed(meta.namespace) {
			continue
		}
		seen[path] = true
		if _, ok := t.open[path]; ok {
			continue
		}
		if of := t.openFile(path, meta); of != nil {
			t.open[path] = of
		}
	}
	// Close handles whose file was deleted — after reading what it still
	// holds: the open handle keeps the unlinked file readable, and a removed
	// pod's last lines are often written just before its logs are deleted. A
	// path that is only briefly absent while its container directory exists
	// is a rotation in progress: keep the handle (drain follows the rotation).
	for key, of := range t.open {
		if seen[key] {
			of.missingSince = time.Time{}
			continue
		}
		if _, err := os.Stat(filepath.Dir(key)); err == nil {
			if of.missingSince.IsZero() {
				of.missingSince = time.Now()
			}
			if time.Since(of.missingSince) < missingGrace {
				continue
			}
		}
		if _, err := t.drain(ctx, of); err != nil && ctx.Err() != nil {
			return
		}
		t.flushAndClose(ctx, of)
		delete(t.open, key)
		delete(t.delivered, key)
	}
	t.initialScanDone = true
	if t.recon != nil {
		t.recon.setFilesOpen(t.node, len(t.open))
	}
}

func (t *tailer) nsAllowed(ns string) bool {
	if len(t.excludeNS) > 0 && t.excludeNS[ns] {
		return false
	}
	if len(t.includeNS) > 0 {
		return t.includeNS[ns]
	}
	return true
}

// openFile opens a log file and positions it using the persisted registry.
// Rotation is detected by fingerprint mismatch or size regression, in which
// case reading restarts at offset 0 so no rotated-in data is skipped.
func (t *tailer) openFile(path string, meta podMeta) *openLogFile {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	fp := fingerprint(f)

	var start int64
	if saved, ok := t.resumePoint(path); ok && fpMatch(saved.Fingerprint, fp) {
		start = saved.Offset
	} else if ok {
		// A saved position exists but the fingerprint differs — the file at this
		// path is a fresh one (containerd rotates by rename+recreate). If the
		// file the position belongs to is still on disk as an uncompressed
		// rotated sibling, finish it from the saved offset (and any newer
		// rotated siblings) before the live file; otherwise read the live file
		// from the start. Either way no rotated-in data is skipped.
		if t.recon != nil {
			t.recon.addRotation(t.node)
		}
		if of := t.resumeRotated(path, meta, saved); of != nil {
			f.Close()
			return of
		}
	} else if !t.fromBeginning && !t.positions.existed && !t.initialScanDone {
		// fromBeginning=false skips only the history present when the
		// collector first starts on a node (no registry yet). Files that
		// appear later — new pods, container restarts, also while the
		// collector was restarting — are read from their first line.
		if fi, err := f.Stat(); err == nil {
			start = fi.Size()
		}
	}
	if fi, err := f.Stat(); err == nil && start > fi.Size() {
		start = 0 // truncated/rotated since last run
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil
	}
	return &openLogFile{
		key:    path,
		f:      f,
		reader: bufio.NewReaderSize(f, readBufSize),
		offset: start,
		fp:     fp,
		meta:   meta,
		st:     t.newStitchers(),
	}
}

// fpMatch reports whether a file with fingerprint cur is the file a position
// with fingerprint saved was recorded for. A file first seen while shorter
// than fpLen has a shorter saved fingerprint, which must be a prefix of the
// current one.
func fpMatch(saved, cur string) bool {
	return saved == cur || (saved != "" && len(saved) < len(cur) && strings.HasPrefix(cur, saved))
}

// rotatedSiblings returns the rotated files of path in rotation order,
// oldest first. kubelet names them <path>.<YYYYMMDD-hhmmss> and later gzips
// all but the most recent to <path>.<ts>.gz; the order comes from the
// timestamp in the name (a .gz file's mtime is when it was compressed). While
// kubelet compresses, both copies may exist briefly: the uncompressed one is
// listed, so a file is never read twice.
func rotatedSiblings(path string) []string {
	matches, _ := filepath.Glob(path + ".*")
	byTS := map[string]string{}
	for _, m := range matches {
		if strings.HasSuffix(m, ".tmp") {
			continue
		}
		fi, err := os.Stat(m)
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		ts := strings.TrimSuffix(strings.TrimPrefix(m, path+"."), ".gz")
		if prev, ok := byTS[ts]; ok && !strings.HasSuffix(prev, ".gz") {
			continue // keep the uncompressed copy
		}
		byTS[ts] = m
	}
	keys := make([]string, 0, len(byTS))
	for k := range byTS {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ps := make([]string, len(keys))
	for i, k := range keys {
		ps[i] = byTS[k]
	}
	return ps
}

// fingerprintPath fingerprints the (decompressed) content of the file at
// path: a compressed rotation has the same fingerprint as the original.
func fingerprintPath(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if !strings.HasSuffix(path, ".gz") {
		return fingerprint(f)
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		return ""
	}
	defer zr.Close()
	buf := make([]byte, fpLen)
	n, _ := io.ReadFull(zr, buf)
	return hex.EncodeToString(buf[:n])
}

// resumeRotated looks for the rotated sibling the saved position belongs to
// and returns a handle on it at the saved offset, keyed by path; the files
// rotated after it and the live file follow (see advance).
func (t *tailer) resumeRotated(path string, meta podMeta, saved position) *openLogFile {
	for _, sp := range rotatedSiblings(path) {
		if !fpMatch(saved.Fingerprint, fingerprintPath(sp)) {
			continue
		}
		of := t.openFileAt(sp, meta, saved.Offset)
		if of == nil {
			return nil
		}
		of.key = path
		of.rotated = true
		return of
	}
	return nil
}

// advance handles the end of a drained file: when the handle no longer refers
// to the file at its path (rotation), it finishes the old file — its partial
// last line and pending multiline record — and moves on from offset 0 to the
// next file in rotation order: the oldest uncompressed rotated sibling newer
// than the old file (several rotations happened while it was being read),
// else the live file. The order is recomputed each time, so files that
// vanished are skipped and a rotation in the middle is never jumped over. It
// returns true when it switched (the caller keeps draining).
func (t *tailer) advance(ctx context.Context, of *openLogFile) bool {
	old, err := of.f.Stat()
	if err != nil {
		return false
	}
	cur, curErr := os.Stat(of.key)
	if !of.rotated {
		if curErr != nil {
			return false // path gone: rotation in progress, or pod removed
		}
		if os.SameFile(cur, old) {
			of.rotatedSeen = time.Time{}
			return false
		}
		// The runtime may still flush a last write into the renamed file.
		if of.rotatedSeen.IsZero() {
			of.rotatedSeen = time.Now()
			return false
		}
		since := of.rotatedSeen
		if of.lastData.After(since) {
			since = of.lastData
		}
		if time.Since(since) < t.rotationGrace {
			return false
		}
	}
	// Locate the file just finished among the rotated siblings — by identity,
	// else by content (kubelet may have replaced it with its .gz copy) — and
	// take the next one after it. If it is gone (deleted as the oldest), every
	// remaining sibling is newer.
	sibs := rotatedSiblings(of.key)
	at := -1
	for i, sp := range sibs {
		if fi, err := os.Stat(sp); err == nil && os.SameFile(fi, old) {
			at = i
			break
		}
	}
	if at < 0 {
		for i, sp := range sibs {
			if fp := fingerprintPath(sp); fp != "" && (fpMatch(of.fp, fp) || fpMatch(fp, of.fp)) {
				at = i
				break
			}
		}
	}
	next, rotated := "", false
	for _, sp := range sibs[at+1:] {
		fi, err := os.Stat(sp)
		if err != nil || (curErr == nil && os.SameFile(fi, cur)) {
			continue
		}
		next, rotated = sp, true
		break
	}
	if next == "" {
		if curErr != nil || os.SameFile(cur, old) {
			return false // nothing newer yet; scan decides about a vanished path
		}
		next = of.key
	}
	nf := t.openFileAt(next, of.meta, 0)
	if nf == nil {
		return false // vanished meanwhile: the next drain recomputes the order
	}
	t.finish(ctx, of)
	nf.key = of.key
	nf.rotated = rotated
	of.f.Close()
	*of = *nf
	if t.recon != nil {
		t.recon.addRotation(t.node)
	}
	return true
}

// finish emits what a file that will receive no more writes still holds: an
// unterminated last line and the pending multiline record.
func (t *tailer) finish(ctx context.Context, of *openLogFile) {
	if len(of.partial) > 0 {
		full := of.partial
		of.partial = nil
		start := of.offset
		of.offset += int64(len(full))
		if rec, ok := of.st.push(parseCRILine(strings.TrimRight(string(full), "\r\n")), start, of.offset); ok {
			t.emit(ctx, of, rec)
		}
	}
	for _, rec := range of.st.flush() {
		t.emit(ctx, of, rec)
	}
}

// drain reads all currently-available complete lines from one file, stitches
// them into logical records, and emits them. It returns the number of records
// emitted. Rotation detected mid-stream causes a reopen from offset 0.
func (t *tailer) drain(ctx context.Context, of *openLogFile) (int, error) {
	if fi, err := of.f.Stat(); err == nil && !of.gz && fi.Size() < of.offset {
		// File was truncated or replaced in place — reopen from the start.
		if t.recon != nil {
			t.recon.addRotation(t.node)
		}
		// Open the replacement handle BEFORE closing the current one: if the
		// reopen fails, the existing handle is left intact so the caller
		// (run/runBounded) closes it exactly once on the returned error — this
		// avoids double-closing an already-closed handle (the old code closed
		// first, then the caller closed again on the io.EOF). Two fds briefly
		// coexist for the same path, which is harmless.
		reopened := t.openFileAt(of.key, of.meta, 0)
		if reopened == nil {
			return 0, io.EOF
		}
		of.f.Close()
		*of = *reopened
	}

	// A file first seen shorter than fpLen got a short fingerprint; complete
	// it once the file has grown so positions and idempotency keys use the
	// same stable fingerprint a restarted collector computes.
	if !of.gz && len(of.fp) < 2*fpLen {
		if fi, err := of.f.Stat(); err == nil && fi.Size() >= fpLen {
			of.fp = fingerprint(of.f)
		}
	}

	emitted := 0
	for {
		line, err := of.reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] != '\n' {
			// Incomplete trailing line — accumulate it (it may span several
			// drains) and stop without advancing the offset.
			of.partial = append(of.partial, line...)
			if t.maxLineBytes <= 0 || len(of.partial) < t.maxLineBytes {
				if t.advance(ctx, of) {
					continue
				}
				return emitted, nil
			}
			// Oversized unterminated line: emit what we have as one line.
			line = nil
		} else if err == io.EOF {
			if t.advance(ctx, of) {
				continue
			}
			return emitted, nil
		} else if err != nil {
			return emitted, err
		}
		full := line
		if len(of.partial) > 0 {
			full = append(of.partial, line...)
		}
		// Advance by every byte of the logical line, including the partial
		// chunks consumed by earlier drains.
		lineStart := of.offset
		of.offset += int64(len(full))
		text := strings.TrimRight(string(full), "\r\n")
		if cap(of.partial) > 64<<10 {
			of.partial = nil // don't pin a large one-off buffer
		} else {
			of.partial = of.partial[:0]
		}

		cri := parseCRILine(text)
		of.lastData = time.Now()
		rec, ok := of.st.push(cri, lineStart, of.offset)
		if !ok {
			continue
		}
		if !t.emit(ctx, of, rec) {
			return emitted, ctx.Err()
		}
		emitted++
	}
}

func (t *tailer) emit(ctx context.Context, of *openLogFile, rec logRecord) bool {
	er := enrichedRecord{
		meta:      of.meta,
		stream:    rec.stream,
		timestamp: rec.timestamp,
		message:   rec.message,
		fileKey:   of.key,
		endOffset: rec.endOffset,
		// Positions may only advance to the first byte still buffered in
		// any stream's stitcher (another stream may hold earlier lines).
		commitOffset: of.st.safeOffset(rec.endOffset),
		fp:           of.fp,
	}
	select {
	case t.out <- er:
		if t.delivered == nil {
			t.delivered = map[string]position{}
		}
		if d, ok := t.delivered[of.key]; !ok || d.Fingerprint != of.fp || er.commitOffset > d.Offset {
			t.delivered[of.key] = position{Offset: er.commitOffset, Fingerprint: of.fp}
		}
		return true
	case <-ctx.Done():
		return false
	}
}

// resumePoint returns the position to resume path from: the in-process
// delivered position when it is ahead of the persisted one for the same file.
func (t *tailer) resumePoint(path string) (position, bool) {
	saved, ok := t.positions.get(path)
	if d, dok := t.delivered[path]; dok && (!ok || d.Fingerprint != saved.Fingerprint || d.Offset > saved.Offset) {
		return d, true
	}
	return saved, ok
}

func (t *tailer) openFileAt(path string, meta podMeta, start int64) *openLogFile {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	of := &openLogFile{
		key:    path,
		f:      f,
		offset: start,
		meta:   meta,
		st:     t.newStitchers(),
	}
	if strings.HasSuffix(path, ".gz") {
		// A kubelet-compressed rotation: read it decompressed. Offsets and
		// the fingerprint refer to the decompressed content, i.e. the same
		// values as for the original file.
		zr, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil
		}
		if _, err := io.CopyN(io.Discard, zr, start); err != nil && start > 0 {
			f.Close()
			return nil
		}
		of.gz = true
		of.fp = fingerprintPath(path)
		of.reader = bufio.NewReaderSize(zr, readBufSize)
		return of
	}
	of.fp = fingerprint(f)
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil
	}
	of.reader = bufio.NewReaderSize(f, readBufSize)
	return of
}

// flushAndClose emits a deleted file's pending stitched record, then closes
// it. The emit blocks until the consumer accepts it (or ctx is cancelled):
// dropping it when the output buffer is momentarily full would silently lose
// the file's last record.
func (t *tailer) flushAndClose(ctx context.Context, of *openLogFile) {
	t.finish(ctx, of)
	of.f.Close()
}

func (t *tailer) closeAll() {
	for key, of := range t.open {
		of.f.Close()
		delete(t.open, key)
	}
}

// flushAll emits any pending stitched record still buffered in each open file's
// stitcher. Called on shutdown so a trailing multiline record (e.g. the last
// stack trace) is delivered rather than dropped.
func (t *tailer) flushAll(ctx context.Context) {
	for _, of := range t.open {
		for _, rec := range of.st.flush() {
			if !t.emit(ctx, of, rec) {
				return
			}
		}
	}
}

// fingerprint returns a hex string of the first fpLen bytes of the file,
// leaving the read offset unchanged.
func fingerprint(f *os.File) string {
	buf := make([]byte, fpLen)
	n, _ := f.ReadAt(buf, 0)
	return hex.EncodeToString(buf[:n])
}

// parsePodPath extracts pod metadata from a CRI log file path.
func parsePodPath(root, path string) (podMeta, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return podMeta{}, false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 3 {
		return podMeta{}, false
	}
	podDir := parts[0]
	container := parts[1]
	fields := strings.SplitN(podDir, "_", 3)
	if len(fields) != 3 {
		return podMeta{}, false
	}
	return podMeta{
		namespace: fields[0],
		pod:       fields[1],
		uid:       fields[2],
		container: container,
	}, true
}
