package podlog

import (
	"bufio"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// fpLen is the number of leading bytes hashed into a file fingerprint used to
// detect rotation (a fresh file at the same path has different first bytes).
const fpLen = 256

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
	endOffset int64
	fp        string
}

// openLogFile is an actively tailed CRI log file.
type openLogFile struct {
	key    string // == path
	f      *os.File
	reader *bufio.Reader
	offset int64
	fp     string
	meta   podMeta
	st     *stitcher
	// partial holds bytes read past the last newline (an incomplete final line).
	// Its length is NOT added to offset until the line is completed, so a crash
	// never checkpoints a half-written line.
	partial []byte
	// lastData is when this file last produced a complete line; used to flush a
	// pending multiline record once the file has been idle for flushIdle.
	lastData time.Time
}

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
	// flushIdle is how long a file may be idle before a pending stitched
	// (multiline) record is emitted so trailing records are not held forever.
	flushIdle time.Duration
	// maxLines caps stitched lines per logical record (0 = unlimited); passed to
	// each file's stitcher.
	maxLines int
	// bounded runs a single scan → drain → flush → close pass instead of an
	// unbounded poll loop. Used by tests and batch/drain scenarios so ReadBatch
	// terminates deterministically with io.EOF.
	bounded bool
	// recon receives files-open and rotation counts for reconciliation metrics.
	// May be nil (metrics disabled path in tests).
	recon *reconCounters

	open map[string]*openLogFile
	out  chan enrichedRecord
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
		t.scan()
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
					if rec, ok := of.st.flush(); ok {
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
		t.scan()
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
func (t *tailer) scan() {
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
	// Close handles whose file was deleted.
	for key, of := range t.open {
		if !seen[key] {
			t.flushAndClose(of)
			delete(t.open, key)
		}
	}
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
	if saved, ok := t.positions.get(path); ok && saved.Fingerprint == fp {
		start = saved.Offset
	} else if ok {
		// A saved position exists but the fingerprint differs — the file at this
		// path is a fresh one (containerd rotates by rename+recreate). Read from
		// the start so no rotated-in data is skipped, and count the rotation.
		if t.recon != nil {
			t.recon.addRotation(t.node)
		}
	} else if !t.fromBeginning {
		// Brand-new file and configured to skip history: start at end.
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
		reader: bufio.NewReaderSize(f, 1<<20),
		offset: start,
		fp:     fp,
		meta:   meta,
		st:     &stitcher{startPattern: t.startPattern, maxLines: t.maxLines},
	}
}

// drain reads all currently-available complete lines from one file, stitches
// them into logical records, and emits them. It returns the number of records
// emitted. Rotation detected mid-stream causes a reopen from offset 0.
func (t *tailer) drain(ctx context.Context, of *openLogFile) (int, error) {
	if fi, err := of.f.Stat(); err == nil && fi.Size() < of.offset {
		// File was truncated or replaced in place — reopen from the start.
		if t.recon != nil {
			t.recon.addRotation(t.node)
		}
		of.f.Close()
		reopened := t.openFileAt(of.key, of.meta, 0)
		if reopened == nil {
			return 0, io.EOF
		}
		*of = *reopened
	}

	emitted := 0
	for {
		line, err := of.reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] != '\n' {
			// Incomplete trailing line — stash it and stop without advancing.
			of.partial = append(of.partial[:0], line...)
			return emitted, nil
		}
		if err == io.EOF {
			return emitted, nil
		}
		if err != nil {
			return emitted, err
		}
		full := line
		if len(of.partial) > 0 {
			full = append(of.partial, line...)
			of.partial = of.partial[:0]
		}
		of.offset += int64(len(line)) // advance only by newly consumed bytes

		text := strings.TrimRight(string(full), "\r\n")
		cri := parseCRILine(text)
		of.lastData = time.Now()
		rec, ok := of.st.push(cri, of.offset)
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
		fp:        of.fp,
	}
	select {
	case t.out <- er:
		return true
	case <-ctx.Done():
		return false
	}
}

func (t *tailer) openFileAt(path string, meta podMeta, start int64) *openLogFile {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	fp := fingerprint(f)
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil
	}
	return &openLogFile{
		key:    path,
		f:      f,
		reader: bufio.NewReaderSize(f, 1<<20),
		offset: start,
		fp:     fp,
		meta:   meta,
		st:     &stitcher{startPattern: t.startPattern, maxLines: t.maxLines},
	}
}

func (t *tailer) flushAndClose(of *openLogFile) {
	if rec, ok := of.st.flush(); ok {
		// Best-effort emit of a trailing record; ignore ctx here.
		select {
		case t.out <- enrichedRecord{
			meta: of.meta, stream: rec.stream, timestamp: rec.timestamp,
			message: rec.message, fileKey: of.key, endOffset: rec.endOffset, fp: of.fp,
		}:
		default:
		}
	}
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
		if rec, ok := of.st.flush(); ok {
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
