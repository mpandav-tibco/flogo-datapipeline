package podlog

import (
	"strings"
)

// criLine is a single physical line parsed from a containerd/CRI-O log file.
//
// The CRI log format (used by containerd and CRI-O) is one line per entry:
//
//	<RFC3339Nano timestamp> <stream> <tag> <message>
//
// e.g.
//
//		2026-07-14T10:00:00.123456789Z stdout F Started BW6 engine
//		2026-07-14T10:00:00.200000000Z stderr P java.lang.RuntimeException: boom
//
//	  - stream is "stdout" or "stderr".
//	  - tag is "F" (full/final line) or "P" (partial — the runtime split a long
//	    line; the following line(s) continue it until an "F" tag is seen).
type criLine struct {
	timestamp string // RFC3339Nano, as emitted by the runtime
	stream    string // stdout | stderr
	partial   bool   // true when tag == "P"
	message   string // the log payload (no trailing newline)
	valid     bool   // false when the line could not be parsed as CRI
}

// parseCRILine parses a single physical line (without trailing newline) in the
// containerd/CRI-O log format. If the line does not match the format it is
// returned with valid=false and the raw line preserved as the message, so no
// data is ever dropped silently.
func parseCRILine(raw string) criLine {
	// Expected: timestamp SP stream SP tag SP message
	// Split into at most 4 fields so the message keeps its own spaces.
	first := strings.IndexByte(raw, ' ')
	if first < 0 {
		return criLine{message: raw, valid: false}
	}
	ts := raw[:first]
	rest := raw[first+1:]

	second := strings.IndexByte(rest, ' ')
	if second < 0 {
		return criLine{timestamp: ts, message: rest, valid: false}
	}
	stream := rest[:second]
	rest = rest[second+1:]

	third := strings.IndexByte(rest, ' ')
	var tag, msg string
	if third < 0 {
		// Tag with an empty message (rare but legal).
		tag = rest
		msg = ""
	} else {
		tag = rest[:third]
		msg = rest[third+1:]
	}

	if stream != "stdout" && stream != "stderr" {
		return criLine{timestamp: ts, message: raw, valid: false}
	}
	if tag != "F" && tag != "P" {
		return criLine{timestamp: ts, stream: stream, message: raw, valid: false}
	}

	return criLine{
		timestamp: ts,
		stream:    stream,
		partial:   tag == "P",
		message:   msg,
		valid:     true,
	}
}

// logRecord is one logical log entry, after CRI partial-line reassembly and
// (optionally) multiline stitching of continuation lines.
type logRecord struct {
	timestamp string
	stream    string
	message   string
	// endOffset is the file byte offset immediately after the LAST physical line
	// that belongs to this logical record. This is the offset that may be
	// durably committed once this record is written — never the file's current
	// read position, which for multiline records has already advanced past the
	// next record's first line. Committing the current position instead would
	// skip an unwritten record on resume (silent loss).
	endOffset int64
}

// stitcher reassembles physical CRI lines into logical log records.
//
// Two levels of joining are performed:
//
//  1. CRI partial lines: consecutive lines tagged "P" are concatenated (no
//     separator) until an "F" line completes the entry. This is the runtime's
//     own mechanism for lines that exceed its 16KiB buffer.
//
//  2. Application multiline (optional): when startPattern is set, a completed
//     CRI entry whose message does NOT match startPattern is treated as a
//     continuation of the previous logical record and appended with a newline.
//     This is what stitches BW6 / Java stack traces (whose subsequent lines
//     each arrive as their own "F" CRI line) back into a single record.
//
// The stitcher is single-goroutine; one instance is kept per open file.
type stitcher struct {
	startPattern matcher

	// CRI partial-line accumulator.
	partialBuf strings.Builder
	partialTS  string
	partialStr string
	partialEnd int64
	inPartial  bool

	// Application-multiline accumulator (the pending logical record).
	pending    *logRecord
	pendingBuf strings.Builder
	// pendingEnd is the file offset immediately after the last physical line
	// folded into the pending record. It becomes the completed record's
	// endOffset when the record is flushed.
	pendingEnd int64
	// maxLines caps the number of physical lines stitched into one logical
	// record (0 = unlimited). When the cap is hit mid-record the record is
	// emitted early and the next line opens a fresh record, bounding memory for
	// a runaway stack trace.
	maxLines int
	// pendingLines counts the physical lines folded into the pending record so
	// far (1 for the start line, +1 per continuation).
	pendingLines int
}

// matcher abstracts the start-of-record test so the tailer can inject a
// compiled regexp (or a nil no-op matcher when multiline stitching is off).
type matcher interface {
	MatchString(s string) bool
}

// push feeds one physical CRI line into the stitcher. lineEnd is the file byte
// offset immediately after this physical line. It returns zero or one completed
// logical record, whose endOffset is the offset after that record's OWN last
// line (not the current read position). A record is emitted when:
//   - a non-partial CRI entry completes and multiline stitching is off, or
//   - a new start-of-record line arrives, flushing the previously pending one.
//
// Records still buffered when the file goes idle are released via flush().
func (s *stitcher) push(line criLine, lineEnd int64) (logRecord, bool) {
	// --- level 1: reassemble CRI partial lines ---
	if line.partial {
		if !s.inPartial {
			s.inPartial = true
			s.partialTS = line.timestamp
			s.partialStr = line.stream
			s.partialBuf.Reset()
		}
		s.partialBuf.WriteString(line.message)
		s.partialEnd = lineEnd
		return logRecord{}, false
	}
	if s.inPartial {
		s.partialBuf.WriteString(line.message)
		line.message = s.partialBuf.String()
		line.timestamp = s.partialTS
		line.stream = s.partialStr
		s.inPartial = false
		s.partialBuf.Reset()
	}

	entry := logRecord{timestamp: line.timestamp, stream: line.stream, message: line.message, endOffset: lineEnd}

	// --- level 2: application multiline stitching ---
	if s.startPattern == nil {
		return entry, true
	}

	if s.pending == nil {
		// First entry — becomes the pending record regardless of pattern.
		s.startPending(entry, lineEnd)
		return logRecord{}, false
	}

	if s.startPattern.MatchString(entry.message) {
		// A new logical record begins: flush the pending one (with the offset of
		// ITS last line), then buffer this line as the new pending record.
		completed := s.finishPending()
		s.startPending(entry, lineEnd)
		return completed, true
	}

	// Continuation line: append to the pending record and extend its offset.
	s.pendingBuf.WriteByte('\n')
	s.pendingBuf.WriteString(entry.message)
	s.pendingEnd = lineEnd
	s.pendingLines++
	if s.maxLines > 0 && s.pendingLines >= s.maxLines {
		// Safety cap reached: emit the (capped) record now. The next line opens a
		// fresh pending record, so overflow lines are not lost.
		return s.finishPending(), true
	}
	return logRecord{}, false
}

func (s *stitcher) startPending(entry logRecord, lineEnd int64) {
	rec := entry
	s.pending = &rec
	s.pendingBuf.Reset()
	s.pendingBuf.WriteString(entry.message)
	s.pendingEnd = lineEnd
	s.pendingLines = 1
}

func (s *stitcher) finishPending() logRecord {
	rec := *s.pending
	rec.message = s.pendingBuf.String()
	rec.endOffset = s.pendingEnd
	s.pending = nil
	s.pendingBuf.Reset()
	return rec
}

// flush returns any record still buffered (a trailing multiline record, or a
// dangling CRI partial line). Called when a file is closed or has been idle
// long enough that further continuation lines are not expected.
func (s *stitcher) flush() (logRecord, bool) {
	if s.inPartial {
		rec := logRecord{
			timestamp: s.partialTS,
			stream:    s.partialStr,
			message:   s.partialBuf.String(),
			endOffset: s.partialEnd,
		}
		s.inPartial = false
		s.partialBuf.Reset()
		if s.pending == nil {
			return rec, true
		}
		// Extremely rare: dangling partial after a pending multiline record.
		s.pendingBuf.WriteByte('\n')
		s.pendingBuf.WriteString(rec.message)
		s.pendingEnd = s.partialEnd
	}
	if s.pending != nil {
		return s.finishPending(), true
	}
	return logRecord{}, false
}
