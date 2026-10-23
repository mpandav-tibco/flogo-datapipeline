package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/httpretry"
	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
)

// resumableReader streams one object and survives a dropped connection (store
// restart, idle-timeout reset while the pipeline applied backpressure, network
// blip) by re-opening the object with a ranged GET from the exact byte offset
// already delivered. Without it, a streamed object failed the whole run on any
// mid-object disconnect — the one risk streaming adds over spooling, where the
// download completes before parsing starts.
//
// Resumption is pinned to the object version listed (ETag) via
// VersionedRangeReader, so an object overwritten mid-read fails the read
// instead of splicing two versions. Only transport errors trigger it: a clean
// io.EOF is the end of the stream (net/http reports a body cut short of its
// Content-Length as io.ErrUnexpectedEOF), and nothing is resumed once the
// listed size has been delivered.
type resumableReader struct {
	ctx      context.Context
	rr       objstore.VersionedRangeReader
	key      string
	etag     string
	size     int64 // known object size; resumption needs it
	off      int64 // bytes delivered so far
	rc       io.ReadCloser
	attempts int // consecutive failed resumptions (reset on progress)

	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

var resumeLog = corelog.ChildLogger(corelog.RootLogger(), "objstore")

func newResumableReader(ctx context.Context, rr objstore.VersionedRangeReader, key, etag string, size int64, rc io.ReadCloser) *resumableReader {
	return &resumableReader{
		ctx: ctx, rr: rr, key: key, etag: etag, size: size, rc: rc,
		maxAttempts: 6, baseDelay: 500 * time.Millisecond, maxDelay: 8 * time.Second,
	}
}

func (r *resumableReader) Read(p []byte) (int, error) {
	for {
		if r.rc != nil {
			n, err := r.rc.Read(p)
			r.off += int64(n)
			if n > 0 {
				r.attempts = 0
			}
			switch {
			case err == nil:
				return n, nil
			case err == io.EOF:
				return n, io.EOF
			case r.off >= r.size:
				// Everything listed was delivered; nothing to resume.
				return n, err
			}
			// A transport error mid-object: resume from r.off.
			resumeLog.Warnf("object %q: stream interrupted at byte %d of %d (%v); resuming with a ranged read", r.key, r.off, r.size, err)
			_ = r.rc.Close()
			r.rc = nil
			if n > 0 {
				return n, nil // deliver what we have; reconnect on next Read
			}
			if r.ctx.Err() != nil {
				return 0, r.ctx.Err()
			}
		}
		if r.attempts >= r.maxAttempts {
			return 0, fmt.Errorf("object %q: stream interrupted at byte %d of %d; gave up after %d resume attempts", r.key, r.off, r.size, r.attempts)
		}
		r.attempts++
		if !httpretry.Sleep(r.ctx, httpretry.Backoff(r.attempts, r.baseDelay, r.maxDelay)) {
			return 0, r.ctx.Err()
		}
		rc, err := r.rr.GetRangeIfMatch(r.ctx, r.key, r.etag, r.off, r.size-r.off)
		if errors.Is(err, objstore.ErrObjectChanged) {
			return 0, fmt.Errorf("object %q: cannot resume at byte %d: %w", r.key, r.off, err)
		}
		if err != nil {
			resumeLog.Warnf("object %q: resume attempt %d/%d failed: %v", r.key, r.attempts, r.maxAttempts, err)
			continue // counts as a failed attempt; back off and retry
		}
		resumeLog.Infof("object %q: stream resumed at byte %d", r.key, r.off)
		r.rc = rc
	}
}

func (r *resumableReader) Close() error {
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}
