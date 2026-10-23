// Package httpretry holds the retry policy shared by the REST source and sink:
// capped exponential backoff with jitter, Retry-After support, cancellable
// sleeps and bounded body reads.
package httpretry

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// DefaultMaxDelay caps any single wait, including server-requested ones.
const DefaultMaxDelay = 30 * time.Second

// Backoff returns the wait before retry number attempt (1-based): base·2^(attempt-1)
// capped at maxDelay, with "equal jitter" — a uniform pick in [d/2, d] — so
// many clients failing together don't retry in lockstep, while still keeping
// at least half the nominal spacing.
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if maxDelay <= 0 {
		maxDelay = DefaultMaxDelay
	}
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}

// RetryAfter parses a Retry-After header (delta-seconds or HTTP-date), capped
// at maxDelay. ok is false when the header is absent or unparseable.
func RetryAfter(resp *http.Response, maxDelay time.Duration) (time.Duration, bool) {
	if resp == nil {
		return 0, false
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if maxDelay <= 0 {
		maxDelay = DefaultMaxDelay
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		d = time.Duration(secs) * time.Second
	} else if at, err := http.ParseTime(v); err == nil {
		d = time.Until(at)
	} else {
		return 0, false
	}
	if d < 0 {
		d = 0
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d, true
}

// Delay picks the wait before retry number attempt: the server's Retry-After
// when present, otherwise jittered exponential backoff.
func Delay(attempt int, resp *http.Response, base, maxDelay time.Duration) time.Duration {
	if d, ok := RetryAfter(resp, maxDelay); ok {
		return d
	}
	return Backoff(attempt, base, maxDelay)
}

// Sleep waits for d or until ctx is done, reporting whether the full wait
// elapsed.
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ReadLimited reads at most n bytes of r and reports whether more remained.
func ReadLimited(r io.Reader, n int64) (data []byte, truncated bool, err error) {
	data, err = io.ReadAll(io.LimitReader(r, n+1))
	if int64(len(data)) > n {
		return data[:n], true, err
	}
	return data, false, err
}

// Drain discards up to 64 KiB of a response body and closes it, so the
// underlying keep-alive connection can be reused.
func Drain(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
	_ = resp.Body.Close()
}
