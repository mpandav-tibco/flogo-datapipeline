package httpretry

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBackoffBoundsAndGrowth(t *testing.T) {
	base, maxD := 100*time.Millisecond, time.Second
	for attempt := 1; attempt <= 8; attempt++ {
		nominal := base << (attempt - 1)
		if nominal > maxD {
			nominal = maxD
		}
		for i := 0; i < 200; i++ {
			d := Backoff(attempt, base, maxD)
			if d < nominal/2 || d > nominal {
				t.Fatalf("attempt %d: %v outside [%v,%v]", attempt, d, nominal/2, nominal)
			}
		}
	}
	if Backoff(3, 0, maxD) != 0 {
		t.Fatal("zero base must not wait")
	}
}

func TestRetryAfter(t *testing.T) {
	mk := func(v string) *http.Response { return &http.Response{Header: http.Header{"Retry-After": []string{v}}} }
	if d, ok := RetryAfter(mk("2"), time.Minute); !ok || d != 2*time.Second {
		t.Fatalf("seconds: %v %v", d, ok)
	}
	if d, ok := RetryAfter(mk("3600"), 5*time.Second); !ok || d != 5*time.Second {
		t.Fatalf("cap: %v %v", d, ok)
	}
	future := time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)
	if d, ok := RetryAfter(mk(future), time.Minute); !ok || d <= 0 || d > 3*time.Second {
		t.Fatalf("date: %v %v", d, ok)
	}
	if _, ok := RetryAfter(mk("soon"), time.Minute); ok {
		t.Fatal("garbage accepted")
	}
	if _, ok := RetryAfter(&http.Response{Header: http.Header{}}, time.Minute); ok {
		t.Fatal("absent header accepted")
	}
}

func TestSleepCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if Sleep(ctx, time.Hour) || time.Since(start) > time.Second {
		t.Fatal("Sleep ignored cancellation")
	}
}

func TestReadLimited(t *testing.T) {
	b, trunc, err := ReadLimited(strings.NewReader("abcdef"), 4)
	if err != nil || string(b) != "abcd" || !trunc {
		t.Fatalf("%q %v %v", b, trunc, err)
	}
	b, trunc, _ = ReadLimited(strings.NewReader("ab"), 4)
	if string(b) != "ab" || trunc {
		t.Fatalf("%q %v", b, trunc)
	}
}
