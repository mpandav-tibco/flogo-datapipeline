package api

import (
	"net/http"
	"strings"
	"testing"
)

// TestJSONRequestBodySizeLimit proves limitBody's http.MaxBytesReader wrapping
// actually cuts off an oversized JSON request body with a clear 400 error,
// rather than letting json.Decode buffer an unbounded body into memory — a
// real memory-exhaustion DoS vector that existed before this guard because
// http.Server's ReadTimeout only bounds how LONG a read may take, not how
// much data it may transfer (a fast connection can send many MB well within
// 30s). Exercised via /v1/introspect/xml, but the fix (limitBody) is shared by
// every JSON-decoding handler in this package.
func TestJSONRequestBodySizeLimit(t *testing.T) {
	s := newTestServerWithPipeline(t)

	oversized := `{"path":"` + strings.Repeat("a", maxJSONRequestBytes+1) + `"}`
	w := do(t, s, http.MethodPost, "/v1/introspect/xml", oversized)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body too large); body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(strings.ToLower(w.Body.String()), "too large") {
		t.Errorf("error body = %q, want a message indicating the request body was too large", w.Body.String())
	}
}
