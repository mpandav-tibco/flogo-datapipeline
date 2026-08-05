package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
)

// servedHandler returns the actual handler Start() would serve (mux wrapped by
// the auth middleware) — s.mux.ServeHTTP alone bypasses auth by design (see
// requireAuth's doc comment), so auth tests must go through s.httpSrv.Handler.
func servedHandler(s *Server) http.Handler { return s.httpSrv.Handler }

func newAuthTestServer(t *testing.T) *Server {
	t.Helper()
	f := &pipeline.Factory{}
	def := mustDef(t, `{"source":{"type":"csv","options":{"path":"/tmp/a.csv"}},"sink":{"type":"null"}}`)
	if _, err := f.Replace("res://pipeline:demo", def); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}
	return NewServer(f, ":0")
}

func TestAPIAuth_DisabledByDefault(t *testing.T) {
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (no DP_API_TOKEN configured -> auth disabled)", w.Code)
	}
}

func TestAPIAuth_HealthAlwaysExempt(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "s3cr3t")
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (/v1/health must never require auth)", w.Code)
	}
}

func TestAPIAuth_MissingCredentialRejected(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "s3cr3t")
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (no credential presented)", w.Code)
	}
	if got := w.Header().Get("WWW-Authenticate"); got == "" {
		t.Error("expected a WWW-Authenticate header on 401")
	}
}

func TestAPIAuth_WrongCredentialRejected(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "s3cr3t")
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (wrong bearer token)", w.Code)
	}
}

func TestAPIAuth_BearerTokenAccepted(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "s3cr3t")
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
	req.Header.Set("Authorization", "Bearer s3cr3t")
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (correct bearer token)", w.Code)
	}
}

func TestAPIAuth_ApiKeyHeaderAccepted(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "s3cr3t")
	s := newAuthTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
	req.Header.Set("X-Api-Key", "s3cr3t")
	w := httptest.NewRecorder()
	servedHandler(s).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (correct X-Api-Key)", w.Code)
	}
}

func TestAPIAuth_RotationAcceptsEitherToken(t *testing.T) {
	t.Setenv("DP_API_TOKEN", "old-token, new-token")
	s := newAuthTestServer(t)

	for _, tok := range []string{"old-token", "new-token"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/pipelines", nil)
		req.Header.Set("X-Api-Key", tok)
		w := httptest.NewRecorder()
		servedHandler(s).ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("token %q: status = %d, want 200", tok, w.Code)
		}
	}
}

func TestParseAPITokens(t *testing.T) {
	cases := map[string][]string{
		"":           nil,
		"a":          {"a"},
		"a,b":        {"a", "b"},
		" a , b ,, ": {"a", "b"},
	}
	for in, want := range cases {
		got := parseAPITokens(in)
		if len(got) != len(want) {
			t.Errorf("parseAPITokens(%q) = %v, want %v", in, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("parseAPITokens(%q) = %v, want %v", in, got, want)
				break
			}
		}
	}
}
