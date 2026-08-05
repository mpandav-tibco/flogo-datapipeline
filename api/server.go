package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	corelog "github.com/project-flogo/core/support/log"

	"github.com/mpandav-tibco/flogo-datapipeline/action/pipeline"
)

var slog = corelog.ChildLogger(corelog.RootLogger(), "pipeline.api")

// ringBuffer is a fixed-capacity circular buffer of PipelineRunResponse records.
// It is safe for concurrent access.
type ringBuffer struct {
	mu   sync.Mutex
	buf  []PipelineRunResponse
	cap  int
	head int // index of the oldest entry
	size int // number of valid entries
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{
		buf: make([]PipelineRunResponse, capacity),
		cap: capacity,
	}
}

// push appends a run result, evicting the oldest entry when the buffer is full.
func (r *ringBuffer) push(resp PipelineRunResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size < r.cap {
		r.buf[(r.head+r.size)%r.cap] = resp
		r.size++
	} else {
		// Overwrite oldest entry and advance head.
		r.buf[r.head] = resp
		r.head = (r.head + 1) % r.cap
	}
}

// snapshot returns all entries ordered from oldest to newest.
func (r *ringBuffer) snapshot() []PipelineRunResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PipelineRunResponse, r.size)
	for i := 0; i < r.size; i++ {
		out[i] = r.buf[(r.head+i)%r.cap]
	}
	return out
}

// latest returns the most recent entry, or the zero value and false when empty.
func (r *ringBuffer) latest() (PipelineRunResponse, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return PipelineRunResponse{}, false
	}
	idx := (r.head + r.size - 1) % r.cap
	return r.buf[idx], true
}

// cancelEntry tracks an in-flight run's cancel function.
type cancelEntry struct {
	cancel context.CancelFunc
	ref    string
}

// Server is the Pipeline Management REST API server.
// It is constructed via NewServer and started via Start.
type Server struct {
	factory    *pipeline.Factory
	listenAddr string
	mux        *http.ServeMux
	httpSrv    *http.Server

	// rings is a per-ref ring buffer of run results (capacity ringCap).
	ringsMu sync.RWMutex
	rings   map[string]*ringBuffer

	// running tracks the current status of each pipeline ref.
	// Values are "idle" | "running" | "completed" | "partial" | "failed".
	runningMu sync.RWMutex
	running   map[string]string

	// cancels maps runId -> cancelEntry so DELETE /…/run/{runId} can cancel in-flight runs.
	cancels sync.Map // map[string]*cancelEntry

	// tokens holds the configured API credentials (from DP_API_TOKEN), each
	// compared in constant time. Empty means authentication is disabled — the
	// server's historical, still-default behavior. See requireAuth.
	tokens []string
}

const ringCap = 100

// apiTokenEnvVar is the environment variable that opts the management API into
// authentication. Unset or empty (the default) leaves the API open, matching
// the documented "bind it to loopback or a trusted network" guidance in
// docs/security.md. Set it to enable a shared-secret gate — a comma-separated
// list of tokens is accepted so a credential can be rotated without downtime
// (add the new token, roll callers over, then remove the old one).
const apiTokenEnvVar = "DP_API_TOKEN"

// NewServer constructs a Server that shares the factory's action cache.
// listenAddr is the TCP address to listen on (e.g. ":8888").
//
// If the DP_API_TOKEN environment variable is set (non-empty, optionally
// comma-separated for rotation), every request except GET /v1/health must
// present a matching credential via "Authorization: Bearer <token>" or
// "X-Api-Key: <token>" — otherwise the API remains open, as documented in
// docs/security.md. This is a shared-secret gate, not RBAC/multi-tenancy; for
// anything beyond that, front the API with a real gateway.
func NewServer(factory *pipeline.Factory, listenAddr string) *Server {
	s := &Server{
		factory:    factory,
		listenAddr: listenAddr,
		rings:      make(map[string]*ringBuffer),
		running:    make(map[string]string),
		tokens:     parseAPITokens(os.Getenv(apiTokenEnvVar)),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/pipelines", s.handleListPipelines)
	mux.HandleFunc("/v1/pipelines/", s.handlePipelinesPrefix)
	mux.HandleFunc("/v1/introspect/tables", s.handleIntrospectTables)
	mux.HandleFunc("/v1/introspect/columns", s.handleIntrospectColumns)
	mux.HandleFunc("/v1/introspect/source-schema", s.handleIntrospectSourceSchema)
	mux.HandleFunc("/v1/introspect/objects", s.handleIntrospectObjects)
	mux.HandleFunc("/v1/introspect/kafka-topics", s.handleIntrospectKafkaTopics)
	mux.HandleFunc("/v1/introspect/schema-registry", s.handleIntrospectSchemaRegistry)
	mux.HandleFunc("/v1/introspect/xml", s.handleIntrospectXML)
	s.mux = mux
	s.httpSrv = &http.Server{
		Addr:         listenAddr,
		Handler:      s.requireAuth(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // unbounded — synchronous runs may take arbitrarily long
		IdleTimeout:  120 * time.Second,
	}
	return s
}

// parseAPITokens splits a comma-separated credential list, trimming whitespace
// and dropping empty segments. A nil/empty result means auth is disabled.
func parseAPITokens(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	for _, tok := range strings.Split(raw, ",") {
		if tok = strings.TrimSpace(tok); tok != "" {
			out = append(out, tok)
		}
	}
	return out
}

// requireAuth wraps next with the DP_API_TOKEN shared-secret check. GET
// /v1/health always passes through unauthenticated (so container/K8s liveness
// probes never need a credential). When no tokens are configured, every
// request passes through — the historical, still-default behavior.
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.tokens) == 0 || r.URL.Path == "/v1/health" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.hasValidToken(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="flogo-datapipeline"`)
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API credential")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// hasValidToken extracts a credential from "Authorization: Bearer <token>" or
// "X-Api-Key: <token>" and compares it, in constant time, against every
// configured token. Constant-time comparison avoids leaking how many leading
// characters matched through response-timing side channels.
func (s *Server) hasValidToken(r *http.Request) bool {
	cred := r.Header.Get("X-Api-Key")
	if cred == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			cred = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	if cred == "" {
		return false
	}
	for _, tok := range s.tokens {
		if subtle.ConstantTimeCompare([]byte(cred), []byte(tok)) == 1 {
			return true
		}
	}
	return false
}

// Start registers all routes and begins serving HTTP in the calling goroutine.
// The caller is responsible for running Start in a separate goroutine.
// Start returns a non-nil error if ListenAndServe fails (excluding ErrServerClosed).
func (s *Server) Start() error {
	slog.Infof("pipeline API server listening on %s", s.listenAddr)
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("pipeline API server: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the HTTP server with the given context deadline.
func (s *Server) Stop(ctx context.Context) error {
	slog.Info("pipeline API server shutting down")
	return s.httpSrv.Shutdown(ctx)
}

// ringFor returns (creating if necessary) the ring buffer for a given ref.
func (s *Server) ringFor(ref string) *ringBuffer {
	s.ringsMu.RLock()
	rb, ok := s.rings[ref]
	s.ringsMu.RUnlock()
	if ok {
		return rb
	}
	s.ringsMu.Lock()
	defer s.ringsMu.Unlock()
	if rb, ok = s.rings[ref]; ok {
		return rb
	}
	rb = newRingBuffer(ringCap)
	s.rings[ref] = rb
	return rb
}

// setStatus atomically records the lifecycle status for a ref.
func (s *Server) setStatus(ref, status string) {
	s.runningMu.Lock()
	s.running[ref] = status
	s.runningMu.Unlock()
}

// getStatus returns the stored lifecycle status for a ref, defaulting to "idle".
func (s *Server) getStatus(ref string) string {
	s.runningMu.RLock()
	st, ok := s.running[ref]
	s.runningMu.RUnlock()
	if !ok {
		return "idle"
	}
	return st
}

// newRunID generates a random 16-hex-character run identifier.
func newRunID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// writeJSON serialises v as JSON and sends it with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		slog.Warnf("pipeline API: writeJSON encode error: %v", err)
	}
}

// writeError sends a standard ErrorResponse.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorResponse{Code: code, Message: message})
}
