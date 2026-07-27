// Package audit provides an optional, append-only governance audit trail for
// pipeline runs. It writes structured JSON records (one object per line, JSONL)
// of run-lifecycle events — never any secret values — to stdout or a durable
// file, so an operator can answer "who ran what, when, and with what outcome."
//
// Auditing is OFF by default. Enable it per pipeline via the observability
// config:
//
//	"observability": { "audit": { "enabled": true, "file": "/var/log/flogo/audit.jsonl" } }
//
// or globally via environment variables (which override the config):
//
//	DP_AUDIT=on|1|true          enable auditing
//	DP_AUDIT_FILE=/path/log     write to this append-only file (else stdout)
//
// The records intentionally carry only component TYPES and run counters — never
// option values, DSNs, credentials, or row data — so the trail can be shipped to
// a central log store without leaking secrets.
package audit

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Auditor records governance audit events. Implementations must be safe for
// concurrent use by multiple goroutines.
type Auditor interface {
	// Log writes one audit record: the event name plus fields, stamped with a
	// UTC timestamp. Callers must never place secret values in fields.
	Log(event string, fields map[string]interface{})
	// Close flushes and releases the underlying writer (a no-op for stdout).
	Close() error
}

// Config controls auditor construction (from the pipeline observability block).
type Config struct {
	Enabled bool   `json:"enabled,omitempty"`
	File    string `json:"file,omitempty"` // append-only path; empty = stdout
}

// Noop is an Auditor that discards everything — the zero-cost default when
// auditing is disabled.
type Noop struct{}

// Log discards the record.
func (Noop) Log(string, map[string]interface{}) {}

// Close is a no-op.
func (Noop) Close() error { return nil }

// New builds an Auditor from cfg, with environment overrides:
//
//	DP_AUDIT = "on" | "1" | "true"  → force-enable
//	DP_AUDIT_FILE = path            → append-only file (else stdout)
//
// It returns Noop{} when auditing is disabled. When a file is configured it is
// opened in append mode; an open error is returned so misconfiguration surfaces
// at startup rather than silently dropping the trail.
func New(cfg Config) (Auditor, error) {
	enabled := cfg.Enabled
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DP_AUDIT"))) {
	case "on", "1", "true", "yes":
		enabled = true
	}
	if !enabled {
		return Noop{}, nil
	}

	file := cfg.File
	if env := strings.TrimSpace(os.Getenv("DP_AUDIT_FILE")); env != "" {
		file = env
	}
	if file == "" {
		return &writerAuditor{w: os.Stdout}, nil
	}
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &writerAuditor{w: f, closer: f}, nil
}

type writerAuditor struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when w is a file we own
}

func (a *writerAuditor) Log(event string, fields map[string]interface{}) {
	rec := make(map[string]interface{}, len(fields)+2)
	for k, v := range fields {
		rec[k] = v
	}
	// event/ts are authoritative — set last so a stray caller field can't shadow them.
	rec["event"] = event
	rec["ts"] = time.Now().UTC().Format(time.RFC3339Nano)

	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	b = append(b, '\n')

	a.mu.Lock()
	_, _ = a.w.Write(b) // os.File.Write is a direct syscall — no user-space buffer to lose on crash
	a.mu.Unlock()
}

func (a *writerAuditor) Close() error {
	if a.closer != nil {
		return a.closer.Close()
	}
	return nil
}
