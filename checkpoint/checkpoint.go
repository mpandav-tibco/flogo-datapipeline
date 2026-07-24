// Package checkpoint defines the Checkpoint interface for pipeline
// restart/resume capability. A checkpoint records the last successfully
// processed position so a pipeline can resume from that point after a crash
// or planned restart, avoiding reprocessing already-written records.
//
// Checkpoint tokens are format-specific:
//   - CSV / text / fixed-width: int64 byte offset in the file
//   - Parquet: int row-group index + row-within-group count
//   - JSON-lines: int64 byte offset
//   - Multi-file: string path of the last completed file
package checkpoint

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Token carries the format-specific position of the last committed row.
// The pipeline engine treats it as opaque; the source interprets it on resume.
//
// Resume contract:
//   - If Payload is non-nil, the source MUST use it for an O(1) seek (byte
//     offset for CSV/NDJSON, row-group index for Parquet, etc.) and may ignore
//     RowsProcessed for positioning purposes (it is kept for metrics/logging).
//   - If Payload is nil, the source falls back to a linear Skip(RowsProcessed).
//
// Save contract:
//   - Callers MUST populate Payload by calling CursorProvider.Cursor() on the
//     source and assigning the result here. Saving with Payload=null defeats the
//     purpose of format-specific checkpointing and forces expensive linear replay.
type Token struct {
	// Format identifies the source format this token belongs to.
	Format string `json:"format"`
	// Payload is the format-specific position data (byte offset, row-group index,
	// etc.). It is marshalled as a JSON object whose schema is defined by each
	// source implementation. It MUST be populated on every Save; a null Payload
	// forces a linear Skip(RowsProcessed) on resume.
	Payload json.RawMessage `json:"payload"`
	// RowsProcessed is the cumulative count of rows committed at this position.
	// Used as the fallback seek depth when Payload is nil, and always kept for
	// metrics and progress reporting.
	RowsProcessed int64 `json:"rowsProcessed"`
	// WindowState is an optional, opaque snapshot of a stream-processing window
	// operator's keyed state, captured at the same instant as Payload so that a
	// streaming pipeline resumes with window aggregations and the source offset in
	// sync. Backends persist it verbatim; the pipeline engine populates it from the
	// window transform's Snapshot() and replays it via Restore() on resume. Empty
	// for pipelines without a window transform.
	WindowState json.RawMessage `json:"windowState,omitempty"`
}

// CursorProvider is an optional interface that pipeline sources implement to
// expose their current read position as a JSON blob suitable for Token.Payload.
// The pipeline engine checks for this interface via a type assertion after each
// checkpoint interval and calls Cursor() to populate Token.Payload, enabling
// O(1) format-specific seeks on resume instead of linear Skip scans.
//
// Implementations must be safe to call concurrently with ReadBatch if the
// source uses internal locking, or must only be called when ReadBatch is not
// in flight (the pipeline engine guarantees the latter).
type CursorProvider interface {
	// Cursor returns a JSON-encoded snapshot of the current read position.
	// The returned bytes are stored verbatim in Token.Payload and passed back
	// to the source's Resume method (or used by Skip when the engine calls it
	// with the token). Must not return nil; return []byte("null") if the source
	// has no meaningful cursor (e.g. it has not yet read any data).
	Cursor() json.RawMessage
}

// NewToken constructs a Token, optionally populating Payload from a
// CursorProvider. Pass the source as the third argument when it implements
// CursorProvider; pass nil to skip Payload population (results in linear
// Skip-based resume).
//
//	t := checkpoint.NewToken(format, rowsProcessed, src)
//	if err := ckpt.Save(runID, t); err != nil { ... }
func NewToken(format string, rowsProcessed int64, cp CursorProvider) *Token {
	t := &Token{Format: format, RowsProcessed: rowsProcessed}
	if cp != nil {
		t.Payload = cp.Cursor()
	}
	return t
}

// Checkpoint persists and retrieves pipeline progress tokens.
// Implementations must be goroutine-safe.
type Checkpoint interface {
	// Save persists the token for the given partition ID.
	// Called every checkpointIntervalRows rows and at pipeline completion.
	Save(partitionID string, t *Token) error

	// Load retrieves the last saved token for the given partition ID.
	// Returns nil (not an error) if no checkpoint exists for this partition.
	Load(partitionID string) (*Token, error)

	// Delete removes the checkpoint for a partition (called on successful completion
	// when the pipeline should restart from scratch on the next run).
	Delete(partitionID string) error

	// Close flushes any buffered state and releases resources.
	Close() error
}

// Factory constructs a Checkpoint from a generic config map.
type Factory func(cfg map[string]interface{}) (Checkpoint, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a checkpoint type name with its Factory.
func Register(typ string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[typ]; ok {
		panic(fmt.Sprintf("checkpoint: type %q already registered", typ))
	}
	registry[typ] = f
}

// New creates a Checkpoint for the given type using the provided config.
func New(typ string, cfg map[string]interface{}) (Checkpoint, error) {
	mu.RLock()
	f, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("checkpoint: unknown type %q", typ)
	}
	return f(cfg)
}

// NoopCheckpoint is a checkpoint that does nothing — used when checkpointing
// is disabled or for sources where restart is cheap (small files, idempotent).
type NoopCheckpoint struct{}

func (NoopCheckpoint) Save(string, *Token) error        { return nil }
func (NoopCheckpoint) Load(string) (*Token, error)      { return nil, nil }
func (NoopCheckpoint) Delete(string) error              { return nil }
func (NoopCheckpoint) Close() error                     { return nil }
