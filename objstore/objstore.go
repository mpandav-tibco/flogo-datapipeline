// Package objstore defines the ObjectStore abstraction for cloud object-storage
// backends (S3, GCS, ADLS Gen2) and the global provider registry.
//
// Each backend ships its own sub-package that calls Register() in its init()
// function.  Callers activate a backend with a blank import:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/gcs"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/adls"
package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Object describes a single object in a cloud store.
type Object struct {
	// Key is the full path within the bucket, e.g. "prefix/2024/01/file.parquet".
	Key string
	// Size is the object size in bytes; 0 if the backend did not return it.
	Size int64
	// ETag is the object's entity tag as returned by the storage service.
	ETag string
}

// Store abstracts a cloud object-storage backend.
// All implementations must be goroutine-safe.
type Store interface {
	// List returns all objects whose key matches the given prefix.
	// prefix may end with '*' as a trailing wildcard; in that case the
	// implementation strips the '*', uses the literal prefix for the API call,
	// and filters results with filepath.Match against the full glob pattern.
	// If prefix contains no wildcard, only objects with keys that begin with
	// prefix are returned (i.e. a plain prefix filter).
	List(ctx context.Context, prefix string) ([]Object, error)

	// GetReader opens the object at key for streaming read.
	// The caller must Close the returned ReadCloser when done.
	// For formats that require random access (e.g. Parquet), spool to a temp
	// file first rather than using the stream directly.
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)

	// PutWriter opens a write stream to key.
	// The upload is committed atomically when the caller calls Close on the
	// returned WriteCloser.  For S3 this uses multipart upload; for GCS/ADLS
	// a resumable upload.  If the caller abandons the write (e.g. process crash),
	// incomplete multipart parts may accrue charges — configure a lifecycle rule
	// to abort incomplete uploads after 24 h.
	PutWriter(ctx context.Context, key string) (io.WriteCloser, error)

	// Delete removes the object at key.
	// A no-op (no error) if the key does not exist.
	Delete(ctx context.Context, key string) error

	// Close releases cached credentials, SDK sessions, or HTTP connection pools.
	Close() error
}

// RangeReader is an optional Store capability: reading a byte range of an object
// via a single ranged GET. Backends that support HTTP range requests (S3, GCS,
// ADLS) implement it so seekable formats such as Parquet can be read with
// column/row-group pushdown — fetching only the footer and the selected column
// chunks — instead of downloading the whole object to a temp file.
//
// Callers should type-assert a Store to RangeReader and fall back to GetReader
// (whole-object spool) when the assertion fails.
type RangeReader interface {
	// GetRange returns a reader over [offset, offset+length) of the object.
	// The caller must Close the returned reader. length is always > 0.
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// ErrObjectChanged is returned (wrapped) by GetRangeIfMatch when the object no
// longer matches the expected version, or cannot be range-read byte-exactly.
var ErrObjectChanged = errors.New("object changed or not byte-addressable")

// VersionedRangeReader is an optional Store capability: a ranged read pinned to
// the object version identified by etag (as returned in Object.ETag by List).
// It must fail with ErrObjectChanged — never silently return bytes of a
// different version — when the object was overwritten, and when the stored
// bytes are transformed on download (e.g. GCS decompressive transcoding of
// Content-Encoding: gzip objects), since offsets would not line up.
// Streaming readers use it to resume after a dropped connection.
type VersionedRangeReader interface {
	GetRangeIfMatch(ctx context.Context, key, etag string, offset, length int64) (io.ReadCloser, error)
}

// NewRangeReaderAt adapts a RangeReader into an io.ReaderAt over the object at
// key, whose total size is size bytes. Each ReadAt is served by an independent
// ranged GET, so the returned value is safe for concurrent use (matching
// parquet-go's concurrent column-chunk reads). ctx bounds every underlying
// request.
func NewRangeReaderAt(ctx context.Context, rr RangeReader, key string, size int64) io.ReaderAt {
	return &rangeReaderAt{ctx: ctx, rr: rr, key: key, size: size}
}

type rangeReaderAt struct {
	ctx  context.Context
	rr   RangeReader
	key  string
	size int64
}

// ReadAt implements io.ReaderAt. It clamps the requested range to the object
// size and honours the io.ReaderAt contract: a short read (fewer than len(p)
// bytes, only possible at end-of-object) returns io.EOF.
func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("objstore: ReadAt negative offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	shortAtEnd := false
	if off+want > r.size {
		want = r.size - off
		shortAtEnd = true
	}
	rc, err := r.rr.GetRange(r.ctx, r.key, off, want)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, p[:want])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if err == nil && shortAtEnd {
		err = io.EOF
	}
	return n, err
}

// Factory constructs a Store from a generic config map.
// Keys are provider-specific; secrets must be supplied via environment-variable
// references (e.g. "accessKeyIdEnv", "secretAccessKeyEnv") rather than inline
// literal values so that they never appear in pipeline JSON resource files.
type Factory func(cfg map[string]interface{}) (Store, error)

var (
	mu       sync.RWMutex
	registry = map[string]Factory{}
)

// Register associates a cloud provider name with its Factory.
// Called from each backend sub-package's init() function.
// Panics on duplicate registration (programming error, not a runtime condition).
func Register(provider string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[provider]; ok {
		panic("objstore: provider " + provider + " already registered")
	}
	registry[provider] = f
}

// New creates a Store for the given provider using the provided config map.
// Returns an error if the provider has not been registered.
func New(provider string, cfg map[string]interface{}) (Store, error) {
	mu.RLock()
	f, ok := registry[provider]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("objstore: unknown provider %q — import the provider sub-package", provider)
	}
	return f(cfg)
}

// Providers returns the names of all currently registered providers.
func Providers() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}
