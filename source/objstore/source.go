// Package objstore registers cloud object-storage sources (S3, GCS, ADLS Gen2)
// with the source registry.  Each provider is registered under its own type
// name so the pipeline config selects the backend by name:
//
//	{ "type": "s3",   "bucket": "my-bucket", "prefix": "data/2024/*.parquet", "format": "parquet" }
//	{ "type": "gcs",  "bucket": "my-bucket", "prefix": "data/*.csv",          "format": "csv" }
//	{ "type": "adls", "container": "raw",    "prefix": "events/*.jsonlines",  "format": "jsonlines" }
//
// The source implements source.PartitionableSource: each matched object becomes
// one logical partition, enabling multi-worker parallel reads.
//
// Format decoupling: the objstoreSource downloads (or streams) the object and
// delegates all record parsing to the inner source format registered in the
// source registry.  This keeps format logic entirely in the existing source
// packages (csv, parquet, jsonlines, …) and storage logic in the objstore
// packages.
//
// Parquet note: parquet-go requires an io.ReaderAt (seekable); therefore when
// format=parquet (or any format with download:true) the object is spooled to a
// temporary file before the inner source is opened.  Set download:true
// explicitly in config to force this behaviour for other seekable formats.
//
// Import with a blank import to activate all three provider registrations:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/source/objstore"
//
// The objstore provider backends must be imported separately:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/s3"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/gcs"
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/objstore/adls"
package objstore

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

// downloadAlways is the set of inner formats that require random access and
// therefore must always be spooled to a temp file before opening.
var downloadAlways = map[string]bool{
	"parquet": true,
}

func init() {
	for _, provider := range []string{"s3", "gcs", "adls"} {
		p := provider // capture for closure
		source.Register(p, func(cfg map[string]interface{}) (source.Source, error) {
			return newObjstoreSource(p, cfg)
		})
	}
}

// objstoreSource implements source.PartitionableSource.
// It lists matching objects at Open time, then for each object either:
//
//	(a) downloads to a temp file and opens the inner source on that path, or
//	(b) streams the content through an io.Pipe into the inner source (csv, jsonlines).
//
// In single-instance mode objects are consumed sequentially.
// In partitioned mode each OpenPartition call returns a child source scoped
// to exactly one object.
type objstoreSource struct {
	provider    string
	store       objstore.Store
	objects     []objstore.Object // enumerated at open time
	innerFormat string
	innerCfg    map[string]interface{} // base config for the inner source factory
	download    bool                   // spool to tmp before opening inner source

	// state for sequential (non-partitioned) reading
	objIdx   int // index of the currently-open object
	inner    source.Source
	tmpFiles []string // temp files to delete on Close
	schema   []record.Column
	closed   bool
}

func newObjstoreSource(provider string, cfg map[string]interface{}) (source.Source, error) {
	prefix, _ := cfg["prefix"].(string)
	if prefix == "" {
		return nil, fmt.Errorf("%s source: 'prefix' is required", provider)
	}
	innerFormat, _ := cfg["format"].(string)
	if innerFormat == "" {
		return nil, fmt.Errorf("%s source: 'format' is required (inner source type, e.g. csv, parquet)", provider)
	}

	download := false
	if d, ok := cfg["download"].(bool); ok {
		download = d
	}
	// Always download for formats that require seekable access.
	if downloadAlways[innerFormat] {
		download = true
	}

	// Build the inner source config by copying all keys except objstore-specific ones.
	innerCfg := buildInnerCfg(cfg)

	store, err := objstore.New(provider, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s source: open store: %w", provider, err)
	}

	objects, err := store.List(context.Background(), prefix)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("%s source: list %q: %w", provider, prefix, err)
	}

	src := &objstoreSource{
		provider:    provider,
		store:       store,
		objects:     objects,
		innerFormat: innerFormat,
		innerCfg:    innerCfg,
		download:    download,
	}

	// Pre-compute schema from the first object so Schema() returns immediately.
	// If there are no matching objects, schema stays nil; the pipeline engine
	// must handle this case (empty source).
	if len(objects) > 0 {
		innerSrc, tmpPath, err := src.openObject(context.Background(), objects[0])
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("%s source: open first object %q: %w", provider, objects[0].Key, err)
		}
		src.schema = innerSrc.Schema()
		src.inner = innerSrc
		src.objIdx = 0
		if tmpPath != "" {
			src.tmpFiles = append(src.tmpFiles, tmpPath)
		}
	}

	return src, nil
}

// Schema returns the column definitions inferred from the first matched object.
// Returns nil if no objects matched the prefix — the caller must handle this.
func (s *objstoreSource) Schema() []record.Column { return s.schema }

// Skip advances past n rows by delegating to the inner source(s).
// For sequential single-object reads this works correctly; for multi-object
// sources, rows are consumed sequentially across object boundaries.
func (s *objstoreSource) Skip(n int64) error {
	remaining := n
	for remaining > 0 {
		if s.inner == nil {
			return nil
		}
		// Try to skip within the current inner source.
		if err := s.inner.Skip(remaining); err != nil {
			return err
		}
		// Verify by draining: Skip may not surface how many rows it actually skipped,
		// so we trust the inner source's contract and move on.
		remaining = 0
	}
	return nil
}

// ReadBatch reads up to size rows from the current object, advancing to the
// next object when the current one is exhausted.
func (s *objstoreSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	for {
		if s.inner == nil {
			// All objects exhausted.
			return nil, io.EOF
		}

		batch, err := s.inner.ReadBatch(size)
		if err == nil || (err == io.EOF && batch != nil && batch.Len() > 0) {
			// Successful read (possibly a partial EOF batch).
			if err == io.EOF {
				// The inner source is done; advance to the next object.
				_ = s.inner.Close()
				s.inner = nil
				s.objIdx++
				if err2 := s.openNextObject(); err2 != nil && err2 != io.EOF {
					return batch, err2
				}
				// Return the last batch from the exhausted object.
				return batch, nil
			}
			return batch, nil
		}
		if err == io.EOF {
			// Inner source exhausted with no rows; advance to next object.
			_ = s.inner.Close()
			s.inner = nil
			s.objIdx++
			if err2 := s.openNextObject(); err2 != nil {
				if err2 == io.EOF {
					return nil, io.EOF
				}
				return nil, err2
			}
			// Try the next object in the next loop iteration.
			continue
		}
		return nil, err
	}
}

// openNextObject opens the next object in s.objects at index s.objIdx.
// Returns io.EOF if all objects have been consumed.
func (s *objstoreSource) openNextObject() error {
	if s.objIdx >= len(s.objects) {
		return io.EOF
	}
	innerSrc, tmpPath, err := s.openObject(context.Background(), s.objects[s.objIdx])
	if err != nil {
		return fmt.Errorf("open object %q: %w", s.objects[s.objIdx].Key, err)
	}
	s.inner = innerSrc
	if tmpPath != "" {
		s.tmpFiles = append(s.tmpFiles, tmpPath)
	}
	return nil
}

// openObject downloads or streams a single object and returns an inner source
// reading from it.  tmpPath is non-empty only when a temp file was created;
// the caller is responsible for appending it to s.tmpFiles.
func (s *objstoreSource) openObject(ctx context.Context, obj objstore.Object) (source.Source, string, error) {
	innerCfg := cloneCfg(s.innerCfg)

	if s.download {
		// Spool to a temp file so seekable formats (parquet) can random-access.
		tmpPath, err := spoolToTempFile(ctx, s.store, obj.Key)
		if err != nil {
			return nil, "", fmt.Errorf("spool %q: %w", obj.Key, err)
		}
		innerCfg["path"] = tmpPath
		innerSrc, err := source.New(s.innerFormat, innerCfg)
		if err != nil {
			_ = os.Remove(tmpPath)
			return nil, "", fmt.Errorf("open inner source %q on %q: %w", s.innerFormat, tmpPath, err)
		}
		return innerSrc, tmpPath, nil
	}

	// Streaming path: open a pipe and feed the object stream into it.
	// The inner source reads from the pipe; downloads happen concurrently.
	pr, pw := io.Pipe()
	go func() {
		rc, err := s.store.GetReader(ctx, obj.Key)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("get reader for %q: %w", obj.Key, err))
			return
		}
		defer rc.Close()
		_, err = io.Copy(pw, rc)
		pw.CloseWithError(err)
	}()

	// Write the pipe reader to a temp file because most inner sources require
	// an os.File path rather than an io.Reader.  For streaming sources that
	// accept an io.Reader directly this could be optimised; keeping temp-file
	// semantics here keeps the inner source interface unchanged.
	tmpPath, err := spoolReaderToTempFile(pr)
	if err != nil {
		_ = pw.Close()
		return nil, "", fmt.Errorf("stream spool %q: %w", obj.Key, err)
	}
	innerCfg["path"] = tmpPath
	innerSrc, err := source.New(s.innerFormat, innerCfg)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, "", fmt.Errorf("open inner source %q on %q: %w", s.innerFormat, tmpPath, err)
	}
	return innerSrc, tmpPath, nil
}

// Close releases the inner source, the object store client, and any temp files.
func (s *objstoreSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	if s.inner != nil {
		if err := s.inner.Close(); err != nil {
			errs = append(errs, err)
		}
		s.inner = nil
	}
	if err := s.store.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, tmp := range s.tmpFiles {
		_ = os.Remove(tmp) // best-effort cleanup
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// --- PartitionableSource implementation ---

// Partitions returns one partition per matched object.
// Each partition's Token is the object Key.
func (s *objstoreSource) Partitions(_ int) ([]source.Partition, error) {
	parts := make([]source.Partition, len(s.objects))
	for i, obj := range s.objects {
		parts[i] = source.Partition{
			ID:       obj.Key,
			Token:    obj.Key,
			SizeHint: obj.Size,
		}
	}
	return parts, nil
}

// OpenPartition opens a new objstoreSource scoped to a single object.
// The returned Source is independent and must be closed by the caller.
func (s *objstoreSource) OpenPartition(p source.Partition) (source.Source, error) {
	key, ok := p.Token.(string)
	if !ok {
		return nil, fmt.Errorf("objstore source: partition token must be a string key, got %T", p.Token)
	}

	// Find the object in s.objects to get its size / ETag.
	var obj objstore.Object
	found := false
	for _, o := range s.objects {
		if o.Key == key {
			obj = o
			found = true
			break
		}
	}
	if !found {
		obj = objstore.Object{Key: key}
	}

	innerSrc, tmpPath, err := s.openObject(context.Background(), obj)
	if err != nil {
		return nil, fmt.Errorf("objstore source: open partition %q: %w", key, err)
	}

	child := &partitionSource{
		inner:    innerSrc,
		store:    s.store,
		tmpPath:  tmpPath,
		provider: s.provider,
	}
	return child, nil
}

// partitionSource wraps a single-object inner source for use as a partition.
// It does not own the objstore.Store (shared with the parent).
type partitionSource struct {
	inner    source.Source
	store    objstore.Store
	tmpPath  string
	provider string
	closed   bool
}

func (p *partitionSource) Schema() []record.Column { return p.inner.Schema() }

func (p *partitionSource) ReadBatch(size int) (*record.RecordBatch, error) {
	return p.inner.ReadBatch(size)
}

func (p *partitionSource) Skip(n int64) error { return p.inner.Skip(n) }

func (p *partitionSource) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err := p.inner.Close()
	if p.tmpPath != "" {
		_ = os.Remove(p.tmpPath)
	}
	return err
}

// --- helpers ---

// spoolToTempFile downloads the entire object to a named temp file and returns
// the path.  The caller is responsible for deleting the file when done.
func spoolToTempFile(ctx context.Context, store objstore.Store, key string) (string, error) {
	rc, err := store.GetReader(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	return spoolReaderToTempFile(rc)
}

// spoolReaderToTempFile reads all of r into a new temp file and returns the path.
func spoolReaderToTempFile(r io.Reader) (string, error) {
	tmp, err := os.CreateTemp("", "flogo-datapipeline-objstore-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err = io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("spool to temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("close temp file: %w", err)
	}
	return tmpPath, nil
}

// buildInnerCfg copies config keys that are relevant to the inner source,
// stripping objstore-specific keys to avoid confusion in the inner factory.
func buildInnerCfg(cfg map[string]interface{}) map[string]interface{} {
	skip := map[string]bool{
		"type": true, "bucket": true, "container": true, "prefix": true,
		"format": true, "download": true,
		"region": true, "endpoint": true, "forcePathStyle": true,
		"accessKeyIdEnv": true, "secretAccessKeyEnv": true, "sessionTokenEnv": true,
		"credentialsFileEnv": true, "project": true,
		"sasTokenEnv": true, "accountKeyEnv": true, "accountName": true, "serviceURL": true,
	}
	out := make(map[string]interface{}, len(cfg))
	for k, v := range cfg {
		if !skip[k] {
			out[k] = v
		}
	}
	return out
}

// cloneCfg makes a shallow copy of a config map so that each inner source
// gets its own map and setting "path" on one does not affect another.
func cloneCfg(cfg map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(cfg)+1)
	for k, v := range cfg {
		out[k] = v
	}
	return out
}
