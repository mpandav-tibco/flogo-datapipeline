// Package objstore registers cloud object-storage sinks (S3, GCS, ADLS Gen2)
// with the sink registry.  Each provider is registered under its own type name:
//
//	{ "type": "s3-sink",   "bucket": "my-bucket", "key": "out/{YYYY}/{MM}/{DD}/data.parquet", "format": "parquet" }
//	{ "type": "gcs-sink",  "bucket": "my-bucket", "key": "out/{YYYY}/{MM}/{DD}/data.csv",     "format": "csv" }
//	{ "type": "adls-sink", "container": "out",     "key": "data/{YYYY}/{MM}/{DD}.jsonlines",   "format": "jsonlines" }
//
// Key macros: the destination key may contain date macros that are expanded at
// sink creation time:
//
//	{YYYY}  — four-digit year   (UTC)
//	{MM}    — two-digit month   (UTC)
//	{DD}    — two-digit day     (UTC)
//	{HH}    — two-digit hour    (UTC)
//	{mm}    — two-digit minute  (UTC)
//
// Format decoupling: the sink delegates all serialisation to an inner sink
// registered in the sink registry (csv, parquet, jsonlines, …).  For streaming
// inner sinks (csv, jsonlines) the bytes flow through an io.Pipe directly to
// the object store without touching disk.  For seekable inner sinks (parquet)
// a temporary file is written first, then uploaded on Close.
//
// Import with a blank import to activate all three provider registrations:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/objstore"
//
// The objstore provider backends must be imported separately:
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
	"os"
	"strings"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/objstore"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

// streamingFormats is the set of inner sink formats that stream directly to the
// object store via an io.Pipe. It is currently empty: all inner formats write to
// a temp file that is uploaded synchronously on Close (see uploadTmpFile), which
// gives deterministic completion and proper error propagation. The csv/jsonlines
// file sinks write by path, so the temp-file path handles them correctly without
// the extra pipe goroutine (whose completion was racy).
var streamingFormats = map[string]bool{}

func init() {
	for _, provider := range []string{"s3", "gcs", "adls"} {
		p := provider // capture for closure
		sink.Register(p+"-sink", func(cfg map[string]interface{}) (sink.Sink, error) {
			return newObjstoreSink(p, cfg)
		})
	}
}

// objstoreSink wraps an inner sink and, on Close, uploads the output to the
// configured object store path.
//
// Streaming path (csv, jsonlines): the inner sink writes to an io.Pipe; the
// upload goroutine reads from the pipe concurrently and pushes bytes to the
// object store.  Close() signals EOF to the upload goroutine and waits for it
// to finish, surfacing any SDK error.
//
// Temp-file path (parquet, …): the inner sink writes to a local temp file;
// Close() uploads the file and then deletes it.
type objstoreSink struct {
	provider string
	store    objstore.Store
	key      string       // destination object key (macros already expanded)
	inner    sink.Sink
	tmpPath  string       // non-empty on temp-file path
	pw       io.WriteCloser // write end of the pipe (streaming path)
	uploadDone chan error  // receives the upload goroutine result (streaming path)
	streaming  bool
}

func newObjstoreSink(provider string, cfg map[string]interface{}) (sink.Sink, error) {
	keyTemplate, _ := cfg["key"].(string)
	if keyTemplate == "" {
		return nil, fmt.Errorf("%s sink: 'key' (destination object key) is required", provider)
	}
	innerFormat, _ := cfg["format"].(string)
	if innerFormat == "" {
		return nil, fmt.Errorf("%s sink: 'format' is required (inner sink type, e.g. csv, parquet)", provider)
	}

	// Expand date macros in the key template at sink creation time.
	key := expandKeyMacros(keyTemplate)

	store, err := objstore.New(provider, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s sink: open store: %w", provider, err)
	}

	streaming := streamingFormats[innerFormat]

	// Build the inner sink config.
	innerCfg := buildInnerCfg(cfg, innerFormat)

	var (
		inner      sink.Sink
		tmpPath    string
		pw         io.WriteCloser
		uploadDone chan error
	)

	if streaming {
		// Open a PutWriter for the object store; feed it via an io.Pipe.
		objectWriter, err := store.PutWriter(context.Background(), key)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("%s sink: open put writer for %q: %w", provider, key, err)
		}

		pr, pipeWriter := io.Pipe()
		done := make(chan error, 1)

		go func() {
			_, copyErr := io.Copy(objectWriter, pr)
			closeErr := objectWriter.Close()
			pr.CloseWithError(copyErr)
			if copyErr != nil {
				done <- copyErr
			} else {
				done <- closeErr
			}
		}()

		// Route the inner format sink through pipeWriterSink: the inner sink writes
		// to a temp file, and pipeWriterSink.Close() copies that file to the pipe
		// writer on Close, which the upload goroutine forwards to the object store.
		// This keeps existing inner sink packages unchanged while enabling streaming.
		innerSink, err := newPipeWriterSink(innerFormat, innerCfg, pipeWriter)
		if err != nil {
			_ = pipeWriter.Close()
			_ = store.Close()
			return nil, fmt.Errorf("%s sink: open inner sink %q: %w", provider, innerFormat, err)
		}

		inner = innerSink
		pw = pipeWriter
		uploadDone = done
	} else {
		// Temp-file path: inner sink writes to a local file; Close uploads it.
		tmp, err := os.CreateTemp("", "flogo-datapipeline-objsink-*")
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("%s sink: create temp file: %w", provider, err)
		}
		tmpPath = tmp.Name()
		// Close the file descriptor immediately; the inner sink will reopen it
		// by path.  This avoids double-open issues on Windows.
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpPath)
			_ = store.Close()
			return nil, fmt.Errorf("%s sink: close temp file: %w", provider, err)
		}
		innerCfg["path"] = tmpPath
		inner, err = sink.New(innerFormat, innerCfg)
		if err != nil {
			_ = os.Remove(tmpPath)
			_ = store.Close()
			return nil, fmt.Errorf("%s sink: open inner sink %q: %w", provider, innerFormat, err)
		}
	}

	return &objstoreSink{
		provider:   provider,
		store:      store,
		key:        key,
		inner:      inner,
		tmpPath:    tmpPath,
		pw:         pw,
		uploadDone: uploadDone,
		streaming:  streaming,
	}, nil
}

func (s *objstoreSink) WriteBatch(batch *record.RecordBatch) error {
	return s.inner.WriteBatch(batch)
}

func (s *objstoreSink) Flush() error {
	return s.inner.Flush()
}

func (s *objstoreSink) Close() error {
	// Flush and close the inner sink first.
	innerErr := s.inner.Close()

	var uploadErr error
	if s.streaming {
		// Close the write end of the pipe to signal EOF to the upload goroutine.
		if s.pw != nil {
			_ = s.pw.Close()
		}
		// Wait for the upload goroutine to finish.
		if s.uploadDone != nil {
			uploadErr = <-s.uploadDone
		}
	} else if s.tmpPath != "" {
		// Upload the temp file to the object store.
		uploadErr = s.uploadTmpFile()
		// Always remove the temp file, even on upload error.
		_ = os.Remove(s.tmpPath)
	}

	storeErr := s.store.Close()

	return errors.Join(innerErr, uploadErr, storeErr)
}

// uploadTmpFile opens the temp file and uploads it to the object store.
func (s *objstoreSink) uploadTmpFile() error {
	f, err := os.Open(s.tmpPath)
	if err != nil {
		return fmt.Errorf("%s sink: open temp file for upload: %w", s.provider, err)
	}
	defer f.Close()

	pw, err := s.store.PutWriter(context.Background(), s.key)
	if err != nil {
		return fmt.Errorf("%s sink: open put writer for %q: %w", s.provider, s.key, err)
	}

	if _, err = io.Copy(pw, f); err != nil {
		_ = pw.Close()
		return fmt.Errorf("%s sink: upload %q: %w", s.provider, s.key, err)
	}
	// Close commits the upload.
	if err = pw.Close(); err != nil {
		return fmt.Errorf("%s sink: commit upload %q: %w", s.provider, s.key, err)
	}
	return nil
}

// expandKeyMacros replaces date/time macros in the key template with UTC values.
func expandKeyMacros(key string) string {
	now := time.Now().UTC()
	r := strings.NewReplacer(
		"{YYYY}", fmt.Sprintf("%04d", now.Year()),
		"{MM}", fmt.Sprintf("%02d", int(now.Month())),
		"{DD}", fmt.Sprintf("%02d", now.Day()),
		"{HH}", fmt.Sprintf("%02d", now.Hour()),
		"{mm}", fmt.Sprintf("%02d", now.Minute()),
	)
	return r.Replace(key)
}

// buildInnerCfg copies config keys that are relevant to the inner sink,
// stripping objstore-specific keys.
func buildInnerCfg(cfg map[string]interface{}, format string) map[string]interface{} {
	skip := map[string]bool{
		"type": true, "bucket": true, "container": true, "key": true,
		"format": true,
		"region": true, "endpoint": true, "forcePathStyle": true,
		"accessKeyIdEnv": true, "secretAccessKeyEnv": true, "sessionTokenEnv": true,
		"credentialsFileEnv": true, "project": true,
		"sasTokenEnv": true, "accountKeyEnv": true, "accountName": true, "serviceURL": true,
	}
	out := make(map[string]interface{}, len(cfg)+2)
	for k, v := range cfg {
		if !skip[k] {
			out[k] = v
		}
	}
	out["type"] = format
	return out
}

// --- pipeWriterSink ---
// pipeWriterSink is a thin adapter that re-encodes RecordBatches through the
// inner format sink whose output is directed to an io.Writer (the pipe).
// Because existing inner sinks write to an os.File, we route streaming formats
// through a temporary in-process pipe: we create an os.Pipe(), give the write
// end to the inner sink (as a temp file), and copy from the read end to pw.
//
// This approach avoids modifying any existing sink package while still allowing
// streaming to the object store for line-oriented formats.

type pipeWriterSink struct {
	inner    sink.Sink    // inner sink writing to a named temp pipe file
	tmpPath  string       // temp file that inner sink writes to
	pw       io.Writer    // ultimate destination (object store pipe)
	closed   bool
}

func newPipeWriterSink(format string, cfg map[string]interface{}, pw io.Writer) (sink.Sink, error) {
	// Create a temp file that the inner sink will write to.
	tmp, err := os.CreateTemp("", "flogo-datapipeline-inner-*")
	if err != nil {
		return nil, fmt.Errorf("pipe writer sink: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("pipe writer sink: close temp file: %w", err)
	}

	innerCfg := make(map[string]interface{}, len(cfg)+1)
	for k, v := range cfg {
		innerCfg[k] = v
	}
	innerCfg["path"] = tmpPath

	inner, err := sink.New(format, innerCfg)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}

	return &pipeWriterSink{
		inner:   inner,
		tmpPath: tmpPath,
		pw:      pw,
	}, nil
}

func (p *pipeWriterSink) WriteBatch(batch *record.RecordBatch) error {
	return p.inner.WriteBatch(batch)
}

func (p *pipeWriterSink) Flush() error {
	return p.inner.Flush()
}

// Close finalizes the inner sink, copies the temp file content to the upstream
// pipe writer, and cleans up the temp file.
func (p *pipeWriterSink) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	// Finalize and close the inner sink so it flushes all data to tmpPath.
	if err := p.inner.Close(); err != nil {
		_ = os.Remove(p.tmpPath)
		return fmt.Errorf("pipe writer sink: close inner: %w", err)
	}

	// Copy the temp file to the upstream writer.
	f, err := os.Open(p.tmpPath)
	if err != nil {
		_ = os.Remove(p.tmpPath)
		return fmt.Errorf("pipe writer sink: open temp for copy: %w", err)
	}
	defer f.Close()
	defer os.Remove(p.tmpPath)

	if _, err = io.Copy(p.pw, f); err != nil {
		return fmt.Errorf("pipe writer sink: copy to pipe: %w", err)
	}
	return nil
}
