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
//	{ss}    — two-digit second  (UTC)
//	{ts}    — unix timestamp    (UTC seconds)
//	{uuid}  — random v4 UUID    (unique per run)
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
	"crypto/rand"
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
		ctor := func(cfg map[string]interface{}) (sink.Sink, error) {
			return newObjstoreSink(p, cfg)
		}
		// Register under both the "<provider>-sink" name and the bare provider
		// name so the type is symmetric with the object-store source (which
		// registers as "s3"/"gcs"/"adls") and so Studio-built pipelines that use
		// the bare provider name resolve correctly.
		sink.Register(p+"-sink", ctor)
		sink.Register(p, ctor)
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
	provider   string
	store      objstore.Store
	key        string // destination object key (macros already expanded)
	inner      sink.Sink
	tmpPath    string         // non-empty on temp-file path
	pw         io.WriteCloser // write end of the pipe (streaming path)
	uploadDone chan error     // receives the upload goroutine result (streaming path)
	streaming  bool
}

func newObjstoreSink(provider string, cfg map[string]interface{}) (sink.Sink, error) {
	keyTemplate, _ := cfg["key"].(string)
	if keyTemplate == "" {
		return nil, fmt.Errorf("%s sink: 'key' (destination object key) is required", provider)
	}
	// When the key template contains record-field placeholders (e.g.
	// "{namespace}/{YYYY}/part.ndjson"), route each record to a per-key object so
	// one collector can fan out to namespace/tenant-partitioned destinations.
	// Templates with only date/uuid macros keep the single-object fast path.
	if fields := keyFieldPlaceholders(keyTemplate); len(fields) > 0 {
		return newPartitionedSink(provider, cfg, keyTemplate, fields)
	}
	return newSingleObjstoreSink(provider, cfg)
}

func newSingleObjstoreSink(provider string, cfg map[string]interface{}) (sink.Sink, error) {
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
//
// Two macro syntaxes are supported and may be mixed freely:
//
//   - Brace form (canonical):  {YYYY} {MM} {DD} {HH} {mm} {ss} {ts} {uuid}
//   - Dollar-brace form:       ${date} ${year} ${month} ${day} ${uuid} ${runId}
//
// The dollar-brace form is the syntax surfaced in the Studio field tooltips
// ("supports ${date}, ${uuid}, ${runId}"), so it is honoured here to keep the
// authored key templates and the runtime expansion in sync — otherwise a key
// like "${date}/part-${uuid}.parquet" would be written to the object store
// verbatim.
func expandKeyMacros(key string) string {
	now := time.Now().UTC()
	uuid := newUUID()
	r := strings.NewReplacer(
		"{YYYY}", fmt.Sprintf("%04d", now.Year()),
		"{MM}", fmt.Sprintf("%02d", int(now.Month())),
		"{DD}", fmt.Sprintf("%02d", now.Day()),
		"{HH}", fmt.Sprintf("%02d", now.Hour()),
		"{mm}", fmt.Sprintf("%02d", now.Minute()),
		"{ss}", fmt.Sprintf("%02d", now.Second()),
		"{ts}", fmt.Sprintf("%d", now.Unix()),
		"{uuid}", uuid,
		// Studio dollar-brace forms.
		"${date}", now.Format("2006-01-02"),
		"${year}", fmt.Sprintf("%04d", now.Year()),
		"${month}", fmt.Sprintf("%02d", int(now.Month())),
		"${day}", fmt.Sprintf("%02d", now.Day()),
		"${ts}", fmt.Sprintf("%d", now.Unix()),
		"${uuid}", uuid,
		"${runId}", uuid,
	)
	return r.Replace(key)
}

// reservedMacros are the brace-form {..} tokens that expandKeyMacros handles as
// date/uuid values. Any OTHER {name} token is treated as a record-field
// placeholder for per-record object partitioning.
var reservedMacros = map[string]bool{
	"YYYY": true, "MM": true, "DD": true, "HH": true,
	"mm": true, "ss": true, "ts": true, "uuid": true,
}

// keyFieldPlaceholders returns the distinct record-field names referenced as
// {name} in the key template (excluding the reserved date/uuid macros). These
// drive per-record object partitioning (e.g. "{namespace}/{YYYY}/part.ndjson").
func keyFieldPlaceholders(template string) []string {
	var fields []string
	seen := map[string]bool{}
	for i := 0; i < len(template); i++ {
		if template[i] != '{' {
			continue
		}
		// Skip the ${...} dollar-brace form (handled by expandKeyMacros).
		if i > 0 && template[i-1] == '$' {
			continue
		}
		end := strings.IndexByte(template[i:], '}')
		if end < 0 {
			break
		}
		name := template[i+1 : i+end]
		i += end
		if name == "" || reservedMacros[name] || seen[name] {
			continue
		}
		seen[name] = true
		fields = append(fields, name)
	}
	return fields
}

// partitionedObjstoreSink fans a stream out to multiple objects, one per distinct
// resolved key. The key template's record-field placeholders ({namespace}, …)
// are substituted per row; date/uuid macros are expanded lazily by each child
// single-object sink. Child sinks are created on first use and closed together.
type partitionedObjstoreSink struct {
	provider    string
	cfg         map[string]interface{}
	keyTemplate string
	fields      []string
	children    map[string]sink.Sink // field-resolved key → child sink
}

func newPartitionedSink(provider string, cfg map[string]interface{}, keyTemplate string, fields []string) (sink.Sink, error) {
	if _, ok := cfg["format"].(string); !ok || cfg["format"] == "" {
		return nil, fmt.Errorf("%s sink: 'format' is required (inner sink type, e.g. csv, parquet)", provider)
	}
	return &partitionedObjstoreSink{
		provider:    provider,
		cfg:         cfg,
		keyTemplate: keyTemplate,
		fields:      fields,
		children:    map[string]sink.Sink{},
	}, nil
}

// resolveKey substitutes the field placeholders in the template for one row.
// Missing/empty field values become "_unknown" so records are never dropped.
func (p *partitionedObjstoreSink) resolveKey(batch *record.RecordBatch, row int) string {
	key := p.keyTemplate
	for _, f := range p.fields {
		v := batch.Value(row, f)
		val := "_unknown"
		if v != nil {
			if s := fmt.Sprintf("%v", v); s != "" {
				val = sanitizeKeySegment(s)
			}
		}
		key = strings.ReplaceAll(key, "{"+f+"}", val)
	}
	return key
}

// sanitizeKeySegment keeps a field value safe for use inside an object key
// (no path traversal, no slashes injected by data).
func sanitizeKeySegment(s string) string {
	return strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(s)
}

func (p *partitionedObjstoreSink) child(resolvedKey string) (sink.Sink, error) {
	if c, ok := p.children[resolvedKey]; ok {
		return c, nil
	}
	// Clone the config with the fully field-resolved key; the single-object sink
	// still expands date/uuid macros in it.
	childCfg := make(map[string]interface{}, len(p.cfg))
	for k, v := range p.cfg {
		childCfg[k] = v
	}
	childCfg["key"] = resolvedKey
	c, err := newSingleObjstoreSink(p.provider, childCfg)
	if err != nil {
		return nil, err
	}
	p.children[resolvedKey] = c
	return c, nil
}

func (p *partitionedObjstoreSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	// Group row indices by resolved key, then write one sub-batch per key so each
	// child sink receives a proper RecordBatch (preserving columnar structure).
	groups := map[string][]int{}
	order := []string{}
	for i := 0; i < batch.Len(); i++ {
		k := p.resolveKey(batch, i)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], i)
	}
	for _, k := range order {
		rows := groups[k]
		sub := &record.RecordBatch{Columns: batch.Columns, Rows: make([][]interface{}, 0, len(rows))}
		for _, idx := range rows {
			sub.Rows = append(sub.Rows, batch.Rows[idx])
		}
		c, err := p.child(k)
		if err != nil {
			return err
		}
		if err := c.WriteBatch(sub); err != nil {
			return err
		}
	}
	return nil
}

func (p *partitionedObjstoreSink) Flush() error {
	var errs []error
	for _, c := range p.children {
		if err := c.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *partitionedObjstoreSink) Close() error {
	var errs []error
	for _, c := range p.children {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// newUUID returns a random RFC-4122 v4 UUID string. Used to make object keys
// unique per run (e.g. "orders-{uuid}.parquet") so concurrent writers and
// re-runs never collide.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to a timestamp-based value; collisions are still astronomically
		// unlikely given the surrounding date macros.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
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
		// Inline credentials (Studio connections / local dev) — never forward to the inner sink.
		"accessKeyId": true, "secretAccessKey": true, "sessionToken": true,
		"credentialsJson": true, "sasToken": true, "accountKey": true,
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
	inner   sink.Sink // inner sink writing to a named temp pipe file
	tmpPath string    // temp file that inner sink writes to
	pw      io.Writer // ultimate destination (object store pipe)
	closed  bool
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
