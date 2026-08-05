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
	"encoding/json"
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
// The inner sink writes to a local temp file; Close() uploads that file to the
// object store and then deletes it.
type objstoreSink struct {
	provider string
	store    objstore.Store
	key      string // destination object key (macros already expanded)
	inner    sink.Sink
	tmpPath  string // path of the local temp file the inner sink writes to
	records  int64  // rows written to this object (for the manifest record_count)
	closed   bool   // guards a second Close()
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
	return newLeafSink(provider, cfg)
}

// newLeafSink builds a non-partitioned destination sink: a rolling sink when
// roll thresholds (rollMaxBytes / rollMaxSeconds) are configured, otherwise a
// plain single-object sink. Used both at the top level and as the per-key child
// of the partitioned sink, so namespace partitions also roll.
func newLeafSink(provider string, cfg map[string]interface{}) (sink.Sink, error) {
	maxBytes := int64FromCfg(cfg, "rollMaxBytes")
	maxSeconds := int64FromCfg(cfg, "rollMaxSeconds")
	if maxBytes > 0 || maxSeconds > 0 {
		return newRollingObjstoreSink(provider, cfg, maxBytes, maxSeconds)
	}
	s, err := newSingleObjstoreSink(provider, cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func int64FromCfg(cfg map[string]interface{}, k string) int64 {
	switch v := cfg[k].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

func newSingleObjstoreSink(provider string, cfg map[string]interface{}) (*objstoreSink, error) {
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

	// Build the inner sink config.
	innerCfg := buildInnerCfg(cfg, innerFormat)

	// The inner sink writes to a local temp file; Close() uploads it.
	tmp, err := os.CreateTemp("", "flogo-datapipeline-objsink-*")
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("%s sink: create temp file: %w", provider, err)
	}
	tmpPath := tmp.Name()
	// Close the file descriptor immediately; the inner sink will reopen it by
	// path. This avoids double-open issues on Windows.
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		_ = store.Close()
		return nil, fmt.Errorf("%s sink: close temp file: %w", provider, err)
	}
	innerCfg["path"] = tmpPath
	inner, err := sink.New(innerFormat, innerCfg)
	if err != nil {
		_ = os.Remove(tmpPath)
		_ = store.Close()
		return nil, fmt.Errorf("%s sink: open inner sink %q: %w", provider, innerFormat, err)
	}

	return &objstoreSink{
		provider: provider,
		store:    store,
		key:      key,
		inner:    inner,
		tmpPath:  tmpPath,
	}, nil
}

func (s *objstoreSink) WriteBatch(batch *record.RecordBatch) error {
	if err := s.inner.WriteBatch(batch); err != nil {
		return err
	}
	s.records += int64(batch.Len())
	return nil
}

func (s *objstoreSink) Flush() error {
	return s.inner.Flush()
}

// currentBytes flushes the inner sink and returns the current on-disk size of
// the buffered temp file. Used by the rolling sink to decide when to roll.
func (s *objstoreSink) currentBytes() int64 {
	if s.tmpPath == "" {
		return 0
	}
	_ = s.inner.Flush()
	fi, err := os.Stat(s.tmpPath)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (s *objstoreSink) Close() error {
	// Close is idempotent: the rolling sink can try to close a child again on a
	// failed-roll path, so guard against a second Close.
	if s.closed {
		return nil
	}
	s.closed = true
	// Flush and close the inner sink first.
	innerErr := s.inner.Close()

	var uploadErr error
	if s.tmpPath != "" {
		// Upload the temp file to the object store, then remove it (even on error).
		uploadErr = s.uploadTmpFile()
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
	c, err := newLeafSink(p.provider, childCfg)
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

// rollingObjstoreSink writes a sequence of size/time-bounded objects. When the
// current object exceeds rollMaxBytes or has been open longer than
// rollMaxSeconds, it is closed (uploaded), a manifest sidecar is written, and a
// new object is started with the next sequence number. The key template must
// contain a {seq} placeholder (a zero-padded roll counter); if absent, "-{seq}"
// is inserted before the file extension so rolled objects never collide.
type rollingObjstoreSink struct {
	provider   string
	cfg        map[string]interface{}
	keyTmpl    string
	maxBytes   int64
	maxSeconds int64

	seq     int
	cur     *objstoreSink
	curKey  string
	started time.Time
}

func newRollingObjstoreSink(provider string, cfg map[string]interface{}, maxBytes, maxSeconds int64) (sink.Sink, error) {
	keyTmpl, _ := cfg["key"].(string)
	if keyTmpl == "" {
		return nil, fmt.Errorf("%s sink: 'key' is required", provider)
	}
	if !strings.Contains(keyTmpl, "{seq}") {
		keyTmpl = insertSeqBeforeExt(keyTmpl)
	}
	r := &rollingObjstoreSink{
		provider:   provider,
		cfg:        cfg,
		keyTmpl:    keyTmpl,
		maxBytes:   maxBytes,
		maxSeconds: maxSeconds,
	}
	// Objects are opened lazily on first write, so a run that rolls on its last
	// batch does not leave an empty trailing object (and spurious manifest).
	return r, nil
}

// insertSeqBeforeExt turns "logs/part.ndjson" into "logs/part-{seq}.ndjson".
func insertSeqBeforeExt(key string) string {
	slash := strings.LastIndexByte(key, '/')
	base := key[slash+1:]
	dot := strings.LastIndexByte(base, '.')
	if dot < 0 {
		return key + "-{seq}"
	}
	return key[:slash+1] + base[:dot] + "-{seq}" + base[dot:]
}

func (r *rollingObjstoreSink) openNext() error {
	key := strings.ReplaceAll(r.keyTmpl, "{seq}", fmt.Sprintf("%04d", r.seq))
	childCfg := make(map[string]interface{}, len(r.cfg))
	for k, v := range r.cfg {
		childCfg[k] = v
	}
	childCfg["key"] = key
	c, err := newSingleObjstoreSink(r.provider, childCfg)
	if err != nil {
		return err
	}
	r.cur = c
	r.curKey = c.key // fully macro-expanded key
	r.started = time.Now()
	return nil
}

func (r *rollingObjstoreSink) shouldRoll() bool {
	if r.maxSeconds > 0 && time.Since(r.started) >= time.Duration(r.maxSeconds)*time.Second {
		return true
	}
	if r.maxBytes > 0 && r.cur.currentBytes() >= r.maxBytes {
		return true
	}
	return false
}

func (r *rollingObjstoreSink) roll() error {
	// Detach the current object first so a Close error cannot leave a half-closed
	// child that the deferred sink Close would then try to close again.
	cur := r.cur
	r.cur = nil // next write opens a fresh object lazily
	records := cur.records
	bytes := cur.currentBytes()
	key := r.curKey
	started := r.started
	// Close (upload) the current object, then write its manifest sidecar.
	if err := cur.Close(); err != nil {
		return err
	}
	if err := r.writeManifest(key, records, bytes, started); err != nil {
		return err
	}
	r.seq++
	return nil
}

func (r *rollingObjstoreSink) WriteBatch(batch *record.RecordBatch) error {
	if r.cur == nil {
		if err := r.openNext(); err != nil {
			return err
		}
	}
	if err := r.cur.WriteBatch(batch); err != nil {
		return err
	}
	if r.shouldRoll() {
		return r.roll()
	}
	return nil
}

func (r *rollingObjstoreSink) Flush() error {
	if r.cur == nil {
		return nil
	}
	return r.cur.Flush()
}

func (r *rollingObjstoreSink) Close() error {
	if r.cur == nil {
		return nil
	}
	records := r.cur.records
	bytes := r.cur.currentBytes()
	key := r.curKey
	started := r.started
	if err := r.cur.Close(); err != nil {
		return err
	}
	r.cur = nil
	if records > 0 {
		return r.writeManifest(key, records, bytes, started)
	}
	return nil
}

// writeManifest writes an audit-grade sidecar next to the object: <key>.manifest.json
// with the object key, record count, byte size, and open/close timestamps, so an
// auditor can reconcile "what the node saw" against "what landed" object by object.
func (r *rollingObjstoreSink) writeManifest(objectKey string, records, bytes int64, started time.Time) error {
	manifest := map[string]interface{}{
		"object":       objectKey,
		"record_count": records,
		"bytes":        bytes,
		"opened_at":    started.UTC().Format(time.RFC3339),
		"closed_at":    time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	store, err := objstore.New(r.provider, r.cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	w, err := store.PutWriter(context.Background(), objectKey+".manifest.json")
	if err != nil {
		return fmt.Errorf("%s sink: open manifest writer: %w", r.provider, err)
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("%s sink: write manifest: %w", r.provider, err)
	}
	return w.Close()
}

var _ sink.Sink = (*rollingObjstoreSink)(nil)

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
