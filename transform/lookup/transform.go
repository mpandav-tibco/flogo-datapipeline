// Package lookup registers an enrichment transform that calls a Flogo flow
// (or any HTTP endpoint) to add columns to each row. Results are cached with
// an LRU to avoid hitting the downstream system for repeated keys.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/transform/lookup"
//
// Config:
//
//	{
//	  "type": "lookup",
//	  "keyField": "customerId",
//	  "url": "http://localhost:8080/api/customers/{customerId}",
//	  "method": "GET",
//	  "outputFields": ["name", "tier", "region"],
//	  "concurrency": 10,
//	  "cacheSize": 5000,
//	  "timeoutMs": 3000,
//	  "maxResponseBytes": 1048576,
//	  "cacheNotFound": true,
//	  "onError": "null"
//	}
//
// cacheNotFound (default true) also caches 404 (not-found) answers, so a key
// missing from the reference system costs one HTTP call rather than one per
// row. Not-found answers expire after notFoundTtlMs (default 60000), so a key
// that appears upstream while a long-running/streaming pipeline is up is
// picked up within that window; set cacheNotFound false to never cache them.
// cacheTtlMs (default 0 = never) likewise expires found answers.
// maxResponseBytes (default 1 MiB) caps each response body; larger responses
// are treated as an error.
//
// onError: "null" fills enrichment fields with nil on HTTP error (default).
//
//	"skip" drops the row.
//	"fail" aborts the pipeline.
package lookup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mpandav-tibco/flogo-datapipeline/internal/cfgutil"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/transform"
	"golang.org/x/sync/singleflight"
)

func init() {
	transform.Register("lookup", newLookupTransform)
	transform.Register("enrich", newLookupTransform)
}

type lookupTransform struct {
	keyField     string
	urlTemplate  string
	method       string
	outputFields []string
	concurrency  int
	onError      string
	client       *http.Client
	cache        *lru.Cache[string, cacheEntry]
	sfGroup      singleflight.Group // zero value is ready; no init needed

	cacheNotFound    bool
	cacheTTL         time.Duration // found entries; 0 = never expire
	notFoundTTL      time.Duration // not-found entries; 0 = never expire
	maxResponseBytes int64
	// ctx is the run context (see transform.ContextBinder); requests made with
	// it abort as soon as the pipeline is cancelled.
	ctx context.Context
}

// BindContext implements transform.ContextBinder.
func (l *lookupTransform) BindContext(ctx context.Context) { l.ctx = ctx }

func newLookupTransform(cfg map[string]interface{}) (transform.Transform, error) {
	keyField, _ := cfg["keyField"].(string)
	if keyField == "" {
		return nil, fmt.Errorf("lookup transform: 'keyField' is required")
	}
	url, _ := cfg["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("lookup transform: 'url' is required")
	}
	method, _ := cfg["method"].(string)
	if method == "" {
		method = "GET"
	}
	// JSON-decoded definitions deliver numbers as float64; cfgutil accepts both.
	concurrency := cfgutil.PositiveInt(cfg, "concurrency", 10)
	cacheSize := cfgutil.PositiveInt(cfg, "cacheSize", 1000)
	timeoutMs := cfgutil.PositiveInt(cfg, "timeoutMs", 5000)
	maxResponseBytes := cfgutil.PositiveInt(cfg, "maxResponseBytes", 1<<20)
	cacheNotFound := true
	if b, ok := cfg["cacheNotFound"].(bool); ok {
		cacheNotFound = b
	}
	cacheTTLMs, _ := cfgutil.Int(cfg, "cacheTtlMs")
	notFoundTTLMs := 60000
	if v, ok := cfgutil.Int(cfg, "notFoundTtlMs"); ok && v >= 0 {
		notFoundTTLMs = v
	}
	onError, _ := cfg["onError"].(string)
	if onError == "" {
		onError = "null"
	}

	var outputFields []string
	if of, ok := cfg["outputFields"].([]interface{}); ok {
		for _, f := range of {
			if s, ok := f.(string); ok {
				outputFields = append(outputFields, s)
			}
		}
	}

	return &lookupTransform{
		keyField:     keyField,
		urlTemplate:  url,
		method:       method,
		outputFields: outputFields,
		concurrency:  concurrency,
		onError:      onError,
		client:       &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond},
		cache:        newCache(cacheSize),

		cacheNotFound:    cacheNotFound,
		cacheTTL:         time.Duration(max(cacheTTLMs, 0)) * time.Millisecond,
		notFoundTTL:      time.Duration(notFoundTTLMs) * time.Millisecond,
		maxResponseBytes: int64(maxResponseBytes),
		ctx:              context.Background(),
	}, nil
}

func (l *lookupTransform) Name() string { return "lookup" }

func (l *lookupTransform) Apply(in *record.RecordBatch) (*record.RecordBatch, error) {
	// Build output schema: original columns + enrichment columns.
	outCols := make([]record.Column, len(in.Columns)+len(l.outputFields))
	copy(outCols, in.Columns)
	for i, f := range l.outputFields {
		outCols[len(in.Columns)+i] = record.Column{Name: f, Type: record.TypeAny}
	}

	out := record.Get()
	out.Columns = outCols
	out.Rows = make([][]interface{}, len(in.Rows))

	// Concurrent enrichment with bounded goroutine pool.
	type result struct {
		idx  int
		data map[string]interface{}
		err  error
	}
	results := make([]result, len(in.Rows))

	keyIdx := in.ColIndex(l.keyField)
	keyOf := func(i int) string {
		var v interface{}
		if keyIdx >= 0 && keyIdx < len(in.Rows[i]) {
			v = in.Rows[i][keyIdx]
		}
		return record.FormatValue(v)
	}

	sem := make(chan struct{}, l.concurrency)
	var wg sync.WaitGroup

	for i := range in.Rows {
		key := keyOf(i)
		// Cache hits are served inline; only misses pay for a goroutine.
		if data, ok := l.cached(key); ok {
			results[i] = result{idx: i, data: data}
			continue
		}
		if err := l.ctx.Err(); err != nil {
			results[i] = result{idx: i, err: err}
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, key string) {
			defer wg.Done()
			defer func() { <-sem }()
			data, err := l.fetchWithCache(key)
			results[idx] = result{idx: idx, data: data, err: err}
		}(i, key)
	}
	wg.Wait()
	// Cancellation is not a lookup failure: onError ("null"/"skip") must not
	// turn aborted requests into null-enriched or dropped rows. Fail the batch
	// so nothing degraded is written; it is replayed on restart.
	if err := l.ctx.Err(); err != nil {
		record.Put(out)
		return nil, fmt.Errorf("lookup: run cancelled: %w", err)
	}

	skipIndices := make(map[int]bool)
	for _, r := range results {
		if r.err != nil {
			switch l.onError {
			case "fail":
				record.Put(out)
				return nil, fmt.Errorf("lookup: key %v: %w",
					keyOf(r.idx), r.err)
			case "skip":
				skipIndices[r.idx] = true
				continue
			default: // "null"
				// fill with nils below
			}
		}

		// Merge original row + enrichment fields.
		newRow := make([]interface{}, len(outCols))
		copy(newRow, in.Rows[r.idx])
		for j, f := range l.outputFields {
			if r.data != nil {
				newRow[len(in.Columns)+j] = r.data[f]
			}
		}
		out.Rows[r.idx] = newRow
	}

	// Compact: remove skipped rows.
	if len(skipIndices) > 0 {
		compacted := out.Rows[:0]
		for i, row := range out.Rows {
			if !skipIndices[i] && row != nil {
				compacted = append(compacted, row)
			}
		}
		out.Rows = compacted
	}

	return out, nil
}

// cacheEntry is a cached lookup answer; data nil = not found. A zero expires
// never expires.
type cacheEntry struct {
	data    map[string]interface{}
	expires time.Time
}

// cached returns a live cache entry for key, dropping it if expired.
func (l *lookupTransform) cached(key string) (map[string]interface{}, bool) {
	e, ok := l.cache.Get(key)
	if !ok {
		return nil, false
	}
	if !e.expires.IsZero() && time.Now().After(e.expires) {
		l.cache.Remove(key)
		return nil, false
	}
	return e.data, true
}

func (l *lookupTransform) store(key string, data map[string]interface{}) {
	ttl := l.cacheTTL
	if data == nil {
		if !l.cacheNotFound {
			return
		}
		ttl = l.notFoundTTL
	}
	e := cacheEntry{data: data}
	if ttl > 0 {
		e.expires = time.Now().Add(ttl)
	}
	l.cache.Add(key, e)
}

func (l *lookupTransform) fetchWithCache(key string) (map[string]interface{}, error) {
	if v, ok := l.cached(key); ok {
		return v, nil
	}
	v, err, _ := l.sfGroup.Do(key, func() (interface{}, error) {
		data, err := l.fetch(key)
		if err == nil {
			l.store(key, data) // nil data = not found
		}
		return data, err
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil // 404 / not-found path
	}
	return v.(map[string]interface{}), nil
}

// fetch issues the enrichment HTTP call for key and decodes the JSON response.
// The request carries the bound run context, so cancelling the pipeline aborts
// it immediately rather than waiting for client.Timeout.
func (l *lookupTransform) fetch(key string) (map[string]interface{}, error) {
	// Accept both {keyField} and ${keyField} so users familiar with the source
	// path templating (which uses ${date}) aren't surprised. Replace the
	// ${...} form first so the {...} pass doesn't leave a stray "$".
	escKey := url.PathEscape(key)
	target := l.urlTemplate
	target = strings.ReplaceAll(target, "${"+l.keyField+"}", escKey)
	target = strings.ReplaceAll(target, "{"+l.keyField+"}", escKey)
	req, err := http.NewRequestWithContext(l.ctx, l.method, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain a bounded remainder so the keep-alive connection is reused.
		_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
		resp.Body.Close()
	}()

	if resp.StatusCode == 404 {
		return nil, nil // not found = null enrichment (not an error)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d for key %q", resp.StatusCode, key)
	}

	// Read at most one byte past the cap so an oversized body is detected
	// rather than silently truncated into a decode error.
	body := io.LimitReader(resp.Body, l.maxResponseBytes+1)
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(data)) > l.maxResponseBytes {
		return nil, fmt.Errorf("response for key %q exceeds maxResponseBytes=%d", key, l.maxResponseBytes)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

func (l *lookupTransform) Close() error {
	l.cache.Purge()
	return nil
}

// newCache builds a bounded LRU cache; cacheSize is always > 0 (validated at
// construction), so the only possible New error can't occur here.
func newCache(cap int) *lru.Cache[string, cacheEntry] {
	c, _ := lru.New[string, cacheEntry](cap)
	return c
}
