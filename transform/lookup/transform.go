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
//	  "onError": "null"
//	}
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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

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
	cache        *lruCache
	sfGroup      singleflight.Group // zero value is ready; no init needed
}

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
	concurrency := 10
	if c, ok := cfg["concurrency"].(int); ok && c > 0 {
		concurrency = c
	}
	cacheSize := 1000
	if cs, ok := cfg["cacheSize"].(int); ok && cs > 0 {
		cacheSize = cs
	}
	timeoutMs := 5000
	if tm, ok := cfg["timeoutMs"].(int); ok && tm > 0 {
		timeoutMs = tm
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
		cache:        newLRUCache(cacheSize),
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

	sem := make(chan struct{}, l.concurrency)
	var wg sync.WaitGroup

	for i := range in.Rows {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			key := fmt.Sprintf("%v", in.RowMap(idx)[l.keyField])
			data, err := l.fetchWithCache(key)
			results[idx] = result{idx: idx, data: data, err: err}
		}(i)
	}
	wg.Wait()

	skipIndices := make(map[int]bool)
	for _, r := range results {
		if r.err != nil {
			switch l.onError {
			case "fail":
				record.Put(out)
				return nil, fmt.Errorf("lookup: key %v: %w",
					fmt.Sprintf("%v", in.RowMap(r.idx)[l.keyField]), r.err)
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

func (l *lookupTransform) fetchWithCache(key string) (map[string]interface{}, error) {
	if v, ok := l.cache.Get(key); ok {
		return v, nil
	}
	v, err, _ := l.sfGroup.Do(key, func() (interface{}, error) {
		data, err := l.fetch(key)
		if err == nil && data != nil {
			l.cache.Put(key, data)
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
//
// Known limitation: the request is created with context.Background(), not a
// context derived from the pipeline run, because transform.Transform.Apply
// (see transform/transform.go) takes no context.Context parameter at all —
// threading one through would mean an interface-level change touching every
// transform implementation, not just this one. In practice this only means a
// cancelled pipeline waits for the in-flight request's own client.Timeout
// (default 5s, see the package doc's "timeoutMs" config) to elapse rather
// than aborting immediately; it is a bounded delay, not an unbounded hang.
func (l *lookupTransform) fetch(key string) (map[string]interface{}, error) {
	// Accept both {keyField} and ${keyField} so users familiar with the source
	// path templating (which uses ${date}) aren't surprised. Replace the
	// ${...} form first so the {...} pass doesn't leave a stray "$".
	escKey := url.PathEscape(key)
	target := l.urlTemplate
	target = strings.ReplaceAll(target, "${"+l.keyField+"}", escKey)
	target = strings.ReplaceAll(target, "{"+l.keyField+"}", escKey)
	req, err := http.NewRequestWithContext(context.Background(), l.method, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, nil // not found = null enrichment (not an error)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d for key %q", resp.StatusCode, key)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

func (l *lookupTransform) Close() error {
	l.cache.Clear()
	return nil
}

// lruCache is a simple bounded LRU cache using a doubly-linked list + map.
// Get and Put are concurrent-safe (each acquires c.mu). fetchWithCache uses
// a singleflight.Group to avoid duplicate in-flight fetches for the same key.
type lruCache struct {
	mu    sync.Mutex
	cap   int
	items map[string]*lruNode
	head  *lruNode // most-recently used sentinel
	tail  *lruNode // least-recently used sentinel
}

type lruNode struct {
	key   string
	value map[string]interface{}
	prev  *lruNode
	next  *lruNode
}

func newLRUCache(cap int) *lruCache {
	head := &lruNode{}
	tail := &lruNode{}
	head.next = tail
	tail.prev = head
	return &lruCache{
		cap:   cap,
		items: make(map[string]*lruNode, cap),
		head:  head,
		tail:  tail,
	}
}

func (c *lruCache) Get(key string) (map[string]interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.items[key]; ok {
		c.moveToFront(n)
		return n.value, true
	}
	return nil, false
}

func (c *lruCache) Put(key string, value map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.items[key]; ok {
		n.value = value
		c.moveToFront(n)
		return
	}
	n := &lruNode{key: key, value: value}
	c.items[key] = n
	c.addToFront(n)
	if len(c.items) > c.cap {
		c.evict()
	}
}

func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*lruNode, c.cap)
	c.head.next = c.tail
	c.tail.prev = c.head
}

func (c *lruCache) moveToFront(n *lruNode) {
	n.prev.next = n.next
	n.next.prev = n.prev
	c.addToFront(n)
}

func (c *lruCache) addToFront(n *lruNode) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

func (c *lruCache) evict() {
	lru := c.tail.prev
	if lru == c.head {
		return
	}
	lru.prev.next = c.tail
	c.tail.prev = lru.prev
	delete(c.items, lru.key)
}
