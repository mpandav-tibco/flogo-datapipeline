// Package rest registers a generic paginated HTTP/REST API source. It pulls a
// JSON array of records from an HTTP endpoint — optionally paging through the
// full result set — and streams them as RecordBatches, exactly like any other
// dataset source (file, table, topic). This is a bulk/dataset pull, not an
// event listener: it reads the endpoint's current result set once per run,
// the same way the jdbc/csv/parquet sources read their data once per run.
// Import: import _ "github.com/mpandav-tibco/flogo-datapipeline/source/rest"
//
// Config:
//
//	{
//	  "type": "rest",
//	  "url": "https://api.example.com/v1/orders",
//	  "method": "GET",
//	  "headers": {"Accept": "application/json"},
//	  "queryParams": {"status": "active"},
//
//	  "authType": "bearer",
//	  "bearerTokenEnv": "API_TOKEN",
//
//	  "dataPath": "data.items",
//
//	  "pagination": {
//	    "type": "page",
//	    "pageParam": "page", "pageSizeParam": "limit",
//	    "startPage": 1, "pageSize": 100
//	  },
//
//	  "columns": [{"name": "id", "type": "int"}, {"name": "total", "type": "decimal"}],
//	  "timeoutMs": 30000,
//	  "maxRetries": 3,
//	  "retryBackoffMs": 500
//	}
//
// # Authentication (authType)
//
//   - "none"   (default) — no Authorization header.
//   - "bearer" — `Authorization: Bearer <token>`; token read from the env var
//     named by "bearerTokenEnv" (never inline in the config/pipeline file).
//   - "basic"  — HTTP Basic auth; username/password read from the env vars
//     named by "basicUserEnv" / "basicPasswordEnv".
//   - "apikey" — a custom header named by "apiKeyHeader" (default "X-Api-Key")
//     set to the value of the env var named by "apiKeyEnv".
//
// # dataPath
//
// Dot-separated path into the parsed JSON response locating the array of row
// objects, e.g. `{"data":{"items":[...]}}` → dataPath "data.items". An empty
// dataPath (the default) means the response body itself is the JSON array.
//
// # Pagination (pagination.type)
//
//   - "none"   (default) — a single request; the response's dataPath array is
//     the entire result set.
//   - "page"   — increments an integer page number query parameter
//     (pageParam, default "page", starting at startPage, default 1) and sends
//     a page-size parameter (pageSizeParam, default "limit", value pageSize,
//     default 100). Stops when a page returns fewer rows than pageSize (or 0),
//     or after "maxPages" pages if set (0 = unlimited).
//   - "offset" — increments a row-offset query parameter (offsetParam, default
//     "offset") by the page size (limitParam, default "limit", value
//     pageSize). Same stop rule as "page".
//   - "cursor" — after each page, extracts a cursor value from the response
//     via "nextCursorPath" (a dot path, sent as the "cursorParam" query
//     parameter on the next request) or a full next-page URL via
//     "nextUrlPath" (a dot path to an absolute or relative URL). Stops when
//     the extracted value is empty or the path is not found.
//
// # Resume / checkpoint
//
// This source does not implement an O(1) resume cursor: Skip(n) resumes by
// re-issuing requests (paging as configured) and discarding the first n rows,
// exactly like the csv/jsonlines/plain-Parquet sources without a keyset. This
// is correct but re-fetches already-seen pages on resume — acceptable for the
// bulk/periodic-refresh pattern this source targets.
//
// # Out of scope (documented, not silently missing)
//
// POST-body-carried pagination (cursor/page sent in a JSON request body rather
// than a query parameter), OAuth2 token-refresh flows, and non-JSON response
// bodies are not supported. Use a dedicated source, or a lookup/enrich
// transform, for those cases.
package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/source"
)

func init() {
	source.Register("rest", newRestSource)
}

type restSource struct {
	client  *http.Client
	baseURL string
	method  string
	headers map[string]string
	query   map[string]string

	dataPath string
	pag      paginationConfig

	fields []string // ordered field names; nil = infer from first record
	schema []record.Column

	maxRetries     int
	retryBackoffMs int

	// Pagination cursor state, advanced as pages are fetched.
	state paginationState
	done  bool // true once the source has signalled its last page

	// pending holds records fetched but not yet emitted (buffered across
	// ReadBatch/Skip calls when a fetched page is larger/smaller than the
	// caller's requested batch size).
	pending []map[string]interface{}

	closed bool
}

func newRestSource(cfg map[string]interface{}) (source.Source, error) {
	rawURL, _ := cfg["url"].(string)
	if rawURL == "" {
		return nil, fmt.Errorf("rest source: 'url' is required")
	}
	if _, err := url.Parse(rawURL); err != nil {
		return nil, fmt.Errorf("rest source: invalid 'url': %w", err)
	}

	method, _ := cfg["method"].(string)
	if method == "" {
		method = http.MethodGet
	}

	headers, err := stringMapFromCfg(cfg, "headers")
	if err != nil {
		return nil, fmt.Errorf("rest source: %w", err)
	}
	authHeaders, err := authHeadersFromCfg(cfg)
	if err != nil {
		return nil, err
	}
	for k, v := range authHeaders {
		headers[k] = v
	}

	query, err := stringMapFromCfg(cfg, "queryParams")
	if err != nil {
		return nil, fmt.Errorf("rest source: %w", err)
	}

	dataPath, _ := cfg["dataPath"].(string)

	pag, err := parsePaginationConfig(cfg["pagination"])
	if err != nil {
		return nil, fmt.Errorf("rest source: %w", err)
	}

	var fields []string
	schema := parseColumnDefs(cfg)
	if len(schema) > 0 {
		fields = make([]string, len(schema))
		for i, c := range schema {
			fields[i] = c.Name
		}
	}

	timeoutMs := intFromCfg(cfg, "timeoutMs", 30000)
	maxRetries := intFromCfg(cfg, "maxRetries", 3)
	retryBackoffMs := intFromCfg(cfg, "retryBackoffMs", 500)

	return &restSource{
		client:         &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond},
		baseURL:        rawURL,
		method:         method,
		headers:        headers,
		query:          query,
		dataPath:       dataPath,
		pag:            pag,
		fields:         fields,
		schema:         schema,
		maxRetries:     maxRetries,
		retryBackoffMs: retryBackoffMs,
		state:          pag.initialState(),
	}, nil
}

func (s *restSource) Schema() []record.Column { return s.schema }

// ReadBatch fetches pages from the API as needed to fill a batch of up to size
// rows, converting each JSON object into a row ordered by s.fields (inferring
// the field list — and therefore the schema — from the first record read, if
// "columns" was not declared).
func (s *restSource) ReadBatch(size int) (*record.RecordBatch, error) {
	if s.closed {
		return nil, io.EOF
	}

	batch := record.Get()
	batch.Rows = make([][]interface{}, 0, size)

	for len(batch.Rows) < size {
		if len(s.pending) == 0 {
			if s.done {
				break
			}
			recs, err := s.fetchNextPage()
			if err != nil {
				if len(batch.Rows) > 0 {
					batch.Columns = s.schema
					return batch, err
				}
				record.Put(batch)
				return nil, err
			}
			s.pending = recs
			// Do not infer "done" from an empty page here — s.done is already
			// set correctly by fetchNextPage via pag.advance(). An empty page
			// with more pages remaining (e.g. a cursor-mode page with zero
			// rows in that window) must keep polling, not stop early.
			continue
		}

		obj := s.pending[0]
		s.pending = s.pending[1:]

		if s.schema == nil {
			s.inferSchema(obj)
		}
		batch.Rows = append(batch.Rows, s.extractRow(obj))
	}

	batch.Columns = s.schema
	if len(batch.Rows) == 0 {
		record.Put(batch)
		return nil, io.EOF
	}
	if s.done && len(s.pending) == 0 {
		return batch, io.EOF
	}
	return batch, nil
}

// Skip advances past n rows by fetching (and paging through) records exactly
// as ReadBatch would, discarding them. This is a correct but linear resume
// path — see the package doc's "Resume / checkpoint" section.
func (s *restSource) Skip(n int64) error {
	for n > 0 {
		want := int64(1000)
		if n < want {
			want = n
		}
		batch, err := s.ReadBatch(int(want))
		if batch != nil {
			n -= int64(batch.Len())
			record.Put(batch)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *restSource) Close() error {
	s.closed = true
	s.client.CloseIdleConnections()
	return nil
}

func (s *restSource) inferSchema(obj map[string]interface{}) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	// Simple insertion sort for determinism (map iteration order is random).
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	s.fields = keys
	s.schema = make([]record.Column, len(keys))
	for i, k := range keys {
		s.schema[i] = record.Column{Name: k, Type: record.TypeAny}
	}
}

func (s *restSource) extractRow(obj map[string]interface{}) []interface{} {
	row := make([]interface{}, len(s.fields))
	for i, f := range s.fields {
		row[i] = obj[f]
	}
	return row
}

// fetchNextPage issues the next HTTP request (per the pagination mode and
// current state), extracts the record array via dataPath, and advances the
// pagination state for the following call.
func (s *restSource) fetchNextPage() ([]map[string]interface{}, error) {
	reqURL, err := s.pag.buildRequestURL(s.baseURL, s.query, s.state)
	if err != nil {
		return nil, fmt.Errorf("rest source: build request url: %w", err)
	}

	body, err := s.doWithRetry(reqURL)
	if err != nil {
		return nil, err
	}

	var parsed interface{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("rest source: decode response as JSON: %w", err)
	}

	arr, err := extractArray(parsed, s.dataPath)
	if err != nil {
		return nil, fmt.Errorf("rest source: %w", err)
	}

	recs := make([]map[string]interface{}, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]interface{}); ok {
			recs = append(recs, m)
		}
	}

	if !s.pag.advance(&s.state, parsed, len(recs)) {
		s.done = true
	}
	return recs, nil
}

// doWithRetry issues the HTTP request, retrying on 429/5xx and transport
// errors with a linear backoff (attempt * retryBackoffMs), up to maxRetries
// additional attempts. Any other non-2xx status fails immediately with the
// response body included for debuggability.
func (s *restSource) doWithRetry(reqURL string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*s.retryBackoffMs) * time.Millisecond)
		}

		req, err := http.NewRequest(s.method, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("rest source: build request: %w", err)
		}
		for k, v := range s.headers {
			req.Header.Set(k, v)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("rest source: request %s: %w", reqURL, err)
			continue
		}
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("rest source: read response body: %w", readErr)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("rest source: %s returned %d: %s", reqURL, resp.StatusCode, truncate(respBody, 500))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("rest source: %s returned %d: %s", reqURL, resp.StatusCode, truncate(respBody, 500))
		}
		return respBody, nil
	}
	return nil, fmt.Errorf("rest source: giving up after %d attempts: %w", s.maxRetries+1, lastErr)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// ─── config helpers ──────────────────────────────────────────────────────────

func authHeadersFromCfg(cfg map[string]interface{}) (map[string]string, error) {
	authType, _ := cfg["authType"].(string)
	headers := map[string]string{}
	switch authType {
	case "", "none":
		return headers, nil
	case "bearer":
		envName, _ := cfg["bearerTokenEnv"].(string)
		if envName == "" {
			return nil, fmt.Errorf("rest source: authType 'bearer' requires 'bearerTokenEnv'")
		}
		token := os.Getenv(envName)
		if token == "" {
			return nil, fmt.Errorf("rest source: env var %q (bearerTokenEnv) is empty or unset", envName)
		}
		headers["Authorization"] = "Bearer " + token
	case "basic":
		userEnv, _ := cfg["basicUserEnv"].(string)
		passEnv, _ := cfg["basicPasswordEnv"].(string)
		if userEnv == "" || passEnv == "" {
			return nil, fmt.Errorf("rest source: authType 'basic' requires 'basicUserEnv' and 'basicPasswordEnv'")
		}
		user, pass := os.Getenv(userEnv), os.Getenv(passEnv)
		if user == "" {
			return nil, fmt.Errorf("rest source: env var %q (basicUserEnv) is empty or unset", userEnv)
		}
		req := &http.Request{Header: http.Header{}}
		req.SetBasicAuth(user, pass)
		headers["Authorization"] = req.Header.Get("Authorization")
	case "apikey":
		headerName, _ := cfg["apiKeyHeader"].(string)
		if headerName == "" {
			headerName = "X-Api-Key"
		}
		envName, _ := cfg["apiKeyEnv"].(string)
		if envName == "" {
			return nil, fmt.Errorf("rest source: authType 'apikey' requires 'apiKeyEnv'")
		}
		key := os.Getenv(envName)
		if key == "" {
			return nil, fmt.Errorf("rest source: env var %q (apiKeyEnv) is empty or unset", envName)
		}
		headers[headerName] = key
	default:
		return nil, fmt.Errorf("rest source: unknown authType %q (want none|bearer|basic|apikey)", authType)
	}
	return headers, nil
}

func stringMapFromCfg(cfg map[string]interface{}, key string) (map[string]string, error) {
	out := map[string]string{}
	raw, ok := cfg[key]
	if !ok || raw == nil {
		return out, nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'%s' must be a JSON object of string values", key)
	}
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("'%s.%s' must be a string", key, k)
		}
		out[k] = s
	}
	return out, nil
}

func intFromCfg(cfg map[string]interface{}, key string, def int) int {
	switch v := cfg[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64: // JSON numbers decode as float64
		return int(v)
	}
	return def
}

// parseColumnDefs extracts an explicit []record.Column from cfg["columns"],
// or nil if not declared (schema is then inferred from the first record).
func parseColumnDefs(cfg map[string]interface{}) []record.Column {
	raw, ok := cfg["columns"].([]interface{})
	if !ok {
		return nil
	}
	cols := make([]record.Column, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		col := record.Column{Type: record.TypeString}
		if name, ok := m["name"].(string); ok {
			col.Name = name
		}
		if typ, ok := m["type"].(string); ok && typ != "" {
			col.Type = record.ColumnType(typ)
		}
		if format, ok := m["format"].(string); ok {
			col.Format = format
		}
		if scale, ok := m["scale"].(float64); ok {
			col.Scale = int(scale)
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

// extractArray walks dotPath (dot-separated keys) into parsed and returns the
// array found there. An empty dotPath requires parsed to already be an array.
func extractArray(parsed interface{}, dotPath string) ([]interface{}, error) {
	v, err := walkPath(parsed, dotPath)
	if err != nil {
		return nil, err
	}
	arr, ok := v.([]interface{})
	if !ok {
		if dotPath == "" {
			return nil, fmt.Errorf("response body is not a JSON array (set 'dataPath' to the array field)")
		}
		return nil, fmt.Errorf("dataPath %q does not point to a JSON array", dotPath)
	}
	return arr, nil
}

// walkPath navigates a dot-separated path of object keys into a decoded JSON
// value. An empty path returns v unchanged. Returns an error if an
// intermediate segment is missing or not an object.
func walkPath(v interface{}, dotPath string) (interface{}, error) {
	if dotPath == "" {
		return v, nil
	}
	cur := v
	start := 0
	for i := 0; i <= len(dotPath); i++ {
		if i == len(dotPath) || dotPath[i] == '.' {
			key := dotPath[start:i]
			m, ok := cur.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path %q: expected an object at %q", dotPath, key)
			}
			next, ok := m[key]
			if !ok {
				return nil, fmt.Errorf("path %q: key %q not found", dotPath, key)
			}
			cur = next
			start = i + 1
		}
	}
	return cur, nil
}
