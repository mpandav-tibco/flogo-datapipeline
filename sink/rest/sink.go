// Package rest registers a generic HTTP/REST API sink. It pushes rows to any
// HTTP(S) endpoint as JSON — the write-side ("reverse ETL") counterpart to
// source/rest's read-side generic REST source: syncing pipeline data OUT to a
// SaaS API, exactly the pattern tools like Census/Hightouch call reverse ETL.
//
// Import this package with a blank import to activate it:
//
//	import _ "github.com/mpandav-tibco/flogo-datapipeline/sink/rest"
//
// Config:
//
//	{
//	  "type": "rest",
//	  "url": "https://api.example.com/v1/customers/{id}",
//	  "method": "POST",
//	  "headers": {"Content-Type": "application/json"},
//
//	  "authType": "bearer",
//	  "bearerTokenEnv": "API_TOKEN",
//
//	  "mode": "perRow",
//
//	  "timeoutMs": 30000,
//	  "maxRetries": 3,
//	  "retryBackoffMs": 500,
//	  "maxBackoffMs": 30000,
//	  "concurrency": 1,
//	  "successStatusCodes": [200, 201, 202, 204]
//	}
//
// # Authentication (authType)
//
// Identical vocabulary to source/rest, so a pipeline mirroring data between
// two REST APIs uses the same config shape on both ends:
//
//   - "none"   (default) — no Authorization header.
//   - "bearer" — `Authorization: Bearer <token>`; token read from the env var
//     named by "bearerTokenEnv".
//   - "basic"  — HTTP Basic auth; username/password read from the env vars
//     named by "basicUserEnv" / "basicPasswordEnv".
//   - "apikey" — a custom header named by "apiKeyHeader" (default "X-Api-Key")
//     set to the value of the env var named by "apiKeyEnv".
//
// Credentials are always read from an environment variable by name — never
// written inline in the pipeline file.
//
// # mode
//
//   - "perRow" (default) — issues one HTTP request per row, with the row
//     marshaled as a single JSON object request body. The URL may carry
//     "{columnName}" macros (e.g. ".../customers/{id}") substituted per row
//     from that row's own column value (URL-escaped) — the same templating
//     convention as the lookup transform's "{keyField}".
//   - "batch"  — issues ONE HTTP request per WriteBatch call, with the entire
//     batch marshaled as a JSON array of row objects. The URL is used as-is
//     (no per-row "{field}" substitution, since there is no single row).
//     Use this only for endpoints that accept a bulk array body.
//
// "concurrency" (perRow only, default 1) sends up to that many row requests
// in parallel. Rows of one batch may then reach the API out of order; keep 1
// when the API requires ordered writes.
//
// # successStatusCodes
//
// Defaults to any 2xx status. Set explicitly if the API's success code isn't
// in that range, or to narrow acceptance (e.g. only 201, rejecting 200).
//
// # Retry / idempotency caveat
//
// Retries (429/5xx and transport errors, up to maxRetries) wait for the
// server's Retry-After when given, otherwise an exponential backoff from
// retryBackoffMs with jitter, capped at maxBackoffMs. Retries and the pipeline
// engine's own per-row isolation-on-failure fallback (see
// action/pipeline's rowLevelWrite) both mean a row's HTTP call MAY be issued
// more than once. Unlike a JDBC upsert, most REST APIs are NOT naturally
// idempotent on POST — a retried create can create a duplicate resource. Use
// "PUT" (whole-resource replace) or an API that supports an idempotency-key
// header/mechanism where duplicate delivery must be avoided.
//
// # Out of scope (documented, not silently missing)
//
// OAuth2 token-refresh flows, non-JSON request bodies, and response-body
// parsing (this sink does not read the response back into the pipeline — use
// the lookup transform if you need a per-row response value). Pair with the
// broadcast transform for reverse-ETL fan-out alongside a primary sink.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mpandav-tibco/flogo-datapipeline/internal/httpretry"
	"github.com/mpandav-tibco/flogo-datapipeline/record"
	"github.com/mpandav-tibco/flogo-datapipeline/sink"
)

func init() {
	sink.Register("rest", newRestSink)
}

type restSink struct {
	client  *http.Client
	baseURL string
	method  string
	headers map[string]string

	mode string // "perRow" | "batch"

	maxRetries     int
	retryBackoffMs int
	maxBackoff     time.Duration
	successCodes   map[int]bool // empty = accept any 2xx

	// concurrency bounds parallel perRow requests (1 = sequential).
	concurrency int
	// urlFields lists the "{column}" macros present in baseURL, parsed once so
	// per-row expansion only touches columns the URL actually references.
	urlFields []string

	// ctx is cancelled by Close so in-flight retry waits stop promptly.
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool // set by Close, which may run concurrently with WriteBatch
}

var urlMacro = regexp.MustCompile(`\{([^{}]+)\}`)

func newRestSink(cfg map[string]interface{}) (sink.Sink, error) {
	rawURL, _ := cfg["url"].(string)
	if rawURL == "" {
		return nil, fmt.Errorf("rest sink: 'url' is required")
	}

	method, _ := cfg["method"].(string)
	if method == "" {
		method = http.MethodPost
	}

	headers, err := stringMapFromCfg(cfg, "headers")
	if err != nil {
		return nil, fmt.Errorf("rest sink: %w", err)
	}
	if _, ok := headers["Content-Type"]; !ok {
		headers["Content-Type"] = "application/json"
	}
	authHeaders, err := authHeadersFromCfg(cfg)
	if err != nil {
		return nil, err
	}
	for k, v := range authHeaders {
		headers[k] = v
	}

	mode, _ := cfg["mode"].(string)
	switch mode {
	case "":
		mode = "perRow"
	case "perRow", "batch":
		// valid
	default:
		return nil, fmt.Errorf("rest sink: 'mode' must be 'perRow' or 'batch', got %q", mode)
	}

	successCodes := map[int]bool{}
	if raw, ok := cfg["successStatusCodes"].([]interface{}); ok {
		for _, v := range raw {
			switch n := v.(type) {
			case int:
				successCodes[n] = true
			case float64:
				successCodes[int(n)] = true
			default:
				return nil, fmt.Errorf("rest sink: 'successStatusCodes' entries must be numbers")
			}
		}
	}

	timeoutMs := intFromCfg(cfg, "timeoutMs", 30000)
	maxRetries := intFromCfg(cfg, "maxRetries", 3)
	retryBackoffMs := intFromCfg(cfg, "retryBackoffMs", 500)
	maxBackoffMs := intFromCfg(cfg, "maxBackoffMs", 30000)
	concurrency := intFromCfg(cfg, "concurrency", 1)
	if concurrency < 1 {
		concurrency = 1
	}

	var urlFields []string
	for _, m := range urlMacro.FindAllStringSubmatch(rawURL, -1) {
		urlFields = append(urlFields, m[1])
	}

	// A dedicated transport keeps enough idle connections for the configured
	// concurrency (the default transport keeps only 2 per host).
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = max(concurrency, 2)

	ctx, cancel := context.WithCancel(context.Background())
	return &restSink{
		client:         &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond, Transport: transport},
		baseURL:        rawURL,
		method:         method,
		headers:        headers,
		mode:           mode,
		maxRetries:     maxRetries,
		retryBackoffMs: retryBackoffMs,
		maxBackoff:     time.Duration(maxBackoffMs) * time.Millisecond,
		successCodes:   successCodes,
		concurrency:    concurrency,
		urlFields:      urlFields,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// WriteBatch issues one HTTP request per row (mode "perRow", default) or one
// request for the whole batch as a JSON array (mode "batch"). Rows are
// converted to a JSON object keyed by column name.
//
// On the FIRST row/request that fails (after retries), WriteBatch returns
// immediately with that error — it does not attempt to send every row before
// reporting failure. This is intentional and safe: the pipeline engine's own
// rowLevelWrite already falls back to calling WriteBatch once per single row
// when a whole-batch call fails, so later rows in this batch that would have
// succeeded still get their own isolated attempt on that fallback pass;
// nothing is silently lost, only (at most) a few already-attempted rows are
// retried once more.
func (s *restSink) WriteBatch(batch *record.RecordBatch) error {
	if batch == nil || batch.Len() == 0 {
		return nil
	}
	if s.mode == "batch" {
		body, err := rowsToJSONArray(batch)
		if err != nil {
			return fmt.Errorf("rest sink: marshal batch: %w", err)
		}
		return s.doWithRetry(s.baseURL, body)
	}
	if s.concurrency > 1 && batch.Len() > 1 {
		return s.writeRowsConcurrently(batch)
	}
	var row map[string]interface{}
	for i := range batch.Rows {
		row = batch.RowMapInto(i, row)
		if err := s.sendRow(i, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *restSink) sendRow(i int, row map[string]interface{}) error {
	body, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("rest sink: marshal row %d: %w", i, err)
	}
	if err := s.doWithRetry(s.expandURL(row), body); err != nil {
		return fmt.Errorf("rest sink: row %d: %w", i, err)
	}
	return nil
}

// writeRowsConcurrently sends rows with up to s.concurrency requests in
// flight. After the first failure no new rows are started; the error of the
// lowest failing row index is returned once in-flight requests finish, which
// matches the sequential contract the engine's per-row fallback relies on.
func (s *restSink) writeRowsConcurrently(batch *record.RecordBatch) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstIdx = -1
		firstErr error
	)
	sem := make(chan struct{}, s.concurrency)
	for i := range batch.Rows {
		mu.Lock()
		failed := firstErr != nil
		mu.Unlock()
		if failed {
			break
		}
		row := batch.RowMap(i) // each worker owns its row map
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.sendRow(i, row); err != nil {
				mu.Lock()
				if firstErr == nil || i < firstIdx {
					firstIdx, firstErr = i, err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

func (s *restSink) Flush() error { return nil } // each WriteBatch call is already synchronous

func (s *restSink) Close() error {
	s.closed.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// expandURL substitutes "{columnName}" macros in the base URL with that row's
// own (URL-escaped) value — the same convention as the lookup transform's
// "{keyField}" templating — so a per-row URL like ".../customers/{id}" targets
// the right resource. Unmatched macros (a column not present in the row) are
// left as-is rather than silently dropped, to fail loudly at the API instead
// of masking a config typo.
func (s *restSink) expandURL(row map[string]interface{}) string {
	out := s.baseURL
	for _, col := range s.urlFields {
		val, ok := row[col]
		if !ok {
			continue
		}
		out = strings.ReplaceAll(out, "{"+col+"}", url.PathEscape(record.FormatValue(val)))
	}
	return out
}

// rowsToJSONArray marshals every row in batch as a JSON array of objects
// keyed by column name, for mode "batch".
func rowsToJSONArray(batch *record.RecordBatch) ([]byte, error) {
	rows := make([]map[string]interface{}, batch.Len())
	for i := range batch.Rows {
		rows[i] = batch.RowMap(i)
	}
	return json.Marshal(rows)
}

// doWithRetry issues the HTTP request, retrying on 429/5xx and transport
// errors up to maxRetries additional attempts. Each wait honours the server's
// Retry-After header when present, otherwise uses jittered exponential
// backoff from retryBackoffMs (capped at maxBackoffMs); Close interrupts it.
// Any other non-success status fails immediately with (a bounded prefix of)
// the response body included for debuggability.
func (s *restSink) doWithRetry(reqURL string, body []byte) error {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	var lastResp *http.Response // headers only (for Retry-After); body already closed
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			wait := httpretry.Delay(attempt, lastResp, time.Duration(s.retryBackoffMs)*time.Millisecond, s.maxBackoff)
			if !httpretry.Sleep(ctx, wait) {
				return fmt.Errorf("rest sink: closed while retrying: %w", lastErr)
			}
		}

		req, err := http.NewRequestWithContext(ctx, s.method, reqURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("rest sink: build request: %w", err)
		}
		for k, v := range s.headers {
			req.Header.Set(k, v)
		}

		resp, err := s.client.Do(req)
		lastResp = nil
		if err != nil {
			lastErr = fmt.Errorf("rest sink: request %s: %w", reqURL, err)
			continue
		}
		lastResp = resp
		// Only a prefix is ever reported, so never buffer an unbounded body.
		respBody, _, readErr := httpretry.ReadLimited(resp.Body, 4096)
		httpretry.Drain(resp)
		if readErr != nil {
			lastErr = fmt.Errorf("rest sink: read response body: %w", readErr)
			continue
		}

		if s.isSuccess(resp.StatusCode) {
			return nil
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("rest sink: %s returned %d: %s", reqURL, resp.StatusCode, truncate(respBody, 500))
			continue
		}
		return fmt.Errorf("rest sink: %s returned %d: %s", reqURL, resp.StatusCode, truncate(respBody, 500))
	}
	return fmt.Errorf("rest sink: giving up after %d attempts: %w", s.maxRetries+1, lastErr)
}

func (s *restSink) isSuccess(code int) bool {
	if len(s.successCodes) > 0 {
		return s.successCodes[code]
	}
	return code >= 200 && code < 300
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// ─── config helpers (mirrors source/rest's, kept package-local since the two
// packages don't share an internal helper package — same precedent as the
// jdbc source/sink, which each own their connection setup independently) ──────

func authHeadersFromCfg(cfg map[string]interface{}) (map[string]string, error) {
	authType, _ := cfg["authType"].(string)
	headers := map[string]string{}
	switch authType {
	case "", "none":
		return headers, nil
	case "bearer":
		envName, _ := cfg["bearerTokenEnv"].(string)
		if envName == "" {
			return nil, fmt.Errorf("rest sink: authType 'bearer' requires 'bearerTokenEnv'")
		}
		token := os.Getenv(envName)
		if token == "" {
			return nil, fmt.Errorf("rest sink: env var %q (bearerTokenEnv) is empty or unset", envName)
		}
		headers["Authorization"] = "Bearer " + token
	case "basic":
		userEnv, _ := cfg["basicUserEnv"].(string)
		passEnv, _ := cfg["basicPasswordEnv"].(string)
		if userEnv == "" || passEnv == "" {
			return nil, fmt.Errorf("rest sink: authType 'basic' requires 'basicUserEnv' and 'basicPasswordEnv'")
		}
		user, pass := os.Getenv(userEnv), os.Getenv(passEnv)
		if user == "" {
			return nil, fmt.Errorf("rest sink: env var %q (basicUserEnv) is empty or unset", userEnv)
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
			return nil, fmt.Errorf("rest sink: authType 'apikey' requires 'apiKeyEnv'")
		}
		key := os.Getenv(envName)
		if key == "" {
			return nil, fmt.Errorf("rest sink: env var %q (apiKeyEnv) is empty or unset", envName)
		}
		headers[headerName] = key
	default:
		return nil, fmt.Errorf("rest sink: unknown authType %q (want none|bearer|basic|apikey)", authType)
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
