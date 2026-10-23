# REST/API source — `rest`

Pulls a JSON array of records from an HTTP(S) endpoint — optionally paging
through the full result set — and streams them as `RecordBatch`es, exactly
like any other dataset source. This is a **bulk/dataset pull** (the same
per-run shape as the jdbc/csv/parquet sources), not an event listener. Use it
for the most commonly requested connector class in DI evaluations: a generic
paginated REST API. Implemented in
[`source/rest/source.go`](../../source/rest/source.go); registered as `rest`.

- **Reads:** a JSON array from an HTTP(S) endpoint, page/offset/cursor paginated
- **Partitionable:** no
- **Checkpoint / resume:** linear `Skip(n)` (re-fetches and discards) — see
  [Resume / checkpoint](#resume--checkpoint) below

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `url` | string | — | ✅ | The endpoint URL. |
| `method` | string | `GET` | — | HTTP method. |
| `headers` | object | — | — | Static headers merged into every request. |
| `queryParams` | object | — | — | Static query parameters merged into every request. |
| `authType` | string | `none` | — | `none` \| `bearer` \| `basic` \| `apikey`. |
| `bearerTokenEnv` | string | — | with `bearer` | Env var holding the bearer token. |
| `basicUserEnv` / `basicPasswordEnv` | string | — | with `basic` | Env vars holding the Basic-auth username/password. |
| `apiKeyHeader` | string | `X-Api-Key` | — | Header name for `apikey` auth. |
| `apiKeyEnv` | string | — | with `apikey` | Env var holding the API key. |
| `dataPath` | string | `""` (root array) | — | Dot path into the JSON response locating the record array, e.g. `data.items`. |
| `columns` | array | — | — | Explicit schema (`{name,type,format,scale}`); else inferred from the first record's keys (sorted, all `any`). |
| `timeoutMs` | int | `30000` | — | Per-request HTTP client timeout. |
| `maxRetries` | int | `3` | — | Additional attempts on 429/5xx or a transport error. |
| `retryBackoffMs` | int | `500` | — | Base retry delay: retry *n* waits ~`retryBackoffMs × 2^(n-1)` with jitter (between half and the full value), unless the server sends `Retry-After`. |
| `maxBackoffMs` | int | `30000` | — | Cap on any single retry wait, including a server's `Retry-After`. |
| `maxResponseBytes` | int | `67108864` | — | Maximum size of one page body; a larger page is an error rather than an unbounded allocation. |
| `pagination` | object | `{type:"none"}` | — | See below. |

Credentials are **always** read from an environment variable named in the
config (`bearerTokenEnv`, etc.) — never written inline in the pipeline file.

### `pagination`

| Key | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `type` | string | `none` | — | `none` \| `page` \| `offset` \| `cursor`. |
| `pageParam` / `pageSizeParam` | string | `page` / `limit` | `page` | Query parameter names. |
| `startPage` / `pageSize` | int | `1` / `100` | `page` | Starting page number and page size. |
| `offsetParam` / `limitParam` | string | `offset` / `limit` | `offset` | Query parameter names. |
| `startOffset` | int | `0` | `offset` | Starting row offset. |
| `cursorParam` | string | `cursor` | `cursor` | Query parameter the extracted cursor is sent on. |
| `nextCursorPath` | string | — | `cursor` | Dot path in the response to a cursor **value** sent on the next request. |
| `nextUrlPath` | string | — | `cursor` | Dot path in the response to a **full next-page URL** (used verbatim, no static query re-applied — e.g. a GitHub-style `links.next`). One of `nextCursorPath`/`nextUrlPath` is required for `type: "cursor"`. |
| `maxPages` | int | `0` (unlimited) | all | Safety cap on the number of pages fetched, across every pagination type. |

**Stop conditions:** `page`/`offset` stop when a page returns fewer rows than
`pageSize`; `cursor` stops when the extracted cursor/URL is empty or the path
is not found; `maxPages`, if set, caps any mode.

## Behavior

- **Retries:** HTTP 429 and 5xx responses, and transport errors, are retried
  up to `maxRetries` additional attempts, waiting for the server's `Retry-After`
  when present, otherwise a jittered exponential backoff capped at `maxBackoffMs`.
  Closing the pipeline interrupts a pending wait. Any other
  non-2xx status fails immediately with the response body (truncated) in the
  error for debuggability.
- **Schema:** with no explicit `columns`, the schema is inferred from the
  first record's keys (sorted for determinism) as `record.TypeAny` columns —
  identical to the [jsonlines](jsonlines.md) source. Add a
  [`cast`](../transforms/cast.md) transform to type individual columns.
- **dataPath:** an empty `dataPath` requires the response body itself to be a
  JSON array; otherwise it is the dot path to the array inside the decoded
  response object.

## Resume / checkpoint

This source does not implement an O(1) resume cursor. `Skip(n)` resumes by
re-issuing requests (paging as configured) and discarding the first `n` rows —
the same fallback the csv/jsonlines/plain-Parquet sources use without a
keyset. This is correct but re-fetches already-seen pages on resume, which is
acceptable for the bulk/periodic-refresh pattern this source targets.

## Out of scope (documented, not silently missing)

POST-body-carried pagination (a cursor/page sent in a JSON request body rather
than a query parameter), OAuth2 token-refresh flows, and non-JSON response
bodies are not supported. Use a [lookup/enrich](../transforms/lookup.md)
transform for per-row API enrichment instead.

## Example — page-paginated API with bearer auth

```json
{
  "type": "rest",
  "url": "https://api.example.com/v1/orders",
  "authType": "bearer",
  "bearerTokenEnv": "ORDERS_API_TOKEN",
  "dataPath": "data.items",
  "pagination": {
    "type": "page",
    "pageSize": 100
  },
  "columns": [
    { "name": "id", "type": "int" },
    { "name": "customer", "type": "string" },
    { "name": "amount", "type": "float" },
    { "name": "status", "type": "string" }
  ]
}
```

## Example — cursor-paginated API (GitHub-style `links.next`)

```json
{
  "type": "rest",
  "url": "https://api.example.com/v1/events",
  "authType": "apikey",
  "apiKeyEnv": "EVENTS_API_KEY",
  "pagination": {
    "type": "cursor",
    "nextUrlPath": "links.next"
  }
}
```

## See also

- [Sources index](../sources.md) · [Cast transform](../transforms/cast.md) · [Lookup/enrich transform](../transforms/lookup.md)
- Demo: [`examples/27-rest-api-source`](../../examples/27-rest-api-source)
