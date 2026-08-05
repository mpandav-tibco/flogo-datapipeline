# REST/API sink — `rest`

Pushes rows to any HTTP(S) endpoint as JSON — the **write-side ("reverse
ETL")** counterpart to the [REST source](../sources/rest.md): syncing
pipeline data OUT to a SaaS API, exactly the pattern tools like
Census/Hightouch call reverse ETL. Implemented in
[`sink/rest/sink.go`](../../sink/rest/sink.go); registered as `rest`.

- **Writes:** one HTTP request per row (default) or one request per batch (a
  JSON array), to any HTTP(S) endpoint
- **Auth vocabulary:** identical to the [REST source](../sources/rest.md), so
  a pipeline mirroring data between two REST APIs uses the same config shape
  on both ends

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `url` | string | — | ✅ | The endpoint URL. May carry `{columnName}` macros in `perRow` mode (see below). |
| `method` | string | `POST` | — | HTTP method. |
| `headers` | object | — | — | Static headers merged into every request. Defaults `Content-Type: application/json` if not set. |
| `authType` | string | `none` | — | `none` \| `bearer` \| `basic` \| `apikey`. |
| `bearerTokenEnv` | string | — | with `bearer` | Env var holding the bearer token. |
| `basicUserEnv` / `basicPasswordEnv` | string | — | with `basic` | Env vars holding the Basic-auth username/password. |
| `apiKeyHeader` | string | `X-Api-Key` | — | Header name for `apikey` auth. |
| `apiKeyEnv` | string | — | with `apikey` | Env var holding the API key. |
| `mode` | string | `perRow` | — | `perRow` \| `batch` (see below). |
| `timeoutMs` | int | `30000` | — | Per-request HTTP client timeout. |
| `maxRetries` | int | `3` | — | Additional attempts on 429/5xx or a transport error. |
| `retryBackoffMs` | int | `500` | — | Linear backoff: `attempt * retryBackoffMs`. |
| `successStatusCodes` | `[]int` | any 2xx | — | Explicit success set, e.g. `[201]` to reject 200. |

Credentials are **always** read from an environment variable named in the
config (`bearerTokenEnv`, etc.) — never written inline in the pipeline file.

### `mode`

- **`perRow`** (default) — issues one HTTP request per row, with the row
  marshaled as a single JSON object request body. The URL may carry
  `{columnName}` macros (e.g. `.../customers/{id}`), substituted per row from
  that row's own column value (URL-escaped) — the same templating convention
  as the [`lookup`](../transforms/lookup.md) transform's `{keyField}`.
- **`batch`** — issues **one** HTTP request per `WriteBatch` call, with the
  entire batch marshaled as a JSON array of row objects. The URL is used
  as-is (no per-row `{field}` substitution). Use this only for endpoints that
  accept a bulk array body.

## Behavior

- **Retries:** HTTP 429 and 5xx responses, and transport errors, are retried
  with a linear backoff up to `maxRetries` additional attempts; any other
  non-success status fails immediately with the response body (truncated) in
  the error for debuggability.
- **Failure handling:** on the first row/request that fails (after retries),
  `WriteBatch` returns immediately with that error — it does not attempt
  every row before reporting failure. This is safe: the pipeline engine's own
  per-row isolation-on-failure fallback (see [Pipeline file → Run
  output](../pipeline-file.md#run-output)) already re-issues `WriteBatch`
  once per single row when a whole-batch call fails, so later rows in the
  same batch that would have succeeded still get their own isolated attempt.
- **Row → JSON:** each row is marshaled as a JSON object keyed by column
  name. `decimal` columns marshal as exact JSON numbers (not strings);
  everything else uses Go's standard `encoding/json` conversion.

## Retry / idempotency caveat

Retries and the engine's per-row isolation fallback both mean a row's HTTP
call **may be issued more than once**. Unlike a JDBC upsert, most REST APIs
are **not** naturally idempotent on `POST` — a retried create can create a
duplicate resource. Prefer `PUT` (whole-resource replace) when the target API
supports it, or an API that supports an idempotency-key mechanism, if
duplicate delivery must be avoided.

## Example

Sync enriched customer records to a CRM's REST API, one `PUT` per row keyed
by `id`:

```json
{
  "type": "rest",
  "url": "https://crm.example.com/v1/customers/{id}",
  "method": "PUT",
  "authType": "bearer",
  "bearerTokenEnv": "CRM_API_TOKEN",
  "mode": "perRow",
  "maxRetries": 3
}
```

Push a whole batch to a bulk-import endpoint in one request:

```json
{
  "type": "rest",
  "url": "https://api.example.com/v1/customers/bulk-import",
  "method": "POST",
  "authType": "apikey",
  "apiKeyEnv": "API_KEY",
  "mode": "batch"
}
```

## Out of scope (documented, not silently missing)

OAuth2 token-refresh flows, non-JSON request bodies, and response-body
parsing (this sink does not read the response back into the pipeline — use
the [`lookup`](../transforms/lookup.md) transform if a per-row response value
is needed). Pair with the [`broadcast`](../transforms/broadcast.md) transform
for reverse-ETL fan-out alongside a primary sink.

## See also

- [REST/API source](../sources/rest.md) — the read-side counterpart
- [Sinks index](../sinks.md)
