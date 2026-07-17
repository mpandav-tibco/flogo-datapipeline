# Lookup / Enrich transform — `lookup`, `enrich`

Appends columns to each row from an HTTP endpoint, with an LRU cache and in-flight request
de-duplication so repeated keys hit the cache, not the network. Implemented in
[`transform/lookup/transform.go`](../../transform/lookup/transform.go); registered as
`lookup` and `enrich`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `keyField` | string | — | ✅ | Column whose value is the lookup key. |
| `url` | string | — | ✅ | Endpoint with a `{keyField}` (or `${keyField}`) macro, URL-escaped per row. |
| `method` | string | `GET` | — | HTTP method. |
| `outputFields` | `[]string` | `[]` | — | JSON response fields to append as columns. |
| `concurrency` | int | `10` | — | Worker-pool size for concurrent calls. |
| `cacheSize` | int | `1000` | — | LRU cache capacity (keyed by the lookup value). |
| `timeoutMs` | int | `5000` | — | Per-request timeout. |
| `onError` | string | `null` | — | `null` (fill nils, keep row), `skip` (drop row), or `fail` (abort). |

## Behavior

- Responses are expected to be JSON; the named `outputFields` are appended as `any`
  columns.
- A **thread-safe LRU cache** plus **singleflight** de-duplication collapses repeated and
  concurrent lookups of the same key into a single request.
- HTTP `404` returns nils (not an error); other `4xx`/`5xx` and decode failures trigger
  `onError`.
- The outbound URL is subject to [SSRF protection](../security.md).

## Example

```json
{
  "type": "lookup",
  "keyField": "customer_id",
  "url": "http://crm-api:8080/customers/{customer_id}",
  "outputFields": ["name", "tier", "region"],
  "concurrency": 20,
  "cacheSize": 5000,
  "timeoutMs": 3000,
  "onError": "null"
}
```

## See also

- [Transforms index](../transforms.md) · [Security (SSRF)](../security.md)
- Demo: [`examples/04-http-lookup-enrich`](../../examples/04-http-lookup-enrich)
