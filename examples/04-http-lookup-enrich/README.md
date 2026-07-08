# Example 04 — HTTP Lookup Enrichment

Demonstrates how to enrich a row-level dataset with data fetched from an external HTTP API, using an LRU cache to avoid redundant network calls and a concurrency limit to control outbound request rate.

---

## What This Example Does

1. **Source** — reads `data/users.csv` (10 rows: `user_id`, `name`, `plan_type`, `amount`).
2. **Lookup transform** — for each row, issues a `GET` request to `https://jsonplaceholder.typicode.com/users/{user_id}` and grafts the response fields `username`, `email`, `phone`, `website` onto the row.
   - Results are cached in an in-process LRU cache (capacity 100). Any row with the same `user_id` that appears more than once uses the cached response without making another HTTP call.
   - Up to 5 requests are in-flight concurrently.
   - Requests that time out or return a non-2xx status produce `null` values for the output fields (`onError: "null"`).
3. **Map transform** — selects and renames fields into the final output shape, using `coalesce()` to substitute defaults when the API call did not return a value.
4. **Sink** — `null` (discards rows). Replace with a `jdbc` or `parquet` sink to persist the enriched data.

Because user IDs 1–10 are real records in the JSONPlaceholder dataset, every lookup in this example returns actual data.

---

## File Layout

```
04-http-lookup-enrich/
├── http-lookup-enrich.datapipeline   # pipeline definition
├── run.sh                            # convenience runner script
└── data/
    └── users.csv                     # 10-row sample dataset
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| `flogo-datapipeline` binary | Must be on `PATH` |
| Internet access | Pipeline calls `jsonplaceholder.typicode.com` |
| Go 1.21+ | Only if building the binary from source |

---

## How to Run

```bash
cd examples/04-http-lookup-enrich
chmod +x run.sh
./run.sh
```

Or run the pipeline directly:

```bash
flogo-datapipeline -f http-lookup-enrich.datapipeline
```

---

## Expected Output

The engine logs one enriched record per row. Each record combines the original CSV fields with the API-sourced fields:

```
[row 1]  user_id=1  name="Leanne Graham"  username="Bret"  email="Sincere@april.biz"  phone="1-770-736-8031 x56442"  plan=FREE
[row 2]  user_id=2  name="Ervin Howell"   username="Antonette"  email="Shanna@melissa.tv"  phone="010-692-6593 x09125"  plan=PRO
...
[row 10] user_id=10 name="Clementina DuBuque"  username="Moriah.Stanton"  email="Rey.Padberg@karina.biz"  plan=PRO
Pipeline finished. 10 rows read, 0 rows written (null sink).
```

---

## How to Verify

1. **API responses are correct** — open `https://jsonplaceholder.typicode.com/users/1` in a browser and compare `username`/`email`/`phone`/`website` with what the pipeline logs.
2. **Cache is working** — duplicate the first row in `users.csv` (copy `user_id=1` twice). The engine should log one HTTP call for `user_id=1` and serve the second occurrence from cache (watch for a "cache hit" log line or observe that only 10 unique HTTP requests are issued for 11 rows).
3. **onError fallback** — add a row with `user_id=9999`. The lookup will receive a 404, `onError: "null"` kicks in, and the map transform's `coalesce($.email, 'unknown@example.com')` substitutes the default.
4. **Concurrency** — set `concurrency` to `1` and observe that rows are enriched sequentially; set it to `10` and observe parallel fetches in the logs.

---

## Pipeline Walkthrough

### Source

```json
"source": {
  "type": "csv",
  "path": "./data/users.csv",
  "delimiter": ",",
  "hasHeader": true,
  "columns": [
    { "name": "user_id",   "type": "int"    },
    { "name": "name",      "type": "string" },
    { "name": "plan_type", "type": "string" },
    { "name": "amount",    "type": "float"  }
  ]
}
```

Standard CSV source. `user_id` is declared as `int` so it can be interpolated into the URL template as a bare integer (`/users/1`, not `/users/1.0`).

### Lookup Transform

```json
{
  "type": "lookup",
  "url": "https://jsonplaceholder.typicode.com/users/{user_id}",
  "method": "GET",
  "keyField": "user_id",
  "outputFields": ["username", "email", "phone", "website"],
  "concurrency": 5,
  "cacheSize": 100,
  "timeoutMs": 3000,
  "onError": "null"
}
```

The `{user_id}` placeholder in the URL is replaced at runtime with the value of the `user_id` column for each row.

The transform adds four new fields to each row — `username`, `email`, `phone`, `website` — extracted from the JSON response body. Fields not listed in `outputFields` are ignored.

### Map Transform

```json
{
  "type": "map",
  "fields": {
    "user_id":  "=$user_id",
    "name":     "=$name",
    "username": "=coalesce($.username, $.name)",
    "email":    "=coalesce($.email, 'unknown@example.com')",
    "plan":     "=$plan_type"
  }
}
```

- Renames `plan_type` to `plan`.
- Uses `coalesce()` to fall back to the source `name` column when `username` is null (API error path).
- Uses `coalesce()` to substitute a literal default email when the API did not return one.
- Drops `amount`, `phone`, and `website` from the output (not listed in `fields`).

---

## Key Configuration Reference

### Lookup Transform Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | string | yes | URL template. Wrap column names in `{}` to interpolate row values (e.g. `{user_id}`). |
| `method` | string | yes | HTTP method. Currently `"GET"` is supported. |
| `keyField` | string | yes | Column whose value is used as the cache key. Usually the same column interpolated in `url`. |
| `outputFields` | array of strings | yes | Top-level JSON response keys to graft onto the row. |
| `concurrency` | int | no | Maximum number of parallel HTTP requests. Default: `1`. Increase for high-latency APIs. |
| `cacheSize` | int | no | LRU cache capacity (number of unique keys). `0` disables caching. Default: `0`. |
| `timeoutMs` | int | no | Per-request timeout in milliseconds. Default: `5000`. |
| `onError` | string | no | What to do when the request fails or times out: `"null"` sets output fields to null, `"skip"` drops the row, `"fail"` aborts the pipeline. Default: `"null"`. |

### LRU Cache Behaviour

The cache is keyed on `keyField`. When a row arrives whose key already exists in the cache, the cached response is used immediately — no HTTP request is made. The cache is bounded by `cacheSize`; when it is full, the least-recently-used entry is evicted.

Setting `cacheSize: 0` disables the cache entirely, which is appropriate when the API response for the same key can change between rows (e.g. real-time stock quotes).

### Concurrency and Ordering

Rows are dispatched to the HTTP worker pool up to `concurrency` at a time. The pipeline preserves output row order: results are reassembled in source order before being passed to the next transform, so the map transform always sees rows in the original sequence.

### onError Modes

| Value | Behaviour |
|---|---|
| `"null"` | Output fields are set to null. The row continues through the pipeline. |
| `"skip"` | The row is silently dropped. |
| `"fail"` | The pipeline aborts immediately with a non-zero exit code. |

---

## Adapting This Example

### Change the API endpoint

Replace the `url` and `outputFields` values to point at your own REST service:

```json
"url": "https://api.example.com/products/{product_id}",
"keyField": "product_id",
"outputFields": ["title", "price", "category", "stock"]
```

### Write enriched rows to a file

Replace the null sink with a Parquet sink:

```json
"sink": {
  "type": "parquet",
  "path": "./output/enriched_users.parquet",
  "rowGroupSize": 10000
}
```

### Add authentication headers

For APIs that require a bearer token, set an environment variable and reference it in `dsn`-style configs or use a pre-request hook (see the pipeline SDK documentation for header injection).

### Handle rate-limited APIs

Reduce `concurrency` to `1` or `2` and increase `timeoutMs`. For APIs with strict per-second limits, combine with a `filter` transform upstream to reduce row volume before the lookup stage.
