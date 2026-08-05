# 27 — REST/API Source (paginated HTTP → JSON Lines)

Demonstrates the generic `rest` source: pulling a paginated JSON list from an
HTTP API and landing it as newline-delimited JSON, with **no code** — the
pagination, auth, and record extraction are all declarative pipeline config.

Because a real external API would make this example non-reproducible, a tiny
local fixture API ([fixture/main.go](fixture/main.go)) stands in for one: it
serves 25 fake "orders" at `GET /orders?page=N&limit=M`, page-paginated exactly
like a typical CRUD API.

## Pipeline

```
rest (page pagination, 10 rows/page)  →  jsonlines sink
```

[orders-rest-to-jsonlines.datapipeline](orders-rest-to-jsonlines.datapipeline)
configures the source with:

```json
"source": {
  "type": "rest",
  "url": "http://localhost:8089/orders",
  "pagination": { "type": "page", "pageSize": 10 },
  "columns": [
    { "name": "id", "type": "int" },
    { "name": "customer", "type": "string" },
    { "name": "amount", "type": "float" },
    { "name": "status", "type": "string" }
  ]
}
```

The engine fetches `page=1&limit=10`, then `page=2&limit=10`, then
`page=3&limit=10` — stopping automatically once a page returns fewer rows
than `limit` (page 3 returns the last 5 of 25 orders).

## Run it

```bash
./run.sh
```

This starts the fixture API in the background, runs the pipeline, and prints
a sample of the output. Or do it manually:

```bash
go run ./fixture -addr :8089 &
flogo-datapipeline -f orders-rest-to-jsonlines.datapipeline -v
cat out/orders.jsonl
kill %1
```

Expected: `out/orders.jsonl` with 25 lines, one JSON object per order.

## Other pagination modes the `rest` source supports

Not exercised by this example (see [docs/sources/rest.md](../../docs/sources/rest.md)
for the full reference), the source also supports:

- **`offset`** pagination (`offset`/`limit` query params).
- **`cursor`** pagination — either a cursor value extracted from the response
  body (`nextCursorPath`) or a full next-page link (`nextUrlPath`, e.g. a
  GitHub-style `links.next`).
- **Auth**: `bearer` / `basic` / `apikey`, credentials always read from an
  environment variable (`bearerTokenEnv`, etc.) — never inline in the pipeline
  file.
- **`dataPath`** to locate the record array inside a nested response envelope,
  e.g. `{"data":{"items":[...]}}` → `"dataPath": "data.items"`.

## What this fills

Every other source in this engine reads a file, a table, or a topic. The
`rest` source is the first generic way to pull from **any** paginated JSON
HTTP API — the most commonly requested connector in DI evaluations — without
writing a custom source.
