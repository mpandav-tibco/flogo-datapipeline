# Transforms

A **transform** takes a `RecordBatch` and returns a (possibly reshaped) batch. They
run in the order you declare them, forming the middle of the pipeline. Transforms are
registered with `transform.Register(name, …)`
([`transform/transform.go`](../transform/transform.go)). Some transforms (`validate`,
`dq`) can **route** rejected rows to an error sink / DLQ; see
[Data quality](data-quality.md).

> Full per-key tables live in the [Feature reference](FEATURES.md#transforms).

## At a glance

| Type(s) | Does |
|---|---|
| `filter` | drop rows that fail a boolean expression |
| `map`, `mapper` | project / rename / coerce / derive columns |
| `validate` | per-field rules; violations route, fail, or drop |
| `lookup`, `enrich` | append columns from an HTTP call (LRU-cached) |
| `parse` | extract columns from a text field (regex / JSON) |
| `dq` | evaluate data-quality expectations; emit a report |
| `schemadrift` | detect column add/remove/type changes at runtime |
| `hwm` | observe the max of a column for incremental loads |

## Filter — `filter`

Drops rows for which `expr` is not true.

```json
{ "type": "filter", "options": { "expr": "amount > 0 AND status != 'VOID'" } }
```

## Map / Mapper — `map`, `mapper`

Builds the output row from `fields` (out-name → expression), with optional
`fieldOrder` for deterministic column order. Pass-through when nothing changes.

```json
{ "type": "map", "options": {
    "fields": {
      "order_id":  "=$id",
      "total":     "=multiply($qty, $price)",
      "loaded_at": "=now()"
    }
} }
```

## Validate — `validate`

Per-field `rules` (`notNull`, `min`, `max`, `minLen`, `maxLen`, `pattern`, `in`) with
`onViolation` (`route` / `fail` / `drop`) and an optional
`errorSinkType`/`errorSinkConfig`. See [Data quality](data-quality.md).

## Lookup / Enrich — `lookup`, `enrich`

Appends columns from an HTTP endpoint: `keyField`, `url` (with a `{keyField}` macro),
`method`, `outputFields`, `concurrency` (`10`), `cacheSize` (`1000`, LRU),
`timeoutMs` (`5000`), `onError` (`null`/`skip`/`fail`). Uses a worker pool plus
singleflight de-duplication so repeated keys hit the cache, not the network.

## Parse — `parse`

Turns one text/JSON field into columns: `source`, `mode` (`regex`/`json`), `pattern`
(named groups `(?P<name>…)` for regex), `fields`/`prefix` (json), `onError`
(`keep`/`drop`/`route`), `keepSource` (`true`). The Fluent Bit parser equivalent.

## Data Quality — `dq`

Evaluates `expectations[]` and emits a `DQReport`. Covered in depth in
[Data quality](data-quality.md).

## Schema Drift — `schemadrift`

Baselines the schema from the first batch, then applies a `policy` (`project`
default / `additive` / `strict` / `drop`) when later batches differ, with `onDrift`
(`log`/`alert`/`silent`). See [Data quality](data-quality.md).

## High-Water Mark — `hwm`

A pass-through observer that tracks the maximum value of `column` (`valueType`
`datetime`/`int`/`string`) and exposes it to the [incremental](reliability.md)
watermark store. Does not modify rows.

---

# Expression language

`filter` predicates and `map` field expressions share a small, safe expression
language (no arbitrary code). It is evaluated once per row.

## Field references

- **`=$field`** — the value of an input column (in `map`).
- **`=func(args…)`** — a function call; args may be literals or `$field` refs.
- A bare string with no leading `=` is a **static literal**.

## Filter predicates

SQL-like booleans with three-valued (NULL-aware) logic:

| Category | Operators / forms |
|---|---|
| Comparison | `>` `>=` `<` `<=` `=` `!=` |
| Null | `IS NULL`, `IS NOT NULL` |
| Boolean | `AND`, `OR` |
| Literals | strings `'…'`, numbers, `true`/`false`, `null` |

```
amount > 0 AND status != 'VOID'
```

## Map functions

| Group | Functions |
|---|---|
| String | `concat` `upper` `lower` `trim` `substr` `replace` |
| Math | `add` `subtract` `multiply` `divide` `round` |
| Hash / encode | `md5` `sha1` `sha256` `base64_encode` `base64_decode` |
| Date | `now` `format_date` `parse_date` |
| Utility | `coalesce` `if` `uuid` `random` |

```
=concat(upper($first), ' ', upper($last))
=coalesce($email, 'unknown@example.com')
=if($amount > 1000, 'HIGH', 'NORMAL')
```

## See also

- [Data quality](data-quality.md) — validate / dq / schemadrift and DLQ routing
- [Feature reference](FEATURES.md#transforms) — every key and default
