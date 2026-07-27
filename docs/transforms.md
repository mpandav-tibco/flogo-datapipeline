# Transforms

A **transform** takes a `RecordBatch` and returns a (possibly reshaped) batch. They
run in the order you declare them, forming the middle of the pipeline. Transforms are
registered with `transform.Register(name, …)`
([`transform/transform.go`](../transform/transform.go)). Some transforms (`validate`,
`dq`) can **route** rejected rows to an error sink / DLQ; see
[Data quality](data-quality.md).

> Each transform below links to its own detailed page under [`docs/transforms/`](transforms/),
> and the shared expression language has a [dedicated reference](transforms/expression-language.md).

## At a glance

| Type(s) | Does |
|---|---|
| [`filter`](transforms/filter.md) | drop rows that fail a boolean expression |
| [`map`, `mapper`](transforms/map.md) | project / rename / coerce / derive columns |
| [`validate`](transforms/validate.md) | per-field rules; violations route, fail, or drop |
| [`lookup`, `enrich`](transforms/lookup.md) | append columns from an HTTP call (LRU-cached) |
| [`parse`](transforms/parse.md) | extract columns from a text field (regex / JSON) |
| [`dq`](transforms/dq.md) | evaluate data-quality expectations; emit a report |
| [`schemadrift`](transforms/schemadrift.md) | detect column add/remove/type changes at runtime |
| [`hwm`](transforms/hwm.md) | observe the max of a column for incremental loads |
| [`cast`](transforms/cast.md) | convert column types (string ↔ int / float / decimal / bool / date) |
| [`explode`](transforms/explode.md) | expand an array column into one row per element |
| [`flatten`](transforms/flatten.md) | flatten nested-object columns into dotted columns |
| [`router`, `split`](transforms/router.md) | N-way route rows to different sinks by predicate (conditional split) |
| [`join`](transforms/join.md) | keyed stream-table join against a source-loaded reference dataset |
| [`window`](transforms/window.md) | event-time windowed aggregation (tumbling / sliding / session / global) |

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
`datetime`/`int`/`string`/`decimal`) and exposes it to the [incremental](reliability.md)
watermark store. Does not modify rows.

## Cast — `cast`

Converts the listed `columns` to a target `type` (`string`, `int`, `float`,
`decimal` with optional `scale`, `bool`, `date` with optional `format`) in every
row and updates the batch schema. `onError` is `null` (default — unconvertible
values become null) or `fail`. Decimal casts stay exact. See [cast](transforms/cast.md).

## Explode — `explode`

Expands an array-valued `column` into one output row per element (other columns
duplicated); `into` names the element column. Empty/null arrays drop the row
(unless `keepEmpty`), a scalar yields one row. The map-side way to normalise
arrays-of-objects from JSON/XML. See [explode](transforms/explode.md).

## Flatten — `flatten`

Replaces nested-object columns with one dotted column per leaf
(`address` → `address.city`, `address.zip`). `columns` selects which to flatten
(default all map-valued), `separator` and `depth` tune the output. Arrays are left
as values (use `explode`). See [flatten](transforms/flatten.md).

---

# Expression language

`filter` predicates and `map` field expressions share one small, safe expression
language (no arbitrary code), evaluated once per row. It provides **60+ functions**
across the `string.*`, `number.*`, `math.*`, `datetime.*`, `coerce.*`, `hash.*`,
`encoding.*`, and `util.*` namespaces, plus SQL-like filter predicates with
three-valued (NULL-aware) logic.

➡ **Full, code-verified catalogue with signatures: [Expression language reference](transforms/expression-language.md).**

## See also

- [Data quality](data-quality.md) — validate / dq / schemadrift and DLQ routing
- Per-transform detail: [`docs/transforms/`](transforms/)
