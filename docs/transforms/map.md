# Map transform — `map`, `mapper`

Builds each output row from `fields` — an ordered set of `output-name → expression` pairs.
Use it to **project, rename, coerce, and derive** columns. Implemented in
[`transform/mapper/transform.go`](../../transform/mapper/transform.go); registered as
`map` and `mapper`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `fields` | object | — | ✅ | `outputName → expression`. |
| `fieldOrder` | `[]string` | sorted keys | — | Explicit output column order; must be a permutation of the `fields` keys. |

> **Map is a projection.** The output row contains **only** the fields you list — to keep an
> input column, map it through (`"col": "=$col"`). The Studio pre-fills a new map with a
> passthrough for every upstream column so you start from a valid projection.

## Expression syntax

- **Static literal:** a value with no leading `=` (e.g. `"prod"` or `"$property[ENV]"`).
- **Expression:** a leading `=` marks the rest as an expression.
  - Field reference: `=$field` or a dotted path `=$.customer.name`.
  - Function call: `=upper($name)`, `=decode($status,'NEW','1','2')`.
  - Literals inside expressions: numbers (`=1.1`), quoted strings (`='text'`).
- App-property binding: any value may embed `$property[NAME]` (see [Configuration](../configuration.md)).

There is no ternary operator symbol — use `iif(cond, a, b)`.

## Common functions

`nvl`, `coalesce`, `decode`, `iif`, `equals`, `and`/`or`/`not`, `upper`/`lower`,
`string.concat`/`replace`/`split`/`padLeft`, `regex.replace`/`extract`,
`coerce.toInt`/`toString`/`toBool`, `number.round`, `datetime.now`/`today`/`format`/`parse`,
`hash.sha256`, `util.uuid`, … — **60+ functions across the `string.*`, `number.*`,
`math.*`, `datetime.*`, `coerce.*`, `hash.*`, `encoding.*`, and `util.*` namespaces.** The
full catalogue with signatures is in the [Expression language reference](expression-language.md).

## Example

```json
{
  "type": "map",
  "fields": {
    "order_id":   "=$id",
    "customer":   "=string.toUpper($.customer.name)",
    "total_usd":  "=number.round(divide($cents, 100), 2)",
    "level":      "=upper(nvl($log_level, 'INFO'))",
    "severity":   "=coerce.toInt(decode(upper(nvl($log_level,'INFO')),'ERROR',4,'WARN',3,'INFO',2,2))",
    "loaded_at":  "=datetime.now()",
    "environment": "$property[ENVIRONMENT]"
  },
  "fieldOrder": ["order_id", "customer", "total_usd", "level", "severity", "loaded_at", "environment"]
}
```

## See also

- [Transforms index](../transforms.md) · [Expression language reference](expression-language.md) · [Filter](filter.md) · [Parse](parse.md)
- Demo: [`examples/02-filter-and-map`](../../examples/02-filter-and-map)
