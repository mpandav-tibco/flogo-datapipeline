# Expression language reference

The [`map`](map.md) transform's field expressions and the [`filter`](filter.md)
predicate share one small, safe, no-arbitrary-code expression language, evaluated once per
row. This page is the complete, code-verified catalogue
([`transform/mapper/transform.go`](../../transform/mapper/transform.go)).

## Values and references

- **Static literal** — a value with **no** leading `=` (e.g. `"prod"`).
- **Expression** — a leading `=` compiles the rest.
  - `=$field` — an input column; dotted paths (`=$.a.b`) index nested maps.
  - `=func(arg, …)` — a function call; args are themselves expressions.
  - `=123`, `=1.5`, `='text'` — numeric and single-quoted string literals.
- `$property[NAME]` may be embedded anywhere to bind a value to an app property.

Nulls propagate: a `$field` that is missing/nil stays nil through most operations; numeric
and domain failures (divide-by-zero, `sqrt` of a negative) return nil.

## Filter predicates

SQL-like booleans with three-valued (NULL-aware) logic:

| Category | Forms |
|---|---|
| Comparison | `>` `>=` `<` `<=` `=` `!=` |
| Null | `IS NULL`, `IS NOT NULL` |
| Boolean | `AND`, `OR` (case-insensitive) |
| Literals | `'string'`, numbers, `true`/`false`, `null` |

```
amount > 0 AND status != 'VOID' AND settled_at IS NOT NULL
```

## Map functions

Functions exist both as short **legacy builtins** and inside **namespaces**. Both forms
work; namespaced names are preferred for new pipelines.

### String — `string.*`

`string.concat(…)`, `string.trim(s)`, `string.toUpper(s)`, `string.toLower(s)`,
`string.length(s)`, `string.substring(s, start [, end])`, `string.replace(s, old, new)`,
`string.indexOf(s, sub)`, `string.lastIndexOf(s, sub)`, `string.startsWith(s, p)`,
`string.endsWith(s, sfx)`, `string.contains(s, sub)`, `string.padLeft(s, w [, ch])`,
`string.padRight(s, w [, ch])`, `string.split(s, sep [, index])`,
`string.join(sep, …)`, `string.title(s)`, `string.capitalize(s)`, `string.reverse(s)`,
`string.repeat(s, n)`, `string.equalsIgnoreCase(a, b)`, `string.count(s, sub)`,
`string.trimLeft(s [, cut])`, `string.trimRight(s [, cut])`, `string.trimPrefix(s, p)`,
`string.trimSuffix(s, sfx)`, `string.substringBefore(s, sep)`,
`string.substringAfter(s, sep)`.

Legacy: `concat(…)`, `upper(s)`, `lower(s)`, `trim(s)`, `lpad(s, w [, ch])`,
`rpad(s, w [, ch])`, `scientific(n)`.

### Regex — `regex.*`

`regex.match(s, pattern)` → bool · `regex.replace(s, pattern, repl)` ·
`regex.extract(s, pattern [, group])` (group 0 = whole match). Aliases:
`string.match`, `string.replaceRegex`. Patterns are Go `regexp` syntax.

### Number / Math — `number.*`, `math.*`

`number.parseInt(v)`, `number.parseFloat(v)`, `number.round(v [, dp])`, `number.floor(v)`,
`number.ceil(v)`, `number.abs(v)`, `number.min(a, b)`, `number.max(a, b)`,
`number.mod(a, b)`, `number.add(a, b)`, `number.subtract(a, b)`, `number.multiply(a, b)`,
`number.divide(a, b)`, `number.pow(b, e)`, `number.sqrt(v)`, `number.sign(v)`,
`number.trunc(v)`, `number.random([limit])`, `math.log(v)`, `math.log10(v)`, `math.exp(v)`
(every `number.*` numeric op has a `math.*` alias). Legacy: `multiply`, `divide`, `clamp(v, lo, hi)`.

### Coerce — `coerce.*`

`coerce.toInt(v)`, `coerce.toFloat(v)`, `coerce.toString(v)`, `coerce.toBool(v)`
(true for `true`/`1`/`yes`/`y`, case-insensitive). Failed coercions return nil.

### Date/Time — `datetime.*`

`datetime.now()` (RFC3339), `datetime.today()` (`YYYY-MM-DD`),
`datetime.parse(s [, layout])` (normalize to RFC3339), `datetime.format(s [, layout])`,
`datetime.year/month/day/hour/minute/second(s)`, `datetime.dayOfWeek(s)` /
`datetime.weekday(s)`, `datetime.addDays/addHours/addMinutes/addSeconds(s, n)`,
`datetime.diffDays(a, b)`, `datetime.diffSeconds(a, b)`,
`datetime.epoch(s)` / `datetime.unix(s)` / `datetime.toEpoch(s)`,
`datetime.fromEpoch(sec)`. Inputs accept RFC3339, ISO-8601 variants, and common date
formats.

### Hash / Encoding — `hash.*`, `encoding.*`

`hash.md5(…)`, `hash.sha1(…)`, `hash.sha256(…)` (hex digest of concatenated args) ·
`encoding.base64Encode(s)` / `encoding.base64Decode(s)`, `encoding.hexEncode(s)`
(aliases `encodeBase64`/`decodeBase64`).

### Utility — `util.*`

`util.uuid()` / `uuid()` (v4 UUID) · `util.mask(s [, keepLast])` (mask all but the last N
chars with `*`).

### Logic, conditional, null-handling

`iif(cond, a, b)` (ternary) · `nvl(v, fallback)` / `ifNull(v, fallback)` ·
`nullif(a, b)` (nil when `a = b`) · `coalesce(…)` (first non-nil) ·
`decode(expr, s1, r1, s2, r2, … [, default])` (Oracle-style CASE) ·
`isNull(v)`, `isEmpty(s)`, `equals(a, b)`, `notEquals(a, b)`, `between(v, lo, hi)`,
`greaterThan(a, b)`, `lessThan(a, b)`, `and(…)`, `or(…)`, `not(v)`.

## Notes

- Output columns start as `any`; a concrete type is inferred from the first non-nil value
  in the first batch. All-nil columns stay `any`.
- Map evaluation errors do not abort the run — the affected field becomes nil.

## See also

- [Map transform](map.md) · [Filter transform](filter.md) · [Transforms index](../transforms.md)
