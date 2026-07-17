# Filter transform — `filter`

Drops rows whose boolean expression does not evaluate to true. Implemented in
[`transform/filter/transform.go`](../../transform/filter/transform.go); registered as
`filter`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `expr` | string | — | ✅ | SQL-like boolean expression evaluated once per row. |

## Expression grammar

Three-valued (NULL-aware) logic over row fields:

| Category | Forms |
|---|---|
| Comparison | `>` `>=` `<` `<=` `=` `!=` |
| Null | `IS NULL`, `IS NOT NULL` |
| Boolean | `AND`, `OR` (case-insensitive) |
| Literals | `'string'`, numbers, `true`/`false`, `null` |

- Comparisons are numeric when both operands coerce to numbers, otherwise string.
- `NULL = NULL` is **false** (SQL semantics); use `field IS NULL` to test for absence.
- A field that fails the predicate is simply dropped — there is no routing.

## Example

```json
{ "type": "filter", "expr": "amount > 0 AND status != 'VOID' AND settled_at IS NOT NULL" }
```

## See also

- [Transforms index](../transforms.md) · [Validate](validate.md) (for routing rejects to a DLQ) · [Expression language](expression-language.md)
- Demo: [`examples/02-filter-and-map`](../../examples/02-filter-and-map)
