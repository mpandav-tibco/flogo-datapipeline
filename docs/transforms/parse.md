# Parse transform — `parse`

Structures **one text field into columns** — either by a regex with named groups or by
decoding JSON. This is the grok / Fluent Bit-parser equivalent: the source emits a raw
line, and `parse` extracts typed columns from it. Implemented in
[`transform/parse/transform.go`](../../transform/parse/transform.go); registered as
`parse`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `source` | string | — | ✅ | Name of the text field to parse (e.g. `message`). |
| `mode` | string | `regex` | — | `regex` or `json`. |
| `pattern` | string | — | regex only | Go regex with **named groups** `(?P<name>…)`. |
| `fields` | `[]string` | all keys | json only | Allow-list of JSON keys to extract. |
| `prefix` | string | — | — | Prefix prepended to each extracted column name (JSON mode). |
| `onError` | string | `keep` | — | `keep` (emit with nils), `drop` (skip the row), or `route` (append `_parse_error`). |
| `keepSource` | bool | `true` | — | Keep the original `source` column alongside the extracted ones. |

## How columns are produced

**The number of output columns equals the number of named groups in the pattern** —
there is no fixed count. Add a `(?P<name>…)` group to add a column; remove one to drop it.
The engine derives the column set from `re.SubexpNames()`, and the Studio Schema tab shows
exactly the same derived columns.

```
(?P<log_ts>\S+)\s+(?P<log_level>\w+)   →  2 columns: log_ts, log_level
```

- **Regex mode:** each named group becomes one `string` column (anonymous groups are
  ignored). A pattern with no named groups is rejected at config time. Use `(?s)` to let
  `.` span multiline (stitched) records.
- **JSON mode:** the `source` field is decoded as a JSON object; keys become columns with
  their JSON types. `fields` restricts and orders the output; `prefix` namespaces the
  names.
- **`onError: route`** appends a synthetic `_parse_error` column (`"true"`/`"false"`) so a
  downstream [validate](validate.md) rule can send failures to a DLQ.

## Example — regex (BW / Log4j lines)

```json
{
  "type": "parse",
  "source": "message",
  "mode": "regex",
  "pattern": "(?s)^(?P<log_ts>\\S+)\\s+(?P<log_level>\\w+)\\s+\\[(?P<log_thread>[^\\]]+)\\]\\s+(?P<log_logger>\\S+)\\s+-\\s+(?P<log_text>.*)$",
  "onError": "route",
  "keepSource": false
}
```

## Example — JSON

```json
{
  "type": "parse",
  "source": "json_payload",
  "mode": "json",
  "fields": ["user_id", "action", "timestamp"],
  "prefix": "evt_"
}
```

## See also

- [Transforms index](../transforms.md) · [Map](map.md) · [Validate](validate.md) · [Pod Log source](../sources/podlog.md)
- Demo: [`examples/adidas-log-collection-usecase`](../../examples/adidas-log-collection-usecase)
