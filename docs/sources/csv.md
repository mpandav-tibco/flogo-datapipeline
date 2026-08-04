# CSV / TSV source — `csv`, `tsv`

Reads delimited text files (comma-, tab-, or any single-character-separated) and emits
one record per line. Implemented in
[`source/csv/source.go`](../../source/csv/source.go); registered as `csv` and `tsv`
(the `tsv` factory just presets `delimiter` to a tab before delegating to the CSV
reader).

- **Reads:** delimited text, with or without a header row
- **Partitionable:** no
- **Streaming:** yes — line-by-line, memory is `O(batchSize)`
- **Checkpoint / resume:** approximate byte-offset token (sequential skip on resume)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | File to read. |
| `delimiter` | string | `,` (`tsv` → `\t`) | — | Single-character field separator. |
| `hasHeader` | bool | `true` | — | First row supplies column names. |
| `columns` | `[]Column` | inferred from header | — | Explicit schema; **required when there is no header**. Each entry takes `name`, `type`, optional `format` (date layout) and `scale` (decimals). |

## Behavior

- **Type coercion** is per-column: `int`, `float`, `decimal` (with `scale`), `bool`, and
  `date` (parsed with the column `format`, default RFC3339-style). Unrecognized types are
  kept as raw strings.
- The reader enables **lazy quotes** and **trims leading space**, so mildly malformed CSV
  still parses.
- The empty string, `NULL`, and `null` all coerce to `nil`.
- Malformed lines are **skipped**; missing trailing fields become `nil`.
- Schema is inferred from the header row when `hasHeader` is `true` and no explicit
  `columns` are given.

## Example

```json
{
  "type": "csv",
  "path": "/data/transactions.csv",
  "delimiter": ",",
  "hasHeader": true,
  "columns": [
    { "name": "id", "type": "int" },
    { "name": "amount", "type": "decimal", "scale": 2 },
    { "name": "posted", "type": "date", "format": "2006-01-02" }
  ]
}
```

## See also

- [Sources index](../sources.md) · [CSV sink](../sinks/csv.md)
- Demo: [`examples/01-csv-to-null`](../../examples/01-csv-to-null), [`examples/02-filter-and-map`](../../examples/02-filter-and-map)
