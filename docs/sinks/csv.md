# CSV / TSV sink — `csv`, `tsv`

Writes delimited text, one row per record, with an optional header. Implemented in
[`sink/csv/sink.go`](../../sink/csv/sink.go); registered as `csv` and `tsv` (tab preset).

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Output file (parent directories are created). |
| `delimiter` | string | `,` (`tsv` → `\t`) | — | Single-character field separator. |
| `writeHeader` | bool | `true` | — | Write column names as the first row. |

## Behavior

- The header comes from the first batch's columns; all later batches must share that
  schema. Cells are stringified (`nil` → empty).

## Example

```json
{ "type": "csv", "path": "./out/transactions.csv", "delimiter": ",", "writeHeader": true }
```

## See also

- [Sinks index](../sinks.md) · [CSV source](../sources/csv.md)
