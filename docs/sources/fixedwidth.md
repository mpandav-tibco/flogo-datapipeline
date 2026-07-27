# Fixed-Width / Flat File source — `fixedwidth`, `flatfile`

Reads positional (column-aligned) records from mainframe and legacy flat files, including
EBCDIC-encoded data and implied-decimal numerics. Implemented in
[`source/fixedwidth/source.go`](../../source/fixedwidth/source.go); registered as
`fixedwidth` and `flatfile`.

- **Reads:** positional fixed-width records (COBOL / EBCDIC / ASCII)
- **Partitionable:** no
- **Streaming:** yes — record-by-record, memory is `O(batchSize)`
- **Checkpoint / resume:** row-count token (sequential skip on resume)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | File to read. |
| `encoding` | string | `utf-8` | — | Character set for decoding: `utf-8`/`ascii`, `ebcdic-037`, `ebcdic-1047`, `ebcdic-1140`, `latin-1`/`iso-8859-1`, `latin-9`/`iso-8859-15`, `windows-1252`. Decoded per field so fixed byte positions are preserved. |
| `skipLines` | int | `0` | — | Header/trailer lines to skip at the start. |
| `recordLength` | int | `0` | — | Fixed byte length per record; `0` = newline-delimited. Short lines are space-padded. |
| `columns` | `[]object` | — | ✅ | Positional column definitions (see below). |

Each entry in `columns[]`:

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `name` | string | — | ✅ | Column name. |
| `type` | string | `string` | — | `int`, `float`, `decimal`, `date`, `bool`, `string`. |
| `start` | int | — | ✅ | **1-based** byte position of the field. |
| `length` | int | — | ✅ | Field byte length. |
| `trim` | bool | `false` | — | Strip leading/trailing whitespace. |
| `impliedDecimal` | int | `0` | — | Divide the integer value by 10^N (e.g. `2` → cents to dollars). |
| `format` | string | `20060102` (date) | — | Go date layout for `date` columns. |

## Behavior

- **EBCDIC** input is translated to Unicode when `encoding` names an EBCDIC codepage.
- **Implied-decimal** scaling reconstructs fixed-point numerics stored as scaled integers
  (common in banking / insurance).
- Out-of-range fields (line shorter than `start+length`) return `nil`; malformed records
  are logged and skipped.

## Example

```json
{
  "type": "fixedwidth",
  "path": "/data/mainframe.dat",
  "encoding": "ebcdic-037",
  "skipLines": 1,
  "recordLength": 120,
  "columns": [
    { "name": "accountId", "type": "string", "start": 1, "length": 10, "trim": true },
    { "name": "amount", "type": "decimal", "start": 11, "length": 12, "impliedDecimal": 2 },
    { "name": "postDate", "type": "date", "start": 23, "length": 8, "format": "20060102" }
  ]
}
```

## See also

- [Sources index](../sources.md) · [Sectioned source](sectioned.md)
- Demo: [`examples/05-fixed-width-ebcdic`](../../examples/05-fixed-width-ebcdic)
