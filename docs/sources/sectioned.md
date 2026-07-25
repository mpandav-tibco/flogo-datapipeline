# Sectioned source — `sectioned`

Reads report-style files that begin with a **key/value header block** and then a
**delimited body table** — a common shape for instrument logs, semiconductor test
reports, and legacy mainframe reports. Implemented in
[`source/sectioned/source.go`](../../source/sectioned/source.go); registered as
`sectioned`.

- **Reads:** header key/values + a delimited body table
- **Partitionable:** no
- **Streaming:** loads the file at construction, then emits body rows in batches
- **Checkpoint / resume:** row-index token (sequential skip on resume)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | File to read. |
| `headerKVRegex` | string | `^(\S[\S ]*?)\s{2,}(.*?)\s*$` | — | Regex with **two** capture groups (key, value) matching each header line. |
| `bodyColumnMarker` | string | — | ✅ | Regex identifying the line that names the body columns. |
| `bodyDelimiter` | string | `\s+` | — | Regex that splits each body row into fields. |
| `broadcastHeader` | bool | `true` | — | Stamp every header key/value onto every body row. |

## Behavior

- The header section is parsed into key/value attributes; header keys are sanitized
  (spaces/hyphens → underscores).
- The `bodyColumnMarker` line supplies the body column names; subsequent lines are data
  rows. Rows whose field count does not match the column count are skipped.
- With `broadcastHeader: true`, downstream transforms and the sink see file-level context
  on every row.
- All columns (header and body) are typed as `string`; use a [map](../transforms/map.md)
  transform to coerce.

## Example

```json
{
  "type": "sectioned",
  "path": "/data/measurement.txt",
  "headerKVRegex": "^(\\S[\\S ]*?)\\s{2,}(.*?)\\s*$",
  "bodyColumnMarker": "^MEASUREMENT\\s",
  "bodyDelimiter": "\\s+",
  "broadcastHeader": true
}
```

```
Equipment      XYZ-200
Lot ID         12345678

MEASUREMENT     value       unit
wafer-1         3.45        mm
wafer-2         3.47        mm
```

## See also

- [Sources index](../sources.md) · [Fixed-Width source](fixedwidth.md)
