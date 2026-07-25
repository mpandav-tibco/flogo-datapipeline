# Template sink — `template`

Renders output with Go `text/template`, for fixed-width reports and legacy layouts that a
columnar sink can't express. Implemented in
[`sink/template/sink.go`](../../sink/template/sink.go); registered as `template`.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | Output path (itself a Go template using the same context). |
| `template` | string | — | ✅* | Inline template body. |
| `templatePath` | string | — | ✅* | Path to a template file. |

*Provide **one** of `template` or `templatePath`.

## Render context

The template is executed once at close with:

| Field | Type | Description |
|---|---|---|
| `.Rows` | `[]map[string]any` | Every row as a name→value map. |
| `.Count` | int | `len(.Rows)`. |
| `.Columns` | `[]string` | Column names in order. |
| `.Attr` | `map[string]any` | The first row (file-level attributes). |

Helpers: `rpad(w, v)`, `lpad(w, v)`, `padc(w, ch, v)`, `sci(v)` (signed 6-digit scientific),
`upper`/`lower`/`trim`, `add(a, b)`, `seq(n)`.

## Example

```json
{
  "type": "template",
  "path": "./out/report-{{index .Attr \"date\"}}.txt",
  "template": "{{range .Rows}}{{.id | lpad 10}}{{.name | rpad 20}}\n{{end}}TOTAL {{.Count}}\n"
}
```

## See also

- [Sinks index](../sinks.md) · [Sectioned source](../sources/sectioned.md)
