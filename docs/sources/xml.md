# XML source — `xml`

Streams an XML document, emitting one record per occurrence of a chosen element.
Implemented in [`source/xml/source.go`](../../source/xml/source.go); registered as
`xml`.

- **Reads:** XML, one record per `recordElement`
- **Partitionable:** no
- **Streaming:** yes — SAX-style token scanning, no full-document load
- **Checkpoint / resume:** row-count token (sequential skip on resume)

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `path` | string | — | ✅ | File to read. |
| `recordElement` | string | — | ✅ | Element tag that delimits one record (e.g. `order`). |
| `fields` | `[]object` | — | ✅ | Field definitions extracted from each record. |

Each entry in `fields[]`:

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `name` | string | — | ✅ | Output column name. |
| `xpath` | string | — | ✅ | Simple path: `element/child`, `element/@attr`, or `text`. |
| `type` | string | `string` | — | `int`, `float`, `decimal`, `bool`, `date`, or `string`. |

## Behavior

- The **simple XPath** subset supports element paths, attribute access (`@attr`), and
  element text only — no predicates, axes, or functions.
- Because scanning is streaming (SAX-style), memory stays bounded regardless of document
  size.

## Example

```json
{
  "type": "xml",
  "path": "/data/orders.xml",
  "recordElement": "order",
  "fields": [
    { "name": "id", "xpath": "@id", "type": "int" },
    { "name": "customer", "xpath": "customer/name" },
    { "name": "total", "xpath": "total", "type": "decimal" }
  ]
}
```

## See also

- [Sources index](../sources.md)
- Demo: [`examples/06-xml-source`](../../examples/06-xml-source)
