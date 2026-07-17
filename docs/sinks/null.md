# Null / Discard sink — `null`, `discard`

Drops every row. Use it for benchmarking, dry runs, and as a default DLQ target during
testing. Implemented in [`sink/null/sink.go`](../../sink/null/sink.go); registered as
`null` and `discard`.

## Configuration

None.

## Example

```json
{ "type": "null" }
```

## See also

- [Sinks index](../sinks.md)
- Demo: [`examples/01-csv-to-null`](../../examples/01-csv-to-null)
