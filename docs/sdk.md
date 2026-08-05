# Go SDK (pipeline-as-code)

For programmatic pipelines, [`sdk/pipeline.go`](../sdk/pipeline.go) is a fluent builder
that produces the same **Definition** as a `.datapipeline` file and can run it inline or
serialize it to Flogo resource JSON. Use it for tests, code-generated pipelines, or
embedding the engine in a larger Go program.

```go
result, err := sdk.NewPipeline("daily-load").
    FromCSV("/data/orders.csv").
    Filter("amount > 0").
    Map(map[string]string{ "order_id": "=$id", "total": "=$amount" }).
    ToJDBC("host=localhost dbname=orders", "orders_enriched").
    Run(ctx)
```

## Builder methods

| Stage | Methods |
|---|---|
| **Sources** | `FromCSV` `FromJDBC` `FromKafka` |
| **Transforms** | `Filter` `Map` `Validate` `WithDQ` `WithHWM` `WithDLQ` `WithMetrics` |
| **Sinks** | `ToJDBC` `ToKafka` `ToS3` |
| **Terminal** | `Build()` · `MarshalJSON()` · `Run(ctx)` |

The builder deliberately covers the most common ETL/CDC patterns (SQL, Kafka, CSV,
object storage) rather than every source/sink/transform the engine supports — see
[`sdk/builder.go`](../sdk/builder.go) for the current, authoritative list. For a
type the builder doesn't wrap yet (e.g. the `parquet`, `xml`, `jsonlines`,
`fixedwidth`, or REST sources; the `parquet`, `csv`, `jsonlines`, `template`, or
`null` sinks; the `lookup`, `schemadrift`, or `parse` transforms), either call
`Build()` and mutate the returned `*pipeline.Definition` directly (it's the same
struct a `.datapipeline` file deserialises into — see
[Pipeline file](pipeline-file.md)), or write the `SourceDef` / `SinkDef` /
`TransformDef` JSON by hand.

- `Build()` returns the `Definition` for inspection or embedding.
- `MarshalJSON()` serializes to the same JSON a `.datapipeline` carries — write it to a
  file and run it with the engine binary, or open it in the [Studio](studio.md).
- `Run(ctx)` executes synchronously and returns the run summary (the same shape the
  [REST API](rest-api.md) returns — see [Run output](pipeline-file.md#run-output)).

## Expressions

`Filter` predicates and `Map` values use the same
[expression language](transforms.md#expression-language) as the file format.

## See also

- [Pipeline file](pipeline-file.md) — the Definition the builder produces
- [Feature reference](FEATURES.md#go-sdk-pipeline-as-code)
