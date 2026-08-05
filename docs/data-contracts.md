# Data contracts

A **data contract** is a single, versioned artifact bundling **schema +
data-quality rules + an SLA** — a producer-consumer agreement stating what a
pipeline promises to deliver, in the spirit of the Linux Foundation's [Open
Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/).

This is **not** new runtime machinery. It formalizes three primitives the
engine already has — typed schema (`ColumnDef`), the [`dq`](transforms/dq.md)
transform's expectation engine, and the `stallSeconds` SLA — into one artifact
that's checked at **both** design time (so a broken contract is a CI failure
via `-validate`, not a runtime surprise) and run time. Implemented in
[`action/pipeline/definition.go`](../action/pipeline/definition.go)
(`ContractDef`).

## Configuration

Set the top-level `contract` key on a pipeline definition:

| Key | Type | Default | Description |
|---|---|---|---|
| `version` | string | — | Informational version string (e.g. semver). Not enforced — carried through for documentation/audit purposes. |
| `schema` | `[]ColumnDef` | — | The contract's expected OUTPUT column list. See [Design-time schema check](#design-time-schema-check). |
| `expectations` | `[]object` | — | Same shape as the [`dq`](transforms/dq.md) transform's own `expectations`. See [Auto-appended quality gate](#auto-appended-quality-gate). |
| `qualityThreshold` | float | `0.95` | Same meaning as `dq`'s own default. |
| `onViolation` | string | `route` | Same vocabulary as `dq`'s `onFail`: `route` \| `fail` \| `warn` \| `drop`. |
| `errorSinkType` / `errorSinkConfig` | string / object | — | Passed straight through to the auto-appended `dq` stage when `onViolation: "route"`. |
| `maxDurationMs` | int | `0` (no limit) | SLA ceiling on total run wall-clock time. See [Duration SLA](#duration-sla). |

A contract must declare **at least one** of `schema`, `expectations`, or
`maxDurationMs` — an empty contract (nothing to enforce) is rejected at
`Validate()`.

## Design-time schema check

When **both** `contract.schema` and the source's own `source.columns` are
declared, `Validate()` (and therefore `-validate` / the REST API's pipeline
load path) cross-checks them **at design time** — before the pipeline ever
runs: every contract column must exist in `source.columns` with a matching
type. A mismatch is a load-time error naming the offending column, catching
config drift immediately instead of downstream.

```json
"source": { "type": "csv", "columns": [{"name": "amount", "type": "decimal"}] },
"contract": { "schema": [{"name": "amount", "type": "decimal"}] }
```

**Scope limit (documented, not silently missing):** a contract `schema`
declared **without** `source.columns` is not checked at design time — the
source's schema isn't known until the run starts for sources that introspect
(e.g. `jdbc`) or infer it (e.g. `csv` auto-detect, `jsonlines`, Kafka JSON).

## Auto-appended quality gate

When `contract.expectations` is non-empty, the engine automatically appends a
[`dq`](transforms/dq.md) transform stage — built from
`expectations`/`qualityThreshold`/`onViolation`/`errorSinkType`/`errorSinkConfig`
— to the **end** of the transform chain (after any user-declared transforms,
validating what's about to reach the sink).

If the pipeline **already has** an explicit `dq` transform declared, the
contract's own gate is **not** additionally appended — a pipeline is never
double-gated. A log line notes when this happens.

```json
"contract": {
  "expectations": [
    { "name": "amount not null", "type": "expectColumnNotNull", "column": "amount" }
  ],
  "qualityThreshold": 0.99,
  "onViolation": "fail"
}
```

## Duration SLA

`maxDurationMs` is checked **after** the run completes — there is no cheap way
to preempt a pipeline mid-batch, so an in-progress run is never aborted for
exceeding it. Instead, a run that would otherwise report `"completed"` is
reclassified `"partial"` when it exceeds the ceiling, so the SLA breach is
visible in the run summary, [audit log](observability.md#audit-log-optional),
and [lineage](observability.md#lineage-openlineage) rather than silently
passing as fully successful. The run's data is otherwise complete and correct
— only the status reflects the SLA breach.

## Example

```json
{
  "source": { "type": "jdbc", "driver": "postgres", "dsn": "...", "table": "orders" },
  "sink": { "type": "parquet", "options": { "path": "s3://bucket/orders/" } },
  "contract": {
    "version": "1.0.0",
    "expectations": [
      { "name": "order_id not null", "type": "expectColumnNotNull", "column": "order_id" },
      { "name": "amount not null", "type": "expectColumnNotNull", "column": "amount" }
    ],
    "qualityThreshold": 0.99,
    "onViolation": "fail",
    "maxDurationMs": 600000
  }
}
```

## See also

- [Pipeline file → `contract`](pipeline-file.md#contract-optional)
- [`dq` transform](transforms/dq.md) — the expectation engine the contract wraps
- [Observability](observability.md) — audit log, lineage, and the `stallSeconds` per-batch SLA
