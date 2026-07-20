# Data quality

Three transforms plus the Dead-Letter Queue let you enforce correctness *at ingestion
time* — so bad records are caught and captured, not silently propagated. All three can
route rejects to a durable sink instead of failing the whole run.

- **`validate`** — hard per-field rules (contract enforcement).
- **`dq`** — statistical expectations and a scored quality report (monitoring / gates).
- **`schemadrift`** — structural change detection (the shape changed, not the values).

## Rejection routing

Every quality transform shares the same disposition choice:

| Disposition | Effect |
|---|---|
| `route` (default) | send rejected rows to the error sink / [DLQ](sinks.md) |
| `fail` | abort the run on the first violation |
| `drop` | discard the row silently |
| `warn` (dq) | count and report, but pass the row through |

Routing targets either a pipeline-level `dlq` sink or the transform's own
`errorSinkType` / `errorSinkConfig` — e.g. a validate transform can write its rejects
straight to a `jdbc` quarantine table. Captured rows carry an error envelope
(`errorMessage`, `originalRow`, `runId`, …).

## Validate — `validate`

Enforces a field contract. Each rule targets one `field`:

| Rule | Checks |
|---|---|
| `notNull` | value present |
| `min` / `max` | numeric bounds |
| `minLen` / `maxLen` | string length bounds |
| `pattern` | regex match |
| `in` | value in an allowed set |

```json
{ "type": "validate", "options": {
    "rules": [
      { "field": "email",  "notNull": true, "pattern": "^[^@]+@[^@]+$" },
      { "field": "amount", "min": 0 }
    ],
    "onViolation": "route"
} }
```

See [`examples/03-validate-and-dlq`](../examples/03-validate-and-dlq).

## Data Quality — `dq`

Evaluates dataset-level **expectations** and produces a `DQReport` (per-expectation
results, counts, `overallScore`, `passedGate`) surfaced in the run metadata and the
Studio Run Report.

| Expectation | Checks |
|---|---|
| `expectColumnNotNull` | no nulls in a column |
| `expectColumnValuesBetween` | numeric range |
| `expectColumnValuesInSet` | categorical membership |
| `expectColumnValuesMatchRegex` | format |
| `expectTableRowCountBetween` | row-count band |
| `expectTableRowCountNotZero` | non-empty |

```json
{ "type": "dq", "options": {
    "expectations": [
      { "kind": "expectColumnNotNull", "column": "id" },
      { "kind": "expectColumnValuesBetween", "column": "score", "min": 0, "max": 100 }
    ],
    "onFail": "route",
    "qualityThreshold": 0.95,
    "scoreColumn": "_dq_score",
    "reportSinkType": "jdbc",
    "reportSinkConfig": { "dsn": "…", "table": "dq_runs" }
} }
```

`qualityThreshold` gates the run (`passedGate`); `scoreColumn` appends a per-row
score; `reportSinkType` persists the report (e.g. to a dashboard table). See
[`examples/13-data-quality`](../examples/13-data-quality).

## Schema Drift — `schemadrift`

Baselines the schema from the first batch, then reacts when later batches differ:

| Policy | On drift |
|---|---|
| `project` (default) | keep only baseline columns |
| `additive` | grow the schema to include new columns |
| `strict` | abort the run |
| `drop` | skip the drifted batch |

`onDrift` (`log`/`alert`/`silent`) controls how loudly it reports. Use `additive` for
partner feeds that legitimately add fields; `strict` for regulated pipelines that must
not change shape silently. See [`examples/11-schema-drift`](../examples/11-schema-drift).

## See also

- [Transforms](transforms.md) · [Sinks](sinks.md) (DLQ)
- [Observability](observability.md) — `rows_failed_total` and DQ score metrics
- Per-transform detail: [validate](transforms/validate.md) · [dq](transforms/dq.md) · [schemadrift](transforms/schemadrift.md)
