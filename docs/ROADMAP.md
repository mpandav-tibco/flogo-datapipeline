# Flogo Data Pipeline — Roadmap

> Status: living document · Last updated: 2026-07-27
>
> This roadmap is deliberately **opinionated**. Its primary job is not to list
> everything we *could* build — it is to protect what the tool *is* and make it
> easy to say **no**. Read the doctrine first; every item below earns its place
> against it.
>
> Status keys: ✅ shipped · 🚧 building now · ⬜ planned.

---

## 1. North star

**A lightweight, embeddable, single-binary ETL/ELT engine** that moves and
transforms datasets from a pluggable source to a pluggable sink, with honest
operational primitives (checkpoint, watermark, schema-drift, data-quality,
metrics) and a visual authoring experience in the IDE.

We win by being **fast, small, Flogo-native, and composable** — not by being a
distributed compute framework or a general event processor.

---

## 2. Doctrine — the decision filter

Every proposed feature (especially every transform) is scored against three
questions. If it fails any one, it does **not** belong in the pipeline.

1. **Is it map-side / parallel-safe?** It must process a batch independently, with
   no cross-partition *shuffle*. Anything needing a global repartition (GROUP BY,
   big×big JOIN, global sort/dedup) is a distributed-shuffle problem — delegate it.
2. **Is it dataset-shaped, not event-shaped?** We move and transform *datasets*
   (files, tables, partitions, change-streams). Per-event reaction, branching,
   request/reply and step orchestration are **flow** concerns.
3. **Does it avoid duplicating a Flogo flow or the target?** If a flow already does
   it well (per-event logic) or the warehouse does it better (reduce/aggregate),
   we **compose** with them instead of re-implementing.

### Where work belongs

| Concern | Home |
|---|---|
| Bulk extract → per-row transform → partitioned load | **the pipeline** |
| React to one event / orchestrate steps / request-reply | **a Flogo flow** (compose in the same app) |
| Global GROUP BY / JOIN / sort / dedup (reduce) | **the target** (ELT) or a query engine |

---

## 3. Non-goals (permanent)

We will **not** build these. Writing them down is the point.

- **A distributed shuffle/reduce engine** (global group-by/join/sort). That is
  Spark/Flink/Hadoop. It would destroy the single-binary value prop.
- **A general event/message processor** (one pipeline run per event). That is what
  Flogo flows are for; the pipeline composes with them.
- **A workflow orchestrator/scheduler.** We integrate with triggers, cron, K8s,
  Airflow/Dagster — we do not become one.
- **Unbounded in-memory state.** Any stateful transform must use a bounded,
  pluggable state backend (see §5).

---

## 4. Horizons at a glance

| Horizon | Theme | Highlights |
|---|---|---|
| **Delivered** | Foundation + top transforms + streaming | ✅ typed-schema / decimal fidelity · ✅ bounded-state backend (windowing spill store) · ✅ **dataset / broadcast join** · ✅ **explode / flatten** · ✅ **cast** · ✅ event-time **windowing** (tumbling/session/global, checkpoint-resume) · ✅ schema-registry **read** (Avro consume) · ✅ dry-run preview · ✅ OOTB packaging (lean `dp-build`, distroless) |
| **Now** | Governance + routing + lineage | 🚧 **audit log** · 🚧 **conditional router / split** · 🚧 **lineage (OpenLineage)** · ⬜ **mask / PII policy** · ⬜ Transform-SDK parity + conformance kit + golden-pipeline CI |
| **Next** | Enterprise transforms + adoption | ⬜ **keyed dedup** · ⬜ **partial aggregate (combiner)** · ⬜ field **encryption** · ⬜ Avro **produce** · ⬜ `--dry-run` / validate CI mode · ⬜ first connector expansions (REST/API source, MongoDB) |
| **Later** | Differentiation + platform | ⬜ surrogate-key / sequence · ⬜ partition window fns · ⬜ warehouse & lakehouse connectors (Snowflake / BigQuery / Redshift, Iceberg / Delta) · ⬜ Helm / K8s operator + published images · ⬜ CDC-as-source + delete-capable sink |

---

## Current cycle — active build (2026-07-27)

Three enterprise-governance gaps are being built **now**. Each must obey the
doctrine (§2): map-side, per-batch, plugin-shaped, zero core forks.

| # | Item | What "done" means |
|---|---|---|
| 1 | 🚧 **Audit log** | Structured, append-only JSON audit trail of pipeline lifecycle + governance events (run start/stop/outcome, checkpoint saves, DLQ / router routes, effective config with **secrets redacted**). Off by default; enabled by config/env; emits to stdout or a durable append-only file. No secret value is ever written. |
| 2 | 🚧 **Conditional router / split** | A `router` transform that N-way routes rows to named sinks by ordered predicates (first-match, with an optional default route) — generalizes the 2-way DLQ branch. Map-side, no shuffle; typed-schema preserving. |
| 3 | 🚧 **Lineage (OpenLineage)** | Emit OpenLineage run/job/dataset events (START / COMPLETE / FAIL / ABORT) naming the source & sink datasets plus schema facets, wired through the existing `observability` config. Answers "where did this data come from." |

---

## 5. Track A — Transforms

Current set (13): `filter`, `map`, `validate`, `lookup/enrich` (HTTP), `parse`,
`hwm`, `schemadrift`, `dq`, `cast`, `explode`, `flatten`, `join`, `window`.

### Tier 1 — highest value, pure map

✅ **Shipped:** dataset / broadcast **`join`**, **`explode`**, **`flatten`**, **`cast`**.

Remaining:

| Transform | Problem it solves | Fit | Status |
|---|---|---|---|
| **Conditional router / split** | N-way route rows to different sinks by predicate (generalizes the DLQ branch). "Conditional split" is table-stakes ETL. | per-row | 🚧 building |
| **Mask / tokenize / PII** | Field-level policy (mask / hash / tokenize / encrypt / drop by classification). Enterprise + data-residency / compliance. (A `util.mask()` expression exists today; this is the policy-driven transform.) | per-row | ⬜ next |

### Tier 2 — build carefully, **only on the bounded-state backend** (§6.3)

| Transform | Problem it solves | Guardrail |
|---|---|---|
| **Keyed / windowed dedup** | "latest per key", drop repeats | windowed or state-backed (Redis/Postgres) — never unbounded in memory |
| **Partial aggregate (combiner)** | Per-partition sum/count/min/max/avg by key; emit *partials* for the target to finalize | explicitly **not** a global GROUP BY |
| **Surrogate key / sequence** | Monotonic keys for warehouse dimension loads | use the distributed coordinator for global monotonicity |
| **Partition-scoped window fns** | running total / rank / lag within a key | bounded to the partition key |

### Explicitly out — delegate, don't build

| Not this | Because | Do instead |
|---|---|---|
| Global GROUP BY, big×big JOIN, global sort/dedup | distributed shuffle | **ELT**: land it, reduce in the warehouse/lakehouse |
| Per-event branching / step orchestration | that's a flow | compose flow ↔ pipeline in one app |

---

## 6. Track B — Sustainability (keep it maintainable as transforms multiply)

The transform registry is the spine of the product; sustainability is mostly
about protecting it.

1. **Productize the Transform SDK.** 🚧 Everything is already additive
   (`transform.Register` + `Apply(batch) → batch`) and there is a `sdk/` builder,
   **but the pipeline-as-code builder lags the engine** (missing cast/explode/
   flatten/window/join/parse/lookup + most sources/sinks). Ship a documented SDK, a
   **conformance test-kit** (⬜), and a "write your own transform" guide so every
   new transform — ours or a customer's — is a plugin that never touches core.
2. **Write the doctrine down** (§2) as a `CONTRIBUTING`/design gate. Sustainability
   is the institutional ability to say *no*. (This doc holds it; not yet a gate.)
3. ✅ **One bounded-state abstraction for stateful transforms.** Done for
   windowing (`statestore` + LRU/sharded `spillstore`, bounds + spill). ⬜ Not yet
   *unified* with checkpoint/watermark or reused by future dedup/aggregate. No
   transform may hold unbounded memory.
4. ✅ **Typed schema as a contract end-to-end.** Typed `record` batches propagate
   through transforms; typed sinks (Parquet/JDBC) stay clean (decimal fidelity done).
5. ⬜ **Golden-pipeline CI.** Per-transform unit tests exist **plus** `integration/`
   e2e tests, but there is no fixed input→output **snapshot** harness yet.
   Multiplying transforms without this guarantees regressions.

---

## 7. Track C — Attainability (become a tool teams adopt)

More transforms is *not* the main gap between us and "credible ETL tool." These are.

1. **Connector breadth — the long pole.** Paginated REST/API source; native
   warehouse sinks (Snowflake / BigQuery / Redshift); lakehouse (Iceberg / Delta);
   MongoDB. Reuse Flogo connectors where possible.
2. **Schema & lineage.** ✅ Schema-registry **read** (Avro/Confluent consume +
   `/v1/introspect/schema-registry`); ✅ evolution policy (`schemadrift`); ⬜ Avro
   **produce** is still a JSON stub. 🚧 **lineage emission** (OpenLineage) is
   building now — enterprises need "where did this data come from."
3. **Incremental as the default story.** HWM exists — make delta-not-full-reload the
   headline; make the CDC-as-source bet consciously (see §8).
4. **Pipeline testability in CI.** ✅ dry-run preview + `dq`; ⬜ add a
   `--dry-run`/validate CLI mode + seedable sample data. "Can I test this pipeline
   in CI?" is an adoption gate.
5. **Deployment & packaging.** ✅ OOTB bundled VSIX + lean per-app binary +
   distroless Dockerfile + K8s Job/CronJob/StatefulSet guidance + Studio manifest
   generation. ⬜ Helm chart · K8s operator/CRD · published images.
6. **Security & governance.** ✅ Secret references; 🚧 **audit log** (building now);
   ⬜ field **masking/PII** policy transform; ⬜ field **encryption**.

---

## 8. Open decisions (need an explicit call)

- **CDC as a streaming source** (e.g. `mysql-binlog`, Debezium-style). *Genuinely*
  ETL and differentiating, but real work (snapshot + stream cutover, offset
  checkpointing, schema evolution, and DELETE — which has **no sink today**).
  Greenlight only if change-data ingestion is a deliberate product direction — and
  build it as a proper streaming **source**, not a trigger→data handoff.
- **Delete-capable sink.** Prerequisite for CDC-to-target replication.

---

## 9. Definition of done — new transform

A transform ships only when it:

- [ ] passes the doctrine (§2) — map-side, dataset-shaped, non-duplicating;
- [ ] registers via the SDK (`transform.Register`), zero core edits;
- [ ] propagates typed schema correctly to downstream stages;
- [ ] uses the bounded-state backend if stateful (no unbounded memory);
- [ ] has unit tests **and** a golden end-to-end pipeline;
- [ ] is documented in `docs/FEATURES.md` with a config example;
- [ ] surfaces in Studio with a structured editor (no raw-JSON-only stages).

---

## 10. One-line strategy

Grow only by adding **map-side transforms as plugins** (never shuffle, never event
orchestration), keep every stateful feature on **one bounded state backend**, and
**delegate reduce to the target** — then spend the real roadmap on **connectors +
schema/lineage + testability + packaging**, because that, not more transforms, is
what turns a fast engine into a tool teams bet on.
