# Example 20 — Kubernetes Pod Log Collector (Fluentd Replacement)

> **Status:** Design / implementation plan. This document is the engineering spec for
> the `podlog` source that replaces the customer's Fluentd + custom-glue log stack.
> Code under `source/podlog/` (`cri.go`, `positions.go`, `tailer.go`) is partially
> scaffolded; the remaining work is enumerated in **Section 8 — Work Breakdown**.

---

## 1. Customer Problem Statement

### 1.1 The environment

| Dimension | Value |
|---|---|
| Kubernetes nodes | 3 |
| Namespaces | ~30 |
| Apps per namespace | 5 – 200 TIBCO BW6 applications |
| Total app pods (prod) | ~ total / 3 per node |
| Log volume | ~15 million log records / day (~174 rec/s avg, 500–1,000 rec/s peak) |
| Current collector | **1 Fluentd per namespace** (~30 Fluentd instances) |
| Destination | S3 bucket (JSON) |
| Container runtime | containerd / CRI-O (CRI log format) |

### 1.2 The pain — *not throughput, trust*

The volume is trivial for the pipeline engine. The real, business-critical problem is
**unverifiable data loss in production**:

> When a log line is missing in S3, the customer cannot tell **whether Fluentd dropped
> it, the custom shim corrupted it, or the S3 write silently failed.** In prod,
> unexplained log loss is unacceptable (audit, incident forensics, compliance).

Root causes of the current blind spots:

1. **Tail → buffer loss** — file rotation races, Fluentd restarts losing in-memory
   buffer position, truncation on rotation.
2. **Buffer → S3 loss** — exhausted retries, dropped chunks, partial multipart uploads.
3. **No reconciliation** — nobody counts "lines read on the node" vs "lines landed in
   S3", so loss is undetectable until someone goes looking, by which point the source
   file is already rotated away.
4. **30 opaque collectors** — one Fluentd per namespace means 30 independent buffers and
   30 separate blind spots, none individually accountable.

### 1.3 What we are building

A **node-local pod-log collector** that:

- reads container logs directly from node disk (`/var/log/pods/...`),
- reassembles multiline BW6 / Java stack traces into single records,
- structures each line and stamps namespace / pod / container / ingest-timestamp,
- writes newline-delimited JSON to S3, **and**
- **proves nothing was lost** via an auditable read = written + dlq reconciliation.

It replaces **30 Fluentds with 3 accountable DaemonSet collectors** (one per node).

---

## 2. Goals & Non-Goals

### 2.1 Goals

- **G1 — At-least-once delivery.** Every log line that reaches node disk lands in S3 at
  least once. On crash we replay, producing *detectable duplicates*, never silent loss.
- **G2 — Verifiable no-loss.** End-to-end accounting: `lines_read == lines_written +
  lines_dlq` per node/namespace/file, exposed as metrics and (optionally) per-file S3
  manifests.
- **G3 — BW6-aware multiline.** Java stack traces are stitched into one logical record
  before counting or shipping.
- **G4 — Rotation & truncation safe.** containerd/CRI-O rotation (rename + recreate) and
  in-place truncation never lose or double-skip data.
- **G5 — Fewer moving parts.** 3 per-node DaemonSets replace 30 per-namespace Fluentds.
- **G6 — JSON-lines to S3**, partitioned by namespace and date for downstream tenancy.

### 2.2 Non-Goals (this phase)

- Log **querying / search UI** (S3 + Athena/downstream owns this).
- Exactly-once semantics (customer accepted at-least-once with dedupe key).
- Parsing every BW6 log *layout* into typed columns (Phase 2 — see Section 9).
- Replacing node-level log rotation policy (kubelet owns rotation).
- Windows container logs (Linux CRI nodes only; code stays OS-portable for dev).

---

## 3. Delivery Contract (the core guarantee)

This is the heart of the design and must be reviewed with the customer.

```
                 read from disk        transform         write to S3        advance position
  CRI log file ───────────────► records ─────► JSON rows ─────► S3 object ─────► pos file
       │                                                            │                 │
       │                                                            │ ack (HTTP 200)  │
       └────────────────── replay window ──────────────────────────┴─────────────────┘
```

**Rule: the position file advances only AFTER the sink acknowledges the write.**

- If the process crashes between *write* and *position advance*, on restart we re-read
  from the last persisted offset → the already-written records are **re-emitted**
  (safe duplicates), never lost. → **at-least-once (G1).**
- Every emitted record carries a stable **idempotency key**
  `sha256(node + file-path + fingerprint + end-offset)` so a downstream consumer (or S3
  object naming) can dedupe replays if desired.

**Position file (registry)** — the authoritative delivery record, equivalent to
Fluentd's `pos_file` / Filebeat's registry:

```json
{
  "/var/log/pods/ns-a_app-1_uid/bwappnode/0.log": {
    "offset": 148213,
    "fingerprint": "3f9a...<hex of first 256 bytes>"
  }
}
```

- **`offset`** — byte offset of the last *durably delivered* newline.
- **`fingerprint`** — hex of the first 256 bytes; detects rotation (a fresh file at the
  same path has a different fingerprint → offset resets to 0).
- Written **atomically** (temp file + rename) so a crash mid-write cannot corrupt it.

> **Why a source-owned position file and not the engine checkpoint token?**
> The pipeline engine's resume path calls only `Skip(RowsProcessed)` and does **not**
> feed `Token.Payload` back into the source. For a *tailing, unbounded, multi-file*
> collector, "skip N rows" is meaningless. So the source owns its own registry (like
> every real log shipper) and still implements `CursorProvider.Cursor()` to publish the
> registry into `Token.Payload` for **observability/audit only.**

---

## 4. Architecture

### 4.1 Deployment topology — per-node DaemonSet

```
┌─ Node 1 (DaemonSet pod) ───────────────────────────────────────────────┐
│  hostPath: /var/log/pods  (read-only)                                   │
│  hostPath: /var/lib/flogo-podlog  (read-write — position registry)      │
│                                                                         │
│   /var/log/pods/<ns>_<pod>_<uid>/<container>/*.log   (ALL namespaces)   │
│         │  podlog SOURCE                                                 │
│         │    • glob-scan + tail (poll loop)                             │
│         │    • CRI parse + partial-line reassembly                      │
│         │    • MULTILINE stitch (BW6 / Java stack traces)               │
│         │    • enrich: namespace, pod, container, uid, stream           │
│         │    • byte-offset position registry (post-ack advance)         │
│         ▼                                                                │
│    map (add ingest ts, node) → validate (+DLQ) → …                      │
│         ▼                                                                │
│    jsonlines/S3 sink   s3://logs/{namespace}/{YYYY}/{MM}/{DD}/{node}/…  │
│         ▼                                                                │
│    metrics: lines_read = lines_written + lines_dlq  (Prometheus)        │
└─────────────────────────────────────────────────────────────────────────┘
        × 3 (one per node)  — natural sharding by node, zero coordination
```

- **3 collectors, not 30.** Each node's DaemonSet pod tails *all* namespaces present on
  that node and stamps the namespace onto each record from the CRI path.
- **Natural sharding** — a pod's log file exists on exactly one node, so there is no
  overlap and no distributed coordinator needed (`scaling.mode: single`).
- **Namespace tenancy preserved downstream** via the S3 key partition
  (`{namespace}/...`), regardless of the collector being node-scoped.

> **Alternative considered — per-namespace Deployment (30 collectors).** Only justified
> if hard tenant isolation is required (namespace-scoped RBAC, per-team S3
> buckets/credentials, blast-radius isolation). Otherwise per-node wins decisively on
> simplicity and accountability. **Decision required from customer — see Section 10.**

### 4.2 Data flow within the source

```
physical CRI line
  2026-07-14T10:00:00.123Z stdout F <msg>
        │  parseCRILine()  (cri.go)
        ▼
criLine{ts, stream, partial, message, valid}
        │  stitcher.push()  (cri.go)
        │    level 1: join CRI "P" partial lines → full line
        │    level 2: join app continuation lines (no startPattern match) → 1 record
        ▼
logRecord{ts, stream, message}
        │  tailer.emit()  (tailer.go) — attach podMeta + fileKey + endOffset + fp
        ▼
enrichedRecord ──► channel ──► source.ReadBatch() ──► record.RecordBatch ──► pipeline
```

### 4.3 Component map (source/podlog/)

| File | Responsibility | Status |
|---|---|---|
| `cri.go` | CRI line parser + two-level stitcher (partial + multiline) | **scaffolded** |
| `positions.go` | Crash-safe byte-offset position registry (atomic writes) | **scaffolded** |
| `tailer.go` | Glob-scan, tail, rotation/truncation detection, enrichment | **scaffolded** |
| `source.go` | `source.Source` + `CursorProvider` impl; config parse; registration | **TODO** |
| `source_test.go` | Unit + integration tests (see Section 7) | **TODO** |

---

## 5. Interface Contract (how it plugs into the engine)

The source must satisfy the existing interfaces verbatim (verified against source):

```go
// source.Source — source/source.go
type Source interface {
    Schema() []record.Column
    ReadBatch(size int) (*record.RecordBatch, error)  // io.EOF only in bounded mode
    Skip(n int64) error                               // no-op for tailer (pos file owns resume)
    Close() error
}

// checkpoint.CursorProvider — checkpoint/checkpoint.go  (optional, we implement it)
type CursorProvider interface {
    Cursor() json.RawMessage   // returns the position registry snapshot (audit only)
}

// registration — init()
func init() { source.Register("podlog", newPodlogSource) }
```

### 5.1 Fixed schema emitted by the source

| Column | Type | Source |
|---|---|---|
| `namespace` | string | CRI path `<ns>_<pod>_<uid>` |
| `pod` | string | CRI path |
| `container` | string | CRI path |
| `uid` | string | CRI path |
| `stream` | string | CRI field (`stdout` / `stderr`) |
| `log_time` | string | CRI RFC3339Nano timestamp (when the app emitted it) |
| `message` | string | stitched log payload (may be multiline) |

Downstream `map` transform adds `ingest_time` and `node`; `validate` + DLQ catches
records with empty `message` / unparseable timestamp.

### 5.2 Streaming semantics (matches Kafka source)

- **Unbounded:** `ReadBatch` returns an empty (non-nil, len 0) batch when the poll window
  elapses with no new lines. It returns `io.EOF` **only** when configured in a bounded
  drain mode (used by tests). The pipeline runs until SIGTERM.
- **Backpressure:** the tailer goroutine feeds a buffered channel (depth = batchSize×2);
  when the sink is slow the channel fills and the tailer blocks on disk reads — no
  in-memory unbounded growth.

---

## 6. Configuration Design

### 6.1 Source config (`.datapipeline` resource → `source` block)

```jsonc
"source": {
  "type": "podlog",
  "options": {
    "root": "/var/log/pods",                  // CRI pod-log root (hostPath mount)
    "node": "${NODE_NAME}",                    // from k8s downward API
    "positionFile": "/var/lib/flogo-podlog/positions.json",
    "fromBeginning": false,                    // false = tail new files from EOF
    "pollMs": 500,                             // idle poll interval

    // --- multiline (BW6 / Java) ---
    "multiline": {
      "startPattern": "^\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}",  // record starts with a timestamp
      "maxLines": 500,                         // safety cap per stitched record
      "flushIdleMs": 2000                      // emit a pending record after idle
    },

    // --- namespace filtering (optional) ---
    "includeNamespaces": [],                   // empty = all on node
    "excludeNamespaces": ["kube-system", "kube-public"]
  }
}
```

### 6.2 Downstream stages (recommended default)

```jsonc
"transforms": [
  { "type": "map", "options": { "fields": {
      "namespace": "=$namespace", "pod": "=$pod", "container": "=$container",
      "stream": "=$stream", "log_time": "=$log_time", "message": "=$message",
      "node": "=NODE_ENV_OR_STATIC", "ingest_time": "=now()"
  }}},
  { "type": "validate", "options": {
      "rules": [{ "field": "message", "notNull": true }],
      "onViolation": "route"                   // → DLQ, never drop silently
  }}
],
"dlq": {
  "type": "s3-sink",
  "options": { "bucket": "logs-dlq", "keyTemplate": "dlq/{YYYY}/{MM}/{DD}/{node}.ndjson" }
},
"sink": {
  "type": "s3-sink",
  "options": {
    "bucket": "logs",
    "keyTemplate": "{namespace}/{YYYY}/{MM}/{DD}/{node}/part-{seq}.ndjson",
    "format": "jsonlines",
    "rollMaxBytes": 134217728,                 // 128 MiB object roll
    "rollMaxSeconds": 300                       // or every 5 min
  }
},
"checkpoint": { "type": "file", "path": "/var/lib/flogo-podlog/ckpt", "intervalSeconds": 10 },
"observability": { "metrics": "prometheus", "options": { "listenAddr": ":9090" } },
"scaling": { "mode": "single" }
```

> **Note on S3 sink `keyTemplate`/roll options** — verify against the current
> `sink/objstore` implementation; if unsupported, this is a small sink enhancement
> tracked in Section 8 (T7).

---

## 7. Reconciliation & Observability (the differentiator)

This is what actually solves the customer's stated fear — loss becomes **provable, not
guessable.**

### 7.1 Metrics (Prometheus, per node + namespace)

| Metric | Meaning |
|---|---|
| `podlog_lines_read_total{node,namespace}` | logical records emitted by the source |
| `pipeline_rows_written_total{node,namespace}` | records acked by the S3 sink |
| `pipeline_rows_dlq_total{node,namespace}` | records routed to DLQ |
| `podlog_bytes_committed_total{node,file}` | position-registry advance |
| `podlog_files_open{node}` | currently tailed files |
| `podlog_rotations_total{node}` | detected rotations |

**Invariant alert (Prometheus rule):**

```
read  ==  written + dlq        (per node+namespace, over a rolling window)
```

Any sustained drift raises an alert — the missing-log question becomes a dashboard, not
an investigation.

### 7.2 Per-file S3 manifest (optional, audit-grade)

On each object roll, write a sidecar manifest so an auditor can reconcile
"what the node saw" against "what landed in S3", object by object:

```json
{
  "node": "node-1", "namespace": "ns-a",
  "source_file": "/var/log/pods/ns-a_app-1_uid/bwappnode/0.log",
  "fingerprint": "3f9a...",
  "start_offset": 0, "end_offset": 148213,
  "record_count": 1024,
  "s3_object": "s3://logs/ns-a/2026/07/14/node-1/part-0007.ndjson",
  "idempotency_key_range": ["<first>", "<last>"]
}
```

---

## 8. Work Breakdown (implementation tasks)

| ID | Task | Depends on | Est. |
|---|---|---|---|
| **T1** | `cri.go` — CRI parser + two-level stitcher | — | ✅ scaffolded |
| **T2** | `positions.go` — atomic byte-offset registry | — | ✅ scaffolded |
| **T3** | `tailer.go` — glob-scan, tail, rotation/truncation, enrichment | T1,T2 | ✅ scaffolded |
| **T4** | `source.go` — `Source` + `CursorProvider`, config parse, `init()` registration, post-ack position advance wiring | T1–T3 | **TODO** |
| **T5** | Multiline `flushIdleMs` timer (emit pending record when a file goes idle) | T3 | **TODO** |
| **T6** | Register `podlog` blank-import in `cmd/flogo-datapipeline/main.go` | T4 | **TODO** |
| **T7** | S3/objstore sink: `keyTemplate` macros, size/time roll, manifest sidecar | — | **TODO (verify existing)** |
| **T8** | Reconciliation metrics (`podlog_*` counters) + Prometheus rule sample | T4 | **TODO** |
| **T9** | Unit tests: CRI parse, partial reassembly, multiline stitch, rotation, truncation, resume-after-crash | T1–T4 | **TODO** |
| **T10** | Integration test: synthetic `/var/log/pods` tree → NDJSON, assert read==written | T4–T7 | **TODO** |
| **T11** | K8s manifests: DaemonSet, RBAC, hostPath mounts, downward-API `NODE_NAME` | T4 | **TODO** |
| **T12** | Example assets (`20-podlog/`): sample tree, `run.sh`, `verify.sh`, this README | all | in progress |

### 8.1 Post-ack position advance — the critical wiring (T4)

The one subtlety: the position registry must advance **after** the sink acks, but the
source emits records *before* transforms/sink run. Two viable designs:

- **Option A — sink-driven ack callback (preferred).** The source tracks
  per-file high-water `endOffset` for records it has emitted but not yet confirmed. The
  pipeline/sink signals successful batch write; on that signal the source promotes the
  confirmed offsets into the registry and calls `positions.persist()`. Requires a small
  ack hook (align with how the engine already calls `Flush()` after checkpoint interval —
  see `action/pipeline/action.go`).
- **Option B — checkpoint-interval flush.** Advance the registry inside the source's
  `Flush()` (called by the engine after each checkpoint interval, post-sink). Simpler,
  slightly larger replay window (bounded by `intervalSeconds`). **Recommended for v1.**

> **Decision required — see Section 10, Q4.**

---

## 9. Phased Delivery

- **Phase 1 — Prove the guarantee (MVP).**
  T4–T6, T9–T12 with the `jsonlines`/S3 sink and reconciliation metrics. Deliverable:
  DaemonSet that replaces Fluentd and demonstrates `read == written + dlq` under
  induced crashes and rotations. This is the "we can prove nothing was lost" story the
  customer is buying.

- **Phase 2 — Hardening & structure.**
  Per-file S3 manifests (T7), BW6 log-layout parsing into typed columns
  (timestamp/level/engine/module via `map` regex), backpressure tuning, DLQ dashboards,
  optional BW6 structured/JSON log-layout ingestion (simpler + cleaner accounting).

- **Phase 3 — Scale-out options.**
  Optional Kafka buffer in front for multi-region fan-out; per-namespace credential
  isolation if compliance requires it.

---

## 10. Open Decisions (need customer input)

| # | Question | Options | Recommendation |
|---|---|---|---|
| Q1 | Collection model | per-node DaemonSet (3) vs per-namespace (30) | **Per-node** unless hard RBAC/credential isolation is mandated |
| Q2 | BW6 log format | plain log4j/logback text vs structured JSON | Enable **structured JSON** if possible (cleaner parse + accounting) |
| Q3 | S3 object roll policy | size (128 MiB) / time (5 min) / count | size **or** time, whichever first |
| Q4 | Position advance model | ack-callback (A) vs Flush-interval (B) | **Option B** for v1 (bounded replay window) |
| Q5 | Manifest sidecars | on / off | **On** for audit-grade reconciliation |
| Q6 | DLQ retention | bucket + lifecycle policy | 30-day DLQ retention |

---

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| kubelet rotates/deletes a file before we finish reading it | data loss | Read is node-local and fast; rotation detected by fingerprint; `containerLogMaxFiles` tuned so rotated files persist long enough |
| Multiline `startPattern` misconfigured | fragmented or over-merged records | `maxLines` cap + `flushIdleMs`; validate pattern against real BW6 samples before rollout |
| Position file lost (node disk wipe) | full replay from EOF/beginning | `positionFile` on a persistent hostPath; `fromBeginning:false` bounds replay |
| Duplicate records after crash | downstream double-count | Idempotency key per record + `onConflict`/dedupe downstream (at-least-once is accepted) |
| Very high-cardinality namespace metric labels | Prometheus load | Limit label set to `{node,namespace}`; drop `pod`/`container` from metrics |
| DaemonSet pod OOM under log burst | collection stalls | Bounded channel + backpressure; memory ceiling via `performance.maxMemoryMB` |

---

## 12. Acceptance Criteria

1. Deploying the DaemonSet on a 3-node cluster collects logs from **all** namespaces with
   correct `namespace/pod/container` enrichment.
2. A BW6 Java stack trace lands in S3 as **one** JSON record, not N fragments.
3. Killing the collector mid-run and restarting produces **zero missing** records
   (duplicates allowed) — verified by idempotency-key diff.
4. containerd log rotation during active collection loses **no** records.
5. Prometheus shows `read == written + dlq` per node+namespace across a soak test.
6. Malformed lines are routed to the DLQ, never silently dropped.
7. 3 collectors sustain 15M records/day with headroom (target ≤ 30% of one core/node).
