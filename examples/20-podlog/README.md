# Example 20 — Kubernetes Pod Log Collector (`podlog` source)

> **Status: ✅ Runnable.** A self-contained collector that tails container
> (CRI/containerd) pod logs, stitches multiline stack traces, and ships
> newline-delimited JSON with **verifiable, auditable no-loss**. The full
> engineering spec is in [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md).

Replace a Fluentd-per-namespace log stack (~30 collectors) with **one
`flogo-datapipeline` per node** (a DaemonSet — 3 collectors on a 3-node cluster)
that lands every log line in S3 and can **prove nothing was lost**.

The motivating problem is **trust, not throughput**. The customer's ~15M
records/day is trivial for the engine (this example soaks at >250k rec/s — see
[Throughput](#throughput)). What was missing was the ability to tell whether a
log line absent from S3 was dropped at the tailer, the buffer, or the S3 write.
The `podlog` source is built around a delivery guarantee:

> **The read position advances only after the sink acknowledges the write.**
> A crash replays from the last committed offset — producing *detectable
> duplicates*, never silent loss (at-least-once).

---

## Run it locally

The example runs in **bounded mode** against a synthetic `/var/log/pods` tree
under [`data/pods/`](data/pods) — no Kubernetes node required. It uses the
**App Startup hook** trigger (the correct driver for a long-running collector)
and a **file checkpoint** that advances the position registry.

```bash
./run.sh
# or directly:
flogo-datapipeline -f podlog-collector.datapipeline
```

Output (in `out/`):

| File | Contents |
|---|---|
| `out/pod-logs.ndjson` | Collected records — one JSON object per logical log line |
| `out/dlq.ndjson` | Records that failed validation (empty message) — never dropped |
| `out/positions.json` | The byte-offset position registry (the delivery record) |
| `out/ckpt/` | Checkpoint tokens (carry the registry snapshot for audit) |

The two synthetic pods produce **7 logical records**: `team-a` emits a 3-frame
Java stack trace that is stitched into **one** record; `team-b` emits three
single-line entries.

---

## What the source does

| Stage | File | Responsibility |
|---|---|---|
| CRI parse | [cri.go](../../source/podlog/cri.go) | Parse the containerd/CRI-O line format (timestamp · stream · P/F flag · message) |
| Multiline stitch | [cri.go](../../source/podlog/cri.go) | Reassemble CRI partial lines **and** stitch BW6/Java stack traces into one record |
| Position registry | [positions.go](../../source/podlog/positions.go) | Crash-safe per-file byte offsets (atomic temp+rename), rotation detected by fingerprint |
| Tailing | [tailer.go](../../source/podlog/tailer.go) | Glob-scan + tail `/var/log/pods`, rotation/truncation handling, namespace filtering |
| Source + resume | [source.go](../../source/podlog/source.go) | `source.Source` + `CursorProvider`; post-ack offset advance; idempotency keys |
| Reconciliation | [metrics.go](../../source/podlog/metrics.go) | `podlog_*` Prometheus counters for the read == written + dlq invariant |

### Emitted schema

`namespace`, `pod`, `container`, `uid`, `stream`, `log_time`, `message`,
`node`, `idempotency_key`. A downstream `map` transform adds `ingest_time`
via `datetime.now()`.

Each record carries a stable **idempotency key**
`sha256(node + file + fingerprint + end-offset)` so a downstream consumer can
dedupe replays.

---

## Configuration

```jsonc
"source": {
  "type": "podlog",
  "options": {
    "root": "/var/log/pods",                 // CRI pod-log root (hostPath mount)
    "node": "${NODE_NAME}",                   // from the k8s downward API
    "positionFile": "/var/lib/flogo-podlog/positions.json",
    "fromBeginning": false,                   // false = tail new files from EOF
    "pollMs": 500,                            // idle poll interval
    "bounded": false,                         // true = drain once and exit (demos/tests)
    "excludeNamespaces": ["kube-system", "kube-public"],
    "multiline": {
      "startPattern": "^\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}",
      "flushIdleMs": 2000,                    // emit a pending record after idle
      "maxLines": 500                         // safety cap per stitched record
    }
  }
}
```

> ⚠️ A streaming `podlog` source **requires a checkpoint** (e.g.
> `{"type":"file","intervalSeconds":10}`). Without one, its delivery position is
> never persisted mid-run and a restart replays from the beginning. The Studio
> validator warns when this is missing.

---

## Two ingestion modes

Both are selectable from the Studio's **Pipeline Settings → Trigger** tab.

| Mode | Trigger | Use case |
|---|---|---|
| **Tailer** (this example) | App Startup hook | Node-local continuous collection — a DaemonSet per node |
| **Drop-folder** | File Poller | Watch a directory of exported/rotated log files; each file → one run via `inputPath` |

---

## Reconciliation — proving no-loss

This is the differentiator. Enable Prometheus
(`"observability": {"metrics": "prometheus"}`) and the collector exposes, on the
same `/metrics` endpoint:

| Metric | Meaning |
|---|---|
| `podlog_lines_read_total{node,namespace}` | logical records emitted by the source |
| `flogo_pipeline_rows_written_total{pipeline}` | records acked by the sink |
| `flogo_pipeline_rows_failed_total{pipeline}` | records routed to the DLQ |
| `podlog_bytes_committed_total{node}` | bytes durably delivered (post-ack) |
| `podlog_files_open{node}` | files currently tailed |
| `podlog_rotations_total{node}` | detected rotations/truncations |

**Invariant:** `read == written + dlq`, per node and namespace. The
[`k8s/prometheus-rules.yaml`](k8s/prometheus-rules.yaml) ships the recording rule
plus a `PodlogReconciliationDrift` alert, turning "is anything being lost?" into
a dashboard instead of an investigation. The source also logs an always-on
reconciliation summary at shutdown, so the guarantee holds even without
Prometheus.

---

## Deploy to Kubernetes

[`k8s/daemonset.yaml`](k8s/daemonset.yaml) is a ready-to-apply manifest set:

- **DaemonSet** — one collector pod per node (the "3, not 30" model)
- **ServiceAccount + ClusterRole/Binding** — least-privilege pod/namespace read
- **hostPath mounts** — `/var/log/pods` (read-only) + `/var/lib/flogo-podlog`
  (read-write, persists the position registry across restarts)
- **downward-API `NODE_NAME`** — stamps each record and partitions the S3 key by node
- **tolerations** (lands on every node), **resource limits**, liveness probe,
  Prometheus scrape annotations, and an inline **ConfigMap** pipeline

```bash
# Edit image + S3 bucket first, then:
kubectl apply -f k8s/daemonset.yaml
kubectl -n logging rollout status daemonset/podlog-collector
```

The S3 sink key `logs/{namespace}/{YYYY}/{MM}/{DD}/${NODE_NAME}/part.ndjson`
uses **per-record namespace partitioning** — one node tails many namespaces and
each lands under its own prefix (tenancy preserved). Add `rollMaxBytes` /
`rollMaxSeconds` to roll objects by size/time, each with an audit
`*.manifest.json` sidecar.

---

## Throughput

The soak test ([`soak_test.go`](../../source/podlog/soak_test.go)) drains
**400,000 logical records** (5 namespaces × 4 pods, ~10% multiline traces):

```
go test ./source/podlog/ -run Throughput -v
# soak: 400000 logical records in ~1.6s = ~256000 rec/s (~22B/day projected)
```

That is **~1,400× the customer's 15M/day requirement**, with exact
reconciliation (read count == expected). The real-world bottleneck is the S3
sink, not the collector.

---

## Acceptance criteria (all met)

1. ✅ DaemonSet collects all namespaces with correct enrichment
2. ✅ A Java stack trace lands as **one** record
3. ✅ Kill-and-restart loses **zero** records (duplicates allowed) — resume test
4. ✅ Log rotation/truncation loses **no** records — rotation + truncation tests
5. ✅ `read == written + dlq` per node+namespace — reconciliation metrics
6. ✅ Malformed lines routed to the DLQ, never silently dropped
7. ✅ Sustains 15M records/day with vast headroom — soak test
