# POC Report — adidas BW6 log collection, verified on a 3-node Kubernetes cluster

**Date:** 2026-10-24 · **Branch:** `poc/adidas-3node-hardening`
**Result:** ✅ No log line lost in any of 7 production failure scenarios — proven
event by event, not inferred from counters. The same scenarios on the collector as it
was before this POC lose up to **95%** of the logs.

---

## 1. What adidas needs, and how this POC proves it

| adidas requirement | How the POC proves it |
|---|---|
| *"Cannot prove that no log lines are lost"* | Every log event the BW apps write carries a sequence number. A verifier reads **every object back from S3** (plus the DLQ) and checks that every sequence number of every app instance arrived. "0 lost" below means exactly that. |
| Identify *where* a line went missing (collector, shim, or S3 write) | No shim. Each collector exports, per node, `rows_read == rows_written` (+ DLQ) and `lines_read` per namespace; each S3 object has a `.manifest.json` with its record count; DLQ rows carry the failure reason. |
| Verifiable **at-least-once** | A crash replays only what never reached S3. Every record carries an `idempotency_key` for downstream de-duplication (OpenSearch `_id`). |
| Multiline-aware (Java stack traces) | 100% of stack-trace events arrived as **one** record with all their lines (each trace declares its line count; the verifier checks it). |
| Enriched for OpenSearch | `@timestamp` (CRI event time), `log_level`, numeric `severity`, `is_error`, `service_name`, `index_hint`, `cluster`, `node`, `namespace`, `pod`. 0 records with missing or invalid fields. |
| Portable, **zero pipeline changes** | The cluster, S3 endpoint, credentials and tuning differ from the original minikube setup. Only the ConfigMap and the image tag changed; the `.datapipeline` file is deployed byte-for-byte. |
| Node-local, single collector | One DaemonSet pod per node, reading only that node's `/var/log/pods`. No Fluentd, no conversion service. |

## 2. Test environment

| | |
|---|---|
| Cluster | k3d (k3s v1.30.6, **containerd 1.7**), **3 nodes**: 1 control-plane + 2 workers, all running workloads and a collector |
| Log rotation | kubelet `container-log-max-size=1Mi` (default 10Mi) → rotation every few seconds under load, to stress the hardest path |
| S3 | MinIO (S3 API) at `host.k3d.internal:9000`, bucket `adidas-logs` |
| Workload | BWCE-format emitter (`workload/emitter.py`): real BWCE console lines, Java stack traces, `[seq=n inst=id]` per event |
| Collector | `flogo-datapipeline` DaemonSet, the example pipeline unchanged, limits 1 CPU / 512 Mi |
| Host | Apple M1 Pro laptop (10 cores) running all 3 nodes, MinIO and the workload |

Reproduce: `poc/cluster.sh up && poc/run-all.sh` (see `poc/`).

## 3. Results

| # | Scenario | Result | Events emitted | Events delivered | Not delivered | Gaps (lost mid-stream) | Duplicates | Broken multiline | p95 latency | Collector CPU avg / peak (per node) | Collector peak RSS |
|---|----------|--------|---------------:|-----------------:|--------------:|-----------------------:|-----------:|-----------------:|------------:|------------------------------------:|-------------------:|
| s1 | Steady state — 60 BW-app pods in 12 namespaces, ~1,560 events/s | ✅ PASS | 720,000 | 720,000 | **0** | **0** | 0 | 0 | 20 s | 36m / 50m | 154 Mi |
| s2 | Collector crash — `kill -9` mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 0 | 0 | 29 s | 29m / 40m | 86 Mi |
| s2r | Collector rolling upgrade (SIGTERM) mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 0 | 0 | 29 s | 26m / 37m | 112 Mi |
| s3 | S3 outage, 120 s mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 13,332 ¹ | 0 | 138 s ² | 23m / 83m | 88 Mi |
| s4 | Log-rotation stress — 6 pods × ~1,000 events/s, rotation at 1 MiB | ✅ PASS | 900,000 | 900,000 | **0** | **0** | 0 | 0 | 2 s | 124m / 211m | 129 Mi |
| s5 | Short-lived pods — 40 Jobs, pods deleted 5 s after finishing | ✅ PASS | 120,000 | 120,000 | **0** | **0** | 0 | 0 | 30 s | 73m / 92m | 100 Mi |
| s6 | Worker node restart (kubelet, runtime and its pods) mid-stream | ✅ PASS | 240,000 | 274,021 ³ | **0** | **0** | 0 | 0 | 29 s | 28m / 43m | 109 Mi |
| c2 | **Control:** s2 on the collector *before* this POC | ❌ FAIL | 240,000 | 91,353 | **148,647** | **4,039** | 3,164 | 0 | 77 s | 12m / 47m | 75 Mi |
| c4 | **Control:** s4 on the collector *before* this POC | ❌ FAIL | 900,000 | 48,844 | **851,156** | 0 | 0 | 0 | 5 s | 8m / 121m | 54 Mi |

CPU is in millicores (`50m` = 0.05 core), sampled per collector pod every 10 s.

¹ The checkpoint during the outage failed after some partitions' objects were already
uploaded; the restarted run re-delivers that interval. All duplicates carry the same
`idempotency_key`. ² Includes the 120 s the object store was down. ³ Includes the
partial output of the emitters that died with the node and were restarted (new
instances); every instance's sequence is gap-free.

**In every passing scenario:** every emitted event was delivered (0 not delivered, 0
gaps), every stack trace arrived as one record, every record carried valid OpenSearch
fields, and every object's manifest count matched its content. Per node, the collector's
`rows_read` equalled `rows_written`.

## 4. Footprint and sizing

- **Throughput per node** (s1): ~520 events/s for **0.036 core** on average; under
  rotation stress (s4) ~2,000 events/s per node for 0.12–0.21 core. That's roughly
  **15,000 events/s per core**.
- **Memory:** 86–154 Mi peak RSS per collector (limit 512 Mi).
- **adidas volume:** 15 M records/day ≈ 175 events/s cluster-wide on average. Even at
  a 10× peak that is about 0.1 core for the whole estate. The collector is not a
  sizing concern; the limits (`150m`/`192Mi` request) are generous.
- **Latency:** a record reaches S3 within one checkpoint interval
  (`CHECKPOINT_SECONDS`, 30 s: p95 20–29 s), or sooner when an object fills.

## 5. Hidden issues this POC found — all fixed

Running the real use case under failure surfaced issues that unit tests and the earlier
2-node functional run had not. Each now has a regression test.

| # | Issue | Impact before the fix |
|---|---|---|
| 1 | **Log rotation not followed while running.** After kubelet renamed `0.log`, the collector kept the old file and never opened the new one. | Everything after a pod's first rotation was lost until the collector restarted (c4: 95% of logs). With the default 10 MiB rotation, a busy BW app rotates every few minutes. |
| 2 | **Checkpoint before upload.** Positions were committed every 5 s while records were only uploaded when an object rolled (60 s). | A collector crash, OOM-kill or node loss silently lost up to 60 s of logs per pod (c2: 4,039 lines). |
| 3 | **A failed S3 upload deleted the buffered object.** | An S3 outage during a roll lost that object's records. |
| 4 | **Stopped collector never restarted.** After a failed checkpoint (S3 down), the run ended; the pod stayed `Running`, passed liveness, and collected nothing. | Silent collection stop until someone restarted the pod — and real loss once kubelet deleted the un-read rotated files. Fixed with `restart.onFailure` (retry with backoff from the last checkpoint). |
| 5 | **Shutdown committed un-delivered positions.** `Close()` marked records handed to the pipeline as done. | Lines lost on every graceful stop. Now a SIGTERM *drains*: stop reading, upload, save the exact position (s2r: 0 lost, **0 duplicates** — down from 22,918). |
| 6 | **Rejected rows silently discarded.** `validate` with `onViolation: route` but no error sink dropped rejects into a null sink; a transform error discarded its whole batch (1,000 lines). | Invisible loss for any malformed line. Rejects and failed batches now go to the DLQ with the reason, counted in `rows_failed`. |
| 7 | Deleted pods: the remaining bytes of a removed pod's log were not read before closing. | The last lines of short-lived pods (Jobs, crash-looping pods) could be lost. |
| 8 | Other: duplicate re-reads during the rotation gap; rotated sibling skipped during double rotation; S3 objects could be overwritten when the key has no `{uuid}`; per-pod partition state grew with pod churn. | Duplicates, rare loss, slow memory growth. |
| 9 | Example: `@timestamp` was mapped to the log *message*; the collector image was built without the App Startup trigger (crash loop on start). | Wrong OpenSearch time field; the example didn't start. |
| 10 | Example: `service_name` stripped one segment too many for Job / CronJob / DaemonSet pods (`catalog-service-x7k2p` → `catalog`). Now uses Kubernetes' generated-suffix alphabet to tell `-<rs-hash>-<5>` from `-<5>`. | Wrong grouping in OpenSearch for non-Deployment workloads. |

## 6. Recommendations for adidas production

1. **Object layout and cost.** One object per pod per checkpoint is ideal for forensics but
   means many PUTs at estate scale (up to 6,000 pods × 2 per 30 s). Partition by
   `{namespace}` (pod/service stay fields inside each record) and use
   `CHECKPOINT_SECONDS: 60` to cut PUTs by about 100×. This is a ConfigMap/key change only.
2. **DLQ to S3.** Point the pipeline `dlq` at the bucket (e.g. `dlq/{node}/…`), so S3 holds
   *every* line — delivered or rejected — for audit.
3. **De-duplicate on `idempotency_key`** in OpenSearch (use it as the document `_id`): at-least-once
   duplicates after an S3 outage then become no-ops.
4. **Alerting:** alert on `flogo_pipeline_runs_total{status="failed"}` (restarts),
   `rows_read − rows_written − rows_failed > 0` sustained, and collector restarts.
5. **Kubelet rotation:** keep `containerLogMaxFiles ≥ 5`. The collector reads uncompressed
   rotated files it has not finished; kubelet's gzip-compressed older rotations are not
   read, so an S3 outage longer than ~4 rotations of a pod's log (at 10 MiB) could lose data.
   The restart backoff (≤ 30 s) keeps that window small.
6. **Shutdown budget:** keep `PIPELINE_SHUTDOWN_TIMEOUT` (25 s) below
   `terminationGracePeriodSeconds` (30 s), as in `k8s/40-collector-daemonset.yaml`.

## 7. Limits of this test

- A laptop cluster, not adidas' nodes: absolute throughput on real nodes will be higher.
  The relative findings (loss paths, guarantees) do not depend on hardware.
- MinIO stands in for AWS S3. Both use the S3 API, and the upload/retry path is the same SDK.
- The emitter reproduces the BWCE console format; the real BWCE EAR was validated earlier
  (see [REPORT.md](REPORT.md) §6). The collector pipeline is identical for both.

Raw evidence per scenario (verification JSON, collector metrics, CPU/memory samples,
fault timeline): [`poc/results/`](poc/results/).
