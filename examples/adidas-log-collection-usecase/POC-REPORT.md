# POC Report — adidas BW6 log collection, verified on a 3-node Kubernetes cluster

**Date:** 2026-10-24 · **Branch:** `poc/adidas-corner-cases`
**Result:** ✅ No log line lost in any of 12 production failure and scale scenarios — proven
event by event, not inferred from counters — up to **400 containers per node, 3.6 M events
in 6.5 min, through a 3-minute S3 outage during which a quarter of the pods were deleted**.
The same scenarios on the collector as it was before this POC lose up to **95%** of the logs.

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
| s7 | S3 outage (90 s) while kubelet keeps rotating with **unchanged** settings (5 files) — ~3 rotations per pod while behind, older ones gzip-compressed before being read | ✅ PASS | 180,000 | 180,000 | **0** | **0** | 4,744 ¹ | 0 | 83 s ² | 22m / 61m | 55 Mi |
| s8 | **Scale** — 400 containers per node (1,200 total, 24 namespaces), ~3,200 events/s per node, rotation at 128 KiB (~every 70 s, ~5 per pod) + gzip, stdout/stderr interleaved, 25% of pods deleted 5 s after finishing ⁴ | ✅ PASS | 3,600,000 | 3,600,000 | **0** | **0** | 0 | 0 | 4 s | 139m / 311m | 127 Mi |
| s9 | **Scale + S3 outage (180 s)** — s8 with S3 down while the pods finish and a quarter of them are **deleted**; durable spool on | ✅ PASS | 3,600,000 | 3,600,000 | **0** | **0** | **0** | 0 | 171 s ² | 148m / 339m | 152 Mi |
| s10 | **Scale + S3 outage (180 s) + collector `kill -9`** while its spool holds the outage backlog (1,368 files) | ✅ PASS | 3,600,000 | 3,600,000 | **0** | **0** | 3,200 ¹ | 0 | 183 s ² | 146m / 349m | 127 Mi |
| s11 | **Mixed node** — 300 BW pods per node, 1 in 4 with an istio-proxy-style **sidecar** (lines never match the BW timestamp), every 50th event a **40 KB payload** (written as CRI partial chunks, like containerd) | ✅ PASS | 2,250,000 | 2,250,000 | **0** | **0** | 0 | 0 | 4 s | 130m / 220m | 113 Mi |
| c2 | **Control:** s2 on the collector *before* this POC | ❌ FAIL | 240,000 | 91,353 | **148,647** | **4,039** | 3,164 | 0 | 77 s | 12m / 47m | 75 Mi |
| c4 | **Control:** s4 on the collector *before* this POC | ❌ FAIL | 900,000 | 48,844 | **851,156** | 0 | 0 | 0 | 5 s | 8m / 121m | 54 Mi |
| c7 | **Control:** s7 without reading compressed rotations | ❌ FAIL | 180,000 | 152,231 | **27,769** | **27,769** | 0 | 0 | 44 s | 12m / 45m | 57 Mi |
| c11 | **Control:** s11 on the collector before the corner-case review ⁵ | ❌ FAIL | 2,250,000 | 1,800,675 | **449,325** | **449,325** | 0 | 675 | 5 s | 81m / 218m | 116 Mi |
| c9 | **Control:** s9 with the spool disabled (`SPOOL_DIR: none`) | ❌ FAIL | 3,600,000 | 3,383,108 | **216,892** | 124 | 37,783 | 0 | 185 s | 77m / 495m | 338 Mi |

CPU is in millicores (`50m` = 0.05 core), sampled per collector pod every 10 s.

¹ The checkpoint during an outage failed after some partitions' objects were already
uploaded, or (s10) the killed collector had rows after its last checkpoint; the restarted
run re-delivers that interval. All duplicates carry the same
`idempotency_key`. ² Includes the 120 s the object store was down. ³ Includes the
partial output of the emitters that died with the node and were restarted (new
instances); every instance's sequence is gap-free. ⁴ s8/s9/c9 use a synthetic node writer
(`poc/synthlog.py`, one per node) that produces exactly what kubelet + containerd write
(CRI files, rename-rotation, gzip of older rotations, `containerLogMaxFiles=5`, pod
directories removed on deletion), because 1,200 real pods do not fit on a laptop. The
collector, its configuration and the verifier are unchanged.

⁵ Not lost bytes: every sidecar line reached S3, but glued into 1,000-line records under
the node-wide BW `startPattern` — 675 such records, each one OpenSearch document holding a
thousand unrelated access-log lines (the verifier counts only one event per record).

**s9 vs c9 — why the spool matters.** Without the spool, an S3 outage stops checkpoints;
the collector retries from the last checkpoint, and by then the 301 pods deleted during the
outage (96 + 109 + 96 on the three nodes) have no log files left: exactly those 301 app
instances lost their last ~720 events each (216,892 lines), and the replays produced 37,783
duplicates and 2.2× the memory. With the spool, checkpoints keep advancing to local disk
during the outage (~204 MB per node at the peak), the spool was empty again at the next sample
after S3 came back, and the result was 0 lost and 0 duplicates.

**In every passing scenario:** every emitted event was delivered (0 not delivered, 0
gaps), every stack trace arrived as one record, every record carried valid OpenSearch
fields, and every object's manifest count matched its content. Per node, the collector's
`rows_read` equalled `rows_written`.

## 4. Footprint and sizing

- **Throughput per node** (s1): ~520 events/s for **0.036 core** on average; under
  rotation stress (s4) ~2,000 events/s per node for 0.12–0.21 core; at scale (s8) **~3,200
  events/s from 400 containers per node for 0.14 core average (0.31 peak) and 127 Mi**.
  That's roughly **15,000–20,000 events/s per core**.
- **Memory:** 86–154 Mi peak RSS per collector (limit 512 Mi), including 400 tailed
  containers per node and a 3-minute S3 outage. Memory does not grow with the outage
  length — the backlog is on disk (spool), not in RAM.
- **Spool disk:** ingest rate × outage. s9: ~204 MB per node for 3 min at 3,200 events/s. The
  default cap (5 GiB) holds ~75 min of that rate; at adidas' average (175 events/s
  cluster-wide) it holds days.
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
| 11 | **Compressed rotations skipped.** kubelet gzips every rotated log except the newest; the collector only read uncompressed files. | While behind (S3 outage) or down, a pod could rotate only ~1–2 times before lines were lost (c7: 15%), independent of `containerLogMaxFiles`. Now `.gz` rotations are read too — tolerance is everything kubelet keeps (s7: 0 lost). |
| 12 | **Pods deleted during an S3 outage.** A failed checkpoint restarts the run from the last checkpoint; a pod deleted meanwhile has no files left to replay. | c9: 6% of all lines lost (every deleted pod's tail), 37,783 duplicates. Fixed by the durable node spool: checkpoints advance to local disk while S3 is down (s9: 0 lost, 0 duplicates). |
| 13 | **OOM at high container density.** Each tailed file had a 1 MiB read buffer. | At 400 containers per node the collector was OOM-killed within 1 s of starting (crash loop, no collection). Now 32 KiB per file: 127 Mi at 400 containers. |
| 14 | stdout and stderr of one container were stitched as one stream. | A stack trace on stderr interleaved with stdout lines was split or merged with unrelated lines, and a crash could commit past a still-buffered trace. Now stitched per stream; the committed offset never passes a buffered line. |
| 15 | `fromBeginning: false` also skipped the start of pods created **after** the collector started. | The first lines (often the startup errors) of every new pod could be lost. Now only files present at the very first start are skipped. |
| 16 | Position registry kept every pod ever seen. | Slow growth of memory and position-file size with pod churn (thousands of pods per day). Now entries of removed pods are dropped. |
| 17 | Collector could be evicted first under node pressure. | Pods are evicted under memory/disk pressure in priority order; a log collector should be the last to go. Now `priorityClassName: system-node-critical`. |
| 18 | **Other containers' lines glued together.** The collector reads every container on the node, but the multiline `startPattern` is BW's. Sidecars (istio-proxy), nginx, JSON loggers never match it, so all their lines were stitched into 1,000-line records. | c11: 20% of the node's events unusable as records. Now a line continues a record only if that record began with a start line (or it matches `continuePattern`, for header-less Java traces): s11 0 glued. |
| 19 | **Unbounded memory for large log lines.** A payload the runtime splits into 16 KiB partial chunks was reassembled without limit, and records were bounded only in count (1,024 queued + 1,000 per batch). | One app logging multi-MB payloads (common for BW "log the message") could OOM-kill the collector — which then replays the same lines and crash-loops: no collection on that node. Now records are cut at `maxLineBytes` (1 MiB, split, never dropped) and the queue/batch are bounded in bytes (16/8 MiB). |
| 20 | Committed position could pass the first partial chunk of a still-buffered record (when another record of the same stream completed at the same moment). | A crash at that moment lost that record. Now tracked exactly. |
| 21 | **Spool could cause node disk pressure.** | A long outage filling the 5 GiB spool on a nearly full node would trigger kubelet eviction of the BW apps themselves. Now the spool stops growing below `SPOOL_MIN_FREE_PERCENT` (20%) free, above kubelet's thresholds (10% nodefs, 15% imagefs). |
| 22 | **S3 credentials on AWS (IRSA).** Blanking the key env names falls back to the defaults, and an unset credential env var was a hard error; the DaemonSet required the key secret. | Day-1 blocker on EKS with IRSA: no upload at all. Now unset credential env vars use the AWS default chain, and the secret refs are optional. |
| 23 | **Corrupt state files** (position registry, checkpoint, spool metadata) after a node crash or filesystem error. | The run failed at every start: permanent collection outage on the node. Now they are moved aside (`*.corrupt-<ts>`) and collection resumes (re-read from the start: duplicates, no loss). |

## 6. Recommendations for adidas production

1. **Object layout and cost.** The default key (`S3_KEY`) writes one object per pod per
   checkpoint: ideal for forensics, but many PUTs at estate scale (up to 6,000 pods × 2 per
   30 s). Setting `S3_KEY` to `{namespace}/{YYYY}/{MM}/{DD}/{HH}/{node}-{mm}-{uuid}.ndjson`
   (pod and service stay fields inside each record) and `CHECKPOINT_SECONDS: 60` cuts PUTs
   by about 100×. s8/s9 ran with exactly this layout (8,424 objects for 3.6 M events). Both
   are ConfigMap changes only.
2. **DLQ to S3 — configured.** Rejected lines go to `S3_DLQ_KEY` (`_dlq/{node}/…`) in the same
   bucket, through their own spool, so S3 holds *every* line — delivered or rejected — for
   audit. The verifier reads it back.
3. **De-duplicate on `idempotency_key`** in OpenSearch (use it as the document `_id`): at-least-once
   duplicates after an S3 outage then become no-ops.
4. **Alerting:** alert on `flogo_pipeline_runs_total{status="failed"}` (restarts),
   `rows_read − rows_written − rows_failed > 0` sustained, and collector restarts.
5. **Kubelet rotation — no infra change needed.** The collector follows every rotation and
   reads every rotated file kubelet still keeps, compressed or not. With kubelet's defaults
   (`containerLogMaxSize=10Mi`, `containerLogMaxFiles=5`) a container can log ~40 MiB while
   the collector cannot ship (S3 outage) before kubelet deletes data — e.g. ~45 min for a
   chatty BW app at 15 KB/s. Only an outage longer than that, for that container, can lose
   lines; the restart backoff (≤ 30 s) resumes shipping as soon as S3 is back.
6. **Durable node spool — implemented, keep it on.** The collector keeps reading during an
   outage into a size-capped spool on the node (`SPOOL_DIR`, `hostPath`, default cap
   `SPOOL_MAX_BYTES` 5 GiB): objects are fsynced and sealed there at each checkpoint, and a
   background uploader ships them to S3 with retry, including after a restart. Outage
   tolerance = spool size ÷ node log rate, independent of kubelet's retention and of pod
   deletion (s9 vs c9). Alert on `flogo_objstore_spool_bytes` > 50% of the cap, and size
   the node's disk for the cap. The DLQ has its own spool (`SPOOL_DLQ_DIR`).
7. **Before the POC on adidas' cluster — confirm:** Kubernetes/runtime (containerd or CRI-O;
   `/var/log/pods` must hold real files, not dockershim symlinks); Pod Security Admission
   level of the `logging` namespace (hostPath + root need `privileged`; OpenShift: an SCC);
   credentials (IRSA: ServiceAccount annotated with the role, see the DaemonSet comment; set
   `S3_ENDPOINT` to the regional S3 URL and `S3_FORCE_PATH_STYLE: "false"`); node disk size
   vs `SPOOL_MAX_BYTES`; other log formats on the nodes (sidecars are handled, see 18).
8. **`FROM_BEGINNING: "true"`** (example default) backfills everything kubelet still keeps
   on first install — no loss at cut-over from Fluentd, but expect a burst and duplicates of
   what Fluentd already shipped (de-duplicate on `idempotency_key` or set `"false"`).
9. **Shutdown budget:** keep `PIPELINE_SHUTDOWN_TIMEOUT` (25 s) below
   `terminationGracePeriodSeconds` (30 s), as in `k8s/40-collector-daemonset.yaml`.

## 7. Production-readiness review — every way a log line can be lost

Each row is a way a line can be lost between the BW app's stdout and S3, with its status
and the evidence. "Proven" means a live scenario with event-by-event verification.

| # | Case | Status | Evidence |
|---|------|--------|----------|
| 1 | Collector crash / OOM-kill / `kill -9` | ✅ Proven | s2 (0 lost); s10: killed during an S3 outage with 672 sealed objects (270 MB) in the spool — the restarted collector uploaded them and replayed only the unsealed rest (0 lost) |
| 2 | Rolling upgrade / SIGTERM / node drain | ✅ Proven | s2r (0 lost, 0 duplicates): drain, upload, exact final position |
| 3 | S3 outage — short | ✅ Proven | s3, s7 |
| 4 | S3 outage — longer than kubelet keeps the logs | ✅ Proven | s9 (spool); tolerance = `SPOOL_MAX_BYTES` ÷ node rate, alert on `flogo_objstore_spool_bytes` |
| 5 | Pod deleted while S3 is down (Jobs, scale-down, redeploys) | ✅ Proven | s9 vs c9 (301 deleted pods: 0 vs 216,892 lines lost) |
| 6 | Pod deleted shortly after finishing (normal case) | ✅ Proven | s5, s8 (25% churn); remaining bytes read before handles close |
| 7 | Live rotation (rename + reopen), incl. very fast rotation | ✅ Proven | s4 (1 MiB rotation at ~1,000 events/s/pod), s8 |
| 8 | Rotated files gzip-compressed before being read | ✅ Proven | s7, s8/s9 (~5 rotations per pod, oldest deleted by retention) |
| 9 | Node restart (kubelet, runtime, all pods) | ✅ Proven | s6 |
| 10 | Many containers per node | ✅ Proven | s8/s9: 400 containers/node, 127–152 Mi (found and fixed an OOM, finding 13) |
| 11 | Multiline Java stack traces | ✅ Proven | All scenarios: 0 broken multiline; each trace verified line by line |
| 12 | stdout/stderr interleaved in one CRI file | ✅ Proven | s8/s9 (every 10th event on stderr, traces on both); unit test for position safety |
| 13 | CRI partial lines (`P`, > 16 KiB lines) | ✅ Unit-tested | Reassembled; `maxLineBytes` caps a line that never ends |
| 14 | Malformed / rejected lines | ✅ Proven | Routed to the S3 DLQ with the reason, never dropped; verifier counts DLQ records |
| 15 | Collector starts after pods (first install, `fromBeginning: false`) | ✅ Unit-tested | Only pre-existing files skip their history; every later pod is read from line 1 |
| 16 | Collector evicted under node memory/disk pressure | ✅ Configured | `priorityClassName: system-node-critical`; 512 Mi limit vs 152 Mi peak |
| 17 | Pod churn over weeks (state growth) | ✅ Unit-tested | Registry and in-memory maps drop removed pods |
| 18 | Spool full (outage longer than the cap) | ⚠️ By design | Checkpoints stop (back-pressure) and kubelet retention applies again (case 3/8 behaviour); alert before it happens |
| 19 | Permanent S3 error (expired credentials, bucket policy) | ⚠️ By design | Treated like an outage: retried forever, spool absorbs it; `flogo_objstore_spool_upload_failures` rising + spool growing is the alert |
| 20 | Collector down for longer than kubelet's retention (crash loop, unschedulable) | ⚠️ Operational | Restart policy and priority class make this unlikely; alert on DaemonSet `numberUnavailable > 0` |
| 21 | Node lost permanently (disk gone) before upload | ❌ Out of scope | Lines not yet uploaded (≤ one checkpoint interval, or the spool backlog during an outage) are lost — as with Fluentd or any node-local collector, because the source log files are on the same disk |
| 22 | Duplicates | ℹ️ At-least-once | 0 in s8/s9; possible after a crash or a failed checkpoint (c9); `idempotency_key` makes them idempotent in OpenSearch |
| 23 | Sidecars / non-BW containers on the same node | ✅ Proven | s11 vs c11: one record per line for containers that never log a BW start line |
| 24 | Large payload log lines (> 16 KiB, CRI partial chunks; multi-MB) | ✅ Proven + unit-tested | s11: every 40 KB payload whole; > `maxLineBytes` split, never dropped; memory bounded in bytes |
| 25 | Spool filling the node disk | ✅ Unit-tested | Stops at `SPOOL_MIN_FREE_PERCENT`; flushes then back-pressure (case 18) |
| 26 | Corrupt registry / checkpoint / spool metadata | ✅ Unit-tested | Moved aside, collection continues |
| 27 | AWS IRSA / instance profile credentials | ✅ Unit-tested | Default chain when no key env var is set |

**Known limits (by design, no loss):** a stack trace that straddles a kubelet rotation is
split in two records; the `idempotency_key` of lines in a file's first 256 bytes can differ
after a restart (those few replays are not de-duplicated by `_id`); a line continues a record
only within `flushIdleMs` (500 ms) of the previous line.

**Alerting set for production:** DaemonSet unavailable > 0 for 5 min;
`flogo_pipeline_runs_total{status="failed"}` increasing; `flogo_objstore_spool_bytes` >
50% of `SPOOL_MAX_BYTES`; `flogo_objstore_spool_upload_failures` increasing for 10 min;
`rows_read − rows_written − rows_failed` > 0 for 10 min.

## 8. Limits of this test

- s8/s9/c9 simulate 400 containers per node with a file writer (see ⁴), not real pods;
  the files, rotation and deletion behaviour are what kubelet produces (verified against k3s).
- A laptop cluster, not adidas' nodes: absolute throughput on real nodes will be higher.
  The relative findings (loss paths, guarantees) do not depend on hardware.
- MinIO stands in for AWS S3. Both use the S3 API, and the upload/retry path is the same SDK.
- The emitter reproduces the BWCE console format; the real BWCE EAR was validated earlier
  (see [REPORT.md](REPORT.md) §6). The collector pipeline is identical for both.

Raw evidence per scenario (verification JSON, collector metrics, CPU/memory samples,
fault timeline): [`poc/results/`](poc/results/).
