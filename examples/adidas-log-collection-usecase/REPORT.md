# Test Report — adidas Container Log Collection with Flogo Data Pipeline

**Date:** 2026-07-17
**Environment:** local minikube (Docker driver, Apple Silicon / arm64)
**Result:** ✅ End-to-end verified — BW6/BWCE console logs collected from every
node and delivered as enriched, partitioned NDJSON to S3 (MinIO), with no
intermediate service.

---

## 1. Objective

Prove that a single **Flogo Data Pipeline** collector can replace the customer's
**Fluentd DaemonSet + custom S3-pusher** and move BW6/BWCE app console logs to an
S3 bucket, removing the intermediate hop that feeds the corruption-prone
OpenSearch index.

## 2. Environment under test

| Component | Detail |
|---|---|
| Cluster | minikube profile `adidas-logs`, **2 nodes**, `containerd 2.2.1`, k8s `v1.35.1` |
| Nodes | `adidas-logs` (control-plane), `adidas-logs-m02` (worker) |
| Collector | `flogo-datapipeline:arm64-local` as a **DaemonSet** (1 pod/node) |
| S3 target | host **MinIO** (`miniobench`) via `host.minikube.internal:9000`, bucket `adidas-logs` |
| Workload | 6 namespaces × 2 replicas = **12 BW-app pods** emitting BWCE console logs |

The engine image was built natively for `linux/arm64` (CGO) inside a `golang:1.25`
container; the private `flogo-general` module was resolved from the repo's
`go.work` local `replace` (no credentials needed).

## 3. What was deployed

```
namespaces:  logging + team-{alpha,beta,gamma,delta,epsilon,zeta}
workload:    6 Deployments (2 replicas) → 12 pods, spread across both nodes
collector:   DaemonSet adidas-log-collector → 2 pods (one per node), both Running
pipeline:    podlog → map → parse → validate → s3-sink (+ DLQ), via ConfigMap
secret:      adidas-minio-creds (MinIO access/secret; only env-var NAMES in the pipeline)
```

Both collector pods reached `Running 1/1`; the `${NODE_NAME}` substitution
resolved per pod (`node=adidas-logs` and `node=adidas-logs-m02`).

## 4. Results

### 4.1 Delivery to S3 — verified

- Objects landed under the designed key layout, **both nodes, all 6 namespaces**:

  ```
  team-alpha/order-service-<pod>/2026/07/17/adidas-logs-1109-<uuid>-0000.ndjson       (717 KiB)
  team-beta/inventory-service-<pod>/2026/07/17/adidas-logs-m02-1109-<uuid>-0000.ndjson (594 KiB)
  team-gamma/customer-service-<pod>/2026/07/17/adidas-logs-1109-<uuid>-0000.ndjson     (834 KiB)
  ... (+ a companion .manifest.json per object)
  ```

- **~96 data objects** written in the first ~8 minutes, evenly distributed
  (16 data objects per namespace × 6), each rolling at 60 s / 4 MiB.
- **~98,778 log records** processed and shipped
  (collector `adidas-logs` = 49,328 rows; `adidas-logs-m02` = 49,450 rows),
  ≈ **200 records/s per node while CPU-throttled**.

### 4.2 Record quality — verified

Each line is preprocessed into an **OpenSearch-ready** document — canonical
`@timestamp`, normalized `log_level` + numeric `severity`, `is_error`,
`service_name` (pod-hash stripped), and a daily `index_hint`:

```json
{"@timestamp":"2026-07-17T12:05:11.618997508Z","log_level":"DEBUG","severity":1,"is_error":false,
 "logger":"com.tibco.bw.NotificationService.InventoryLookup","thread":"bwEngThread:In-Memory Process Worker-3",
 "message":"Invoking process InventoryLookup for reqId=d970dce3","service_name":"notification-service",
 "namespace":"team-zeta","pod":"notification-service-...","node":"adidas-logs-m02","container":"bwapp",
 "index_hint":"bwce-logs-2026-07-17","event_time":"2026-07-17T12:05:11.618",
 "cluster":"adidas-logs-demo","environment":"production","collector":"flogo-datapipeline",
 "raw_log":"2026-07-17T12:05:11,618 DEBUG [bwEngThread:...Worker-3] com.tibco.bw...","idempotency_key":"1d43212c..."}
```

`decode` yields the numeric severity (DEBUG=1 … ERROR=4, FATAL=5); `regex.replace`
strips the pod hash for `service_name`; `@timestamp` is the CRI event time in
ISO8601 UTC (the OpenSearch time field). No downstream parsing required.

### 4.3 Multiline stitching — verified

An ERROR + Java stack trace is collapsed into **one** record (newlines embedded in
`log_text`/`message`), so exceptions are never split across records:

```
"log_level":"ERROR",
"log_text":"Unhandled fault in process instance 9916\njava.lang.NullPointerException: Cannot read field \"total\" ...\n\tat com.tibco.bw.Rest_Sample.OrderProcess.computeTotal(OrderProcess.java:77)\n\tat com.tibco.bw.core.runtime.ActivityFacade.execute(ActivityFacade.java:212)\n\tat java.base/java.lang.Thread.run(Thread.java:840)"
```

### 4.4 Reliability — verified

- Checkpoints every 5 s (`checkpoint saved — N rows processed`), advancing the
  crash-safe **position registry** → at-least-once delivery.
- Each record carries a deterministic `idempotency_key` for downstream dedupe.
- `validate` routes null/malformed lines to a node-local **DLQ** rather than
  dropping them silently.

## 5. Old → new, proven

| Old | New (this run) | Status |
|---|---|---|
| Fluentd DaemonSet | `flogo-datapipeline` DaemonSet, 1 pod/node | ✅ both nodes collecting |
| Multiline concat plugin | `podlog` multiline stitching | ✅ stack traces intact |
| Background format converter | in-pipeline `parse` + `map` (OpenSearch prep) | ✅ `@timestamp`/`severity`/`service_name` |
| Custom S3 pusher | `s3-sink` (rolling, partitioned) | ✅ 96 objects to MinIO |
| Lossy drops | `validate` → DLQ + checkpoint | ✅ at-least-once |
| Genuine BWCE app (proof) | real licensed EAR pod via Rosetta | ✅ logs collected in `team-bwce/` |
| Hardcoded per-env config | app properties (`$property[...]`) via ConfigMap | ✅ change env with no pipeline edit |

## 6. Real BWCE app — running in-cluster with a license (verified)

The genuine BWCE 6.12 app (`Rest_Sample_Multiple_Endpoints` EAR) runs as a pod in
the cluster, and its real console logs are collected end to end:

1. **Architecture (solved):** BWCE 6.12 is x86-64 only. The image is built amd64
   (from `mpandav/bwce-base-6.12_linux:base` + the EAR), then repackaged with an
   **arm64 manifest / amd64 content** (`docker buildx --platform linux/arm64` +
   `FROM --platform=linux/amd64`) so containerd on the arm64 nodes accepts it and
   runs it via **Rosetta** (nodes expose a `rosetta` binfmt handler).
2. **Licensing (solved):** the TIBCO `.bin` license, mounted at the BW6 default
   path **`/home/bwce/license`** via a Secret, activates in-cluster — boot log:
   `Entitlement to TIBCO_BW_CCS ... confirmed through Sun Jan 03 2027` +
   `Post-license validation event. Success: true`. (Non-default path → env
   `LICENSE_SOURCE` / `TIB_ACTIVATION`; structured logs → `BW_LOG_FORMAT=JSON`.)
3. **Collected + parsed:** the app's logs land in S3 under `team-bwce/`; the parse
   regex (handling the real `T`/comma BWCE format) structured **616/618** records
   in a sampled object — the 2 misses are pre-boot `start.sh` echoes, correctly
   kept via `onError:keep`. Deploy with `scripts/deploy-real-bwce.sh`.

The `workload/emitter.py` (arm64-native, license-free) provides the same console
format at 6-namespace scale. **The collector pipeline is identical for both.**

## 7. Scaling outlook

The collector is a DaemonSet reading only node-local logs, so throughput scales
linearly with nodes and needs no coordinator. This 2-node laptop cluster sustained
~200 rec/s/node **while CPU-limited**; the customer's 3 real nodes are far larger,
putting the **~25M records/day** target well within reach. S3 remains the durable
source of truth, so any index (OpenSearch/Athena/Snowflake) can be rebuilt from it —
directly addressing the OpenSearch index-corruption failure mode.

## 8. Reproduce

```sh
cd examples/adidas-log-collection-usecase
bash scripts/deploy.sh     # build + deploy everything
bash scripts/verify.sh     # collector pods, sample logs, S3 objects + a decoded record
bash scripts/teardown.sh   # cleanup (FULL=1 also deletes the cluster)
```
