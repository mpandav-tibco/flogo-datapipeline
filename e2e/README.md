# Runtime end-to-end scenarios

These scripts run the **real `flogo-datapipeline` binary** against real
infrastructure (the `docker-compose.test.yml` stack), and check every result
against ground truth: row counts, distinct ids, sums, DLQ contents and HTTP
call counts. They complement the unit and live-integration tests by exercising
process-level behaviour: crashes, restarts, broker/object-store outages,
SIGTERM, and memory and temp-disk footprint.

```sh
make test-integration-up     # Postgres, Kafka, MinIO, Redis, … (once)
e2e/run.sh                   # build binary + harness, start mock API, seed MinIO objects (e2e/seed_objects.sh, once), run all
e2e/run.sh s2 s3b            # run selected scenarios
make test-integration-down   # tear the stack down
```

Results are appended to `$E2E_WORK/results.md` (default `/tmp/dp-e2e`), with
per-run logs, RSS samples and temp-disk samples in `$E2E_WORK/logs`.

| Script | Scenario | Checks |
|---|---|---|
| `s1_postgres_bulk.sh` | CSV 1M rows → filter/map → Postgres, `bulkMode` auto (COPY) vs insert | identical rows/sum/md5; time; peak RSS |
| `s1b_postgres_edges.sh` | CHECK-constraint violations; an updatable view as target | exact rows written vs DLQ; COPY→INSERT fallback |
| `s2_kafka_crash.sh` | Kafka 300k msgs + 20 undecodable → Postgres, Redis checkpoints, `kill -9` mid-stream, restart, idle restart | no missing ids; duplicates ≤ checkpoint interval; no redelivery |
| `s3_objstore_stream.sh` | MinIO CSV/TSV/JSONL objects → Postgres, streaming vs `download:true` | exact rows; time; peak RSS; peak temp disk |
| `s3b_objstore_restart.sh` | MinIO restarted while an object is being read | run completes with every row (stream resumption) |
| `s4_lookup_cache.sh` | lookup enrichment vs the mock API: batch call counts; streaming staleness when keys appear upstream | HTTP calls; null-enrichment counts; stale rows |
| `s5_rest_sink.sh` | REST sink vs mock returning 429 + `Retry-After` | every row exactly once; concurrency speed-up |
| `s6_podlog.sh` | CRI log lines written in several chunks while tailing; SIGTERM | records exact/whole; saved offset = file size |
| `s7_api_graceful.sh` | management API during a streaming run; SIGTERM mid-stream; restart | API 200; `cancelled`; no missing ids |

`harness/` holds the generators and the mock HTTP service
(`harness gen|produce|mock`). Endpoints, credentials and container names
default to the compose stack and can be overridden through the variables at
the top of `lib.sh`.
