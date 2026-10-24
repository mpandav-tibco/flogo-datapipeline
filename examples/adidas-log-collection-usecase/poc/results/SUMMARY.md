| # | Scenario | Result | Events emitted | Events delivered | Not delivered | Gaps (lost mid-stream) | Duplicates | Broken multiline | DLQ | p95 latency | Collector CPU avg / peak (per node) | Collector peak RSS |
|---|----------|--------|---------------:|-----------------:|--------------:|-----------------------:|-----------:|-----------------:|----:|------------:|------------------------------------:|-------------------:|
| s1 | Steady state — 60 BW-app pods, 12 namespaces | ✅ PASS | 720,000 | 720,000 | **0** | **0** | 0 | 0 | 0 | 20s | 36m / 50m | 154 Mi |
| s2 | Collector crash (SIGKILL) mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 0 | 0 | 0 | 29s | 29m / 40m | 86 Mi |
| s2r | Collector rolling upgrade (SIGTERM) mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 0 | 0 | 0 | 29s | 26m / 37m | 112 Mi |
| s3 | S3 outage (120 s) mid-stream | ✅ PASS | 240,000 | 240,000 | **0** | **0** | 13,332 | 0 | 0 | 138s | 23m / 83m | 88 Mi |
| s4 | Log-rotation stress — ~1,000 events/s/pod, rotation at 1 MiB | ✅ PASS | 900,000 | 900,000 | **0** | **0** | 0 | 0 | 0 | 2s | 124m / 211m | 129 Mi |
| s5 | Short-lived pods — 40 Jobs deleted 5 s after finishing | ✅ PASS | 120,000 | 120,000 | **0** | **0** | 0 | 0 | 0 | 30s | 73m / 92m | 100 Mi |
| s6 | Worker node restart mid-stream | ✅ PASS | 240,000 | 274,021 | **0** | **0** | 0 | 0 | 0 | 29s | 28m / 43m | 109 Mi |
| s7 | S3 outage while kubelet rotates + gzips unread logs (~3 rotations/pod) | ✅ PASS | 180,000 | 180,000 | **0** | **0** | 4,744 | 0 | 0 | 83s | 22m / 61m | 55 Mi |
| c2 | CONTROL: collector crash on the pre-fix collector | ❌ FAIL | 240,000 | 91,353 | **148,647** | **4,039** | 3,164 | 0 | 0 | 77s | 12m / 47m | 75 Mi |
| c4 | CONTROL: rotation stress on the pre-fix collector | ❌ FAIL | 900,000 | 48,844 | **851,156** | **0** | 0 | 0 | 0 | 5s | 8m / 121m | 54 Mi |
| c7 | CONTROL: s7 without compressed-rotation support | ❌ FAIL | 180,000 | 152,231 | **27,769** | **27,769** | 0 | 0 | 0 | 44s | 12m / 45m | 57 Mi |
