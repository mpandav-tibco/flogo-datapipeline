# Example 20 — Kubernetes Pod Log Collector (`podlog` source)

> **Status: 🚧 Work in progress (design + partial scaffold).** Unlike examples
> 01–19 and 22, this one is **not yet runnable** — the `podlog` source is being
> built. This directory documents the target design; the engineering spec lives
> in [IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md).

## What it will do

Replace a Fluentd-per-namespace log-collection stack with a single
`flogo-datapipeline` per node that tails container (CRI/containerd) pod logs and
lands them in S3 as JSON — with **end-to-end reconciliation** so missing lines are
detectable, not silent.

The motivating problem is **trust, not throughput**: the customer's ~15M
records/day is trivial for the engine, but today they cannot tell whether a log
line missing from S3 was dropped at the tailer, the buffer, or the S3 write. The
`podlog` source is designed around verifiable delivery:

- **CRI log parsing** ([cri.go](../../source/podlog/cri.go)) — parse the
  containerd/CRI-O log line format (timestamp, stream, partial/full flag, message).
- **Position tracking** ([positions.go](../../source/podlog/positions.go)) —
  persist per-file read offsets so a restart resumes exactly where it left off
  (no re-read, no gap), surviving pod-log rotation.
- **Rotation-aware tailing** ([tailer.go](../../source/podlog/tailer.go)) —
  follow files across rotation/truncation without losing the cursor.

## Current state

| Piece | Status |
|---|---|
| CRI parsing / position store / tailer scaffolding | partial (`source/podlog/`) |
| `source.Register("podlog", …)` factory | not wired yet |
| Blank import in `cmd/flogo-datapipeline/main.go` | not added yet |
| `.datapipeline` example + sample data | not authored yet |

Because the source is not registered, there is intentionally **no
`.datapipeline` file to run here yet**. Follow
[IMPLEMENTATION-PLAN.md](IMPLEMENTATION-PLAN.md) Section 8 (Work Breakdown) for
the remaining tasks. This README will be replaced with a runnable walkthrough
once the source is registered and an example pipeline is added.
