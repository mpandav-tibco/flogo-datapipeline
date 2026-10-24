# Kubernetes Pod Log source — `podlog`

Tails container logs from the node filesystem (CRI / containerd layout under
`/var/log/pods`), reassembles multiline stack traces, filters by namespace, and resumes
from a crash-safe position registry. Designed to run as a **DaemonSet** (one collector
per node). Implemented in [`source/podlog/source.go`](../../source/podlog/source.go);
registered as `podlog`.

- **Reads:** CRI/containerd pod logs on the node
- **Partitionable:** no (a per-node multi-file collector)
- **Streaming:** unbounded (tails forever) unless `bounded: true`
- **Checkpoint / resume:** file position registry (`positionFile`); at-least-once delivery

## Emitted schema

Every record has these fixed columns (all `string`):

| Column | Description |
|---|---|
| `namespace` | Kubernetes namespace. |
| `pod` | Pod name. |
| `container` | Container name. |
| `uid` | Pod UID. |
| `stream` | `stdout` or `stderr`. |
| `log_time` | RFC3339Nano timestamp from the CRI envelope. |
| `message` | The log line — a stitched multiline record when multiline is enabled. |
| `node` | Node name stamped from `node`. |
| `idempotency_key` | Stable hash (`node`+file+fingerprint+offset) for downstream dedup. |

Use a [parse](../transforms/parse.md) transform to structure `message` into typed columns.

## Configuration

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `root` | string | `/var/log/pods` | — | CRI pod-log root directory. |
| `node` | string | — | — | Node name stamped on every record (for multi-node reconciliation). |
| `positionFile` | string | — | — | Crash-safe byte-offset registry file. Strongly recommended in production. |
| `fromBeginning` | bool | `false` | — | `true`: read every existing file from offset 0. `false`: on the first start (no saved position), skip what is already on disk and tail from EOF. Files that appear later (new pods, restarted containers) are always read from offset 0. |
| `pollMs` | int | `500` | — | Idle poll interval (ms). |
| `bounded` | bool | `false` | — | Test/drain mode: return EOF once the tree is drained and idle. |
| `includeNamespaces` | `[]string` | all | — | If non-empty, only these namespaces are tailed. |
| `excludeNamespaces` | `[]string` | none | — | Namespaces to skip (e.g. `["kube-system"]`). |
| `maxLineBytes` | int | `1048576` | — | Cap on one record: a physical line, a line the runtime split into CRI partial chunks (e.g. a multi-MB payload log), or a stitched multiline record. Longer ones are split into consecutive records of about this size, never dropped. |
| `multiline` | object | — | — | Multiline reassembly (see below). |

### `multiline` sub-object

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `startPattern` | string | — | — | Regex marking a **new** record's first line (e.g. a timestamp). Other lines continue a record that **began with a start line**; a container that never logs a start line (sidecar, nginx, JSON logger) keeps one record per line. Empty = disabled. |
| `continuePattern` | string | — | — | Optional regex of lines that continue the previous record even when it did not begin with a start line, e.g. `^(\s+at \|\s*Caused by:)` for a stack trace printed without a header. Can be used alone. |
| `flushIdleMs` | int | `2000` | — | Emit a pending stitched record after the file is idle this long. |
| `maxLines` | int | `500` | — | Safety cap on stitched lines per record (`0` = unlimited). |

## Behavior

- **CRI envelope:** parses `<RFC3339Nano> <stream> <F|P> <message>`; partial (`P`) lines
  are concatenated automatically.
- **Multiline stitching:** when `startPattern` is set, lines that do not match it are
  appended to the previous record if that record began with a start line (or if they match
  `continuePattern`). The collector reads every container on the node, so a pattern written
  for one application does not glue other containers' lines together; a pending record is flushed after `flushIdleMs` of
  inactivity, and capped at `maxLines`. `stdout` and `stderr` share one CRI file but are
  stitched **separately**, so a stack trace on `stderr` interleaved with `stdout` lines stays
  one record. The committed position never passes the first line that either stream still
  buffers.
- **At-least-once:** the position registry is persisted only once the sink has made the
  records durable — at checkpoints, at the end of a run (or of a shutdown drain), or when
  the engine acknowledges written rows without a checkpoint store (`source.Committer`) —
  never for records still in flight. A crash resumes from the last committed offset (a
  few duplicate lines are possible, none are lost).
- **Rotation-safe:** kubelet rotates a log by renaming `0.log` to `0.log.<timestamp>` and
  the runtime reopening a fresh `0.log`. The tailer follows the file it has open to its
  end (with a 1 s grace for the runtime's last write), then switches to the new file. A
  collector that was down across a rotation finishes the rotated file from its saved
  offset before the new one. While back-pressured (e.g. the sink is down) over several
  rotations, every rotated file kubelet still keeps is read in rotation order — including
  the older ones kubelet has gzip-compressed (`0.log.<ts>.gz`, read decompressed; offsets
  and fingerprints refer to the original content, so a position saved before compression
  still resumes). Only files kubelet has already deleted (beyond `containerLogMaxFiles`)
  are out of reach.
- **Pod deletion:** a removed pod's files are read to the end before their handles close.
  Registry entries of pods whose log directory is gone are dropped at the next
  checkpoint, so the position file and in-memory state do not grow with pod churn.
  If the sink is down when a pod is deleted, the source is back-pressured. The open
  handle keeps the deleted file readable only until the run restarts after a failed
  checkpoint. Use the object-store sink's
  [durable spool](../sinks/objstore.md#durable-spool-rolling) so that a sink outage does not
  stall reading.
- **Memory:** about 32 KiB of read buffer per open file, plus pending multiline records.
  Hundreds of containers per node fit in well under 256 MiB. Records are bounded in bytes
  as well as in count: at most 16 MiB of messages queued between the tailer and the
  pipeline, and 8 MiB per batch. Applications that log large payloads cannot run the
  collector out of memory.
- Files are fingerprinted (first 256 bytes) to recognise them across restarts.

## Example — DaemonSet collector with Java/BW stack-trace stitching

```json
{
  "type": "podlog",
  "root": "/var/log/pods",
  "node": "${NODE_NAME}",
  "positionFile": "/var/lib/flogo-datapipeline/positions.json",
  "fromBeginning": false,
  "pollMs": 500,
  "excludeNamespaces": ["kube-system", "kube-public"],
  "multiline": {
    "startPattern": "^\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}",
    "flushIdleMs": 2000,
    "maxLines": 500
  }
}
```

## See also

- [Sources index](../sources.md) · [Parse transform](../transforms/parse.md) · [Object Store sink](../sinks/objstore.md)
- Demos: [`examples/20-podlog`](../../examples/20-podlog), [`examples/adidas-log-collection-usecase`](../../examples/adidas-log-collection-usecase)
