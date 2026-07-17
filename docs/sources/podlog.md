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
| `fromBeginning` | bool | `false` | — | Read existing files from offset 0; otherwise tail from EOF. |
| `pollMs` | int | `500` | — | Idle poll interval (ms). |
| `bounded` | bool | `false` | — | Test/drain mode: return EOF once the tree is drained and idle. |
| `includeNamespaces` | `[]string` | all | — | If non-empty, only these namespaces are tailed. |
| `excludeNamespaces` | `[]string` | none | — | Namespaces to skip (e.g. `["kube-system"]`). |
| `multiline` | object | — | — | Multiline reassembly (see below). |

### `multiline` sub-object

| Key | Type | Default | Required | Description |
|---|---|---|:--:|---|
| `startPattern` | string | — | — | Regex marking a **new** record's first line (e.g. a timestamp). Empty = disabled (one line per record). |
| `flushIdleMs` | int | `2000` | — | Emit a pending stitched record after the file is idle this long. |
| `maxLines` | int | `500` | — | Safety cap on stitched lines per record (`0` = unlimited). |

## Behavior

- **CRI envelope:** parses `<RFC3339Nano> <stream> <F|P> <message>`; partial (`P`) lines
  are concatenated automatically.
- **Multiline stitching:** when `startPattern` is set, lines that do not match it are
  appended to the previous record; a pending record is flushed after `flushIdleMs` of
  inactivity, and capped at `maxLines`.
- **At-least-once:** the position registry is persisted only after the sink acknowledges,
  so a crash resumes from the last committed offset (a few duplicate lines are possible).
- **Rotation-safe:** file fingerprints prevent misresumes across log rotation.

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
