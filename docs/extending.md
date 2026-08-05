# Extending the engine — custom sources, sinks & transforms

Flogo Data Pipeline is a **plugin engine**. Sources, sinks, and transforms are
discovered through a registry, so you can add your own — in your **own Go
module**, without editing the engine — and adding, removing, or updating one
plugin never affects the others.

The public contract lives in the
[`extension`](../extension/extension.go) package. A runnable, out-of-tree example
is in [`examples/extension-plugin/`](../examples/extension-plugin/).

---

## The model

The mechanism is the standard Go registry pattern (the same one `database/sql`
uses for drivers):

1. **Implement** one of the three interfaces (`Source`, `Sink`, `Transform`).
2. **Register** a named factory in your package's `init()`.
3. **Activate** it in the host binary with a blank import of your package.

```
your module                              engine
─────────────                            ──────
init() → extension.RegisterSource(…) ─▶  source registry ─▶ source.New("your-type", cfg)
```

The engine depends only on the interfaces, and each plugin lives in its own
package, so the three concerns are fully decoupled:

| Action | What you do | Blast radius |
|---|---|---|
| **Add** | new package + implement interface + `Register` in `init()` + one blank import | none — engine and other plugins untouched |
| **Remove** | delete the blank import | pipelines naming it get a clear "unknown type" error; everything else works |
| **Update** | change internals; keep satisfying the interface | callers depend on the interface, not the implementation |

---

## The data contract: `RecordBatch`

Every plugin operates on batches, never single rows:

```go
type RecordBatch struct {
    Columns []Column         // the shared schema
    Rows    [][]interface{}  // Rows[i][j] = value of column j in row i
}

type Column struct {
    Name   string
    Type   ColumnType // TypeString | TypeInt | TypeFloat | TypeBool | TypeDate | TypeDecimal | TypeAny
    Format string     // date layout, when Type == TypeDate
    Scale  int        // decimal places, when Type == TypeDecimal
}
```

Helpers on a batch: `Len()`, `ColIndex(name)`, `Value(row, col)`, `RowMap(i)`,
`Clone(capacity)`. All are re-exported (as aliases) from `extension`.

---

## The three interfaces

### Source

```go
type Source interface {
    Schema() []Column                            // columns, called once after open
    ReadBatch(size int) (*RecordBatch, error)    // next ≤size rows; (nil, io.EOF) when exhausted
    Skip(n int64) error                          // advance without emitting (checkpoint resume)
    Close() error
}
```

### Transform

```go
type Transform interface {
    Name() string                                // for logging
    Apply(in *RecordBatch) (*RecordBatch, error) // reshape; return nil to drop the whole batch
    Close() error
}
```

The transform **owns** the returned batch; the caller owns the input. Returning
the same batch you were given (in-place edit) is allowed and common.

### Sink

```go
type Sink interface {
    WriteBatch(batch *RecordBatch) error // should be idempotent for retry safety
    Flush() error                        // called at each checkpoint and at completion
    Close() error
}
```

---

## Registering

```go
package acmeplugins

import ext "github.com/mpandav-tibco/flogo-datapipeline/extension"

func init() {
    ext.RegisterSource("acme-source", newAcmeSource)
    ext.RegisterTransform("acme-redact", newRedact)
    ext.RegisterSink("acme-sink", newAcmeSink)
}
```

`Register*` panics on a **duplicate name**, so a clash surfaces at process start
rather than silently shadowing a built-in. Names are global; pick a prefix
(e.g. `acme-`) to avoid collisions with built-ins.

Then activate in the host binary (or your own `main`) with a blank import:

```go
import _ "github.com/acme/flogo-plugins" // runs init(), registers the plugins
```

Rebuild the binary and your `type: "acme-source"` is usable in any
`.datapipeline` file. (Plugins are linked in at build time — extending means
*add package + import + rebuild*, not runtime `.so` loading.)

---

## Config conventions

A factory receives the plugin's options as a generic map (the JSON object under
the stage in the pipeline file) and **validates at construction**, returning an
error the engine surfaces with context:

```go
func newAcmeSource(cfg map[string]interface{}) (ext.Source, error) {
    endpoint, _ := cfg["endpoint"].(string)
    if endpoint == "" {
        return nil, fmt.Errorf("acme-source: 'endpoint' is required")
    }
    // JSON numbers decode as float64 — coerce as needed.
    return &acmeSource{endpoint: endpoint}, nil
}
```

Do all validation here so a misconfigured pipeline fails fast, before any rows
flow.

---

## Optional capability interfaces (progressive enhancement)

Beyond the base contract, the engine detects **opt-in** interfaces by type
assertion. Implement them only if relevant; omitting them is always safe.

| Interface | On | Gives you |
|---|---|---|
| `PartitionableSource` | Source | distributed / parallel reads (the source enumerates and opens sub-ranges) |
| `Committer` | Source | at-least-once ack of an external position (e.g. Kafka offsets), committed only after the checkpoint is durable |
| `SinkOwner` | Transform | forces the single-threaded path for transforms that own external sink(s) (e.g. a router) so parallel workers don't open duplicate writers |

Because these are separate interfaces, **new capabilities can be added to the
engine over time without changing the base `Source`/`Sink`/`Transform` contract
or any existing plugin.**

---

## Testing your plugin

Unit-test against the interfaces — no engine required. Drive a source → transform
→ sink by hand and assert on the output (see
[`examples/extension-plugin/plugin_test.go`](../examples/extension-plugin/plugin_test.go)),
or register through `extension` and resolve via `source.New` / `sink.New` /
`transform.New` to prove the wiring.

---

## Other extension points

Checkpoint stores, watermark stores, object-store providers, and metrics
recorders follow the same `Register(name, factory)` pattern in their own packages
(`checkpoint`, `watermark`, `objstore`, `metrics`). They are extended less often,
so they are not aliased into `extension` — import the package directly and call
its `Register`.

## Stability

The `extension` package is the intended-stable surface for out-of-tree authors.
It is a thin set of aliases over the engine's internal packages, so it stays in
lock-step with them at zero cost while giving you one documented import to depend
on. The one type to evolve carefully is the shared `RecordBatch`, since every
plugin uses it.
