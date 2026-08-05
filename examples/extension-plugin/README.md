# Out-of-tree extension example

A **separate Go module** (`example.com/acme-flogo-plugins`) that adds three custom
plugins to flogo-datapipeline using **only** the public
[`extension`](../../extension/extension.go) contract — with no change to the
engine. It demonstrates that sources, sinks, and transforms are pluggable from
outside the repository.

| File | Adds | Type name |
|---|---|---|
| [rangesource.go](rangesource.go) | a **Source** emitting `count` rows `{n, label}` | `rangesource` |
| [uppercase.go](uppercase.go) | a **Transform** upper-casing one column | `uppercase` |
| [capturesink.go](capturesink.go) | a **Sink** collecting rows in memory | `capture` |

Each file `Register`s its plugin in an `init()` — exactly what a third-party
author would do in their own repo.

## Run it

```bash
cd examples/extension-plugin
go test ./...
```

[plugin_test.go](plugin_test.go) wires `rangesource → uppercase → capture` and
asserts the output, resolving each stage through the engine's registry
(`source.New` / `transform.New` / `sink.New`) to prove the out-of-tree plugins
are registered identically to the built-ins.

## Using it in a real deployment

1. Put these files in your own module and depend on a tagged engine release
   (drop the `replace` in [go.mod](go.mod)):

   ```
   require github.com/mpandav-tibco/flogo-datapipeline vX.Y.Z
   ```

2. Blank-import your package in the host binary's `main` so its `init()` runs:

   ```go
   import _ "github.com/acme/flogo-plugins"
   ```

3. Rebuild. Your `type: "rangesource"` / `"uppercase"` / `"capture"` are now
   usable in any `.datapipeline` file.

See [docs/extending.md](../../docs/extending.md) for the full guide.
