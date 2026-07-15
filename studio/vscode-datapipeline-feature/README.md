# Data Pipeline feature (`flogo3.datapipeline.*`)

Studio support for **Flogo Data Pipeline** (`*.datapipeline`) inside the existing
`flogo3` VS Code extension. It reuses the extension's frameworks rather than
rebuilding them, and adds only what's genuinely new for the data-pipeline engine.

> Branch: `feature/datapipeline-ux` · gate behind `FLOGO_FEATURE_DATAPIPELINE`.

## What's here (self-contained, `vscode` + node built-ins only)

| File | Role | flogo-3x pattern it mirrors |
|---|---|---|
| `types.ts` | Engine `/v1` + on-disk shapes | `@vsflogo/common` models |
| `model.ts` | Parse `.datapipeline` → canvas graph | serialization helpers |
| `engine-client.ts` | Spawn the engine CLI + `/v1` HTTP client | `common/flogo-bin.ts` |
| `diagnostics.ts` | Validation → Problems panel | `buildDiagnostis()` |
| `pipelines-tree-provider.ts` | "Data Pipelines" tree view | `AppExplorerProvider` |
| `datapipeline-editor-provider.ts` | Custom editor for `*.datapipeline` | `FlowCustomTextEditorProvider` |
| `webview.ts` | Self-contained canvas HTML | (interim; see below) |
| `status-bar.ts` | Live run pulse | `vscodecomponents/status-bar` |
| `register.ts` | Feature entry + commands | `registerFlowEditorUIFeatures` |
| `contributes.datapipeline.jsonc` | `package.json` additions | — |
| `examples/hello-world.datapipeline` | Runnable sample | — |

## Wiring (2 steps)

**1. Activate the feature** — in `extension/src/extension.ts::activate()`:

```ts
import { registerDataPipelineFeature } from './features/datapipeline';

// inside activate(), alongside the other feature registrations:
if (isFeatureEnabled('datapipeline')) {            // FLOGO_FEATURE_DATAPIPELINE
  context.subscriptions.push(registerDataPipelineFeature(context));
}
```

Add a `datapipeline` flag to `config/feature-flags.ts` (same convention as `dtl`).

**2. Merge the manifest** — copy the entries from
`contributes.datapipeline.jsonc` into `extension/package.json` (`activationEvents`,
`contributes.customEditors`, `contributes.views["flogo3-sidebar-view"]`,
`contributes.commands`, `contributes.menus`, `contributes.configuration.properties`).

## Settings

- `flogo3.datapipeline.binaryPath` — path to the `flogo-datapipeline` engine binary (else resolved from `PATH`).
- `flogo3.datapipeline.engineAddr` — a running engine REST server (`host:port`) for the `/v1` API.
- `flogo3.datapipeline.verbose` — pass `-v` so run summaries are captured.

## Run it

1. `pnpm build:dev` (from the extension root), press **F5** to launch the Extension Host.
2. Open a `*.datapipeline` file → it opens in the **canvas** custom editor.
3. **Data Pipelines** view lists workspace pipelines; run from the editor toolbar,
   the tree inline action, or the Command Palette (`Flogo Data Pipeline: Run Pipeline`).
4. The run streams to the **Flogo Data Pipeline** output channel and the status bar;
   validation appears in **Problems**.

## Next steps (not in this first slice)

- Replace `webview.ts` with an Angular app in
  `packages/wi-contributions/projects/datapipeline-ui`, reusing `wi-diagram`
  (canvas) and `@tibco/flogo-mapper` (the `=`-expression inspector).
- **Run Report** webview reusing the `execution-history-ui` pattern.
- **DQ → Testing**: map `qualityReport` expectations onto a `TestController`
  (reusing the unit-test `bin-runner`).
- A `datapipeline` **RuntimeProvider** for Run / Scale / Deploy (K8s StatefulSet).
