# Connections

Connections let you define a database, stream, object store, or HTTP endpoint
**once** and bind it by name on any source or sink — so pipeline files never contain
DSNs or secrets. Connections are a **Studio** concept
([`studio/vscode`](../studio/vscode)); at run time the host resolves a bound
connection into concrete engine config.

## The `*.fgconn` file

Each connection is one JSON file under `connections/` holding only **non-secret**
fields plus a `secretKeys` list naming which keys live in VS Code **Secret Storage**.
Secrets (passwords, keys, tokens) are **never** written to the file.

```
<project>/
├── pipelines/      # *.datapipeline flows
├── connections/    # *.fgconn connections (no secrets)
└── data/           # sample input / output
```

## Supported types

| Family | Types |
|---|---|
| Database (`db`) | PostgreSQL, MySQL, SQL Server, Oracle |
| Stream (`stream`) | Apache Kafka |
| Object store (`obj`) | Amazon S3, Google Cloud Storage, Azure ADLS Gen2 |
| HTTP (`http`) | any HTTP endpoint (lookup / enrich) |

## Create, edit, test

- **Create** — click **＋** on the **Connections** view or run
  **Data Pipeline: New Connection…**. Pick a type, name it, fill in the fields;
  secret fields are entered as passwords and go to Secret Storage.
- **Edit** — click a connection to open the visual editor (Save / Test / View JSON /
  Delete). Existing secrets show as `•••••••• (unchanged)` and are only rewritten if
  you type a new value.
- **Test** — a best-effort TCP reachability probe to `host:port`. It verifies the
  endpoint is reachable, **not** the credentials.

## Bind on a stage

In a source/sink Inspector whose type matches a connection family (e.g. a JDBC sink),
a **Connection** dropdown appears. Selecting a connection:

- stores `connection: "<name>"` on the stage, and
- hides the DSN/secret fields (the connection owns them).

At **run / preview** time the host resolves the bound connection into concrete engine
config (`driver`, `dsn`, `bucket`, `brokers`, …) written to a transient, git-ignored
`.<name>.resolved.datapipeline` sibling file that the engine reads — so the on-disk
pipeline never contains secrets. The transient file is deleted on stop.

## Secrets outside the Studio

When you deploy without the Studio, provide secrets to the engine through the
environment. Config keys that reference secrets do so **by env-var name**
(`passwordEnv`, `saslPasswordEnv`, `apiKeyEnv`, …), which the engine reads at run
time. See [Security](security.md) and [Deployment](deployment.md).

## See also

- [Sources](sources.md) · [Sinks](sinks.md)
- [Security](security.md) — the no-secrets-on-disk model
- [Studio](studio.md) — the full extension guide
