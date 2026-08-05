# Security

The security model has one guiding rule: **secrets are never written to a pipeline
file or an image.** Most of what follows flows from that.

## Secrets are names, not values

Engine config references a secret by the **name** of an environment variable
(`passwordEnv`, `saslPasswordEnv`, `apiKeyEnv`, …); the engine reads the value from the
environment at run time. The `.datapipeline` on disk contains only the *name*.

- In the **Studio**, a connection's secret fields go to VS Code **Secret Storage**; the
  `*.fgconn` file keeps only non-secret fields plus a `secretKeys` list. At run time the
  host resolves the bound connection into a transient, **git-ignored**
  `.<name>.resolved.datapipeline` sibling that the engine reads and that is deleted on
  stop — so the committed pipeline never holds a credential. See
  [Connections](connections.md).
- In **deployment**, supply the same env vars from a Kubernetes `Secret` / secret
  manager. See [Deployment](deployment.md).

## Transport security

- **Kafka** — `saslMechanism` (`PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512`) with
  `saslUser` + `saslPasswordEnv`, and TLS via `tlsCertFile` / `tlsKeyFile` /
  `tlsCACertFile` (source and sink).
- **JDBC** — put `sslmode=require` (or the driver's equivalent) in the `dsn`.
- **Object store** — providers use the cloud SDK's standard credential chain and TLS
  endpoints.

## Network egress (lookup / enrich)

The `lookup` transform makes outbound HTTP calls to the `url` you configure. Treat that
URL as trusted infrastructure: point it at internal enrichment services, set a tight
`timeoutMs`, and prefer `onError: skip` / `null` over `fail` so a hostile or slow
endpoint cannot wedge the run. Do not template the URL from untrusted row data without
validating the key first.

## Smaller attack surface

A lean [`dp-build`](deployment.md) binary links only the components the pipeline uses,
so an app that never touches Kafka or S3 does not carry that code at all. Ship lean
binaries to production.

## Data handling

- **DLQ envelopes** capture the *original row* alongside the error — if rows contain
  PII, the DLQ destination inherits that sensitivity; secure it like the main sink.
- **Metrics** are counts and durations only — no row contents are exported.

## Audit log

Enable an append-only [governance audit trail](observability.md#audit-log)
(`observability.audit` or the `DP_AUDIT` env var) to record who ran which pipeline,
when, and with what outcome. Records carry only component **types** and run
counters — never secret values, option contents, or row data — so they are safe to
centralize.

## The REST API

The management [REST API](rest-api.md) is off unless you set a port. By default it
has **no** authentication — bind it to loopback or a trusted network, or front it
with your own gateway; do not expose it directly to the internet. Set `DP_API_TOKEN`
to require a bearer token / API key on every request except `GET /v1/health` (see
[REST API → Authentication](rest-api.md#authentication-optional)). This is a
shared-secret gate, not RBAC/multi-tenancy.

## See also

- [Connections](connections.md) · [Deployment](deployment.md) · [Configuration](configuration.md)
