# Graph (DAG) Runner - Enterprise Fan-out

The streaming **graph runner** (`action/pipeline/graphexec`) executes a
NON-REDUCIBLE pipeline as a goroutine-per-node dataflow: one source read
is fanned out across typed edges (condition / otherwise / success) to many
sinks **in a single streaming pass**. The linear engine cannot represent
these shapes - it would need one full pass (a whole re-read of the source)
per destination.

Every pipe below engaged the graph runner - the engine logs
`graph run complete - nodes=N edges=M` (a non-reducible DAG). Row counts and
durations are engine-measured; peak RSS and CPU are the kernel rusage of the
single process. Dataset: **53,000,000 rows** (CSV 5.2G).

## Pipelines

| Pipe | Pattern | Nodes/Edges | Rows read | Rows written | Duration | Read thr | Write thr | Peak RSS | CPU |
|------|---------|:-----------:|----------:|-------------:|---------:|---------:|----------:|---------:|----:|
| `graph01_route_region_parquet` | regional fan-out router (5-way) | 7 / 6 | 53,000,000 | 39,750,329 | 78.6s | **674,395/s** | **505,800/s** | 216 MB | 3.2 |
| `graph02_valreject_parquet` | valid / reject split (DLQ) | 4 / 3 | 53,000,000 | 39,750,329 | 72.9s | **727,442/s** | **545,586/s** | 158 MB | 3.3 |
| `graph03_tee_audit_parquet` | tee + high-value audit | 4 / 3 | 53,000,000 | 43,617,427 | 76.7s | **691,256/s** | **568,883/s** | 179 MB | 3.2 |
| `graph04_route_region_s3` | regional fan-out router (5-way) -> S3 | 7 / 6 | 53,000,000 | 39,750,329 | 81.3s | **651,923/s** | **488,946/s** | 276 MB | 3.1 |
| `graph05_multi_consolidate` | multi-source consolidation (3 feeds -> filter/map/validate -> 6 sinks) | 12 / 11 | 159,000,000 | 119,250,987 | 294.4s | **540,010/s** | **405,011/s** | 486 MB | 4.5 |

*Write thr* counts every row handed to a sink, so fan-out pipes (tee)
write more rows than they read. Every run stayed at a few hundred MB of
RAM - streaming, O(batch), not O(file).

## One pass vs the linear equivalent

The point of the graph runner: it produces **N destinations from ONE source
read**. To get the same outputs, the linear engine must run N separate
pipelines, each re-reading and re-filtering the whole 53,000,000-row source.

| Pipe | Destinations | Graph (1 pass) | Linear (N passes) | Speed-up | Source read |
|------|:------------:|---------------:|------------------:|:--------:|------------:|
| `graph01_route_region_parquet` | 5 | 78.6s | 5 x 90.5s = 452.6s | **5.8x** | 1x vs 5x |
| `graph02_valreject_parquet` | 2 | 72.9s | 2 x 90.5s = 181.0s | **2.5x** | 1x vs 2x |
| `graph03_tee_audit_parquet` | 2 | 76.7s | 2 x 90.5s = 181.0s | **2.4x** | 1x vs 2x |
| `graph04_route_region_s3` | 5 | 81.3s | 5 x 92.0s = 459.8s | **5.7x** | 1x vs 5x |

The linear per-pass baseline is the measured single-pass
`csv -> filter -> parquet` case (585,487/s), or `csv -> s3` for the S3 router.
A 5-way router finishes ~**5.7x** sooner and reads the source **once**
instead of five times (5.2G read once, not 5 x).

## Single-pass throughput and CPU

Even ignoring the multi-pass win, one graph pass **matches or beats** the
linear engine while doing strictly more work (routing + several concurrent
writers), because the goroutine-per-node design keeps more cores busy.

| Pipe | Graph read thr | Linear 1-pass (`csv->filter->parquet`) | Graph CPU | Linear CPU |
|------|---------------:|--------------------------------------:|:---------:|:----------:|
| `graph01_route_region_parquet` | **674,395/s** | 585,487/s | 3.2 | 2.0 |
| `graph02_valreject_parquet` | **727,442/s** | 585,487/s | 3.3 | 2.0 |
| `graph03_tee_audit_parquet` | **691,256/s** | 585,487/s | 3.2 | 2.0 |
| `graph04_route_region_s3` | **651,923/s** | 585,487/s | 3.1 | 2.0 |

The graph runner drives **3-4 of 10 cores**; the single-chain linear engine
sits at ~2 (single-threaded parse + single writer). The DAG turns the extra
cores into extra concurrent writers at no throughput cost.

## Multi-source consolidation (the everyday enterprise ETL)

`graph05_multi_consolidate` is the shape most enterprise pipelines actually
are: **several systems of record, in different formats, consolidated, cleaned
and routed to many destinations in a single pass.** It fans THREE sources in
(CSV + JSON Lines + Parquet, 53,000,000 rows each), runs a filter -> map(enrich) ->
validate chain, then fans OUT to five regional lake partitions plus a
quarantine sink for the rows validation rejects.

| Metric | Value |
|--------|-------|
| Sources (fan-in) | **3** - csv + jsonlines + parquet |
| Transforms | filter -> map (region_code, net_amount) -> validate (split) |
| Sinks (fan-out) | **6** - 5 regional partitions + 1 quarantine |
| Rows read / written | 159,000,000 / 119,250,987 |
| Throughput | **540,010/s** &middot; 294.4s |
| Peak RSS / CPU | 486 MB &middot; 4.5 cores |

The linear engine **cannot express this pipeline at all**: it has a single
source (no streaming fan-in - its `union` transform buffers secondary feeds
in memory, bounded to ~1M rows) and a single sink (no fan-out). The graph
runner merges the three feeds into one stream, applies the shared transform
chain once, and splits the result across 6 sinks - 159,000,000 rows in a single
486 MB pass.

## Live object storage (S3 / MinIO)

`graph04_route_region_s3` ran the same 5-way router against **live MinIO**
object storage, writing five regional Parquet partitions to the `datalake`
bucket in one pass: read 53,000,000, written 39,750,329, 651,923/s, peak RSS 276 MB. Each partition
streamed straight to object storage over the network.

## Fan-out reconciliation

Every row is accounted for across the branches (sizes are the persisted
Parquet partitions):

- `graph01_route_region_parquet` - read 53,000,000, written 39,750,329 across 5 branches: west 235.8 MB, north 236.2 MB, central 236.9 MB, east 235.7 MB, south 236.0 MB
- `graph02_valreject_parquet` - read 53,000,000, written 39,750,329 across 2 branches: valid 1.1 GB, reject 60.2 MB
- `graph03_tee_audit_parquet` - read 53,000,000, written 43,617,427 across 2 branches: all 1.2 GB, audit 118.9 MB
- `graph04_route_region_s3` - read 53,000,000, written 39,750,329 across 5 branches: west 235.8 MB, north 236.2 MB, central 236.9 MB, east 235.7 MB, south 236.0 MB
- `graph05_multi_consolidate` - read 159,000,000, written 119,250,987 across 6 branches: west 669.4 MB, north 670.5 MB, central 672.4 MB, east 669.1 MB, south 670.1 MB, reject 180.8 MB

