# ELT Round - In-Database Transforms

The pipeline lands raw rows; the set-based transforms run inside
PostgreSQL against the full `orders_raw` table (server-side timing).

| Case | Transform | Rows scanned | Rows out | Duration | Scan rate |
|------|-----------|-------------:|---------:|---------:|----------:|
| elt01_curate | Filter + normalise + derive net_amount in-database | 53,000,000 | 38,677,277 | 43.7s | **1,212,759/s** |
| elt02_rollup | GROUP BY region x category revenue aggregate | 53,000,000 | 25 | 16.8s | **3,162,480/s** |
| elt03_dedup | Window de-duplication (latest row per order_id) | 53,000,000 | 53,000,000 | 116.4s | **455,154/s** |
