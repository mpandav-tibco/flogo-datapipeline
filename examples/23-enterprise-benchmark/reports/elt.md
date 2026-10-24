# ELT Round - In-Database Transforms

The pipeline lands raw rows; the set-based transforms run inside
PostgreSQL against the full `orders_raw` table (server-side timing).

| Case | Transform | Rows scanned | Rows out | Duration | Scan rate |
|------|-----------|-------------:|---------:|---------:|----------:|
| elt01_curate | Filter + normalise + derive net_amount in-database | 53,000,000 | 38,677,277 | 51.5s | **1,029,586/s** |
| elt02_rollup | GROUP BY region x category revenue aggregate | 53,000,000 | 25 | 8.9s | **5,983,292/s** |
| elt03_dedup | Window de-duplication (latest row per order_id) | 53,000,000 | 53,000,000 | 81.3s | **651,874/s** |
