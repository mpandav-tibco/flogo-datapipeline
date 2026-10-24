# Enterprise Reliability Round

## ent01 - Dead-letter (DLQ) isolation

Invalid rows are routed to a queryable `orders_rejects` table while
valid rows continue to the sink. No row is silently lost.

| Processed | Good (sink) | Rejected (DLQ) | Conserved | Throughput |
|----------:|------------:|---------------:|:---------:|-----------:|
| 53,000,000 | 50,589,628 | 2,410,372 | yes | 589,151/s |

## ent02 - Checkpoint / resume

A run is interrupted mid-flight; the next run resumes from the last
committed row-checkpoint instead of restarting from zero.

| Run 1 | Committed @ kill | Run 2 (resume) | Rows processed on resume |
|-------|-----------------:|----------------|-------------------------:|
| timeout | 11,000,000 | completed | 53,000,000 |

