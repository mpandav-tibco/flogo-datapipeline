# Example 26 — Enterprise Orders Quality Gate

A production-shaped **data-quality ingestion pipeline**. Raw order rows arrive as
untyped CSV; the pipeline casts them to typed values, gates every row through
five quality rules, dead-letters the rejects (with the reason), keeps only
high-value orders, projects a curated record, and lands it as JSON lines — while
writing a governance **audit trail** of the run.

It is the canonical **"trust boundary" pattern**: never let a bad row reach the
curated zone, but never silently drop it either — route it somewhere an operator
can inspect.

---

## What This Example Demonstrates

| Feature | Detail |
|---|---|
| **Cast** transform | `amount` → `decimal(2)` (money precision), `quantity` → `int`; un-castable values become `null` (`onError: "null"`) so the validator catches them |
| **Validate** transform | Five rules: `order_id` pattern, `customer_id` not-null, `amount` range `[0.01, 99999.99]`, `status` in a set, `email` pattern |
| **Dead-letter routing** | `onViolation: "route"` + `errorSinkType: "csv"` — rejected rows are written verbatim to `data/rejected-orders.csv` instead of being dropped |
| **Filter** transform | `amount >= 100` — a business rule that keeps only high-value orders in the curated feed |
| **Mapper** transform | Projects the four curated columns (drops `email`, `quantity`) |
| **JSON-lines sink** | Writes `out/curated-orders.jsonl`, one compact JSON object per line |
| **Audit log** | `observability.audit` writes `out/audit.jsonl` — a tamper-evident record of *who ran what, when, and with what outcome* (types & counts only — never row data) |

---

## Directory Layout

```
26-enterprise-orders-quality/
├── enterprise-orders-quality.datapipeline   # the pipeline (a flogo:app)
├── data/
│   ├── orders.csv                           # 10 input orders (6 intentionally bad)
│   └── rejected-orders.csv                  # created by the run — the DLQ
├── out/
│   ├── curated-orders.jsonl                 # created by the run — the clean feed
│   └── audit.jsonl                          # created by the run — the audit trail
├── run.sh
└── README.md
```

---

## The Input Data

`data/orders.csv` holds 10 orders. Six are deliberately invalid so you can watch
the quality gate work:

| order_id | why it fails |
|---|---|
| `ORD-000003` | negative `amount` (`-25.00`) |
| `ORD-000004` | `status` = `UNKNOWN` (not in the allowed set) |
| `ORD-000005` | `email` = `not-an-email` |
| `BADID`      | `order_id` doesn't match `^ORD-[0-9]{6}$` |
| `ORD-000008` | empty `customer_id` |
| `ORD-000010` | `amount` (`99999999.00`) exceeds the max |

Of the four valid rows, one (`ORD-000002`, `amount 89.50`) is a legitimate order
that is simply below the high-value threshold, so it is filtered out of the
curated feed — but **not** dead-lettered (it isn't an error).

---

## Run It

```bash
# 1. Build the engine once (from the repo root):
cd ../../ && CGO_ENABLED=1 go build -o flogo-datapipeline ./cmd/flogo-datapipeline && cd -

# 2. Run the pipeline:
./run.sh
```

The pipeline is triggered by a timer that fires once on startup; `run.sh` starts
the engine, waits for the run to complete, prints the summary, and stops it.

---

## Expected Result

```
pipeline run completed — processed=10 written=3 failed=0 dropped=0
```

| Stage | Rows |
|---|---|
| Read from CSV | **10** |
| Rejected by the quality gate → `data/rejected-orders.csv` | **6** |
| Valid but below the high-value filter | **1** |
| **Curated → `out/curated-orders.jsonl`** | **3** |

`out/curated-orders.jsonl`:

```json
{"amount":149.99,"customer_id":"CUST-101","order_id":"ORD-000001","status":"SHIPPED"}
{"amount":320.00,"customer_id":"CUST-106","order_id":"ORD-000006","status":"SHIPPED"}
{"amount":500.00,"customer_id":"CUST-109","order_id":"ORD-000009","status":"DELIVERED"}
```

Note the money values keep exactly two decimal places (`320.00`, `500.00`) —
that is the `cast` transform's `decimal(2)` at work, not float rounding.

---

## Author It Visually

Open `enterprise-orders-quality.datapipeline` in the **Flogo Data Pipeline
Studio** (VS Code) to see the pipeline on the canvas:

```
csv ──▶ cast ──▶ validate ──▶ filter ──▶ mapper ──▶ jsonlines
                    │
                    └─▶ rejected-orders.csv  (dead-letter)
```

Every node's configuration — the cast columns, the five validation rules, the
filter expression, the projection, and the audit setting — is editable from the
node's properties panel. Studio and the CLI read the same file, so what you see
on the canvas is exactly what the engine runs.
