# 28 — Graph-native two-port join (Phase B demo)

Proves the engine's port-aware multi-input join: the join transform's "right"
side is wired from a **separate graph source node** via a named `left`/`right`
port edge, instead of the older inline `right` source config.

```
orders.csv    (leftSrc)  ──"left"──▶
                                     join(leftKey=customerId, rightKey=id) ──▶ out (joined.jsonl)
customers.csv (rightSrc) ──"right"─▶
```

`customerId=3` in `orders.csv` has no matching `id` in `customers.csv` and is
dropped (inner join, the default `joinType`).

## Run it

```bash
../../flogo-datapipeline -f graph-two-port-join.datapipeline &
sleep 2 && kill %1
cat out/joined.jsonl
```

Expect 3 of the 4 order rows (customerId=1 x2, customerId=2), each enriched
with `cust_name`/`cust_tier` from the matched customer row.
