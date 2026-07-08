# Example 01 — CSV to Null (Hello World)

**Mode:** EL (Extract-Load, no transforms)
**Source:** CSV file
**Sink:** Null (discard)

This is the simplest possible Flogo Data Pipeline. It reads 20 rows from a
local CSV file and immediately discards them using the null sink. No data is
written anywhere. The goal is to confirm that:

1. The engine starts correctly.
2. The CSV source can open, parse headers, and type-cast columns.
3. The pipeline runs to completion and exits cleanly.

Think of it as the `print("Hello, World!")` of data pipelines.

---

## Directory Layout

```
01-csv-to-null/
├── employees-to-null.datapipeline   # Pipeline definition (JSON)
├── data/
│   └── employees.csv                # 20-row sample dataset
├── run.sh                           # Convenience run script
└── README.md                        # This file
```

---

## What is EL Mode?

A pipeline can operate in three modes:

| Mode | Meaning                          | Transforms allowed? |
|------|----------------------------------|---------------------|
| `el` | Extract then Load (no transform) | No                  |
| `etl`| Extract, Transform, Load         | Yes                 |
| `elt`| Extract, Load, Transform (in DB) | Sink-side only      |

`el` mode is the lightest-weight option. The engine reads from the source and
feeds rows directly into the sink with zero processing overhead. Use it when
the source data is already in the shape you need, or — as here — when you just
want to verify connectivity and throughput.

Setting `"mode": "el"` in the pipeline file and leaving `"transforms": []`
empty is the correct way to express this. The engine will reject a non-empty
transforms array when mode is `el`.

---

## Pipeline Definition Explained

```json
{
  "name": "employees-to-null",
  "mode": "el",
  "source": {
    "type": "csv",
    "path": "./data/employees.csv",
    "delimiter": ",",
    "hasHeader": true,
    "columns": [
      { "name": "id",         "type": "int"    },
      { "name": "name",       "type": "string" },
      { "name": "department", "type": "string" },
      { "name": "salary",     "type": "float"  },
      { "name": "hire_date",  "type": "string" }
    ]
  },
  "transforms": [],
  "sink": { "type": "null" },
  "performance": { "batchSize": 1000, "maxMemoryMB": 512 },
  "scaling":     { "mode": "single" },
  "checkpoint":  { "type": "none" }
}
```

### Key fields

| Field | Value | Purpose |
|-------|-------|---------|
| `mode` | `"el"` | No transforms; rows go straight from source to sink |
| `source.type` | `"csv"` | Reads a delimited text file |
| `source.path` | `"./data/employees.csv"` | Relative to the working directory when the engine is invoked |
| `source.hasHeader` | `true` | First row is treated as column names, not data |
| `source.delimiter` | `","` | Standard comma-separated values |
| `source.columns` | array | Schema declaration; the engine casts each field to the declared type |
| `transforms` | `[]` | Empty — required key but no-op in `el` mode |
| `sink.type` | `"null"` | Discards every row; writes nothing to disk or network |
| `performance.batchSize` | `1000` | Rows buffered per internal batch (overkill for 20 rows, realistic for production) |
| `scaling.mode` | `"single"` | Single-threaded; no partitioning |
| `checkpoint.type` | `"none"` | No restart/resume state persisted |

---

## Sample Data

`data/employees.csv` contains 20 realistic employee records:

```
id,name,department,salary,hire_date
1,Alice Nguyen,Engineering,95000.00,2019-03-12
2,Bob Ramirez,Marketing,72000.00,2020-07-01
...
20,Teresa Novak,Engineering,96000.00,2018-12-01
```

Column types after parsing:

| Column | CSV text | Engine type |
|--------|----------|-------------|
| `id` | integer string | `int` |
| `name` | free text | `string` |
| `department` | free text | `string` |
| `salary` | decimal string | `float` |
| `hire_date` | ISO-8601 date string | `string` (no date coercion) |

---

## How to Run

### Prerequisites

- `flogo-datapipeline` binary on your `PATH`.
- Run from the `01-csv-to-null/` directory (so relative paths resolve correctly).

### Option A — run script

```bash
chmod +x run.sh
./run.sh
```

### Option B — direct invocation

```bash
cd 01-csv-to-null/
flogo-datapipeline -f employees-to-null.datapipeline
```

### Option C — absolute path to data file

If you need to invoke the engine from a different working directory, update
`source.path` in the pipeline file to an absolute path, or pass the working
directory as a flag (see `flogo-datapipeline --help`).

---

## Expected Output

```
INFO  pipeline starting               name=employees-to-null mode=el
INFO  csv source opened               path=./data/employees.csv rows=20
INFO  batch processed                 rows=20 batch=1
INFO  pipeline complete               rows_read=20 rows_written=0 duration=12ms
```

Key observations:

- `rows_read=20` — all 20 data rows were parsed (the header row is excluded).
- `rows_written=0` — the null sink consumed but did not persist any rows.
- Exit code `0` — clean termination.

If you see a type-cast error (e.g., a non-numeric value in the `salary`
column), the engine will log the offending row and exit with a non-zero code.
Fix the CSV data and re-run.

---

## How to Verify

After the run completes:

1. **Exit code:** `echo $?` should print `0`.
2. **No output files created** — the null sink writes nothing. Confirm no new
   files appear in the working directory.
3. **Log line `rows_read=20`** — confirms the CSV parser saw all 20 data rows.
4. **Log line `rows_written=0`** — confirms the null sink discarded them all.

---

## How to Open in Studio

1. Open TIBCO Flogo Studio (VS Code extension or web UI).
2. Use **File > Open Pipeline** and select
   `employees-to-null.datapipeline`.
3. The canvas shows the source node (CSV) connected directly to the sink node
   (Null) with no transform nodes in between — the visual representation of
   `el` mode.
4. Click **Run** in the Studio toolbar to execute the pipeline from within the
   IDE. Studio streams the engine log to the Output panel.

---

## Configuration Reference

### CSV Source

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `path` | string | Yes | — | Absolute or relative to CWD |
| `delimiter` | string | No | `","` | Single character |
| `hasHeader` | bool | No | `false` | Skip and use first row as column names |
| `columns` | array | Yes | — | `[{name, type}]`; types: `string`, `int`, `float`, `bool` |

### Null Sink

The null sink accepts no configuration fields. An empty object `{}` is valid
and equivalent to omitting all fields.

### Performance

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `batchSize` | int | `5000` | Rows per internal buffer flush |
| `maxMemoryMB` | int | `512` | Soft heap limit; engine may pause ingestion if exceeded |

### Scaling

| Field | Value options | Notes |
|-------|---------------|-------|
| `mode` | `"single"` | Single-threaded, no partitioning |

### Checkpoint

| Field | Value options | Notes |
|-------|---------------|-------|
| `type` | `"none"` | No state persisted; pipeline always starts from the beginning of the file |

---

## Next Steps

| Example | What it adds |
|---------|-------------|
| [02-csv-to-parquet](../02-csv-to-parquet/) | Write rows to a Parquet file instead of discarding them |
| [03-csv-filter-map](../03-csv-filter-map/) | Add `filter` and `map` transforms between source and sink |
| [04-csv-to-jdbc](../04-csv-to-jdbc/) | Load parsed rows into a relational database table |
