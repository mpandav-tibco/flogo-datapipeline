# Example 06 — XML Streaming Source

**Feature demonstrated:** XML source with SAX streaming, `recordElement` selection, XPath field extraction (child elements and attributes).

---

## Overview

Most XML processing libraries load the entire document into a DOM tree before you can query it. For a 10 MB file that is fine; for a 2 GB product catalog it is catastrophic.

This example shows how the data-pipeline engine reads XML in **SAX (Simple API for XML) streaming mode**. The parser fires events as bytes arrive off disk — `<product>` open, child text, `</product>` close — and never holds more than a single record in memory at a time. Memory consumption is **O(1) with respect to file size**: a 100-record file and a 100-million-record file consume the same working set.

---

## Directory layout

```
06-xml-source/
├── data/
│   └── products.xml            # 15-record sample catalog
├── products-xml-source.datapipeline
└── README.md
```

---

## Sample data — `data/products.xml`

The file contains a root `<catalog>` element with 15 `<product>` children. Each product record carries:

```xml
<product>
  <id>P001</id>
  <name>Wireless Keyboard</name>
  <category>Electronics</category>
  <price>49.99</price>
  <stock quantity="120"/>   <!-- note: value lives in an XML attribute -->
  <sku>ELEC-WKB-001</sku>
</product>
```

The dataset is deliberately mixed:

| Condition              | Count | Expected outcome          |
|------------------------|-------|---------------------------|
| price > 10 AND stock > 0 | 8   | Passes filter — emitted   |
| price <= 10.0          | 4     | Filtered out              |
| stock = 0              | 3     | Filtered out              |

> P014 has `price = 10.00` exactly; the filter expression uses strict `>`, so it is excluded.

---

## Pipeline configuration

```json
{
  "source": {
    "type": "xml",
    "path": "./data/products.xml",
    "recordElement": "product",
    "columns": [
      { "name": "id",       "xpath": "id",             "type": "string" },
      { "name": "name",     "xpath": "name",            "type": "string" },
      { "name": "category", "xpath": "category",        "type": "string" },
      { "name": "price",    "xpath": "price",           "type": "float"  },
      { "name": "stock",    "xpath": "stock/@quantity", "type": "int"    },
      { "name": "sku",      "xpath": "sku",             "type": "string" }
    ]
  }
}
```

### Key configuration fields

| Field           | Type            | Required | Description |
|-----------------|-----------------|----------|-------------|
| `type`          | string          | yes      | Must be `"xml"` |
| `path`          | string          | yes      | Path to the XML file (absolute or relative to the pipeline file) |
| `recordElement` | string          | yes      | Local element name that delimits one logical record (e.g. `"product"`, `"row"`, `"item"`) |
| `columns`       | array of objects | yes     | Columns to extract from each record |
| `columns[].name`  | string        | yes      | Output column name |
| `columns[].xpath` | string        | yes      | XPath expression relative to the record element root |
| `columns[].type`  | string        | yes      | Target type: `string`, `int`, `float`, `bool` |

---

## XPath expressions

XPath expressions in `columns[].xpath` are evaluated **relative to the matched record element**, not the document root.

### Child element text content

```json
{ "name": "price", "xpath": "price", "type": "float" }
```

Extracts the text content of `<price>49.99</price>` inside each `<product>`.

### Element attribute

```json
{ "name": "stock", "xpath": "stock/@quantity", "type": "int" }
```

Extracts the `quantity` attribute from `<stock quantity="120"/>`. Use the `@attributeName` suffix on any step in the path.

### Nested child elements

```json
{ "name": "city", "xpath": "address/city", "type": "string" }
```

Slashes navigate through nested elements. Depth is not limited.

---

## How SAX streaming works

```
Disk                     SAX parser              Engine
─────                    ──────────              ──────
bytes ──► tokeniser ──► open  <product>  ──►  start accumulating
                    ──► text  <id>P001…  ──►  buffer child text
                    ──► open  <stock …>  ──►  read attribute "quantity"
                    ──► close </product> ──►  finalise record → emit row
                                               (record memory is freed)
                    ──► open  <product>  ──►  start next record …
```

The engine never materialises the full document tree. The only allocation kept alive is the **current record's field buffer** — typically a few hundred bytes — regardless of whether the file is 1 MB or 100 GB.

---

## Filter transform

```json
{
  "type": "filter",
  "expr": "price > 10.0 AND stock > 0"
}
```

The filter expression references the extracted column names directly. Both `price` (float) and `stock` (int) have already been type-cast by the source before the transform runs, so numeric comparisons work without any casting in the expression.

---

## How to run

```bash
# From the repository root
./flogo-datapipeline run \
  --pipeline examples/06-xml-source/products-xml-source.datapipeline
```

Or on Windows:

```powershell
.\flogo-datapipeline.exe run `
  --pipeline examples\06-xml-source\products-xml-source.datapipeline
```

---

## What to expect

The engine logs each phase. Look for lines similar to:

```
INFO  source started  type=xml path=./data/products.xml recordElement=product
INFO  pipeline run complete  source_rows=15 filter_pass=8 filter_drop=7 sink_rows=8 elapsed=12ms
```

- `source_rows=15` — all 15 `<product>` elements were parsed
- `filter_pass=8` — 8 records satisfy `price > 10.0 AND stock > 0`
- `filter_drop=7` — 4 cheap items + 3 out-of-stock items dropped
- `sink_rows=8` — the null sink discards these 8 rows (no output file written)

Memory usage reported by the engine should remain flat and close to the baseline JVM/Go runtime footprint throughout the run, demonstrating the O(1) property.

---

## Verifying the filter

To manually count which products pass, run:

```bash
# List products with price > 10 AND stock attribute > 0
grep -A5 '<product>' data/products.xml | awk '...'
```

Or use any XML-aware tool:

```bash
# xmllint (libxml2)
xmllint --xpath \
  "//product[price > 10 and stock/@quantity > 0]/id/text()" \
  data/products.xml
```

Expected output: `P001 P003 P005 P007 P009 P011 P013 P015` (8 products).

---

## Handling files larger than RAM

Because no DOM tree is built, you can point `path` at arbitrarily large files:

```json
{ "path": "/mnt/data/full-catalog-2gb.xml", "recordElement": "product" }
```

The only practical limits are:

1. **Individual record size** — if a single `<product>` element contains megabytes of embedded CDATA the field buffer for that one record grows accordingly. Keep record granularity sane.
2. **Batch size** — `performance.batchSize` controls how many extracted records are held in memory before being flushed downstream. The default of 5000 is appropriate for records up to ~1 KB each (~5 MB peak batch memory).

---

## Extending this example

| Goal | Change |
|------|--------|
| Write passing records to a database | Change `sink.type` to `jdbc` and configure `driver`, `dsn`, `table` |
| Write to Parquet | Change `sink.type` to `parquet` and set `path` |
| Add more XPath columns | Add entries to `source.columns` |
| Parse a different root element | Change `recordElement` to match your schema |
| Add a map transform to reshape fields | Insert a `map` transform after the filter |
