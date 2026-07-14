# 22 — Custom File-Format Conversion (generic, BW6-style data integration)

A **generic** data-integration pipeline that reproduces a classic BW6 customer
pattern — converting a multi-section instrument file into a bespoke fixed-width /
scientific-notation layout — using two new, **format-agnostic** engine components.

The concrete demo converts a DaVinci-style **overlay-metrology** file (key/value
header + measurement table) into an **MC300/KLM**-style output. But nothing here
is hard-coded to DaVinci: the parsing and the output layout are entirely driven
by config + a template, so the same two components handle mainframe reports,
instrument logs, EDI-like layouts, and other legacy formats.

## Clean ETL separation of concerns

The pipeline follows a strict boundary — **source deserializes, transforms hold
all value logic, sink serializes**:

```
sectioned (parse header + body)        →  Extract  (deserialize only)
  → mapper  (scientific() each value,  →  Transform (ALL value logic, visible
              carry header attributes)                on the canvas, streamable, DLQ-able)
  → template (header block + fixed-     →  Load     (layout / positioning only)
              width layout of values)
```

Value formatting (signed scientific notation, padding, clamping) lives in the
`mapper` as reusable builtins — **`scientific`, `lpad`, `rpad`, `clamp`** — not in
the sink. The sink only lays out already-formatted strings, so the transformation
steps are explicit on the canvas and flow through the batch pipeline (checkpoint /
DLQ friendly).

## Run it

```bash
# uses the engine built for example 21 (or run ../21-usecase-matrix/scripts/build.sh)
../21-usecase-matrix/bin/dp -f davinci-to-mc300.datapipeline -v
cat out/*.mc300
```

Input [data/overlay_input.txt](data/overlay_input.txt) → output `out/<WaferID>.mc300`.

## The two new generic components

### `source/sectioned` — multi-section document source
Parses a file made of a **key/value header block** followed by a **delimited body
table**, and (optionally) broadcasts every header attribute onto every body row so
downstream stages see file-level context. Fully config-driven:

| Option | Meaning |
|---|---|
| `headerKVRegex` | regex with 2 groups (key, value) for header lines |
| `bodyColumnMarker` | regex identifying the line that names the body columns |
| `bodyDelimiter` | regex splitting each body row into fields |
| `broadcastHeader` | add the header attributes to every emitted row |

This fills the **"non-tabular / document" parsing** gap — the engine's other
sources are flat-record only.

### `sink/template` — custom text-template sink
Renders buffered rows through a Go `text/template`, producing arbitrary text. The
template receives `.Rows` (each row as a map), `.Count`, `.Columns`, and `.Attr`
(the header attributes), plus helpers **`rpad` / `lpad` / `padc` / `sci` / `upper`
/ `lower` / `trim` / `add` / `seq`** — enough for fixed-width + scientific layouts.
See [mc300.tmpl](mc300.tmpl). The output `path` is itself templated on the header
attributes, so one input routes to a per-wafer file name.

This fills the **"custom text output"** gap — the Parquet/JDBC/Kafka/object-store
sinks can't express bespoke report formats.

## How it maps to the BW6 app

| BW6 (`SampleCodeForCloud`) | flogo-datapipeline |
|---|---|
| JMS/File trigger | timer trigger here (any Flogo trigger in production — not the focus) |
| ReadFile + line parse (`ConvertDavinci`) | `source/sectioned` (config, no code) |
| Header key/value extraction | `headerKVRegex` + `broadcastHeader` |
| Java `util` padding + `%+.6E` formatting | `mapper` builtins `scientific`/`lpad`/`rpad`/`clamp` (value logic in the transform) |
| WriteFile of the MC300 layout | `sink/template` + `mc300.tmpl` (layout only) |
| Config-driven output folder | templated sink `path` (`{{index .Attr "WaferID"}}`) |

## What this proves — and what's next

Proven end-to-end: **multi-section parsing, header→row context broadcast, and
fully custom fixed-width + scientific-notation output**, all generic and reusable.

Deliberately left as the next generic component (flagged, not built here): an
in-flight **`reshape`** transform (group-by + pivot + sort + dedup) for cases where
measurement rows must be pivoted into wide records before formatting. That is the
same "no GROUP BY operator" gap the ELT benchmark hit — a strong signal to build it
once and reuse it across analytics *and* format-conversion workloads.
