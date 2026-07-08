# Example 05 — Fixed-Width / EBCDIC Mainframe Files

Demonstrates reading a **mainframe-style fixed-width flat file** with the
`fixedwidth` source. Key features exercised:

- `encoding: "ebcdic-037"` — activates the full 256-byte EBCDIC-037 translation
  table so each byte is converted to its Unicode equivalent before field
  extraction.
- `impliedDecimal` — packed numeric fields stored without a decimal point; the
  engine inserts it at the configured position.
- `recordLength` — the engine reads exactly N bytes per record (plus the line
  terminator on text-mode files), so no delimiter is required.
- `filter` transform — keeps only records whose `quality_flag` equals `G`
  (good), dropping records flagged `B` (bad).

---

## Sample Data Note

> **The sample file `data/sensor_readings.dat` is plain ASCII, not real binary
> EBCDIC.** Real mainframe extract files are raw binary where each byte is an
> EBCDIC code point (e.g., the digit `0` is `0xF0`, not `0x30`). A binary
> EBCDIC file cannot be committed to a Git repository and viewed in a text
> editor without a translation layer. For testability and readability the sample
> uses ASCII, which is a valid subset of the translation table (the first 128
> code points are identical between ASCII and EBCDIC for the printable digit and
> uppercase letter ranges used here). When you run this example against a real
> mainframe extract, replace `data/sensor_readings.dat` with the binary file and
> the EBCDIC-037 decoder will handle the full 256-byte table correctly.

---

## Record Layout

Each record is exactly **60 bytes** (plus a line terminator):

| Field         | Start | Length | Type    | Notes                                      |
|---------------|------:|-------:|---------|--------------------------------------------|
| `sensor_id`   |     1 |      8 | string  | Left-justified, space-padded; `trim:true` strips padding |
| `timestamp`   |     9 |     14 | string  | Format `YYYYMMDDHHmmss`                    |
| `temperature` |    23 |      8 | decimal | `impliedDecimal:3` — see below             |
| `humidity`    |    31 |      6 | decimal | `impliedDecimal:2`                         |
| `pressure`    |    37 |      8 | decimal | `impliedDecimal:2`                         |
| `quality_flag`|    45 |      1 | string  | `G` = good, `B` = bad                     |
| (filler)      |    46 |     15 | —       | Unused padding to reach 60 bytes           |

Positions are **1-based** (matching the `.datapipeline` schema).

---

## Implied Decimal Explained

Mainframe systems often store numeric values as packed integers without a decimal
point to avoid transmitting the `.` character. The `impliedDecimal` property
tells the engine where to insert the decimal point:

```
Raw bytes in file: 00023456
impliedDecimal: 3
Parsed value:    23.456
```

```
Raw bytes in file: 007512
impliedDecimal: 2
Parsed value:    75.12
```

```
Raw bytes in file: 10132500
impliedDecimal: 2
Parsed value:    101325.00
```

The formula is: `actual_value = integer_value / 10^impliedDecimal`

---

## EBCDIC-037 Encoding

EBCDIC (Extended Binary Coded Decimal Interchange Code) is a character encoding
used on IBM mainframe systems. Code page 037 is the US English variant. It
differs substantially from ASCII — for example:

| Character | ASCII (hex) | EBCDIC-037 (hex) |
|-----------|-------------|------------------|
| `A`       | 0x41        | 0xC1             |
| `0`       | 0x30        | 0xF0             |
| `G`       | 0x47        | 0xC7             |
| space     | 0x20        | 0x40             |

The engine holds an internal 256-entry lookup table indexed by each EBCDIC byte
value. When `encoding: "ebcdic-037"` is set, every byte read from the file is
passed through this table before field boundaries are applied. The translation
table covers all 256 possible byte values, including:

- Printable characters (letters, digits, punctuation)
- Control characters (mapped to their Unicode equivalents or substitutes)
- IBM-specific characters (cent sign, logical NOT, etc.)

The full table is tested in the engine's unit test suite against the official
IBM EBCDIC-037 character mapping.

---

## How to Obtain Real EBCDIC Test Data

1. **From a mainframe job (JCL):** Run a REPRO or IEBGENER job to copy a VSAM
   or sequential dataset to a USS file, then transfer via SFTP (binary mode,
   not ASCII mode — ASCII mode would corrupt the data by translating it).

2. **Using `dd` on USS:** On z/OS Unix System Services you can convert with:
   ```sh
   iconv -f IBM-037 -t UTF-8 input.ebcdic > output.utf8
   ```
   Run this on-platform and commit the UTF-8 output if you want a readable file,
   or commit the raw EBCDIC binary for round-trip fidelity testing.

3. **Synthetic generation (Python):** Install the `ebcdic` package and encode
   your test strings:
   ```python
   import ebcdic
   record = "SEN00001202607010800000002345600751210132500G               "
   encoded = record.encode('cp037')   # 60 binary bytes
   with open('sensor_readings.dat', 'wb') as f:
       f.write(encoded)
   ```
   This produces a true binary EBCDIC-037 file the engine will decode correctly.

---

## Pipeline Configuration Reference

```json
{
  "source": {
    "type": "fixedwidth",
    "path": "./data/sensor_readings.dat",
    "encoding": "ebcdic-037",
    "recordLength": 60,
    "skipLines": 0,
    "columns": [
      { "name": "sensor_id",    "type": "string",  "start": 1,  "length": 8,  "trim": true },
      { "name": "timestamp",    "type": "string",  "start": 9,  "length": 14 },
      { "name": "temperature",  "type": "decimal", "start": 23, "length": 8,  "impliedDecimal": 3 },
      { "name": "humidity",     "type": "decimal", "start": 31, "length": 6,  "impliedDecimal": 2 },
      { "name": "pressure",     "type": "decimal", "start": 37, "length": 8,  "impliedDecimal": 2 },
      { "name": "quality_flag", "type": "string",  "start": 45, "length": 1 }
    ]
  }
}
```

### Key `fixedwidth` source properties

| Property         | Required | Description                                                   |
|------------------|----------|---------------------------------------------------------------|
| `path`           | yes      | Path to the flat file (absolute or relative to pipeline file) |
| `encoding`       | yes      | `"utf-8"` or `"ebcdic-037"`                                   |
| `recordLength`   | yes      | Number of bytes per record (excluding line terminator)        |
| `skipLines`      | no       | Number of header lines to skip (default `0`)                  |
| `columns[].start`| yes      | 1-based byte offset of the field within the record            |
| `columns[].length`| yes     | Number of bytes the field occupies                            |
| `columns[].trim` | no       | Strip leading/trailing whitespace after extraction            |
| `columns[].impliedDecimal` | no | Insert decimal point N places from the right     |
| `columns[].format`| no      | Date/time parse format for `date`/`datetime` typed columns    |

---

## Sample Data Walkthrough

The file contains 10 sensor readings from station SEN00001 through SEN00010,
all timestamped on 2026-07-01. Three records (SEN00003, SEN00006, SEN00008)
carry `quality_flag = B` and are dropped by the filter. The remaining seven
pass through.

| Sensor   | Timestamp      | Temp (C) | Humidity (%) | Pressure (hPa) | Flag |
|----------|----------------|----------|--------------|----------------|------|
| SEN00001 | 20260701 08:00 |   23.456 |        75.12 |      101325.00 | G    |
| SEN00002 | 20260701 08:05 |   24.781 |        73.25 |      101289.00 | G    |
| SEN00003 | 20260701 08:10 |   19.923 |        81.67 |      101356.00 | **B** |
| SEN00004 | 20260701 08:15 |   21.034 |        78.90 |      101302.00 | G    |
| SEN00005 | 20260701 08:20 |   25.612 |        72.01 |      101267.00 | G    |
| SEN00006 | 20260701 08:25 |   18.456 |        89.34 |      101389.00 | **B** |
| SEN00007 | 20260701 08:30 |   22.345 |        76.45 |      101315.00 | G    |
| SEN00008 | 20260701 08:35 |   26.789 |        70.12 |      101243.00 | **B** |
| SEN00009 | 20260701 08:40 |   20.567 |        80.23 |      101337.00 | G    |
| SEN00010 | 20260701 08:45 |   23.901 |        77.56 |      101298.00 | G    |

Expected output: **7 rows** emitted to the sink (null sink discards them; the
engine logs the row count).

---

## How to Run

```bash
# From the repository root
./flogo-datapipeline run \
  examples/05-fixed-width-ebcdic/sensor-readings-ebcdic.datapipeline
```

Or from within this example directory:

```bash
../../flogo-datapipeline run sensor-readings-ebcdic.datapipeline
```

### Expected Log Output

```
INFO  source=fixedwidth  records_read=10
INFO  transform=filter   records_in=10  records_out=7  records_dropped=3
INFO  sink=null          records_written=7
INFO  pipeline finished  elapsed=12ms
```

---

## How to Verify

1. **Row count:** Confirm the log shows `records_read=10`, `records_dropped=3`,
   `records_written=7`.

2. **Implied decimal:** Add a `map` transform before the filter (temporarily)
   to log the `temperature` field. The raw bytes `00023456` should appear as
   the decimal value `23.456`.

3. **Filter correctness:** Change `quality_flag = 'G'` to `quality_flag = 'B'`
   and re-run. You should see `records_written=3` (the three bad records) and
   `records_dropped=7`.

4. **EBCDIC round-trip (optional):** Use Python's `ebcdic` library to encode
   the sample file as true EBCDIC-037 binary and re-run — the output counts
   should be identical.

---

## Related Examples

- **01-csv-to-null** — CSV source basics; good first example.
- **03-parquet-source** — columnar binary format vs. fixed-width text.
- **06-xml-source** — hierarchical text format with XPath field extraction.
