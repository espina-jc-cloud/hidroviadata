# HidrovíaData — Ingestion Pipeline Status

_Generated: 2026-03-20_

---

## Ingestion Pipeline (file flow)

```
pdfs/*.pdf
   ↓  parser.py: parse_pdf()
      · pdfplumber extracts tables row-by-row
      · clean_vessel_name / clean_agency / parse_date / etc. clean each cell
      · _pdf_date and _pdf_file tracked as internal fields
   ↓  parser.py: consolidate_within_pdf()
      · Groups by (buque, eta, material, cliente)
      · SUMS tons across duplicate rows within same PDF   ← BUG (see below)
   ↓  parser.py: consolidate_across_pdfs()
      · Sorts by _pdf_date desc, keeps most-recent
      · Dedup window: ±3 days ETA                        ← TOO NARROW
   ↓  output/data.json                                   (770+ records)
   ↓  migrate.py
      · Reads data.json, inserts into shipments table
      · No additional dedup at this stage
   ↓  hidroviadata.db → shipments table
```

---

## Current Shipments Schema

```sql
CREATE TABLE shipments (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    buque     TEXT,
    agencia   TEXT,
    eta       TEXT,
    material  TEXT,
    cliente   TEXT,
    tons      REAL,
    operador  TEXT,
    operacion TEXT,
    muelle    TEXT,
    sector    TEXT,
    origen    TEXT
    -- NO source_id / source_date columns (provenance missing)
);
```

---

## Dedup Logic (BEFORE fix)

| Layer | Where | Key | Window |
|---|---|---|---|
| Intra-PDF | `consolidate_within_pdf()` | (buque, eta, material, cliente) | exact match, sums tons |
| Inter-PDF | `consolidate_across_pdfs()` | (buque, material, cliente) raw strings | ±3 days ETA |

### Problems identified

1. **Window too narrow (3 days)**: Weekly lineups update the same vessel voyage every 7 days.
   A voyage visible in 8 consecutive lineups (CLIPPER I-STAR case) spans 32 days — all 8
   survive because each pair is > 3 days apart after the sliding-window check.

2. **No canonicalization**: "CARGIL" and "CARGILL" treated as different clients. Vessels
   named "REM. DOÑA CARMEN" and "REM.DOÑA CARMEN" treated as different vessels.

3. **Within-PDF SUM is wrong**: `consolidate_within_pdf` sums tons, creating inflated values
   (e.g. CLIPPER I-STAR + BUNGE shows 2850t in Oct 14 PDF = 2×1425 summed, actual = 1425).

4. **No provenance stored**: `_pdf_date` and `_pdf_file` are stripped before data.json,
   so the DB has no record of which import batch created each row.

---

## Provenance (BEFORE fix)

- `_pdf_date`: captured internally in parser, stripped by `strip_internal()` → NOT in DB.
- `_pdf_file`: same — stripped → NOT in DB.
- The DB has **zero provenance fields**. No way to know which lineup created a row.

---

## Key numbers (BEFORE fix, snapshot 2026-03-20)

- Total records in data.json: **744**
- Total tons: **4,203,116**
- Null tons: 8 records
- Empty material: 9 records
- Groups with same (buque+mat+cli) and ETA span > 3 days: **101 groups / 259 records**
- Worst offender: CLIPPER I-STAR | MAP | BUNGE → **8 rows, ETA span 32 days**

---

## What the fix changes (TASK 2)

- Window: 3 days → **21 days**
- Canonicalization: vessel name / material / client / operation used for dedup key
- Within-PDF: SUM → **KEEP FIRST** (no artificial ton doubling)
- Strong override: same base_key + identical tons → collapse regardless of ETA gap
- Provenance: add `source_id` and `source_date` columns to DB

