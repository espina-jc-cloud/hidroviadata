# HidrovíaData — Daily Runbook
**For non-technical users · Estimated time: 5–10 minutes/day**

---

## What you need open
- Terminal (or your shortcut to run Python)
- VesselFinder / MarineTraffic tab — "Expected Arrivals" for San Nicolás / Recalada
- This runbook open alongside

---

## STEP 1 — Ingest a new lineup PDF (do this when you receive a new lineup)

1. Copy the PDF file into the `pdfs/` folder inside the project.
2. Open Terminal, go to the project folder:
   ```
   cd ~/Desktop/hidroviadata
   ```
3. Run the two commands:
   ```
   python3 parser.py
   python3 migrate.py --reset
   ```
4. You should see something like:
   ```
   Registros finales: 585
   shipments: 585 rows inserted
   ```
5. Done. The dashboard now shows the updated data.

> **How often?** Every time a new lineup PDF arrives (typically weekly).

---

## STEP 2 — Add AIS observations (do this daily, 5–10 min)

1. Open VesselFinder or MarineTraffic.
2. Go to "Expected Arrivals" for the Paraná / San Nicolás area.
3. For each vessel that looks interesting, note:
   - Vessel name
   - Last port (where it came from)
   - AIS destination (what it says on the ship's transponder)
   - Vessel type (usually "Bulk Carrier")
   - DWT if visible (gross tonnage in thousands)

4. Open the file `output/templates/observations.json` in any text editor.
   It looks like this:
   ```json
   [
     {
       "vessel_name":     "CLIPPER I-STAR",
       "last_port":       "MESAIEED",
       "ais_destination": "RECALADA",
       "vessel_type":     "BULK CARRIER",
       "dwt":             28500
     }
   ]
   ```
5. Replace the example entries with today's vessels. Save the file.
   > **Tip**: You can leave `"dwt": null` if you don't have the tonnage.

6. Run:
   ```
   python3 detect_candidates.py output/templates/observations.json
   ```

7. For each vessel you'll see a score card:
   ```
   Score : 80 / 80  [████████████████████]  HIGH  (predicted)
   Product : MAP
   Importer: YPF
   ```
   - **HIGH (≥80)** → very likely a fertilizer arrival, worth tracking
   - **MEDIUM (60–79)** → possible, monitor
   - **LOW (<60)** → low signal, probably not fertilizer

8. Results are automatically saved in the dashboard (Buques en Ruta tab).

> **Dry-run first** (preview without saving):
> ```
> python3 detect_candidates.py output/templates/observations.json --dry-run
> ```

---

## STEP 3 — Check DB freshness (optional, 1 min)

```
python3 db_status.py
```

This tells you:
- Which was the last lineup loaded and when
- How many records are in the DB
- ETA range (earliest → latest)

A file is also saved to `output/db_status.md`.

---

## STEP 4 — Run verification weekly (every Friday)

This checks whether your predicted vessels actually showed up in the lineups.

```
python3 verify_candidates.py
```

Output:
```
Confirmed : 3
Expired   : 1
Predicted : 5

Confirmation rate:
  high   : 3/4 confirmed  (75%)
  medium : 0/1 confirmed  (0%)
```

Results are saved to `output/track_record.md`.

> **What to watch**: If your HIGH confidence confirmation rate drops below 50%,
> something is off with the scoring — review the latest predictions.

---

## STEP 5 — Regenerate dedupe report (after ingesting new PDFs)

```
python3 generate_dedupe_report.py
```

This verifies no new duplicates crept in. Check `output/dedupe_report.md`.

---

## Full rebuild (when something goes wrong)

Rebuilds everything from the raw PDFs:

```
python3 parser.py
python3 migrate.py --reset
```

This is safe to run at any time. It drops and recreates the DB from scratch.

---

## Numbers to watch daily

| Metric | Where | Healthy value |
|---|---|---|
| Candidates HIGH score today | Terminal output after detect_candidates | ≥ 1 per session |
| Confirmation rate (HIGH) | track_record.md | ≥ 60% |
| Expired HIGH predictions | track_record.md | Should be rare |
| Latest source_date | db_status.md | Within 7 days |

---

## File map (where things live)

```
hidroviadata/
├── pdfs/                        ← put new lineup PDFs here
├── output/
│   ├── data.json                ← current clean shipments (auto-generated)
│   ├── db_status.md             ← freshness report (python3 db_status.py)
│   ├── track_record.md          ← prediction performance (python3 verify_candidates.py)
│   ├── dedupe_report.md         ← dedup proof (python3 generate_dedupe_report.py)
│   └── templates/
│       └── observations.json    ← edit this with today's AIS observations
├── hidroviadata.db              ← the database (auto-managed, do not edit)
├── parser.py                    ← reads PDFs → data.json
├── migrate.py                   ← data.json → database
├── detect_candidates.py         ← AIS observations → vessel_candidates
└── verify_candidates.py         ← update prediction status (confirmed/expired)
```

---

## Quick reference — all commands

```bash
# Ingest new PDF:
python3 parser.py && python3 migrate.py --reset

# Daily AIS observations:
python3 detect_candidates.py output/templates/observations.json

# Preview without saving:
python3 detect_candidates.py output/templates/observations.json --dry-run

# Weekly verification:
python3 verify_candidates.py

# DB freshness check:
python3 db_status.py

# Full rebuild:
python3 parser.py && python3 migrate.py --reset

# Start the dashboard:
python3 app.py
# then open: http://localhost:5000
```

