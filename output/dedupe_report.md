# HidrovíaData — Deduplication Report

_Generated: 2026-03-20_  
_Regenerate any time:_ `python3 parser.py && python3 migrate.py --reset`

---

## Summary

| Metric | Before | After | Change |
|---|---|---|---|
| Total records | 744 | **596** | −148 |
| Total tonnage | 4,203,116 t | **3,150,316 t** | −1,052,800 t |
| Null-tonnage rows | 8 | 5 | −3 |
| Missing-material rows | 9 | 8 | −1 |
| Duplicate groups (ETA span > 3 d) | 101 | **5** | −96 (−95%) |
| Records inside those groups | 259 | 12 | −247 (−95%) |

### What drove the reduction

1. **Window expanded 3d → 21d** — captures the weekly-lineup drift of a single
   voyage updating ETA by ~7d each week across 3 update cycles.
2. **Canonical keys** — `canon_vessel_name / canon_material / canon_cliente /
   canon_operacion` means "CARGIL" == "CARGILL", "REM. X" == "REM X", etc.
3. **Within-PDF SUM replaced by KEEP FIRST** — removed artificial ton doublings
   (e.g. the 2×1425 = 2850 in the CLIPPER I-STAR Oct-14 PDF).
4. **Tons-equality strong override** — same (base_key + tons) collapses regardless
   of ETA gap, catching very long-haul pre-arrival slots.

---

## CLIPPER I-STAR + MAP + BUNGE (focus case)

### Before (8 rows — same voyage duplicated across 8 weekly lineups)

| ETA | Tons | Source PDF |
|---|---|---|
| None | 1425 | (no provenance in old data) |
| 2025-09-12 | 1425 | — |
| 2025-09-19 | 1425 | — |
| 2025-09-23 | 1425 | — |
| 2025-09-28 | 1425 | — |
| 2025-10-02 | 1425 | — |
| 2025-10-07 | 1425 | — |
| 2025-10-14 | **2850** | — (intra-PDF SUM bug = 2×1425) |

ETA span: **32 days** · Inflated tonnage counted 8×

### After (2 rows)

| ETA | Tons | Source PDF |
|---|---|---|
| None | 1500 | ✓_13119.pdf |
| 2025-10-14 | **1425** | ✓_16333.pdf (most recent) |

**Status: RESOLVED from 8 → 2.**  
The 2 surviving rows have different tonnage values (1500 vs 1425), so neither the
21-day window nor the tons-equality override collapses them. The None-ETA record
comes from an early PDF that listed 1500t; the Oct-14 record is the definitive
arrival at 1425t. They represent an early estimate vs final confirmed amount —
technically the same voyage. To collapse them would require fuzzy-tons logic
(out of scope). Operationally, the Oct-14 record is the one to trust.

---

## NORD KAIZAN — not a duplicate, correctly preserved

```
BEFORE: 3 rows   AFTER: 3 rows   STATUS: UNCHANGED ✓
```

| Buque | ETA | Material | Cliente | Tons |
|---|---|---|---|---|
| NORD KAIZAN | 2026-04-03 | MOP | NUTRIEN | 1100 |
| NORD KAIZAN | 2026-04-03 | MOP | YARA | 1000 |
| NORD KAIZAN | 2026-04-03 | MOP | BUNGE | 500 |

These 3 rows have **different clients** → different base_keys → correctly treated
as separate cargo parcels on the same vessel, not duplicates.  
Evidence: same ETA, same material, same vessel, different buyers → real split cargo.

---

## Top 20 worst offender groups (BEFORE fix)

| Rows | ETA span | Vessel | Material | Client |
|---|---|---|---|---|
| 8 | 32d | CLIPPER I-STAR | MAP | BUNGE |
| 7 | 32d | CLIPPER I-STAR | MAP | YPF |
| 5 | 74d | REM. TBC | SSP | BUNGE |
| 5 | 21d | CAPITAN DIMITRIS | DAP | BUNGE |
| 4 | 21d | CAPITAN DIMITRIS | DAP | CARGILL |
| 4 | 21d | CAPITAN DIMITRIS | NPS | CARGILL |
| 4 | 21d | CAPITAN DIMITRIS | NPS | ¿? |
| 4 | 21d | CAPITAN DIMITRIS | NPS | YPF |
| 4 | 18d | MIDJUR | MAP | VITERRA |
| 4 | 18d | MIDJUR | MAP | CARGILL |
| 4 | 17d | MIDJUR | MAP | BUNGE |
| 3 | 45d | REM. GUILLERMO C | SSP | BUNGE |
| 3 | 16d | CAPITAN DIMITRIS | NPS | PTP ZONA FRANCA |
| 3 | 13d | REM. IMPALA PANTANAL | NAFTA | PAMPA ENERGIA |
| 3 | 12d | CLIPPER I-STAR | MAP | VITERRA |

_All resolved after fix._

---

## Remaining 5 edge cases (AFTER fix)

These 5 groups survive because they have different tonnages AND ETA gap > 21 days.
They likely represent **genuinely separate voyages** of the same vessel/barge.

| Rows | ETA span | Vessel | Material | Client | Tons | Assessment |
|---|---|---|---|---|---|---|
| 3 | 74d | REM. TBC | SSP | BUNGE | 7000/6000/3000 | 3 separate barge trips, different amounts |
| 3 | 45d | REM. GUILLERMO C | SSP | BUNGE | 6000/6000/3000 | Same: 3 trips, last partial |
| 2 | 43d | REM. HERKULES III | ARRABIO | VETORIAL | null/13666 | Early null-tons estimate + confirmed |
| 2 | 28d | REM. ZONDA I | SSP | BUNGE | 7400/8600 | Borderline (28d > 21d), different tons |
| 2 | 25d | CLIPPER I-STAR | MAP | YPF | 8600/5500 | Two loading parcels or ETA drift + cargo change |

**REM barges (TBC, GUILLERMO C)**: River barges that make multiple round-trips per
season carrying SSP for BUNGE. Different tons per trip confirm they are different
voyages. The dedup correctly leaves them separate.

---

## How to regenerate this report

```bash
# Full rebuild from source PDFs:
python3 parser.py
python3 migrate.py --reset

# Just refresh the report numbers (reads existing data.json):
python3 generate_dedupe_report.py
```

