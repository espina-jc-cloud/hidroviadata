"""
build_core_fleet.py
───────────────────
Derives the fertilizer_core_fleet watchlist from vessel_profiles.

Filter: fertilizer_visits >= 2
Sort:   fertilizer_visits DESC, vessel_name ASC

Outputs
───────
  hidroviadata.db              → table: fertilizer_core_fleet  (drops + recreates)
  output/fertilizer_core_fleet.json

Fields
──────
  vessel_name, fertilizer_visits, visits_to_argentina,
  dominant_product, dominant_origin, dominant_importer,
  avg_tonnage, min_tonnage, max_tonnage, confidence_inputs_available

Usage
─────
    python3 build_core_fleet.py
    (also called automatically at the end of migrate.py)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

BASE     = Path(__file__).parent
DB_PATH  = BASE / 'hidroviadata.db'
OUT_JSON = BASE / 'output' / 'fertilizer_core_fleet.json'

DDL_DROP   = "DROP TABLE IF EXISTS fertilizer_core_fleet;"
DDL_CREATE = """
CREATE TABLE fertilizer_core_fleet (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_name                 TEXT UNIQUE NOT NULL,
    fertilizer_visits           INTEGER NOT NULL,
    visits_to_argentina         INTEGER,
    dominant_product            TEXT,
    dominant_origin             TEXT,
    dominant_importer           TEXT,
    avg_tonnage                 REAL,
    min_tonnage                 INTEGER,
    max_tonnage                 INTEGER,
    confidence_inputs_available REAL
);
"""

QUERY = """
    SELECT
        vessel_name,
        fertilizer_visits,
        visits_to_argentina,
        dominant_product,
        dominant_origin,
        dominant_importer,
        avg_tonnage,
        min_tonnage,
        max_tonnage,
        confidence_inputs_available
    FROM vessel_profiles
    WHERE fertilizer_visits >= 2
    ORDER BY fertilizer_visits DESC, vessel_name ASC
"""


def build(con: sqlite3.Connection) -> list[dict]:
    """
    Derive fertilizer_core_fleet from vessel_profiles already in the DB.
    Drops and recreates the target table, then writes output/fertilizer_core_fleet.json.
    Returns the list of records inserted.
    """
    cur = con.cursor()

    # ── Recreate table ────────────────────────────────────────────────────────
    cur.execute(DDL_DROP)
    cur.execute(DDL_CREATE)

    # ── Pull filtered records from vessel_profiles ────────────────────────────
    con.row_factory = sqlite3.Row
    rows = con.execute(QUERY).fetchall()
    records = [dict(r) for r in rows]

    # ── Insert into fertilizer_core_fleet ─────────────────────────────────────
    cur.executemany(
        """
        INSERT INTO fertilizer_core_fleet
            (vessel_name, fertilizer_visits, visits_to_argentina,
             dominant_product, dominant_origin, dominant_importer,
             avg_tonnage, min_tonnage, max_tonnage,
             confidence_inputs_available)
        VALUES
            (:vessel_name, :fertilizer_visits, :visits_to_argentina,
             :dominant_product, :dominant_origin, :dominant_importer,
             :avg_tonnage, :min_tonnage, :max_tonnage,
             :confidence_inputs_available)
        """,
        records,
    )
    con.commit()

    # ── Write JSON ────────────────────────────────────────────────────────────
    OUT_JSON.parent.mkdir(exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    return records


def _print_summary(records: list[dict]) -> None:
    total      = len(records)
    high_conf  = sum(1 for r in records if (r['confidence_inputs_available'] or 0) >= 0.8)
    med_conf   = sum(1 for r in records if 0.5 <= (r['confidence_inputs_available'] or 0) < 0.8)
    low_conf   = total - high_conf - med_conf

    print(f"fertilizer_core_fleet — {total} vessels")
    print(f"  High confidence (≥ 0.8) : {high_conf}")
    print(f"  Medium confidence       : {med_conf}")
    print(f"  Low confidence (< 0.5)  : {low_conf}")
    print()
    print(f"  {'Vessel':<30}  {'Fert':>5}  {'All':>5}  {'Product':<8}  {'Origin':<12}  Conf")
    print(f"  {'─'*30}  {'─'*5}  {'─'*5}  {'─'*8}  {'─'*12}  ────")
    for r in records[:20]:
        print(
            f"  {r['vessel_name']:<30}  "
            f"{r['fertilizer_visits']:>5}  "
            f"{(r['visits_to_argentina'] or 0):>5}  "
            f"{(r['dominant_product'] or '—'):<8}  "
            f"{(r['dominant_origin'] or '—'):<12}  "
            f"{(r['confidence_inputs_available'] or 0):.3f}"
        )
    if total > 20:
        print(f"  … and {total - 20} more")


if __name__ == '__main__':
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run  python3 migrate.py  first.")
        raise SystemExit(1)

    con = sqlite3.connect(DB_PATH)
    try:
        records = build(con)
    finally:
        con.close()

    _print_summary(records)
    print()
    print(f"  DB table : fertilizer_core_fleet  ({len(records)} rows)")
    print(f"  JSON     : {OUT_JSON}")
