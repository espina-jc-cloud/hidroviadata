"""
db_status.py
────────────
Print a quick freshness snapshot of hidroviadata.db and save output/db_status.md.

Usage
─────
    python3 db_status.py
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

BASE    = Path(__file__).parent
DB_PATH = BASE / 'hidroviadata.db'
OUT_MD  = BASE / 'output' / 'db_status.md'


def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run  python3 migrate.py  first.")
        raise SystemExit(1)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # ── Core counts ────────────────────────────────────────────────────────────
    total_rows     = con.execute("SELECT count(*) FROM shipments").fetchone()[0]
    eta_min        = con.execute("SELECT min(eta) FROM shipments WHERE eta != ''").fetchone()[0]
    eta_max        = con.execute("SELECT max(eta) FROM shipments WHERE eta != ''").fetchone()[0]
    latest_src     = con.execute("SELECT max(source_date) FROM shipments").fetchone()[0]
    null_tons      = con.execute("SELECT count(*) FROM shipments WHERE tons IS NULL").fetchone()[0]
    no_material    = con.execute("SELECT count(*) FROM shipments WHERE material = '' OR material IS NULL").fetchone()[0]
    candidates     = con.execute("SELECT count(*) FROM vessel_candidates").fetchone()[0]
    confirmed      = con.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='confirmed'").fetchone()[0]
    expired        = con.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='expired'").fetchone()[0]
    predicted      = con.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='predicted'").fetchone()[0]

    # ── Last 10 source_ids ──────────────────────────────────────────────────────
    last_sources = con.execute(
        """SELECT source_id, source_date, count(*) as rows
           FROM shipments
           WHERE source_id IS NOT NULL
           GROUP BY source_id
           ORDER BY source_date DESC, source_id DESC
           LIMIT 10"""
    ).fetchall()

    # ── Records per import batch ────────────────────────────────────────────────
    batches = con.execute(
        """SELECT source_date, count(*) as rows,
                  sum(CASE WHEN tons IS NOT NULL THEN 1 ELSE 0 END) as with_tons
           FROM shipments
           WHERE source_date IS NOT NULL
           GROUP BY source_date
           ORDER BY source_date DESC"""
    ).fetchall()

    con.close()

    # ── Build output ───────────────────────────────────────────────────────────
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    lines = [
        f"# HidrovíaData — DB Status",
        f"",
        f"_Generated: {now}_",
        f"",
        f"## Shipments",
        f"",
        f"| Field | Value |",
        f"|---|---|",
        f"| Total rows | {total_rows:,} |",
        f"| Earliest ETA | {eta_min or '—'} |",
        f"| Latest ETA | {eta_max or '—'} |",
        f"| Most recent source_date | {latest_src or '—'} |",
        f"| Null-tonnage rows | {null_tons} |",
        f"| Missing-material rows | {no_material} |",
        f"",
        f"## Vessel Candidates",
        f"",
        f"| Status | Count |",
        f"|---|---|",
        f"| predicted | {predicted} |",
        f"| confirmed | {confirmed} |",
        f"| expired | {expired} |",
        f"| **total** | **{candidates}** |",
        f"",
        f"## Last 10 Import Batches (source_id)",
        f"",
        f"| source_id | source_date | rows |",
        f"|---|---|---|",
    ]
    for row in last_sources:
        lines.append(f"| {row['source_id']} | {row['source_date'] or '—'} | {row['rows']} |")

    lines += [
        f"",
        f"## Rows per source_date (all batches)",
        f"",
        f"| source_date | rows | rows_with_tons |",
        f"|---|---|---|",
    ]
    for b in batches:
        lines.append(f"| {b['source_date']} | {b['rows']} | {b['with_tons']} |")

    md = "\n".join(lines) + "\n"
    OUT_MD.parent.mkdir(exist_ok=True)
    OUT_MD.write_text(md, encoding='utf-8')

    # ── Print to stdout ─────────────────────────────────────────────────────────
    print(f"╔══════════════════════════════════════════════╗")
    print(f"║  HidrovíaData — DB Status  ({now})  ║")
    print(f"╚══════════════════════════════════════════════╝")
    print(f"  Total shipments   : {total_rows:,}")
    print(f"  ETA range         : {eta_min or '—'} → {eta_max or '—'}")
    print(f"  Latest import     : {latest_src or '—'}")
    print(f"  Null-ton rows     : {null_tons}")
    print(f"  Candidates        : {candidates}  "
          f"(predicted={predicted} confirmed={confirmed} expired={expired})")
    print()
    print(f"  Last imports:")
    for row in last_sources[:5]:
        print(f"    {row['source_date'] or '—'}  {row['source_id']:<55} {row['rows']:>4} rows")
    print()
    print(f"  → {OUT_MD}")


if __name__ == '__main__':
    main()
