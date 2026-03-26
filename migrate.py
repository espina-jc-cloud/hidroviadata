"""
migrate.py
──────────
One-time migration: creates hidroviadata.db and loads all data from the
existing JSON files into three base tables, then derives a fourth.

Tables
──────
  shipments             ← output/data.json               (770 records)
  vessel_profiles       ← output/vessel_profiles.json    (173 records)
  vessel_candidates     ← output/buques_en_ruta.json     (varies)
  fertilizer_core_fleet ← derived from vessel_profiles   (fertilizer_visits ≥ 2)

JSON columns (lists / dicts) are stored as JSON text and re-serialised
by the API layer before being returned to the frontend.

Usage
─────
    python3 migrate.py          # creates / re-creates hidroviadata.db
    python3 migrate.py --reset  # drops all tables first, then re-creates
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from build_core_fleet import build as _build_core_fleet

BASE     = Path(__file__).parent
DB_PATH  = BASE / 'hidroviadata.db'
DB_TMP   = BASE / 'hidroviadata.db.tmp'   # atomic swap target for --reset
DATA     = BASE / 'output' / 'data.json'
PROFILES = BASE / 'output' / 'vessel_profiles.json'
BUQUES   = BASE / 'output' / 'buques_en_ruta.json'

RESET = '--reset' in sys.argv


# ── Schema ────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS shipments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    buque       TEXT,
    agencia     TEXT,
    eta         TEXT,
    material    TEXT,
    cliente     TEXT,
    tons        REAL,
    operador    TEXT,
    operacion   TEXT,
    muelle      TEXT,
    sector      TEXT,
    origen      TEXT,
    source_id   TEXT,   -- PDF filename that created this row
    source_date TEXT    -- Issue date of that PDF (YYYY-MM-DD)
);

CREATE TABLE IF NOT EXISTS vessel_profiles (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_name                TEXT UNIQUE NOT NULL,
    visits_to_argentina        INTEGER,
    fertilizer_visits          INTEGER,
    first_seen_date            TEXT,
    last_seen_date             TEXT,
    dominant_product           TEXT,
    dominant_origin            TEXT,
    dominant_importer          TEXT,
    avg_tonnage                REAL,
    min_tonnage                INTEGER,
    max_tonnage                INTEGER,
    main_ports_in_argentina    TEXT,   -- JSON array
    seasonality_by_month       TEXT,   -- JSON object
    confidence_inputs_available REAL
);

CREATE TABLE IF NOT EXISTS vessel_candidates (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_name           TEXT,
    last_position         TEXT,
    last_port             TEXT,
    ais_destination       TEXT,
    eta_estimated         TEXT,
    probable_product      TEXT,
    probable_importer     TEXT,
    probable_tonnage_range TEXT,  -- JSON array [min, max] or null
    probability_score        INTEGER,
    probability_level        TEXT,
    prediction_status        TEXT,   -- predicted / confirmed / expired
    scoring_reasons          TEXT,   -- JSON array
    confirmed_eta            TEXT,   -- ISO date when vessel appeared in shipments
    confirmed_match_reason   TEXT,   -- brief description of the matching shipment row
    created_at               TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quality_reports (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TEXT NOT NULL,
    source_date   TEXT,
    source_id     TEXT,
    status        TEXT NOT NULL,   -- PASS / WARNING / BLOCK
    blocks_json   TEXT,            -- JSON array of block reasons
    warnings_json TEXT,            -- JSON array of warning reasons
    summary_json  TEXT             -- JSON object with computed metrics
);
"""

DROP_DDL = """
DROP TABLE IF EXISTS fertilizer_core_fleet;
DROP TABLE IF EXISTS quality_reports;
DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS vessel_profiles;
DROP TABLE IF EXISTS vessel_candidates;
"""


def compute_quality_report(records: list[dict]) -> dict:
    """
    Compute BLOCK / WARNING / PASS purely from the in-memory records list
    (from output/data.json) and the existing real DB (if present).

    No intermediate files. Source of truth is the DB for previous-lineup
    comparison; the incoming records list for all new-lineup checks.

    Duplicate-PDF check is skipped when RESET=True because the DB is
    being fully rebuilt — re-importing all source_ids is expected.
    """
    now_str = datetime.now().isoformat(timespec='seconds')

    # ── Newest source_date and its rows ───────────────────────────────────
    dates = sorted(
        {r.get('source_date') for r in records if r.get('source_date')}, reverse=True
    )
    if not dates:
        return {
            'timestamp': now_str, 'source_date': None, 'source_id': None,
            'status': 'BLOCK',
            'blocks': ['No source_date found in any record.'],
            'warnings': [], 'summary': {},
        }

    newest_date = dates[0]
    newest_rows = [r for r in records if r.get('source_date') == newest_date]
    source_ids  = sorted({r.get('source_id') for r in newest_rows if r.get('source_id')})
    source_id   = source_ids[0] if source_ids else None
    n_rows      = len(newest_rows)
    tons_list   = [r['tons'] for r in newest_rows if r.get('tons') is not None]
    total_tons  = sum(tons_list)

    # ── Previous lineup from REAL DB (not the temp being built) ──────────
    prev_tons = None
    dup_count = 0
    if DB_PATH.exists():
        try:
            con = sqlite3.connect(str(DB_PATH))
            row = con.execute(
                'SELECT source_date FROM shipments ORDER BY source_date DESC LIMIT 1'
            ).fetchone()
            if row and row[0] and row[0] != newest_date:
                pt = con.execute(
                    'SELECT sum(tons) FROM shipments WHERE source_date=? AND tons IS NOT NULL',
                    (row[0],),
                ).fetchone()
                prev_tons = float(pt[0]) if pt and pt[0] else None
            # Duplicate check — only meaningful for non-reset runs
            if source_id and not RESET:
                dc = con.execute(
                    'SELECT count(*) FROM shipments WHERE source_id=?', (source_id,)
                ).fetchone()
                dup_count = dc[0] if dc else 0
            con.close()
        except Exception:
            pass

    # ── Ton delta ─────────────────────────────────────────────────────────
    delta_pct = None
    if prev_tons and prev_tons > 0 and total_tons > 0:
        delta_pct = (total_tons - prev_tons) / prev_tons

    # ── Ratio metrics ─────────────────────────────────────────────────────
    miss_cli = sum(1 for r in newest_rows if not (r.get('cliente') or '').strip())
    miss_mat = sum(1 for r in newest_rows
                   if not (r.get('material') or '').strip()
                   or r.get('material') == 'UNKNOWN')

    today    = datetime.now().date()
    old_etas = []
    for r in newest_rows:
        try:
            eta_dt = datetime.fromisoformat(r['eta']).date()
            if (today - eta_dt).days > 30:
                old_etas.append(f"{r.get('buque')} eta={r['eta']}")
        except (KeyError, ValueError, TypeError):
            pass

    # ── BLOCK checks ──────────────────────────────────────────────────────
    blocks: list[str] = []

    if n_rows == 0:
        blocks.append('Newest lineup has 0 rows.')
    if delta_pct is not None and abs(delta_pct) > 0.25:
        blocks.append(
            f'Tons delta {delta_pct:+.1%} exceeds ±25% '
            f'(prev={prev_tons:,.0f}  new={total_tons:,.0f}).'
        )
    short = [r.get('buque', '') for r in newest_rows if len(r.get('buque') or '') < 3]
    if short:
        blocks.append(f'Vessel name < 3 chars: {short[:5]}')
    mega = [(r.get('buque'), r.get('tons'))
            for r in newest_rows if (r.get('tons') or 0) > 100_000]
    if mega:
        blocks.append(f'Single record tons > 100,000: {mega[:3]}')
    if dup_count > 0:
        blocks.append(f'Duplicate PDF: "{source_id}" already in DB ({dup_count} rows).')

    # ── WARNING checks ────────────────────────────────────────────────────
    warnings: list[str] = []

    if delta_pct is not None and 0.10 <= abs(delta_pct) <= 0.25:
        warnings.append(
            f'Tons delta {delta_pct:+.1%} in 10–25% caution band '
            f'(prev={prev_tons:,.0f}  new={total_tons:,.0f}).'
        )
    if n_rows > 0:
        if miss_cli / n_rows > 0.05:
            warnings.append(
                f'Missing cliente: {miss_cli}/{n_rows} rows ({miss_cli/n_rows:.1%}).'
            )
        if miss_mat / n_rows > 0.03:
            warnings.append(
                f'Missing material: {miss_mat}/{n_rows} rows ({miss_mat/n_rows:.1%}).'
            )
    if old_etas:
        warnings.append(f'ETA >30 days in past: {old_etas[:3]}')

    status = 'BLOCK' if blocks else ('WARNING' if warnings else 'PASS')

    return {
        'timestamp':   now_str,
        'source_date': newest_date,
        'source_id':   source_id,
        'status':      status,
        'blocks':      blocks,
        'warnings':    warnings,
        'summary': {
            'n_rows':           n_rows,
            'total_tons':       total_tons,
            'prev_tons':        prev_tons,
            'delta_pct':        round(delta_pct * 100, 2) if delta_pct is not None else None,
            'missing_cliente':  miss_cli,
            'missing_material': miss_mat,
            'old_eta_count':    len(old_etas),
        },
    }


def _print_quality_report(report: dict) -> None:
    """Print quality report to stdout."""
    status  = report['status']
    icon    = {'PASS': '✅', 'WARNING': '⚠️ ', 'BLOCK': '❌'}.get(status, '?')
    div     = '═' * 58
    print(f'\n{div}')
    print(f'  {icon} QUALITY GATE: {status}   '
          f"source={report.get('source_id') or '?'}")
    for b in report.get('blocks', []):
        print(f'     BLOCK   • {b}')
    for w in report.get('warnings', []):
        print(f'     WARNING • {w}')
    s = report.get('summary', {})
    dpct = s.get('delta_pct')
    print(f"     rows={s.get('n_rows')}  tons={s.get('total_tons') or 0:,.0f}  "
          f"delta={f'{dpct:+}%' if dpct is not None else 'n/a'}  "
          f"miss_cli={s.get('missing_cliente')}  miss_mat={s.get('missing_material')}")
    print(div + '\n')


def _write_quality_report(con: sqlite3.Connection, report: dict) -> None:
    """Persist the quality report to the quality_reports table."""
    con.execute(
        'INSERT INTO quality_reports '
        '  (timestamp, source_date, source_id, status, blocks_json, warnings_json, summary_json) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)',
        (
            report.get('timestamp'),
            report.get('source_date'),
            report.get('source_id'),
            report.get('status'),
            json.dumps(report.get('blocks', [])),
            json.dumps(report.get('warnings', [])),
            json.dumps(report.get('summary', {})),
        ),
    )


def migrate() -> None:
    # ── 1. Load records from data.json into memory ────────────────────────
    records = json.loads(DATA.read_text(encoding='utf-8'))

    # ── 2. Compute quality report (reads REAL DB for deltas; no temp file) ─
    quality_report = compute_quality_report(records)
    _print_quality_report(quality_report)

    # ── 3. BLOCK → abort without touching any DB ─────────────────────────
    if quality_report['status'] == 'BLOCK':
        print('  ❌ BLOCK — DB NOT modified.  Fix issues above and re-run.')
        sys.exit(1)

    # ── 4. Choose write target ────────────────────────────────────────────
    # --reset: build into a temp file first, then atomically swap → real DB
    #   so the real DB is never half-built if something fails mid-migration.
    # non-reset: write directly (CREATE TABLE IF NOT EXISTS + INSERT).
    if RESET:
        db_target = DB_TMP
        if db_target.exists():
            db_target.unlink()
    else:
        db_target = DB_PATH

    con = sqlite3.connect(str(db_target))
    cur = con.cursor()

    # Temp DB (--reset) is always fresh. Non-reset uses CREATE TABLE IF NOT EXISTS.
    cur.executescript(DDL)

    # ── Shipments ──────────────────────────────────────────────────────────────
    cur.executemany(
        """
        INSERT INTO shipments
            (buque, agencia, eta, material, cliente, tons,
             operador, operacion, muelle, sector, origen,
             source_id, source_date)
        VALUES
            (:buque, :agencia, :eta, :material, :cliente, :tons,
             :operador, :operacion, :muelle, :sector, :origen,
             :source_id, :source_date)
        """,
        [
            {
                'buque':       r.get('buque')       or '',
                'agencia':     r.get('agencia')     or '',
                'eta':         r.get('eta')          or '',
                'material':    r.get('material')    or '',
                'cliente':     r.get('cliente')     or '',
                'tons':        r.get('tons'),
                'operador':    r.get('operador')    or '',
                'operacion':   r.get('operacion')   or '',
                'muelle':      r.get('muelle')      or '',
                'sector':      r.get('sector')      or '',
                'origen':      r.get('origen')      or '',
                'source_id':   r.get('source_id'),
                'source_date': r.get('source_date'),
            }
            for r in records
        ],
    )
    n_shipments = cur.rowcount
    print(f"  shipments          : {n_shipments:>4} rows inserted")

    # ── Vessel profiles ────────────────────────────────────────────────────────
    profiles = json.loads(PROFILES.read_text(encoding='utf-8'))
    cur.executemany(
        """
        INSERT OR REPLACE INTO vessel_profiles
            (vessel_name, visits_to_argentina, fertilizer_visits,
             first_seen_date, last_seen_date,
             dominant_product, dominant_origin, dominant_importer,
             avg_tonnage, min_tonnage, max_tonnage,
             main_ports_in_argentina, seasonality_by_month,
             confidence_inputs_available)
        VALUES
            (:vessel_name, :visits, :fert_visits,
             :first_seen, :last_seen,
             :dom_product, :dom_origin, :dom_importer,
             :avg_t, :min_t, :max_t,
             :ports, :season,
             :confidence)
        """,
        [
            {
                'vessel_name':  p['vessel_name'],
                'visits':       p.get('visits_to_argentina'),
                'fert_visits':  p.get('fertilizer_visits'),
                'first_seen':   p.get('first_seen_date'),
                'last_seen':    p.get('last_seen_date'),
                'dom_product':  p.get('dominant_product'),
                'dom_origin':   p.get('dominant_origin'),
                'dom_importer': p.get('dominant_importer'),
                'avg_t':        p.get('avg_tonnage'),
                'min_t':        p.get('min_tonnage'),
                'max_t':        p.get('max_tonnage'),
                'ports':        json.dumps(p.get('main_ports_in_argentina') or [], ensure_ascii=False),
                'season':       json.dumps(p.get('seasonality_by_month') or {}, ensure_ascii=False),
                'confidence':   p.get('confidence_inputs_available'),
            }
            for p in profiles
        ],
    )
    n_profiles = cur.rowcount
    print(f"  vessel_profiles    : {n_profiles:>4} rows inserted")

    # ── Vessel candidates ──────────────────────────────────────────────────────
    candidates = json.loads(BUQUES.read_text(encoding='utf-8'))
    cur.executemany(
        """
        INSERT INTO vessel_candidates
            (vessel_name, last_position, last_port, ais_destination, eta_estimated,
             probable_product, probable_importer, probable_tonnage_range,
             probability_score, probability_level, prediction_status, scoring_reasons)
        VALUES
            (:vessel_name, :last_position, :last_port, :ais_destination, :eta_estimated,
             :probable_product, :probable_importer, :probable_tonnage_range,
             :probability_score, :probability_level, :prediction_status, :scoring_reasons)
        """,
        [
            {
                'vessel_name':           c.get('vessel_name'),
                'last_position':         c.get('last_position'),
                'last_port':             c.get('last_port'),
                'ais_destination':       c.get('ais_destination'),
                'eta_estimated':         c.get('eta_estimated'),
                'probable_product':      c.get('probable_product'),
                'probable_importer':     c.get('probable_importer'),
                'probable_tonnage_range': json.dumps(c.get('probable_tonnage_range'), ensure_ascii=False),
                'probability_score':     c.get('probability_score'),
                'probability_level':     c.get('probability_level'),
                'prediction_status':     c.get('prediction_status'),
                'scoring_reasons':       json.dumps(c.get('scoring_reasons') or [], ensure_ascii=False),
            }
            for c in candidates
        ],
    )
    n_candidates = cur.rowcount
    print(f"  vessel_candidates  : {n_candidates:>4} rows inserted")

    con.commit()

    # ── Fertilizer core fleet (derived from vessel_profiles) ───────────────────
    core_fleet = _build_core_fleet(con)
    print(f"  fertilizer_core_fleet : {len(core_fleet):>3} rows derived  "
          f"(fertilizer_visits ≥ 2)")

    # ── Persist quality report ─────────────────────────────────────────────
    _write_quality_report(con, quality_report)
    con.commit()
    print(f"  quality_reports    :   1 row written  (status={quality_report['status']})")

    con.close()

    # ── 5. For --reset: atomic swap temp → real ───────────────────────────
    if RESET:
        db_target.replace(DB_PATH)   # atomic on POSIX (Railway/Linux)
        print(f"  → swapped {db_target.name} → {DB_PATH.name}")

    print(f"\nDatabase written → {DB_PATH}")


if __name__ == '__main__':
    print(f"Migrating to {DB_PATH} …")
    migrate()
    print("Done.")
