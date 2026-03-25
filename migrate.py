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
from pathlib import Path

from build_core_fleet import build as _build_core_fleet

BASE           = Path(__file__).parent
DB_PATH        = BASE / 'hidroviadata.db'
DATA           = BASE / 'output' / 'data.json'
PROFILES       = BASE / 'output' / 'vessel_profiles.json'
BUQUES         = BASE / 'output' / 'buques_en_ruta.json'
QUALITY_REPORT = BASE / 'output' / 'quality_report.json'

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


def _check_quality_gate() -> dict | None:
    """
    Read output/quality_report.json (written by parser.py).
    - BLOCK  → print reasons and sys.exit(1) WITHOUT touching the DB.
    - WARNING → print reasons but continue.
    - PASS / missing report → continue silently.
    Returns the report dict (or None if no report file).
    """
    if not QUALITY_REPORT.exists():
        print("[quality] No quality_report.json found — run python3 parser.py first.")
        return None

    report = json.loads(QUALITY_REPORT.read_text(encoding='utf-8'))
    status = report.get('status', 'PASS')

    # When --reset is used, the DB will be fully rebuilt from data.json, so a
    # "Duplicate PDF" block is expected and safe — waive it only for --reset.
    blocks = report.get('blocks', [])
    if RESET:
        dup_blocks = [b for b in blocks if b.startswith('Duplicate PDF')]
        if dup_blocks:
            blocks = [b for b in blocks if not b.startswith('Duplicate PDF')]
            print(f'[quality] --reset mode: waiving duplicate-PDF block '
                  f'(DB will be fully rebuilt).')
            status = 'BLOCK' if blocks else ('WARNING' if report.get('warnings') else 'PASS')
            # Update report so the effective status is stored in the DB
            report = {**report, 'status': status, 'blocks': blocks}

    if status == 'BLOCK':
        print('\n' + '═' * 60)
        print('  ❌  QUALITY GATE: BLOCK — DB NOT modified (previous DB kept intact)')
        for reason in blocks:
            print(f'       • {reason}')
        print('  Fix the issues above, re-run parser.py, then retry migrate.py.')
        print('═' * 60 + '\n')
        sys.exit(1)

    if status == 'WARNING':
        print('\n' + '─' * 60)
        print('  ⚠   QUALITY GATE: WARNING — publishing with caveats')
        for reason in report.get('warnings', []):
            print(f'       • {reason}')
        print('─' * 60)

    return report


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
    # ── Quality gate: abort on BLOCK before touching the DB ───────────────
    quality_report = _check_quality_gate()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    if RESET:
        print("Dropping existing tables …")
        cur.executescript(DROP_DDL)

    cur.executescript(DDL)

    # ── Shipments ──────────────────────────────────────────────────────────────
    records = json.loads(DATA.read_text(encoding='utf-8'))
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
    if quality_report:
        _write_quality_report(con, quality_report)
        con.commit()
        print(f"  quality_reports    :   1 row written  (status={quality_report.get('status')})")

    con.close()
    print(f"\nDatabase written → {DB_PATH}")


if __name__ == '__main__':
    print(f"Migrating to {DB_PATH} …")
    migrate()
    print("Done.")
