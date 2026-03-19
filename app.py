"""
app.py
──────
Minimal Flask backend for HidrovíaData dashboard.

Endpoints
─────────
  GET  /                            → serves dashboard.html
  GET  /api/shipments               → all shipment records (770 rows)
  GET  /api/vessel_profiles         → all vessel profiles  (173 rows)
  GET  /api/vessel_candidates       → all predictive vessel entries
  GET  /api/fertilizer_core_fleet   → watchlist: vessels with ≥2 fertilizer visits

Database
────────
  hidroviadata.db  (created by migrate.py — run that first)

Usage
─────
    python3 migrate.py              # first time only
    python3 build_core_fleet.py    # rebuild watchlist (after profile updates)
    python3 app.py                  # start dev server on http://localhost:5000
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from flask import Flask, Response, g, jsonify, send_from_directory

BASE_DIR = Path(__file__).parent
DATABASE = BASE_DIR / 'hidroviadata.db'

app = Flask(__name__, static_folder=None)


# ── Database helpers ──────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    db = getattr(g, '_database', None)
    if db is None:
        if not DATABASE.exists():
            raise RuntimeError(
                f"Database not found at {DATABASE}. "
                "Run  python3 migrate.py  first."
            )
        db = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
        g._database = db
    return db


@app.teardown_appcontext
def close_connection(exception: Exception | None) -> None:
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def _rows_to_list(rows) -> list[dict]:
    """Convert sqlite3.Row objects to plain dicts."""
    return [dict(row) for row in rows]


# ── Static file ───────────────────────────────────────────────────────────────

@app.route('/')
def index() -> Response:
    return send_from_directory(str(BASE_DIR), 'dashboard.html')


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/shipments')
def api_shipments() -> Response:
    """
    Return all shipment records.

    Each record matches the original RAW_DATA schema:
        buque, agencia, eta, material, cliente, tons,
        operador, operacion, muelle, sector, origen
    """
    rows = get_db().execute(
        'SELECT buque, agencia, eta, material, cliente, tons, '
        '       operador, operacion, muelle, sector, origen '
        'FROM shipments'
    ).fetchall()
    return jsonify(_rows_to_list(rows))


@app.route('/api/vessel_profiles')
def api_vessel_profiles() -> Response:
    """
    Return all vessel profiles with JSON columns deserialised.

    JSON columns: main_ports_in_argentina (array), seasonality_by_month (object).
    """
    rows = get_db().execute('SELECT * FROM vessel_profiles').fetchall()
    result: list[dict] = []
    for row in rows:
        d = dict(row)
        d['main_ports_in_argentina'] = json.loads(d['main_ports_in_argentina'] or '[]')
        d['seasonality_by_month']    = json.loads(d['seasonality_by_month']    or '{}')
        result.append(d)
    return jsonify(result)


@app.route('/api/vessel_candidates')
def api_vessel_candidates() -> Response:
    """
    Return all predictive vessel candidates with JSON columns deserialised.

    JSON columns: probable_tonnage_range (array or null), scoring_reasons (array).
    Ordered by probability_score descending (highest confidence first).
    """
    rows = get_db().execute(
        'SELECT vessel_name, last_position, last_port, ais_destination, eta_estimated, '
        '       probable_product, probable_importer, probable_tonnage_range, '
        '       probability_score, probability_level, prediction_status, scoring_reasons '
        'FROM vessel_candidates '
        'ORDER BY probability_score DESC'
    ).fetchall()
    result: list[dict] = []
    for row in rows:
        d = dict(row)
        d['probable_tonnage_range'] = json.loads(d['probable_tonnage_range'] or 'null')
        d['scoring_reasons']        = json.loads(d['scoring_reasons']        or '[]')
        result.append(d)
    return jsonify(result)


@app.route('/api/fertilizer_core_fleet')
def api_fertilizer_core_fleet() -> Response:
    """
    Return the fertilizer core fleet watchlist.

    Vessels with fertilizer_visits >= 2, ordered by fertilizer_visits DESC.
    Fields: vessel_name, fertilizer_visits, visits_to_argentina,
            dominant_product, dominant_origin, dominant_importer,
            avg_tonnage, min_tonnage, max_tonnage, confidence_inputs_available
    """
    rows = get_db().execute(
        'SELECT vessel_name, fertilizer_visits, visits_to_argentina, '
        '       dominant_product, dominant_origin, dominant_importer, '
        '       avg_tonnage, min_tonnage, max_tonnage, '
        '       confidence_inputs_available '
        'FROM fertilizer_core_fleet '
        'ORDER BY fertilizer_visits DESC, vessel_name ASC'
    ).fetchall()
    return jsonify(_rows_to_list(rows))


# ── Dev server ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if not DATABASE.exists():
        print("ERROR: hidroviadata.db not found.")
        print("Run  python3 migrate.py  first, then restart app.py.")
        raise SystemExit(1)

    import os
    port = int(os.environ.get('PORT', 5000))
    print("HidrovíaData API + dashboard")
    print(f"  Database : {DATABASE}")
    print(f"  URL      : http://localhost:{port}")
    print(f"  (macOS: if port 5000 conflicts with AirPlay, run: PORT=5001 python3 app.py)")
    print()
    app.run(debug=True, port=port, host='0.0.0.0')
