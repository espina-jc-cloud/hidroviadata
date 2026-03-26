"""
app.py
──────
Minimal Flask backend for HidrovíaData dashboard.

Endpoints
─────────
  GET  /                            → serves dashboard.html
  GET  /api/shipments               → all shipment records (normalised at API layer)
  GET  /api/shipments/quality       → data-quality summary for shipments
  GET  /api/vessel_profiles         → all vessel profiles  (173 rows)
  GET  /api/vessel_candidates       → all predictive vessel entries (with lead_time_days)
  GET  /api/fertilizer_core_fleet   → watchlist: vessels with ≥2 fertilizer visits
  GET  /api/lineup_confirmed        → ALL fertilizer rows from the latest lineup PDF
  GET  /api/track_record            → prediction accuracy metrics
  GET  /api/status                  → DB freshness snapshot

Admin endpoints
───────────────
  POST /api/admin/reset_candidates  → DELETE all rows from vessel_candidates
  POST /api/admin/add_candidate     → score + insert one AIS observation

Database
────────
  hidroviadata.db  (created by migrate.py — run that first)

Usage
─────
    python3 migrate.py              # first time only
    python3 build_core_fleet.py    # rebuild watchlist (after profile updates)
    python3 app.py                  # start dev server on http://localhost:5000
    python3 app.py                  # start dev server (admin endpoints open)
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, Response, g, jsonify, request, send_from_directory

# Reuse the scoring helpers from the CLI pipeline — pure functions, no side effects.
from detect_candidates import _score_observation, _load_core_fleet, _insert_candidate

BASE_DIR    = Path(__file__).parent
DATABASE    = BASE_DIR / 'hidroviadata.db'
PDFS_DIR    = BASE_DIR / 'pdfs'
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN', '')

# In-memory preview state (lost on restart — intentional per spec)
_last_preview: dict | None = None

app = Flask(__name__, static_folder=None)


# ── Git SHA helper ─────────────────────────────────────────────────────────────

def _git_sha() -> str:
    """Return 7-char commit SHA.  Falls back to RAILWAY_GIT_COMMIT_SHA env var."""
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stderr=subprocess.DEVNULL, cwd=str(BASE_DIR),
        ).decode().strip()
    except Exception:
        return os.environ.get('RAILWAY_GIT_COMMIT_SHA', 'unknown')[:7]


# ── DB bootstrap (runs at module import — safe for gunicorn workers) ──────────

def _bootstrap_db() -> None:
    """
    If the DB is absent or has no shipments rows, rebuild it by running
    migrate.py.  This is the primary safety net for Railway ephemeral
    filesystems.  It is a no-op when hidroviadata.db is already populated.
    """
    needs_build = False

    if not DATABASE.exists():
        print(f"[bootstrap] {DATABASE.name} not found — rebuilding from data.json …", flush=True)
        needs_build = True
    else:
        try:
            con = sqlite3.connect(DATABASE)
            n   = con.execute('SELECT count(*) FROM shipments').fetchone()[0]
            con.close()
            if n == 0:
                print('[bootstrap] DB exists but shipments table is empty — rebuilding …', flush=True)
                needs_build = True
        except Exception as exc:
            print(f'[bootstrap] DB unreadable ({exc}) — rebuilding …', flush=True)
            needs_build = True

    if needs_build:
        res = subprocess.run(
            [sys.executable, str(BASE_DIR / 'migrate.py')],
            capture_output=True, text=True,
        )
        if res.returncode == 0:
            print('[bootstrap] migrate.py finished OK', flush=True)
        else:
            print(f'[bootstrap] migrate.py FAILED:\n{res.stderr}', flush=True)


_bootstrap_db()


# ── Startup log — always visible in Railway logs ──────────────────────────────

def _startup_log() -> None:
    """Print a one-time summary to stdout so Railway logs confirm which version
    is running and what data the DB contains."""
    try:
        con = sqlite3.connect(DATABASE)
        n_ship   = con.execute('SELECT count(*) FROM shipments').fetchone()[0]
        tons_row = con.execute('SELECT sum(tons) FROM shipments WHERE tons IS NOT NULL').fetchone()
        t_tons   = int(round(tons_row[0])) if tons_row and tons_row[0] else 0
        latest   = con.execute(
            'SELECT source_date, source_id FROM shipments ORDER BY source_date DESC LIMIT 1'
        ).fetchone()
        n_cand   = con.execute('SELECT count(*) FROM vessel_candidates').fetchone()[0]
        con.close()
        src_date = latest[0] if latest else 'n/a'
        src_id   = latest[1] if latest else 'n/a'
    except Exception as exc:
        n_ship = t_tons = n_cand = 0
        src_date = src_id = f'ERROR: {exc}'

    sha = _git_sha()
    print('─' * 60, flush=True)
    print(f'HidrovíaData startup  sha={sha}', flush=True)
    print(f'  DB           : {DATABASE}', flush=True)
    print(f'  shipments    : {n_ship:,}  total_tons={t_tons:,}', flush=True)
    print(f'  latest_lineup: {src_date}  ({src_id})', flush=True)
    print(f'  candidates   : {n_cand}', flush=True)
    print('─' * 60, flush=True)


_startup_log()


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


def _get_latest_quality() -> dict | None:
    """
    Return the most recent row from quality_reports as a plain dict.
    Returns None if the table is missing or empty (safe for old DBs).
    """
    try:
        row = get_db().execute(
            'SELECT status, blocks_json, warnings_json, summary_json, timestamp, source_date, source_id '
            'FROM quality_reports ORDER BY id DESC LIMIT 1'
        ).fetchone()
        if not row:
            return None
        return {
            'status':      row['status'],
            'blocks':      json.loads(row['blocks_json']   or '[]'),
            'warnings':    json.loads(row['warnings_json'] or '[]'),
            'summary':     json.loads(row['summary_json']  or '{}'),
            'timestamp':   row['timestamp'],
            'source_date': row['source_date'],
            'source_id':   row['source_id'],
        }
    except Exception:
        return None


# ── Shipment normalisation ─────────────────────────────────────────────────────
# Applied at the API layer only — the DB schema is unchanged.

# Canonical Spanish country / port names.
# Keys cover every raw value seen in the data (typos, abbreviations, aliases).
# Values are the canonical form used in scoring and analytics.
_ORIGIN_CANONICAL: dict[str, str] = {
    # ── Typos / English aliases ────────────────────────────────────────────────
    'MARRECOS':       'MARRUECOS',      # common OCR / transcription typo
    'RUSSIA':         'RUSIA',          # English → Spanish
    'ARABIA':         'ARABIA SAUDITA', # abbreviation
    'EEUU':           'ESTADOS UNIDOS', # abbreviation
    # ── Already-canonical (membership defines the confirmed-origin set) ────────
    'MARRUECOS':      'MARRUECOS',
    'RUSIA':          'RUSIA',
    'QATAR':          'QATAR',
    'ARGELIA':        'ARGELIA',
    'CHINA':          'CHINA',
    'NIGERIA':        'NIGERIA',
    'FINLANDIA':      'FINLANDIA',
    'RUMANIA':        'RUMANIA',
    'OMAN':           'OMAN',
    'CANADA':         'CANADA',
    'MEXICO':         'MEXICO',
    'GEORGIA':        'GEORGIA',
    'NORUEGA':        'NORUEGA',
    'HOLANDA':        'HOLANDA',
    'COLOMBIA':       'COLOMBIA',
    'ESTADOS UNIDOS': 'ESTADOS UNIDOS',
    'ARABIA SAUDITA': 'ARABIA SAUDITA',
    'PERU':           'PERU',
    'ISRAEL':         'ISRAEL',
    'INDIA':          'INDIA',
    'BELGICA':        'BELGICA',
    'JORDANIA':       'JORDANIA',
    'EGIPTO':         'EGIPTO',
    'JAPON':          'JAPON',
    # ── Specific ports (kept as-is — more precise than country) ───────────────
    'ARZEW':          'ARZEW',       # Algeria — urea terminal
    'MESAIEED':       'MESAIEED',    # Qatar
    'YUZHNE':         'YUZHNE',      # Ukraine
    'VENTSPILS':      'VENTSPILS',   # Latvia
    'KOTKA':          'KOTKA',       # Finland
}

# Substrings that mark an origen value as operational text, not a real origin.
# Using a tuple so the `any(kw in upper for kw in ...)` check is short-circuit.
_AMBIGUOUS_SUBSTRINGS: tuple[str, ...] = (
    'ETA ', 'ETC ', 'EXTC ',
    'TRANSITO', 'TRASBORDO', 'TRABORDO',
    'EXPO', 'EXPORTACION',
    'SEGUNDA ANDANA', 'FINALIZADO', 'CAMBIO DE',
    'DESCARGA', 'REGLAMENTO', 'NAVEGACION',
)


def _try_extract_origin(text: str) -> str | None:
    """
    Scan an ambiguous string for an embedded canonical origin.
    Returns the canonical name on first match, or None.
    Example: 'ETC SAN LORENZO MARRUECOS' → 'MARRUECOS'
    """
    upper = text.upper()
    for raw, canonical in _ORIGIN_CANONICAL.items():
        if raw in upper:
            return canonical
    return None


def normalize_shipment(row: dict) -> dict:
    """
    Lightweight normalisation applied to every record returned by /api/shipments.

    Field rules
    ───────────
    tons              float | null → int(round()) | null
    material          '' | null    → 'UNKNOWN'
    origen            typos fixed, abbreviations expanded, ambiguous text flagged
    origin_raw        new — original DB value before any change (null when empty)
    origin_confidence new — 'confirmed' | 'ambiguous' | 'unknown'

    The DB is not touched; all changes are in-flight.
    """
    d = dict(row)

    # ── tons ──────────────────────────────────────────────────────────────────
    if d.get('tons') is not None:
        try:
            d['tons'] = int(round(float(d['tons'])))
        except (TypeError, ValueError):
            d['tons'] = None

    # ── material ──────────────────────────────────────────────────────────────
    if not d.get('material'):
        d['material'] = 'UNKNOWN'

    # ── origin ────────────────────────────────────────────────────────────────
    raw = (d.get('origen') or '').strip()
    d['origin_raw'] = raw or None   # null when the DB value was empty

    if not raw:
        d['origen']            = None
        d['origin_confidence'] = 'unknown'
    else:
        upper        = raw.upper()
        is_ambiguous = any(kw in upper for kw in _AMBIGUOUS_SUBSTRINGS)

        if is_ambiguous:
            d['origin_confidence'] = 'ambiguous'
            # Best-effort: try to salvage a country name embedded in the noise
            d['origen'] = _try_extract_origin(raw) or raw
        elif upper in _ORIGIN_CANONICAL:
            d['origen']            = _ORIGIN_CANONICAL[upper]
            d['origin_confidence'] = 'confirmed'
        else:
            # Non-empty, no ambiguous keywords, not in known-set.
            # Keep the raw value — may be a valid but unrecognised origin.
            d['origen']            = raw
            d['origin_confidence'] = 'confirmed'

    return d


# ── Health check ──────────────────────────────────────────────────────────────

@app.route('/health')
def health() -> Response:
    return jsonify('ok')


# ── Debug endpoint ────────────────────────────────────────────────────────────

@app.route('/api/debug')
def api_debug() -> Response:
    """Snapshot of what the running server actually sees in its DB."""
    db           = get_db()
    placeholders = ','.join('?' * len(_FERT_MATERIALS))

    ship_count  = db.execute('SELECT count(*) FROM shipments').fetchone()[0]
    ship_tons   = db.execute('SELECT sum(tons) FROM shipments WHERE tons IS NOT NULL').fetchone()[0]
    fert_count  = db.execute(f'SELECT count(*) FROM shipments WHERE material IN ({placeholders})', _FERT_MATERIALS).fetchone()[0]
    fert_tons   = db.execute(f'SELECT sum(tons)  FROM shipments WHERE material IN ({placeholders}) AND tons IS NOT NULL', _FERT_MATERIALS).fetchone()[0]
    latest_row  = db.execute('SELECT source_date, source_id FROM shipments ORDER BY source_date DESC LIMIT 1').fetchone()
    cand_count  = db.execute('SELECT count(*) FROM vessel_candidates').fetchone()[0]
    cand_by_status: dict = {}
    for r in db.execute(
        'SELECT prediction_status, count(*) FROM vessel_candidates GROUP BY prediction_status'
    ).fetchall():
        cand_by_status[r[0] or 'unknown'] = r[1]

    return jsonify({
        'git_sha':                     _git_sha(),
        'db_path':                     str(DATABASE),
        'shipments_count':             ship_count,
        'shipments_tons':              int(round(ship_tons))  if ship_tons  else 0,
        'fert_count':                  fert_count,
        'fert_tons':                   int(round(fert_tons))  if fert_tons  else 0,
        'latest_source_date':          latest_row['source_date'] if latest_row else None,
        'latest_source_id':            latest_row['source_id']   if latest_row else None,
        'vessel_candidates':           cand_count,
        'vessel_candidates_by_status': cand_by_status,
        'quality':                     _get_latest_quality(),
        'as_of':                       datetime.now().isoformat(timespec='seconds'),
    })


# ── Static file ───────────────────────────────────────────────────────────────

@app.route('/')
def index() -> Response:
    # no-store so Railway/browsers never serve a stale dashboard.html
    resp = send_from_directory(str(BASE_DIR), 'dashboard.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route('/api/shipments')
def api_shipments() -> Response:
    """
    Return all shipment records with lightweight normalisation applied.

    Added fields (not in DB):
        origin_raw        — original origen value before normalisation
        origin_confidence — 'confirmed' | 'ambiguous' | 'unknown'

    Changed fields:
        tons     — rounded to int (null stays null)
        material — empty → 'UNKNOWN'
        origen   — typos / abbreviations corrected; ambiguous strings flagged
    """
    print(f"[{datetime.now().isoformat(timespec='seconds')}] GET /api/shipments", flush=True)
    rows = get_db().execute(
        'SELECT buque, agencia, eta, material, cliente, tons, '
        '       operador, operacion, muelle, sector, origen '
        'FROM shipments'
    ).fetchall()
    return jsonify([normalize_shipment(dict(r)) for r in rows])


@app.route('/api/shipments/quality')
def api_shipments_quality() -> Response:
    """
    Data-quality summary for the shipments table.

    Returns
    ───────
    total_records
    material_unknown_count / _pct   rows where material was empty in the DB
    origin_ambiguous_count / _pct   rows where origen is operational text
    origin_unknown_count   / _pct   rows where origen was empty
    tons_null_count        / _pct   rows where tons is null
    top_10_raw_origins              raw DB values with counts (before normalisation)
    """
    rows = get_db().execute(
        'SELECT material, origen, tons FROM shipments'
    ).fetchall()
    records = [normalize_shipment(dict(r)) for r in rows]

    total             = len(records)
    mat_unknown       = sum(1 for r in records if r['material'] == 'UNKNOWN')
    origin_ambiguous  = sum(1 for r in records if r['origin_confidence'] == 'ambiguous')
    origin_unknown    = sum(1 for r in records if r['origin_confidence'] == 'unknown')
    tons_null         = sum(1 for r in records if r['tons'] is None)

    def pct(n: int) -> float:
        return round(100 * n / total, 1) if total else 0.0

    top_raw = get_db().execute(
        'SELECT origen, count(*) as n '
        'FROM shipments '
        'GROUP BY origen '
        'ORDER BY n DESC '
        'LIMIT 10'
    ).fetchall()

    return jsonify({
        'total_records':          total,
        'material_unknown_count': mat_unknown,
        'material_unknown_pct':   pct(mat_unknown),
        'origin_ambiguous_count': origin_ambiguous,
        'origin_ambiguous_pct':   pct(origin_ambiguous),
        'origin_unknown_count':   origin_unknown,
        'origin_unknown_pct':     pct(origin_unknown),
        'tons_null_count':        tons_null,
        'tons_null_pct':          pct(tons_null),
        'top_10_raw_origins':     [
            {'origen': r['origen'] or '', 'count': r['n']}
            for r in top_raw
        ],
    })


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

    JSON columns  : probable_tonnage_range (array or null), scoring_reasons (array).
    Derived field : lead_time_days — days between created_at and confirmed_eta
                    (only present when prediction_status = 'confirmed').
    Ordered by probability_score descending (highest confidence first).
    """
    print(f"[{datetime.now().isoformat(timespec='seconds')}] GET /api/vessel_candidates", flush=True)
    rows = get_db().execute(
        'SELECT vessel_name, last_position, last_port, ais_destination, eta_estimated, '
        '       probable_product, probable_importer, probable_tonnage_range, '
        '       probability_score, probability_level, prediction_status, scoring_reasons, '
        '       confirmed_eta, confirmed_match_reason, created_at '
        'FROM vessel_candidates '
        'ORDER BY probability_score DESC'
    ).fetchall()
    result: list[dict] = []
    for row in rows:
        d = dict(row)
        d['probable_tonnage_range'] = json.loads(d['probable_tonnage_range'] or 'null')
        d['scoring_reasons']        = json.loads(d['scoring_reasons']        or '[]')
        # Compute lead_time_days for confirmed predictions
        d['lead_time_days'] = None
        if d.get('prediction_status') == 'confirmed' \
                and d.get('confirmed_eta') and d.get('created_at'):
            try:
                t_confirmed = datetime.fromisoformat(d['confirmed_eta'][:10])
                t_created   = datetime.fromisoformat(d['created_at'][:10])
                d['lead_time_days'] = (t_confirmed - t_created).days
            except (ValueError, TypeError):
                pass
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


# Fertilizer materials recognised by the pipeline
_FERT_MATERIALS: tuple[str, ...] = (
    'UREA', 'MAP', 'DAP', 'MOP', 'TSP', 'NPS', 'UAN', 'AMSUL',
    'NP', 'NPK', 'SSP', 'STP', 'GMOP', 'FERTILIZANTE',
    'NITRODOBLE', 'NITRATO DE AMONIO',
)


# ── Startup log (printed once per gunicorn worker, captured by Railway logs) ──

def _startup_log() -> None:
    sha = _git_sha()
    try:
        con  = sqlite3.connect(DATABASE)
        ph   = ','.join('?' * len(_FERT_MATERIALS))
        n    = con.execute('SELECT count(*) FROM shipments').fetchone()[0]
        ft   = con.execute(f'SELECT sum(tons) FROM shipments WHERE material IN ({ph}) AND tons IS NOT NULL', _FERT_MATERIALS).fetchone()[0]
        lat  = con.execute('SELECT source_date, source_id FROM shipments ORDER BY source_date DESC LIMIT 1').fetchone()
        cand = con.execute('SELECT count(*) FROM vessel_candidates').fetchone()[0]
        con.close()
        print(f'[startup] sha={sha}  db={DATABASE.name}', flush=True)
        print(f'[startup] shipments={n}  fert_tons={int(ft or 0)}  candidates={cand}', flush=True)
        print(f'[startup] latest_source_date={lat[0] if lat else "N/A"}  latest_source_id={lat[1] if lat else "N/A"}', flush=True)
    except Exception as exc:
        print(f'[startup] sha={sha}  DB read failed: {exc}', flush=True)


_startup_log()


@app.route('/api/lineup_confirmed')
def api_lineup_confirmed() -> Response:
    """
    ALL rows from the single latest imported lineup PDF
    (identified by MAX(source_date)).  No material whitelist — rows with
    any material (or empty material) are included so that incomplete rows
    (eta=null, cliente='', tons=null) are not silently dropped.

    Each row is one shipment record — different clients for the same vessel
    are NOT collapsed, because they are distinct trade positions.

    Returns
    ───────
    {
      "latest_source_date": "2026-03-20",
      "latest_source_id":   "LINE UP PUERTO SAN NICOLAS 200326.pdf",
      "row_count":          43,
      "total_tons":         133210,
      "rows": [ {buque, eta, material, cliente, tons, origen,
                 agencia, muelle, sector, source_id, source_date}, … ]
    }
    Ordered by eta ASC NULLS LAST, then buque ASC.
    """
    print(f"[{datetime.now().isoformat(timespec='seconds')}] GET /api/lineup_confirmed", flush=True)
    db = get_db()

    # Find the latest lineup date and its primary source_id
    latest_meta  = db.execute(
        'SELECT source_date, source_id FROM shipments '
        'ORDER BY source_date DESC, source_id DESC LIMIT 1'
    ).fetchone()
    if not latest_meta:
        return jsonify({'latest_source_date': None, 'latest_source_id': None,
                        'row_count': 0, 'total_tons': 0, 'rows': []})

    latest_date = latest_meta['source_date']
    latest_sid  = latest_meta['source_id']

    rows = db.execute(
        'SELECT buque, eta, material, cliente, tons, origen, '
        '       agencia, muelle, sector, source_id, source_date '
        'FROM shipments '
        'WHERE source_date = ? '
        'ORDER BY CASE WHEN eta IS NULL OR eta = "" THEN 1 ELSE 0 END ASC, '
        '         eta ASC, buque ASC',
        (latest_date,),
    ).fetchall()

    result = []
    total_tons = 0
    for row in rows:
        d = dict(row)
        if d.get('tons') is not None:
            try:
                d['tons'] = int(round(float(d['tons'])))
                total_tons += d['tons']
            except (TypeError, ValueError):
                d['tons'] = None
        result.append(d)

    return jsonify({
        'latest_source_date': latest_date,
        'latest_source_id':   latest_sid,
        'row_count':          len(result),
        'total_tons':         total_tons,
        'quality':            _get_latest_quality(),
        'rows':               result,
    })


@app.route('/api/track_record')
def api_track_record() -> Response:
    """
    Prediction accuracy metrics for vessel_candidates.

    Returns
    ───────
    total           — total candidates ever created
    predicted       — still open (no match yet)
    confirmed       — matched to a lineup entry
    expired         — window passed with no match (false positive)
    confirm_rate_all  — confirmed / (confirmed + expired) %
    confirm_rate_high — same, restricted to probability_level = 'high'
    confirm_rate_med  — same, restricted to probability_level = 'medium'
    avg_lead_time_days — mean (confirmed_eta − created_at) for confirmed rows
    recently_confirmed — last 5 confirmed candidates (vessel_name, confirmed_eta,
                         confirmed_match_reason, lead_time_days)
    """
    rows = [dict(r) for r in get_db().execute(
        'SELECT vessel_name, prediction_status, probability_level, '
        '       confirmed_eta, confirmed_match_reason, created_at '
        'FROM vessel_candidates'
    ).fetchall()]

    total     = len(rows)
    n_pred    = sum(1 for r in rows if r['prediction_status'] == 'predicted')
    n_conf    = sum(1 for r in rows if r['prediction_status'] == 'confirmed')
    n_exp     = sum(1 for r in rows if r['prediction_status'] == 'expired')
    resolved  = n_conf + n_exp

    def _rate(candidates: list[dict]) -> float | None:
        res = [c for c in candidates if c['prediction_status'] in ('confirmed', 'expired')]
        if not res:
            return None
        conf = sum(1 for c in res if c['prediction_status'] == 'confirmed')
        return round(conf / len(res) * 100, 1)

    # Lead-time computation
    lead_times: list[int] = []
    recently_confirmed: list[dict] = []
    for r in rows:
        if r['prediction_status'] == 'confirmed' \
                and r.get('confirmed_eta') and r.get('created_at'):
            try:
                t_conf    = datetime.fromisoformat(r['confirmed_eta'][:10])
                t_created = datetime.fromisoformat(r['created_at'][:10])
                lt        = (t_conf - t_created).days
                lead_times.append(lt)
                recently_confirmed.append({
                    'vessel_name':            r['vessel_name'],
                    'confirmed_eta':          r['confirmed_eta'],
                    'confirmed_match_reason': r['confirmed_match_reason'],
                    'lead_time_days':         lt,
                })
            except (ValueError, TypeError):
                pass

    recently_confirmed.sort(key=lambda x: x['confirmed_eta'] or '', reverse=True)

    return jsonify({
        'total':                total,
        'predicted':            n_pred,
        'confirmed':            n_conf,
        'expired':              n_exp,
        'confirm_rate_all':     _rate(rows),
        'confirm_rate_high':    _rate([r for r in rows if r['probability_level'] == 'high']),
        'confirm_rate_med':     _rate([r for r in rows if r['probability_level'] == 'medium']),
        'avg_lead_time_days':   round(sum(lead_times) / len(lead_times), 1) if lead_times else None,
        'recently_confirmed':   recently_confirmed[:5],
    })


@app.route('/api/status')
def api_status() -> Response:
    """
    Quick DB freshness snapshot.

    Returns
    ───────
    total_shipments          total rows in shipments
    total_tons               sum of all tons (int)
    fert_shipments           rows where material is a known fertilizer
    fert_tons                tons for those rows
    latest_source_date       most recent PDF issue date loaded
    latest_source_id         filename of that PDF
    latest_lineup_fert_rows  fertilizer row count for the latest PDF
    latest_lineup_fert_tons  fertilizer tonnage for the latest PDF
    candidates_by_status     {predicted, confirmed, expired}
    as_of                    ISO timestamp of this response
    """
    db           = get_db()
    placeholders = ','.join('?' * len(_FERT_MATERIALS))

    ship_count   = db.execute('SELECT count(*) FROM shipments').fetchone()[0]
    ship_tons    = db.execute('SELECT sum(tons) FROM shipments WHERE tons IS NOT NULL').fetchone()[0]
    fert_count   = db.execute(
        f'SELECT count(*) FROM shipments WHERE material IN ({placeholders})',
        _FERT_MATERIALS,
    ).fetchone()[0]
    fert_tons    = db.execute(
        f'SELECT sum(tons) FROM shipments WHERE material IN ({placeholders}) AND tons IS NOT NULL',
        _FERT_MATERIALS,
    ).fetchone()[0]

    latest_row   = db.execute(
        'SELECT source_date, source_id FROM shipments ORDER BY source_date DESC LIMIT 1'
    ).fetchone()
    latest_src   = latest_row['source_date'] if latest_row else None
    latest_sid   = latest_row['source_id']   if latest_row else None

    lf_rows, lf_tons = 0, 0
    if latest_src:
        r = db.execute(
            f'SELECT count(*), sum(tons) FROM shipments '
            f'WHERE source_date=? AND material IN ({placeholders}) AND tons IS NOT NULL',
            (latest_src, *_FERT_MATERIALS),
        ).fetchone()
        lf_rows = r[0] or 0
        lf_tons = int(round(r[1])) if r[1] else 0

    n_pred = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='predicted'").fetchone()[0]
    n_conf = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='confirmed'").fetchone()[0]
    n_exp  = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='expired'").fetchone()[0]

    return jsonify({
        'total_shipments':          ship_count,
        'total_tons':               int(round(ship_tons))  if ship_tons  else 0,
        'fert_shipments':           fert_count,
        'fert_tons':                int(round(fert_tons))  if fert_tons  else 0,
        'latest_source_date':       latest_src,
        'latest_source_id':         latest_sid,
        'latest_lineup_fert_rows':  lf_rows,
        'latest_lineup_fert_tons':  lf_tons,
        'candidates_by_status':     {'predicted': n_pred, 'confirmed': n_conf, 'expired': n_exp},
        'quality':                  _get_latest_quality(),
        'as_of':                    datetime.now().isoformat(timespec='seconds'),
    })


# ── Admin helpers ─────────────────────────────────────────────────────────────

def _check_admin_token() -> bool:
    """Return True when the request carries the configured ADMIN_TOKEN."""
    if not ADMIN_TOKEN:
        return False  # token not set → all admin endpoints disabled
    tok = (request.headers.get('X-Admin-Token')
           or request.args.get('token', ''))
    return tok == ADMIN_TOKEN


# ── Admin endpoints ────────────────────────────────────────────────────────────

@app.route('/api/admin/reset_candidates', methods=['POST'])
def api_admin_reset_candidates() -> Response:
    """
    DELETE all rows from vessel_candidates.

    Returns: {deleted_count: N}
    """
    db  = get_db()
    cur = db.execute('DELETE FROM vessel_candidates')
    db.commit()
    return jsonify({'deleted_count': cur.rowcount})


@app.route('/api/admin/add_candidate', methods=['POST'])
def api_admin_add_candidate() -> Response:
    """
    Score + insert one AIS observation into vessel_candidates.

    Body (JSON):
        vessel_name      string  required
        last_port        string  optional
        ais_destination  string  optional
        vessel_type      string  optional
        dwt              int     optional
        eta_estimated    string  optional  (YYYY-MM-DD)
        last_position    string  optional

    Returns the fully inserted row (same shape as /api/vessel_candidates).
    """
    obs = request.get_json(force=True, silent=True)
    if not obs or not obs.get('vessel_name'):
        return jsonify({'error': 'vessel_name is required in JSON body.'}), 400

    db         = get_db()
    core_fleet = _load_core_fleet(db)
    candidate  = _score_observation(obs, core_fleet)

    # Preserve last_position if caller supplied it
    if obs.get('last_position'):
        candidate['last_position'] = obs['last_position']

    row_id     = _insert_candidate(db, candidate)

    # Read back the full row so the response matches /api/vessel_candidates
    row = db.execute(
        'SELECT vessel_name, last_position, last_port, ais_destination, eta_estimated, '
        '       probable_product, probable_importer, probable_tonnage_range, '
        '       probability_score, probability_level, prediction_status, scoring_reasons, '
        '       confirmed_eta, confirmed_match_reason, created_at '
        'FROM vessel_candidates WHERE id = ?',
        (row_id,),
    ).fetchone()

    d = dict(row)
    d['probable_tonnage_range'] = json.loads(d['probable_tonnage_range'] or 'null')
    d['scoring_reasons']        = json.loads(d['scoring_reasons']        or '[]')
    d['lead_time_days']         = None
    d['id']                     = row_id
    return jsonify(d), 201


# ── Admin: Upload / Preview / Publish ─────────────────────────────────────────

@app.route('/api/admin/upload_lineup', methods=['POST'])
def api_admin_upload_lineup() -> Response:
    """
    Accept multipart/form-data PDF upload, save to pdfs/, run parser.py,
    then run migrate.py --preview to compute the quality report without
    writing hidroviadata.db.

    Returns: {preview: {source_id, source_date, n_rows, total_tons,
                         quality:{status,blocks,warnings,summary}}}
    """
    global _last_preview
    if not _check_admin_token():
        return jsonify({'error': 'Unauthorized'}), 401

    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'error': 'No file uploaded. Use multipart/form-data field "file".'}), 400

    from werkzeug.utils import secure_filename
    filename = secure_filename(f.filename)
    if not filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are accepted.'}), 400

    PDFS_DIR.mkdir(exist_ok=True)
    dest = PDFS_DIR / filename
    f.save(str(dest))
    print(f'[admin] uploaded {filename} → {dest}', flush=True)

    # Step 1: regenerate output/data.json
    parser_res = subprocess.run(
        [sys.executable, str(BASE_DIR / 'parser.py')],
        capture_output=True, text=True, cwd=str(BASE_DIR),
    )
    if parser_res.returncode != 0:
        return jsonify({
            'error':  'parser.py failed',
            'stderr': parser_res.stderr[-2000:],
        }), 500

    # Step 2: quality preview — NO DB write
    preview_res = subprocess.run(
        [sys.executable, str(BASE_DIR / 'migrate.py'), '--preview'],
        capture_output=True, text=True, cwd=str(BASE_DIR),
    )

    preview_data: dict | None = None
    for line in preview_res.stdout.splitlines():
        if line.startswith('__PREVIEW_JSON__:'):
            try:
                preview_data = json.loads(line[len('__PREVIEW_JSON__:'):])
            except json.JSONDecodeError:
                pass
            break

    if preview_data is None:
        return jsonify({
            'error':  'migrate.py --preview did not return valid JSON',
            'stdout': preview_res.stdout[-2000:],
            'stderr': preview_res.stderr[-2000:],
        }), 500

    _last_preview = {**preview_data, 'uploaded_file': filename}
    return jsonify({'preview': _last_preview})


@app.route('/api/admin/publish_lineup', methods=['POST'])
def api_admin_publish_lineup() -> Response:
    """
    Publish: run migrate.py --reset (safe atomic swap).
    Only allowed when last preview status is PASS or WARNING.

    Returns: {ok, git_sha, latest_source_date, latest_source_id}
    """
    global _last_preview
    if not _check_admin_token():
        return jsonify({'error': 'Unauthorized'}), 401

    if not _last_preview:
        return jsonify({'error': 'No preview found. Upload a PDF first.'}), 400

    pstatus = (_last_preview.get('quality') or {}).get('status', '')
    if pstatus not in ('PASS', 'WARNING'):
        return jsonify({
            'error':   f'Publish blocked — last preview status is {pstatus!r}.',
            'quality': _last_preview.get('quality'),
        }), 400

    pub_res = subprocess.run(
        [sys.executable, str(BASE_DIR / 'migrate.py'), '--reset'],
        capture_output=True, text=True, cwd=str(BASE_DIR),
    )
    if pub_res.returncode != 0:
        return jsonify({
            'error':  'migrate.py --reset failed',
            'stdout': pub_res.stdout[-2000:],
            'stderr': pub_res.stderr[-2000:],
        }), 500

    # Read updated DB state
    try:
        con    = sqlite3.connect(str(DATABASE))
        latest = con.execute(
            'SELECT source_date, source_id FROM shipments ORDER BY source_date DESC LIMIT 1'
        ).fetchone()
        con.close()
        latest_source_date = latest[0] if latest else None
        latest_source_id   = latest[1] if latest else None
    except Exception:
        latest_source_date = latest_source_id = None

    _last_preview = None  # clear after successful publish
    return jsonify({
        'ok':                 True,
        'git_sha':            _git_sha(),
        'latest_source_date': latest_source_date,
        'latest_source_id':   latest_source_id,
    })


@app.route('/api/admin/last_preview')
def api_admin_last_preview() -> Response:
    """Return the last in-memory preview result (lost on server restart)."""
    if not _check_admin_token():
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify(_last_preview or {})


def _norm_name(name: str | None) -> str:
    """Normalise vessel name: uppercase, hyphens/dots→space, strip non-alphanum."""
    if not name:
        return ''
    s = name.upper()
    s = re.sub(r'[-_.]', ' ', s)
    s = re.sub(r'[^A-Z0-9 ]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


@app.route('/api/admin/upload_candidates_csv', methods=['POST'])
def api_admin_upload_candidates_csv() -> Response:
    """
    Batch-insert AIS candidates from a CSV file.

    Required column  : vessel_name
    Optional columns : last_port, ais_destination, vessel_type, dwt

    Dedup rule: if an active candidate (status predicted/estimated/probable/review)
    with the same normalised vessel_name already exists → skip (count as duplicate).

    Returns: {ok, inserted, duplicates, rejected, errors:[{row, reason}]}
    """
    if not _check_admin_token():
        return jsonify({'error': 'Unauthorized'}), 401

    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'error': 'No file uploaded. Use multipart/form-data field "file".'}), 400

    # Decode — handle optional UTF-8 BOM
    try:
        content = f.read().decode('utf-8-sig')
    except UnicodeDecodeError:
        return jsonify({'error': 'CSV must be UTF-8 encoded.'}), 400

    reader = csv.DictReader(io.StringIO(content))

    if not reader.fieldnames:
        return jsonify({'error': 'CSV has no header row.'}), 400

    # Normalise header names (strip surrounding whitespace)
    reader.fieldnames = [h.strip() for h in reader.fieldnames]

    if 'vessel_name' not in reader.fieldnames:
        return jsonify({'error': 'CSV is missing required column: vessel_name'}), 400

    db = get_db()

    # Build set of already-active normalised names for dedup
    active_rows  = db.execute(
        "SELECT vessel_name FROM vessel_candidates "
        "WHERE prediction_status IN ('predicted','estimated','probable','review')"
    ).fetchall()
    active_norms = {_norm_name(r['vessel_name']) for r in active_rows}

    inserted   = 0
    duplicates = 0
    rejected   = 0
    errors: list[dict] = []

    for i, row in enumerate(reader, start=2):   # row 1 = header
        vname = (row.get('vessel_name') or '').strip()
        if not vname:
            rejected += 1
            errors.append({'row': i, 'reason': 'missing vessel_name'})
            continue

        norm = _norm_name(vname)
        if norm in active_norms:
            duplicates += 1
            continue

        last_port       = (row.get('last_port')       or '').strip() or None
        ais_destination = (row.get('ais_destination') or '').strip() or None

        db.execute(
            """
            INSERT INTO vessel_candidates
                (vessel_name, last_port, ais_destination,
                 probability_score, probability_level,
                 prediction_status, scoring_reasons)
            VALUES (?, ?, ?, 0, 'low', 'predicted', '["batch_upload_csv"]')
            """,
            (vname, last_port, ais_destination),
        )
        active_norms.add(norm)   # prevent intra-file duplicates
        inserted += 1

    db.commit()
    print(f'[admin] csv_upload inserted={inserted} dup={duplicates} rej={rejected}', flush=True)
    return jsonify({
        'ok':         True,
        'inserted':   inserted,
        'duplicates': duplicates,
        'rejected':   rejected,
        'errors':     errors,
    })


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
