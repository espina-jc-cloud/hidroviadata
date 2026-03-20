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
  GET  /api/lineup_confirmed        → fertilizer arrivals in the rolling [−30d, +60d] window
  GET  /api/track_record            → prediction accuracy metrics
  GET  /api/status                  → DB freshness snapshot

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
from datetime import datetime, timedelta
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


# ── Static file ───────────────────────────────────────────────────────────────

@app.route('/')
def index() -> Response:
    return send_from_directory(str(BASE_DIR), 'dashboard.html')


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


@app.route('/api/lineup_confirmed')
def api_lineup_confirmed() -> Response:
    """
    Fertilizer arrivals from the official San Nicolás lineup for the rolling
    window [today − 30 days, today + 60 days].

    Each row is one shipment record (not collapsed per vessel) so the caller
    can aggregate as needed.  Fields:
        buque, eta, material, cliente, tons, origen, agencia, muelle, sector,
        source_id, source_date
    Ordered by eta ASC.
    """
    today      = datetime.now().date()
    date_from  = (today - timedelta(days=30)).isoformat()
    date_to    = (today + timedelta(days=60)).isoformat()
    placeholders = ','.join('?' * len(_FERT_MATERIALS))

    rows = get_db().execute(
        f'SELECT buque, eta, material, cliente, tons, origen, '
        f'       agencia, muelle, sector, source_id, source_date '
        f'FROM shipments '
        f'WHERE material IN ({placeholders}) '
        f'  AND eta >= ? AND eta <= ? '
        f'ORDER BY eta ASC',
        (*_FERT_MATERIALS, date_from, date_to),
    ).fetchall()

    result = []
    for row in rows:
        d = dict(row)
        if d.get('tons') is not None:
            try:
                d['tons'] = int(round(float(d['tons'])))
            except (TypeError, ValueError):
                d['tons'] = None
        result.append(d)

    return jsonify(result)


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
    shipments_count      — total rows in shipments
    shipments_tons_total — sum of all tons (int)
    fert_tons_total      — sum of tons where material is a known fertilizer
    latest_source_date   — most recent PDF issue date loaded
    candidates_by_status — {predicted, confirmed, expired}
    as_of                — ISO timestamp of this response
    """
    db = get_db()
    placeholders = ','.join('?' * len(_FERT_MATERIALS))

    ship_count   = db.execute('SELECT count(*) FROM shipments').fetchone()[0]
    ship_tons    = db.execute('SELECT sum(tons) FROM shipments WHERE tons IS NOT NULL').fetchone()[0]
    fert_tons    = db.execute(
        f'SELECT sum(tons) FROM shipments WHERE material IN ({placeholders}) AND tons IS NOT NULL',
        _FERT_MATERIALS,
    ).fetchone()[0]
    latest_src   = db.execute('SELECT max(source_date) FROM shipments').fetchone()[0]
    n_pred       = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='predicted'").fetchone()[0]
    n_conf       = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='confirmed'").fetchone()[0]
    n_exp        = db.execute("SELECT count(*) FROM vessel_candidates WHERE prediction_status='expired'").fetchone()[0]

    return jsonify({
        'shipments_count':      ship_count,
        'shipments_tons_total': int(round(ship_tons))  if ship_tons  else 0,
        'fert_tons_total':      int(round(fert_tons))  if fert_tons  else 0,
        'latest_source_date':   latest_src,
        'candidates_by_status': {
            'predicted': n_pred,
            'confirmed': n_conf,
            'expired':   n_exp,
        },
        'as_of': datetime.now().isoformat(timespec='seconds'),
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
