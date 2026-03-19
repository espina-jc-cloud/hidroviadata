"""
detect_candidates.py
────────────────────
CLI pipeline: input AIS observations for vessels → score against the
fertilizer_core_fleet watchlist → insert results into vessel_candidates.

Two modes
─────────
  python3 detect_candidates.py                        # interactive (one vessel)
  python3 detect_candidates.py observations.json      # batch from JSON file
  python3 detect_candidates.py observations.json --dry-run  # preview, no DB write

Observation schema (JSON batch file — array of objects)
────────────────────────────────────────────────────────
  [
    {
      "vessel_name":     "CLIPPER I-STAR",   # required
      "last_port":       "MESAIEED",         # last known port / origin area
      "ais_destination": "RECALADA",         # raw AIS destination text
      "vessel_type":     "BULK CARRIER",     # e.g. "BULK CARRIER", "GENERAL CARGO"
      "dwt":             28500,              # deadweight tonnage (integer)
      "eta_estimated":   "2025-10-14"        # ISO date YYYY-MM-DD (optional override)
    }
  ]

Scoring (max 80 points)
───────────────────────
  +25  vessel found in fertilizer_core_fleet  (≥ 2 historical fertilizer visits)
  +20  last_port matches known fertilizer origin corridor
  +15  ais_destination matches Argentina / River Plate keywords
  +10  vessel_type is a bulk carrier
  +10  dwt is in 25 000 – 60 000 t range

Probability levels
──────────────────
  ≥ 80  → high    / probable
  60–79 → medium  / estimated
  < 60  → low     / estimated

Output → vessel_candidates table (hidroviadata.db)
──────────────────────────────────────────────────
  vessel_name, last_port, ais_destination, eta_estimated,
  probable_product, probable_importer, probable_tonnage_range (JSON),
  probability_score, probability_level, prediction_status = 'predicted',
  scoring_reasons (JSON), created_at (auto)
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

# Reuse knowledge tables and helper functions from vessel_scorer.
# _match_keywords, _infer_product, _estimate_eta are internal helpers but
# live in the same project and carry the battle-tested origin/product logic.
from vessel_scorer import (
    FERTILIZER_ORIGINS,
    ARGENTINA_DEST_KEYWORDS,
    BULK_VESSEL_TYPES,
    _match_keywords,        # (text, keywords) -> bool
    _infer_product,         # (profile_stub | None, origin_str) -> str | None
    _estimate_eta,          # (ais_destination_str) -> ISO date str
)

BASE    = Path(__file__).parent
DB_PATH = BASE / 'hidroviadata.db'

# DWT sweet-spot for this pipeline (handysize to upper-panamax fertilizer vessels)
DWT_MIN = 25_000
DWT_MAX = 60_000


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_con() -> sqlite3.Connection:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run  python3 migrate.py  first.")
        raise SystemExit(1)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _load_core_fleet(con: sqlite3.Connection) -> dict[str, dict]:
    """Return the fertilizer_core_fleet table keyed by vessel_name (upper-cased)."""
    rows = con.execute(
        'SELECT vessel_name, fertilizer_visits, visits_to_argentina, '
        '       dominant_product, dominant_origin, dominant_importer, '
        '       avg_tonnage, min_tonnage, max_tonnage '
        'FROM fertilizer_core_fleet'
    ).fetchall()
    return {dict(r)['vessel_name'].upper(): dict(r) for r in rows}


# ── Scoring ───────────────────────────────────────────────────────────────────

def _score_observation(obs: dict, core_fleet: dict[str, dict]) -> dict:
    """
    Score one AIS observation.

    Returns a fully populated candidate dict ready for insertion into
    vessel_candidates (all values are DB-ready — JSON columns pre-serialised).
    """
    name      = (obs.get('vessel_name')     or '').strip().upper()
    last_port = (obs.get('last_port')       or '').strip().upper()
    ais_dest  = (obs.get('ais_destination') or '').strip().upper()
    vtype     = (obs.get('vessel_type')     or '').strip().upper()
    dwt       = obs.get('dwt')
    eta_in    = obs.get('eta_estimated')    # caller-supplied override (optional)

    core_rec  = core_fleet.get(name)        # None for cold-start vessels

    score   = 0
    reasons: list[str] = []

    # ── +25: vessel is in the fertilizer core fleet ───────────────────────────
    if core_rec:
        score += 25
        fv = core_rec['fertilizer_visits']
        reasons.append(
            f"+25 — In fertilizer core fleet "
            f"({fv} fertilizer visit{'s' if fv != 1 else ''} on record)"
        )

    # ── +20: last_port matches a known fertilizer origin country / port ───────
    if last_port and _match_keywords(last_port, FERTILIZER_ORIGINS):
        score += 20
        reasons.append(
            f"+20 — Last port '{obs.get('last_port')}' matches known "
            f"fertilizer origin corridor"
        )

    # ── +15: AIS destination suggests Argentina / River Plate ─────────────────
    if ais_dest and _match_keywords(ais_dest, ARGENTINA_DEST_KEYWORDS):
        score += 15
        reasons.append(
            f"+15 — AIS destination '{obs.get('ais_destination')}' matches "
            f"Argentina / River Plate pattern"
        )

    # ── +10: vessel type is a bulk carrier ────────────────────────────────────
    if vtype and _match_keywords(vtype, BULK_VESSEL_TYPES):
        score += 10
        reasons.append(
            f"+10 — Vessel type '{obs.get('vessel_type')}' matches bulk "
            f"fertilizer carrier"
        )

    # ── +10: DWT falls within the target handysize–panamax range ─────────────
    if dwt is not None and DWT_MIN <= int(dwt) <= DWT_MAX:
        score += 10
        reasons.append(
            f"+10 — DWT {int(dwt):,} within target range "
            f"({DWT_MIN:,}–{DWT_MAX:,} t)"
        )

    if not reasons:
        reasons.append("No matching signals found.")

    final_score = min(score, 100)

    if final_score >= 80:
        level  = 'high'
        status = 'probable'
    elif final_score >= 60:
        level  = 'medium'
        status = 'estimated'
    else:
        level  = 'low'
        status = 'estimated'

    # ── Product inference (reuses vessel_scorer 4-level priority chain) ───────
    # Build a minimal profile stub from the core fleet record so that
    # _infer_product can apply priority 1 (dominant_product) and priority 2/3
    # (dominant_importer) before falling back to the origin (last_port).
    profile_stub: dict | None = None
    if core_rec:
        profile_stub = {
            'dominant_product':  core_rec.get('dominant_product'),
            'dominant_importer': core_rec.get('dominant_importer'),
        }

    probable_product  = _infer_product(profile_stub, last_port)
    probable_importer = core_rec.get('dominant_importer') if core_rec else None

    # Tonnage range: use core fleet historical min/max when available
    tonnage_range: list[int] | None = None
    if core_rec:
        lo = core_rec.get('min_tonnage') or 0
        hi = core_rec.get('max_tonnage') or 0
        if lo and hi:
            tonnage_range = [lo, hi]

    # ETA: honour caller-supplied override; otherwise estimate from AIS text
    eta = eta_in or _estimate_eta(obs.get('ais_destination') or '')

    return {
        'vessel_name':            obs.get('vessel_name', '').strip(),
        'last_port':              obs.get('last_port'),
        'ais_destination':        obs.get('ais_destination'),
        'eta_estimated':          eta,
        'probable_product':       probable_product,
        'probable_importer':      probable_importer,
        'probable_tonnage_range': json.dumps(tonnage_range),
        'probability_score':      final_score,
        'probability_level':      level,
        'prediction_status':      'predicted',   # always 'predicted' from this pipeline
        'scoring_reasons':        json.dumps(reasons, ensure_ascii=False),
    }


# ── DB insert ─────────────────────────────────────────────────────────────────

def _insert_candidate(con: sqlite3.Connection, candidate: dict) -> int:
    """Insert one candidate dict into vessel_candidates. Returns the new row id."""
    cur = con.execute(
        """
        INSERT INTO vessel_candidates
            (vessel_name, last_port, ais_destination, eta_estimated,
             probable_product, probable_importer, probable_tonnage_range,
             probability_score, probability_level, prediction_status,
             scoring_reasons)
        VALUES
            (:vessel_name, :last_port, :ais_destination, :eta_estimated,
             :probable_product, :probable_importer, :probable_tonnage_range,
             :probability_score, :probability_level, :prediction_status,
             :scoring_reasons)
        """,
        candidate,
    )
    con.commit()
    return cur.lastrowid


# ── Pretty print ──────────────────────────────────────────────────────────────

def _print_candidate(candidate: dict, row_id: int | None = None) -> None:
    reasons  = json.loads(candidate['scoring_reasons'])
    tonnage  = json.loads(candidate['probable_tonnage_range'] or 'null')
    score    = candidate['probability_score']

    # Score bar scaled to 80-point max (each block = 4 pts)
    filled   = score // 4
    empty    = 20 - filled
    bar      = '█' * filled + '░' * empty

    footer = (
        f"  DB row id : {row_id}  (vessel_candidates)"
        if row_id is not None
        else "  (dry run — not inserted)"
    )

    print()
    print(f"  ╔══════════════════════════════════════════════════╗")
    print(f"  ║  {candidate['vessel_name']:<46}  ║")
    print(f"  ╚══════════════════════════════════════════════════╝")
    print(f"  Score   : {score:>3} / 80  [{bar}]  "
          f"{candidate['probability_level'].upper()}  "
          f"({candidate['prediction_status']})")
    print(f"  Product : {candidate['probable_product'] or '—'}")
    print(f"  Importer: {candidate['probable_importer'] or '—'}")
    if tonnage:
        print(f"  Tonnage : {tonnage[0]:,} – {tonnage[1]:,} t")
    print(f"  ETA     : {candidate['eta_estimated']}")
    print(f"  Status  : {candidate['prediction_status']}")
    print()
    for r in reasons:
        print(f"    {r}")
    print(f"\n  {footer}")


# ── Interactive mode ──────────────────────────────────────────────────────────

def _prompt_observation() -> dict:
    """Prompt the user for one AIS observation interactively."""
    print()
    print("  Enter AIS observation (press Enter to skip optional fields)")
    print("  " + "─" * 50)

    def ask(label: str, required: bool = False) -> str | None:
        while True:
            val = input(f"  {label}: ").strip()
            if val:
                return val
            if not required:
                return None
            print(f"  ✗ {label} is required.")

    name     = ask("Vessel name                            (required)", required=True)
    lp       = ask("Last port      e.g. MESAIEED / CHINA  (optional)")
    ais_dest = ask("AIS destination e.g. RECALADA / ROSARIO(optional)")
    vtype    = ask("Vessel type    e.g. BULK CARRIER       (optional)")
    dwt_raw  = ask("DWT            e.g. 32000              (optional)")
    eta_raw  = ask("ETA override   YYYY-MM-DD              (optional)")

    dwt: int | None = None
    if dwt_raw:
        try:
            dwt = int(dwt_raw.replace(',', '').replace('.', '').replace(' ', ''))
        except ValueError:
            print(f"  ⚠ Invalid DWT '{dwt_raw}' — ignored.")

    return {
        'vessel_name':     name,
        'last_port':       lp,
        'ais_destination': ais_dest,
        'vessel_type':     vtype,
        'dwt':             dwt,
        'eta_estimated':   eta_raw,
    }


# ── Batch mode ────────────────────────────────────────────────────────────────

def _load_batch(path: Path) -> list[dict]:
    """Load observations from a JSON file. Accepts an array or a single object."""
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse {path}: {e}")
        raise SystemExit(1)

    if isinstance(data, dict):
        print(f"  ℹ Single object detected — wrapped in array.")
        data = [data]

    if not isinstance(data, list):
        print(f"ERROR: {path} must contain a JSON array or single object.")
        raise SystemExit(1)

    return data


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    dry_run    = '--dry-run' in sys.argv
    args       = [a for a in sys.argv[1:] if not a.startswith('--')]
    batch_file = Path(args[0]) if args else None

    if batch_file and not batch_file.exists():
        print(f"ERROR: File not found: {batch_file}")
        raise SystemExit(1)

    con        = _get_con()
    core_fleet = _load_core_fleet(con)
    print(f"\n  Fertilizer core fleet loaded : {len(core_fleet)} vessels")

    if batch_file:
        observations = _load_batch(batch_file)
        print(f"  Observations in file         : {len(observations)}")
    else:
        observations = [_prompt_observation()]

    inserted = 0
    for obs in observations:
        candidate = _score_observation(obs, core_fleet)
        if dry_run:
            _print_candidate(candidate, row_id=None)
        else:
            row_id = _insert_candidate(con, candidate)
            _print_candidate(candidate, row_id=row_id)
            inserted += 1

    # ── Sync output/buques_en_ruta.json from DB ──────────────────────────────
    if not dry_run and inserted > 0:
        con.row_factory = sqlite3.Row
        rows = con.execute(
            'SELECT vessel_name, last_position, last_port, ais_destination, '
            '       eta_estimated, probable_product, probable_importer, '
            '       probable_tonnage_range, probability_score, probability_level, '
            '       prediction_status, scoring_reasons '
            'FROM vessel_candidates ORDER BY probability_score DESC'
        ).fetchall()
        out_records = []
        for row in rows:
            d = dict(row)
            d['probable_tonnage_range'] = json.loads(d['probable_tonnage_range'] or 'null')
            d['scoring_reasons']        = json.loads(d['scoring_reasons']        or '[]')
            out_records.append(d)
        out_json = BASE / 'output' / 'buques_en_ruta.json'
        out_json.write_text(
            json.dumps(out_records, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        print(f"  output/buques_en_ruta.json synced ({len(out_records)} candidates)")

    con.close()
    print()
    if dry_run:
        print(f"  Dry run — {len(observations)} candidate(s) scored, nothing written to DB.")
    else:
        print(f"  Done — {inserted} candidate(s) inserted into vessel_candidates.")
    print()


if __name__ == '__main__':
    main()
