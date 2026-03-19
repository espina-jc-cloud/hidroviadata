"""
buques_en_ruta.py
─────────────────
Schema definition, validation, mock generator, and save/load helpers
for the "Buques en Ruta" internal data structure.

This module is the single source of truth for the schema.
It is intentionally decoupled from both parser.py and dashboard.html
so it can be populated later from any AIS provider without touching
existing code.

Lifecycle
─────────
1. Now   → generate mocks or load an empty list         (this file)
2. Soon  → AIS poller calls `build_entry()` per vessel  (this file)
3. Later → scorer enriches each entry via vessel_scorer  (vessel_scorer.py)
4. Final → dashboard reads BUQUES_EN_RUTA_DATA injected  (dashboard.html)
"""

from __future__ import annotations

import json
import datetime
from pathlib import Path
from typing import Literal

# ── Output path (consistent with project convention) ──────────────────────────
OUTPUT_PATH = Path(__file__).parent / "output" / "buques_en_ruta.json"

# ── Allowed literals ──────────────────────────────────────────────────────────
PredictionStatus = Literal["estimated", "probable", "confirmed"]
ProbabilityLevel  = Literal["high", "medium", "low"]


# ── Schema (Python dict spec — used for validation and documentation) ─────────
SCHEMA: dict[str, str] = {
    # Identity
    "vessel_name":            "str   — IMO vessel name as seen in AIS",
    # AIS position snapshot
    "last_position":          "str   — lat/lon string or human label, e.g. '34.2°S 52.1°W'",
    "last_port":              "str   — last confirmed port of departure",
    "ais_destination":        "str   — raw AIS destination text from transponder",
    "eta_estimated":          "str   — ISO date YYYY-MM-DD, estimated arrival at San Nicolás",
    # Prediction outputs (from vessel_scorer.py)
    "probable_product":       "str|None — most likely fertilizer product",
    "probable_importer":      "str|None — most likely Argentine importer",
    "probable_tonnage_range": "[int, int]|None — [min_tons, max_tons]",
    "probability_score":      "int   — 0–100 additive score from vessel_scorer",
    "probability_level":      "str   — 'high' | 'medium' | 'low'",
    # Lifecycle state
    "prediction_status":      "str   — 'estimated' | 'probable' | 'confirmed'",
    "scoring_reasons":        "list[str] — human-readable list of signals that fired",
}


# ── Validation ────────────────────────────────────────────────────────────────
REQUIRED_FIELDS = {
    "vessel_name", "last_position", "last_port",
    "ais_destination", "eta_estimated",
    "probability_score", "probability_level", "prediction_status",
}

STATUS_VALUES = {"estimated", "probable", "confirmed"}
LEVEL_VALUES  = {"high", "medium", "low"}


def validate(entry: dict) -> list[str]:
    """Return a list of validation errors (empty = valid)."""
    errors = []
    for f in REQUIRED_FIELDS:
        if f not in entry or entry[f] is None:
            errors.append(f"Missing required field: {f!r}")
    status = entry.get("prediction_status")
    if status and status not in STATUS_VALUES:
        errors.append(f"prediction_status must be one of {STATUS_VALUES}, got {status!r}")
    level = entry.get("probability_level")
    if level and level not in LEVEL_VALUES:
        errors.append(f"probability_level must be one of {LEVEL_VALUES}, got {level!r}")
    score = entry.get("probability_score")
    if score is not None and not (0 <= score <= 100):
        errors.append(f"probability_score must be 0–100, got {score!r}")
    return errors


# ── Builder (called by future AIS integration) ────────────────────────────────
def build_entry(
    vessel_name:            str,
    last_position:          str,
    last_port:              str,
    ais_destination:        str,
    eta_estimated:          str,
    probability_score:      int,
    probability_level:      ProbabilityLevel,
    prediction_status:      PredictionStatus,
    probable_product:       str | None = None,
    probable_importer:      str | None = None,
    probable_tonnage_range: list[int] | None = None,
    scoring_reasons:        list[str] | None = None,
) -> dict:
    """
    Construct a validated Buques en Ruta entry.
    Raises ValueError if validation fails.

    Intended call-site (future AIS poller):
        ais_data = fetch_from_ais_provider(mmsi)
        score    = vessel_scorer.score_vessel({...})
        entry    = buques_en_ruta.build_entry(
                       vessel_name    = ais_data['name'],
                       last_position  = ais_data['position'],
                       last_port      = ais_data['last_port'],
                       ais_destination= ais_data['destination'],
                       eta_estimated  = ais_data['eta'],
                       **score,
                       prediction_status = 'estimated',
                   )
        buques_en_ruta.upsert(entry)
    """
    entry = {
        "vessel_name":            vessel_name,
        "last_position":          last_position,
        "last_port":              last_port,
        "ais_destination":        ais_destination,
        "eta_estimated":          eta_estimated,
        "probable_product":       probable_product,
        "probable_importer":      probable_importer,
        "probable_tonnage_range": probable_tonnage_range,
        "probability_score":      probability_score,
        "probability_level":      probability_level,
        "prediction_status":      prediction_status,
        "scoring_reasons":        scoring_reasons or [],
    }
    errors = validate(entry)
    if errors:
        raise ValueError("Invalid entry:\n" + "\n".join(f"  • {e}" for e in errors))
    return entry


# ── Upsert / persistence ──────────────────────────────────────────────────────
def load() -> list[dict]:
    """Load current buques_en_ruta.json; return empty list if file missing."""
    if not OUTPUT_PATH.exists():
        return []
    with open(OUTPUT_PATH, encoding="utf-8") as f:
        return json.load(f)


def save(entries: list[dict]) -> None:
    """Persist entries to output/buques_en_ruta.json."""
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def upsert(entry: dict) -> None:
    """
    Insert or replace a vessel entry (matched by vessel_name).
    Safe to call repeatedly from an AIS polling loop.
    """
    entries = load()
    name    = entry["vessel_name"]
    entries = [e for e in entries if e["vessel_name"] != name]
    entries.append(entry)
    # Sort by probability_score desc so dashboard can render top candidates first
    entries.sort(key=lambda e: e["probability_score"], reverse=True)
    save(entries)


# ── Mock generator (for development / dashboard preview) ─────────────────────
def generate_mocks() -> list[dict]:
    today = datetime.date.today()

    mocks_raw = [
        dict(
            vessel_name            = "LUCKY STAR",
            last_position          = "25.4°S  44.1°W",
            last_port              = "ARZEW (ARGELIA)",
            ais_destination        = "RECALADA",
            eta_estimated          = (today + datetime.timedelta(days=8)).isoformat(),
            probability_score      = 80,
            probability_level      = "high",
            prediction_status      = "probable",
            probable_product       = "UREA",
            probable_importer      = "CARGIL",
            probable_tonnage_range = [7_000, 12_000],
            scoring_reasons        = [
                "+20 — Origin 'ARGELIA' matches known fertilizer origin corridor",
                "+15 — AIS destination 'RECALADA' matches Argentina / River Plate / upriver pattern",
                "+10 — Vessel type 'BULK CARRIER' matches fertilizer carrier profile",
                "+10 — DWT 28,500 within 8,000–85,000 t range",
                "+25 — Known vessel: 4 previous visits to Argentina",
            ],
        ),
        dict(
            vessel_name            = "NOVA GRAIN",
            last_position          = "18.2°S  36.7°W",
            last_port              = "MESAIEED (QATAR)",
            ais_destination        = "SAN NICOLAS",
            eta_estimated          = (today + datetime.timedelta(days=14)).isoformat(),
            probability_score      = 65,
            probability_level      = "high",
            prediction_status      = "estimated",
            probable_product       = "DAP",
            probable_importer      = "BUNGE",
            probable_tonnage_range = [5_000, 9_000],
            scoring_reasons        = [
                "+20 — Origin 'QATAR' matches known fertilizer origin corridor",
                "+15 — AIS destination 'SAN NICOLAS' matches Argentina / River Plate / upriver pattern",
                "+10 — Vessel type 'BULK CARRIER' matches fertilizer carrier profile",
                "+20 — 3 fertilizer visits on record (dominant product: DAP)",
            ],
        ),
        dict(
            vessel_name            = "ATLAS PIONEER",
            last_position          = "10.5°S  28.3°W",
            last_port              = "KOTKA (FINLANDIA)",
            ais_destination        = "ROSARIO",
            eta_estimated          = (today + datetime.timedelta(days=22)).isoformat(),
            probability_score      = 45,
            probability_level      = "medium",
            prediction_status      = "estimated",
            probable_product       = "NPS",
            probable_importer      = None,
            probable_tonnage_range = [3_000, 12_000],
            scoring_reasons        = [
                "+20 — Origin 'FINLANDIA' matches known fertilizer origin corridor",
                "+15 — AIS destination 'ROSARIO' matches Argentina / River Plate / upriver pattern",
                "+10 — Vessel type 'BULK CARRIER' matches fertilizer carrier profile",
            ],
        ),
    ]

    entries = []
    for raw in mocks_raw:
        try:
            entries.append(build_entry(**raw))
        except ValueError as e:
            print(f"Mock validation error: {e}")
    return entries


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mocks = generate_mocks()
    save(mocks)
    print(f"output/buques_en_ruta.json written — {len(mocks)} mock entries")
    print()
    for e in mocks:
        print(
            f"  [{e['probability_level'].upper():6}] {e['vessel_name']:25} "
            f"score={e['probability_score']:3}  "
            f"status={e['prediction_status']:10}  "
            f"ETA={e['eta_estimated']}"
        )
