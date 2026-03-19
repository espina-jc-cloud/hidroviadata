"""
predict_vessels.py
──────────────────
Bridge script: score candidate vessels → produce buques_en_ruta.json.

Usage
─────
    python3 predict_vessels.py                  # use built-in CANDIDATE_VESSELS
    python3 predict_vessels.py candidates.json  # load candidates from JSON file

Candidate JSON format
─────────────────────
The file must contain a JSON array of objects (or a single object, which is
auto-wrapped).  All fields except vessel_name are optional, but omitting
last_port or ais_destination suppresses the two strongest scoring signals.

    [
      {
        "vessel_name":     "LUCKY STAR",       // str or null  — AIS name; null = scored anonymously
        "last_port":       "ARZEW (ARGELIA)",  // str or null  — last port / country of origin
        "ais_destination": "RECALADA",         // str or null  — raw AIS destination text
        "vessel_type":     "BULK CARRIER",     // str or null  — e.g. BULK CARRIER, HANDYMAX
        "dwt":             28500,              // int or null  — deadweight tonnage (must be integer)
        "current_month":   "03",              // str "01"–"12" or null — defaults to today
        "last_position":   "25.4°S 44.1°W"    // str or null  — display only, not scored
      }
    ]

Field reference
───────────────
    vessel_name     Optional. Used for profile look-up in vessel_profiles.json.
                    If null or omitted the vessel is scored on AIS signals alone.
    last_port       Recommended. Matched against FERTILIZER_ORIGINS (+20 pts).
    ais_destination Recommended. Matched against ARGENTINA_DEST_KEYWORDS (+15 pts).
    vessel_type     Optional. Matched against BULK_VESSEL_TYPES (+10 pts).
    dwt             Optional. Must be an integer. Checked against 8 000–85 000 t range (+10 pts).
    current_month   Optional. Two-digit string "01"–"12". Defaults to the current calendar month.
    last_position   Optional. Stored as-is for display in the dashboard; not used for scoring.

Output
──────
    output/buques_en_ruta.json   — ready to embed in dashboard.html
    Run embed_predictions.py after this script to update the dashboard.

Note
────
    This is NOT a live AIS integration. Candidates must be supplied manually
    or from an intermediate file populated by an AIS poller.
    Only vessel_scorer.py + vessel_profiles.json are required to run.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import vessel_scorer
import buques_en_ruta

# ── Score threshold — entries below this are discarded ────────────────────────
# Keeps the tab focused on realistic candidates only.
SCORE_THRESHOLD = 30

# ── Sample candidates (replace with real AIS data when integration is live) ───
# These represent vessels that might realistically be heading to Argentina.
# Populate this list by running an AIS query for vessels in the South Atlantic
# or Eastern Atlantic with destinations suggesting Argentina / River Plate.
CANDIDATE_VESSELS: list[dict] = [
    {
        "vessel_name":     "LUCKY STAR",
        "last_position":   "25.4°S 44.1°W",
        "last_port":       "ARZEW (ARGELIA)",
        "ais_destination": "RECALADA",
        "vessel_type":     "BULK CARRIER",
        "dwt":             28_500,
    },
    {
        "vessel_name":     "NOVA GRAIN",
        "last_position":   "18.2°S 36.7°W",
        "last_port":       "MESAIEED (QATAR)",
        "ais_destination": "SAN NICOLAS",
        "vessel_type":     "BULK CARRIER",
        "dwt":             35_000,
    },
    {
        "vessel_name":     "ATLAS PIONEER",
        "last_position":   "10.5°S 28.3°W",
        "last_port":       "KOTKA (FINLANDIA)",
        "ais_destination": "ROSARIO",
        "vessel_type":     "BULK CARRIER",
        "dwt":             22_000,
    },
    {
        "vessel_name":     "CLIPPER I-STAR",
        "last_position":   "31.0°S 49.2°W",
        "last_port":       "CHINA",
        "ais_destination": "RECALADA",
        "vessel_type":     "BULK CARRIER",
        "dwt":             18_500,
    },
    {
        "vessel_name":     None,          # unknown vessel — scored on signals only
        "last_position":   "5.1°N 15.3°W",
        "last_port":       "MARRUECOS",
        "ais_destination": "UPRIVER",
        "vessel_type":     "HANDYMAX",
        "dwt":             45_000,
    },
]


# ── Candidate schema ──────────────────────────────────────────────────────────
# Used for validation and documentation.  Values describe the field; they are
# not parsed — validation logic lives in validate_candidate() below.

CANDIDATE_SCHEMA: dict[str, str] = {
    "vessel_name":     "str | null  — AIS vessel name; null = scored anonymously",
    "last_port":       "str | null  — last port or country of origin (RECOMMENDED: +20 pts)",
    "ais_destination": "str | null  — raw AIS destination text   (RECOMMENDED: +15 pts)",
    "vessel_type":     "str | null  — e.g. 'BULK CARRIER', 'HANDYMAX'",
    "dwt":             "int | null  — deadweight tonnage; must be integer not string",
    "current_month":   "str | null  — '01'–'12'; defaults to today's calendar month",
    "last_position":   "str | null  — lat/lon display string; not used for scoring",
}

# Fields that directly feed the two strongest scoring signals.
# Missing these silently reduces the maximum achievable score.
_RECOMMENDED = {"last_port", "ais_destination"}

# All known field names — extras generate a warning.
_KNOWN_FIELDS = set(CANDIDATE_SCHEMA)


# ── Candidate validation ──────────────────────────────────────────────────────

def validate_candidate(c: dict, idx: int) -> list[str]:
    """
    Return a list of human-readable warnings for the candidate at position idx.
    Warnings are informational only — they do not block scoring.
    """
    warnings: list[str] = []
    label = f"[{idx}] {(c.get('vessel_name') or 'UNKNOWN')!r}"

    # Unknown keys
    unknown = set(c) - _KNOWN_FIELDS
    if unknown:
        warnings.append(
            f"{label}: unrecognised field(s): {', '.join(sorted(unknown))} "
            f"(will be ignored)"
        )

    # Recommended fields missing or empty
    for field in _RECOMMENDED:
        if not c.get(field):
            warnings.append(
                f"{label}: '{field}' is missing or null "
                f"— scoring signal will not fire"
            )

    # dwt must be int (or null)
    dwt = c.get("dwt")
    if dwt is not None and not isinstance(dwt, int):
        warnings.append(
            f"{label}: 'dwt' must be an integer, got "
            f"{type(dwt).__name__!r} ({dwt!r}) — value will be ignored by scorer"
        )

    # current_month must be "01"–"12" if provided
    month = c.get("current_month")
    if month is not None:
        valid_month = (
            isinstance(month, str)
            and len(month) == 2
            and month.isdigit()
            and 1 <= int(month) <= 12
        )
        if not valid_month:
            warnings.append(
                f"{label}: 'current_month' must be '01'–'12', got {month!r} "
                f"— scorer will use today's month instead"
            )

    return warnings


# ── Candidate file loader ─────────────────────────────────────────────────────

def load_candidates(path: Path) -> list[dict]:
    """
    Load and validate candidate vessels from a JSON file.

    Accepts:
      - A JSON array of candidate objects  →  used as-is
      - A single JSON object               →  auto-wrapped in a list

    Per-entry validation warnings are printed to stdout but do not stop
    processing.  Entries that are not dicts are skipped with an error message.

    Returns a (possibly empty) list of candidate dicts.
    """
    if not path.exists():
        print(f"ERROR: File not found: {path}")
        sys.exit(1)

    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as exc:
        print(f"ERROR: Invalid JSON in {path}: {exc}")
        sys.exit(1)

    # Accept a single object as well as an array
    if isinstance(raw, dict):
        raw = [raw]
        print("  ℹ  Single JSON object detected — treated as a one-entry list")

    if not isinstance(raw, list):
        print(f"ERROR: Expected a JSON array at the top level, got {type(raw).__name__!r}")
        sys.exit(1)

    candidates: list[dict] = []
    total_warnings = 0

    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            print(f"  ✗  Entry [{idx}] is not a JSON object "
                  f"(got {type(item).__name__!r}) — skipped")
            continue

        warns = validate_candidate(item, idx)
        for w in warns:
            print(f"  ⚠  {w}")
        total_warnings += len(warns)

        candidates.append(item)

    if total_warnings:
        print()  # blank line before results

    return candidates


# ── Core runner ───────────────────────────────────────────────────────────────

def run(candidates: list[dict]) -> tuple[list[dict], int]:
    """
    Score each candidate vessel and return (entries, skipped_count).

    Steps per candidate:
        1. Build scorer input from candidate fields.
        2. Call vessel_scorer.score_vessel() — deterministic, no ML.
        3. Derive ETA using vessel_scorer._estimate_eta() placeholder.
        4. Filter out entries below SCORE_THRESHOLD.
        5. Build and validate a buques_en_ruta entry.
        6. Return list sorted by probability_score descending.
    """
    entries: list[dict] = []
    skipped = 0

    for c in candidates:
        name = (c.get("vessel_name") or "").strip() or "UNKNOWN"

        # ── Build scorer input ────────────────────────────────────────────────
        scorer_input = {
            "vessel_name":     c.get("vessel_name"),
            "origin":          c.get("last_port"),
            "ais_destination": c.get("ais_destination"),
            "vessel_type":     c.get("vessel_type"),
            "dwt":             c.get("dwt") if isinstance(c.get("dwt"), int) else None,
            "current_month":   c.get("current_month"),   # None → scorer uses today
        }

        # ── Score ─────────────────────────────────────────────────────────────
        result = vessel_scorer.score_vessel(scorer_input)
        score  = result["probability_score"]

        if score < SCORE_THRESHOLD:
            skipped += 1
            continue

        # ── ETA estimation (placeholder until AIS provides real ETA) ─────────
        eta = vessel_scorer._estimate_eta(c.get("ais_destination", ""))

        # ── Build validated entry ─────────────────────────────────────────────
        try:
            entry = buques_en_ruta.build_entry(
                vessel_name            = name,
                last_position          = c.get("last_position") or "—",
                last_port              = c.get("last_port")      or "—",
                ais_destination        = c.get("ais_destination") or "—",
                eta_estimated          = eta,
                probability_score      = result["probability_score"],
                probability_level      = result["probability_level"],
                prediction_status      = result["prediction_status"],
                probable_product       = result["probable_product"],
                probable_importer      = result["probable_importer"],
                probable_tonnage_range = result["probable_tonnage_range"],
                scoring_reasons        = result["scoring_reasons"],
            )
            entries.append(entry)
        except ValueError as exc:
            print(f"  ⚠  Skipped {name!r}: {exc}")

    # Already sorted by score desc (buques_en_ruta.save does this too, but be explicit)
    entries.sort(key=lambda e: e["probability_score"], reverse=True)
    return entries, skipped


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Load candidates from file or fall back to built-in list
    if len(sys.argv) > 1:
        source_path = Path(sys.argv[1])
        candidates  = load_candidates(source_path)
        print(f"Loaded {len(candidates)} candidate(s) from {source_path}")
    else:
        candidates = CANDIDATE_VESSELS
        print(f"Using {len(candidates)} built-in candidate vessel(s)")

    print(f"Score threshold: ≥{SCORE_THRESHOLD}\n")

    entries, skipped = run(candidates)

    # Persist
    buques_en_ruta.save(entries)

    # Summary
    print(f"{'─'*70}")
    print(f"Results: {len(entries)} prediction(s) saved  |  {skipped} discarded (score < {SCORE_THRESHOLD})")
    print(f"Output:  output/buques_en_ruta.json")
    print(f"{'─'*70}")
    for e in entries:
        prod = (e["probable_product"] or "—").ljust(6)
        print(
            f"  [{e['probability_level'].upper():6}]  "
            f"{e['vessel_name']:25}  "
            f"score={e['probability_score']:3}  "
            f"status={e['prediction_status']:10}  "
            f"product={prod}  "
            f"ETA={e['eta_estimated']}"
        )
    print()
    print("Next step: run  python3 embed_predictions.py  to update dashboard.html")
