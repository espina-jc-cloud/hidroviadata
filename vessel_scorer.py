"""
vessel_scorer.py
Predictive scoring engine — estimates the probability that an externally-detected
vessel is a fertilizer vessel heading to Argentina.

Depends on: output/vessel_profiles.json  (built by build_vessel_profiles.py)
No AIS integration required — scoring is deterministic from the input schema below.

Input schema:
    {
      "vessel_name":    str | None,   # used for profile lookup (optional)
      "origin":         str | None,   # last port / origin country/city from AIS
      "ais_destination":str | None,   # raw AIS destination text
      "vessel_type":    str | None,   # e.g. "BULK CARRIER", "GENERAL CARGO"
      "dwt":            int | None,   # deadweight tonnage
      "current_month":  str | None,   # "01"–"12"  (uses today if omitted)
    }

Output schema:
    {
      "probability_score":      int,        # 0–100
      "probability_level":      str,        # "high" | "medium" | "low"
      "prediction_status":      str,        # "probable" | "estimated"
      "probable_product":       str | None,
      "probable_importer":      str | None,
      "probable_tonnage_range": [int, int] | None,
      "scoring_reasons":        list[str],
    }

Scoring thresholds:
    ≥80  → high   / probable
    60–79 → medium / estimated
    <60  → low    / estimated
"""

import json
import datetime
from pathlib import Path

# ── Knowledge tables (derived from historical data) ───────────────────────────

# Top fertilizer origin countries/ports  (from output/data.json analysis)
FERTILIZER_ORIGINS = {
    'CHINA', 'QATAR', 'MARRUECOS', 'FINLANDIA', 'NIGERIA', 'ARGELIA',
    'RUSSIA', 'RUSIA',                              # RUSIA = Spanish alias for RUSSIA
    'NORUEGA', 'OMAN', 'GEORGIA', 'ARZEW', 'ARABIA', 'BELGICA',
    'EGIPTO', 'JAPON', 'CANADA', 'INDIA', 'ISRAEL', 'JORDANIA', 'PERU',
    'MEXICO',                                        # urea / NPK exports to Argentina
    'RUMANIA',                                       # ammonium nitrate (NITRODOBLE)
    # common port aliases
    'MESAIEED', 'YUZHNE', 'VENTSPILS', 'KOTKA',
}

# AIS destination keywords that indicate River Plate / Argentine upriver
ARGENTINA_DEST_KEYWORDS = {
    'ARGENTINA', 'RECALADA', 'BUENOS AIRES', 'ROSARIO', 'SAN NICOLAS',
    'NICOLAS', 'RIVER PLATE', 'RIO DE LA PLATA', 'UPRIVER', 'UP RIVER',
    'ZARATE', 'CAMPANA', 'LA PLATA', 'DOCK SUD', 'RAMALLO', 'VILLA CONSTITUCION',
    'PARANA', 'SANTA FE', 'ARROYO SECO', 'PORT ROSARIO', 'RIACHUELO',
}

# Vessel types that carry bulk fertilizers
BULK_VESSEL_TYPES = {
    'BULK CARRIER', 'BULK', 'GENERAL CARGO', 'HANDYSIZE', 'HANDYMAX',
    'SUPRAMAX', 'ULTRAMAX', 'PANAMAX', 'GEARLESS BULK', 'OPEN HATCH',
}

# Typical fertilizer DWT range seen at San Nicolás (handysize to panamax)
DWT_FERT_MIN = 8_000
DWT_FERT_MAX = 85_000

# Origin → probable product (lowest-priority fallback — used only when no profile
# or importer signal is available).
# Based on dominant fertilizer exports from each country/port.
ORIGIN_TO_PRODUCT: dict[str, str] = {
    # Phosphates
    'MARRUECOS': 'MAP',  'MOROCCO': 'MAP',   'CHINA': 'MAP',
    'JORDANIA':  'DAP',  'JORDAN':  'DAP',   'ISRAEL': 'DAP',   'INDIA': 'DAP',
    # Nitrogen / UREA
    'NIGERIA':   'UREA', 'ARGELIA': 'UREA',  'ARZEW': 'UREA',
    'QATAR':     'UREA', 'MESAIEED': 'UREA', 'EGIPTO': 'UREA',
    'RUSSIA':    'UREA', 'RUSIA':   'UREA',  # RUSIA = Spanish alias
    'YUZHNE':    'UREA', 'OMAN':    'UREA',  'ARABIA': 'UREA',
    'MEXICO':    'UREA',                      # Mexican urea / NPK exports
    # Potash / nitrogen blends
    'FINLANDIA': 'NPS',  'KOTKA':   'NPS',   'NORUEGA': 'NPS',
    'BELGICA':   'NPS',  'GEORGIA': 'NPS',
    'CANADA':    'MOP',  'VENTSPILS': 'MOP',
    'PERU':      'NPS',
}

# Importer → probable product (priority 3 — takes precedence over origin fallback).
# Derived from historical data (dominant product by record count, minimum 1.5× plurality).
IMPORTER_TO_PRODUCT: dict[str, str] = {
    'BUNGE':   'MAP',    # 27 MAP vs 7 DAP — clear phosphate focus
    'CARGIL':  'UREA',   # 19 UREA vs 11 MAP — nitrogen dominant
    'LDC':     'UREA',   # 10 UREA vs 5 NPS — nitrogen dominant (Louis Dreyfus)
    'COFCO':   'MAP',    # 5 MAP vs 3 TSP — phosphate focus (Chinese state trader)
    'NUTRIEN': 'MAP',    # 7 MAP vs 4 TSP — phosphate dominant (data-driven)
    'ACA':     'UREA',   # 12 UREA vs 9 MAP — nitrogen lean (Argentine cooperative)
    'AFA':     'UREA',   # 5 UREA vs 3 NPS — nitrogen lean (Argentine cooperative)
}

# ── Load vessel profiles ──────────────────────────────────────────────────────
_PROFILES: dict[str, dict] = {}

def _load_profiles():
    global _PROFILES
    if _PROFILES:
        return
    path = Path(__file__).parent / 'output' / 'vessel_profiles.json'
    if not path.exists():
        raise FileNotFoundError(
            "output/vessel_profiles.json not found. "
            "Run build_vessel_profiles.py first."
        )
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    _PROFILES = {p['vessel_name'].upper(): p for p in data}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise(s: str | None) -> str:
    return (s or '').strip().upper()

def _match_keywords(text: str, keywords: set[str]) -> bool:
    t = _normalise(text)
    return any(kw in t for kw in keywords)

def _infer_product_from_origin(origin: str) -> str | None:
    """Map a normalised origin string to the most probable fertilizer product."""
    for key, product in ORIGIN_TO_PRODUCT.items():
        if key in origin:
            return product
    return None


# Recognised fertilizer product codes — guards priority 1 so that a non-fertilizer
# dominant_product on the profile (e.g. 'ARRABIO' for a mixed-cargo vessel) does not
# propagate as the predicted product.
_FERT_PRODUCTS: frozenset[str] = frozenset({
    'UREA', 'MAP', 'DAP', 'MOP', 'TSP', 'NPS', 'UAN', 'AMSUL', 'NP', 'NPK',
    'SSP', 'STP', 'GMOP', 'NPS+ZN', 'NPS+B', 'MAP 10-50', 'NP 1240',
    'NPS 1240', 'NPS 840', 'NP 840', 'NITRODOBLE', 'NITROBOR', 'NITROCOMPLEX',
    'CAN', 'AMIDAS', 'GSSP',
})


def _infer_product(profile: dict | None, origin: str) -> str | None:
    """
    Infer the most probable fertilizer product using a 4-level priority chain.

    Priority 1 — vessel dominant_product from historical profile (highest confidence):
        Use the stored dominant_product only when it is a recognised fertilizer code.
        Skipped if null, blank, or a non-fertilizer material (e.g. 'ARRABIO').

    Priority 2 — vessel product distribution from profile:
        The profile stores one dominant_product summary.  If priority 1 failed (product
        absent or non-fert) but the profile has a dominant_importer on record, that
        importer's known product specialisation provides the next best signal.
        Falls through to priority 3.

    Priority 3 — dominant importer context (IMPORTER_TO_PRODUCT):
        Known Argentine importers have strong product specialisations derived from
        historical data.  If the profile's dominant_importer maps to a product, use it.
        This is more specific than origin because the same origin country ships multiple
        product types depending on the buyer.

    Priority 4 — origin-based fallback (ORIGIN_TO_PRODUCT, lowest confidence):
        Least specific; used only when no profile or importer signal is available.
        Covers cold-start vessels where origin country is the only known signal.
    """
    # ── Priority 1: profile dominant_product is a recognised fertilizer code ──
    if profile:
        dp = (profile.get('dominant_product') or '').strip().upper()
        if dp in _FERT_PRODUCTS:
            return dp

    # ── Priority 2 / 3: use the profile's dominant importer as a product proxy ─
    # (Profile exists but dominant_product is absent or non-fert — the importer
    #  recorded on the profile is the next best available signal.)
    if profile and profile.get('dominant_importer'):
        imp     = _normalise(profile['dominant_importer'])
        product = IMPORTER_TO_PRODUCT.get(imp)
        if product:
            return product

    # ── Priority 4: origin-based fallback ─────────────────────────────────────
    return _infer_product_from_origin(origin)


def _estimate_eta(ais_destination: str) -> str:
    """
    Placeholder ETA estimation from AIS destination text.
    Returns an ISO date string (YYYY-MM-DD).

    Rules (simple, deterministic):
      - Destination contains Argentina keywords → 14 days (midpoint of 10–20 day range)
      - Unknown / no destination              → 30 days (conservative fallback)

    Replace with real AIS ETA field once integration is live.
    """
    dest = _normalise(ais_destination)
    days = 14 if _match_keywords(dest, ARGENTINA_DEST_KEYWORDS) else 30
    return (datetime.date.today() + datetime.timedelta(days=days)).isoformat()


def _typical_tonnage_range(profile: dict | None) -> list[int] | None:
    if not profile:
        return [3_000, 12_000]   # generic fertilizer vessel range at San Nicolás
    lo = profile.get('min_tonnage') or 0
    hi = profile.get('max_tonnage') or 0
    if lo and hi:
        return [lo, hi]
    avg = profile.get('avg_tonnage')
    if avg:
        return [max(1_000, int(avg * 0.6)), int(avg * 1.4)]
    return None

# ── Main scoring function ─────────────────────────────────────────────────────

def score_vessel(input_data: dict) -> dict:
    """
    Score the probability of a vessel being a fertilizer vessel heading to Argentina.

    Parameters
    ----------
    input_data : dict
        See module docstring for schema.

    Returns
    -------
    dict
        See module docstring for output schema.
    """
    _load_profiles()

    vessel_name  = _normalise(input_data.get('vessel_name'))
    origin       = _normalise(input_data.get('origin'))
    ais_dest     = _normalise(input_data.get('ais_destination'))
    vessel_type  = _normalise(input_data.get('vessel_type'))
    dwt          = input_data.get('dwt')
    month        = input_data.get('current_month')

    if not month:
        month = datetime.date.today().strftime('%m')

    profile = _PROFILES.get(vessel_name) if vessel_name else None

    score   = 0
    reasons = []

    # ── Signal 1a: Previous visits to Argentina (+25) ────────────────────────
    if profile and profile.get('visits_to_argentina', 0) > 0:
        score += 25
        v = profile['visits_to_argentina']
        reasons.append(
            f"+25 — Known vessel: {v} previous visit{'s' if v != 1 else ''} to Argentina "
            f"(first: {profile.get('first_seen_date')}, last: {profile.get('last_seen_date')})"
        )

    # ── Signal 1b: Calls San Nicolás / Paraná corridor specifically (+10) ────
    if profile and profile.get('main_ports_in_argentina'):
        score += 10
        ports = ', '.join(profile['main_ports_in_argentina'][:3])
        reasons.append(
            f"+10 — Vessel specifically calls San Nicolás / Paraná corridor "
            f"(recorded berths: {ports})"
        )

    # ── Signal 2: Previous fertilizer visits (+20) ────────────────────────────
    if profile and profile.get('fertilizer_visits', 0) > 0:
        score += 20
        fv = profile['fertilizer_visits']
        reasons.append(
            f"+20 — {fv} fertilizer visit{'s' if fv != 1 else ''} on record "
            f"(dominant product: {profile.get('dominant_product')})"
        )

    # ── Signal 3: Origin matches fertilizer origin patterns (+20) ─────────────
    if origin and _match_keywords(origin, FERTILIZER_ORIGINS):
        score += 20
        reasons.append(
            f"+20 — Origin '{input_data.get('origin')}' matches known fertilizer "
            f"origin corridor (China/Qatar/Morocco/Finland/…)"
        )

    # ── Signal 4: AIS destination matches Argentina upriver (+15) ────────────
    if ais_dest and _match_keywords(ais_dest, ARGENTINA_DEST_KEYWORDS):
        score += 15
        reasons.append(
            f"+15 — AIS destination '{input_data.get('ais_destination')}' matches "
            f"Argentina / River Plate / upriver pattern"
        )

    # ── Signal 5: Vessel type/size matches fertilizer patterns (+10) ──────────
    type_match = vessel_type and _match_keywords(vessel_type, BULK_VESSEL_TYPES)
    size_match = dwt and DWT_FERT_MIN <= dwt <= DWT_FERT_MAX

    if type_match or size_match:
        score += 10
        parts = []
        if type_match:
            parts.append(f"type '{input_data.get('vessel_type')}'")
        if size_match:
            parts.append(f"DWT {dwt:,} (within {DWT_FERT_MIN:,}–{DWT_FERT_MAX:,} t range)")
        reasons.append(f"+10 — Vessel profile matches bulk fertilizer carrier: {', '.join(parts)}")

    # ── Signal 6: Current month matches historical seasonality (+10) ──────────
    if profile and profile.get('seasonality_by_month'):
        season = profile['seasonality_by_month']
        if month in season and season[month] > 0:
            score += 10
            peak = max(season, key=season.get)
            reasons.append(
                f"+10 — Month {month} matches vessel's historical seasonality "
                f"(peak month: {peak} with {season[peak]} visit{'s' if season[peak] != 1 else ''})"
            )

    # ── Probability level (new thresholds) ───────────────────────────────────
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

    if not reasons:
        reasons.append("No matching signals found in historical data or input fields.")

    # ── Probable importer: profile only (no external inference) ──────────────
    probable_importer = profile.get('dominant_importer') if profile else None

    # ── Probable product: 4-level priority inference ──────────────────────────
    # Priority order: profile dominant_product → profile importer context →
    # IMPORTER_TO_PRODUCT → ORIGIN_TO_PRODUCT fallback.
    # See _infer_product() for full documentation.
    probable_product = _infer_product(profile, origin)

    # ── Tonnage range ─────────────────────────────────────────────────────────
    tonnage_range = _typical_tonnage_range(profile)

    return {
        'probability_score':      final_score,
        'probability_level':      level,
        'prediction_status':      status,
        'probable_product':       probable_product,
        'probable_importer':      probable_importer,
        'probable_tonnage_range': tonnage_range,
        'scoring_reasons':        reasons,
    }


# ── CLI demo ──────────────────────────────────────────────────────────────────
if __name__ == '__main__':

    test_cases = [
        {
            '_label': 'Known vessel, strong signals',
            'vessel_name':     'CLIPPER I-STAR',
            'origin':          'CHINA',
            'ais_destination': 'RECALADA',
            'vessel_type':     'BULK CARRIER',
            'dwt':             18_500,
            'current_month':   '10',
        },
        {
            '_label': 'Unknown vessel, medium signals',
            'vessel_name':     None,
            'origin':          'QATAR',
            'ais_destination': 'SAN NICOLAS',
            'vessel_type':     'BULK CARRIER',
            'dwt':             35_000,
            'current_month':   '03',
        },
        {
            '_label': 'Ambiguous vessel — low confidence',
            'vessel_name':     None,
            'origin':          'ROTTERDAM',
            'ais_destination': 'BSAS',
            'vessel_type':     'TANKER',
            'dwt':             None,
            'current_month':   '06',
        },
    ]

    for case in test_cases:
        label = case.pop('_label')
        print(f"\n{'─'*60}")
        print(f"Case: {label}")
        print(f"Input: {json.dumps(case, indent=2)}")
        result = score_vessel(case)
        print(f"Output: {json.dumps(result, indent=2, ensure_ascii=False)}")
