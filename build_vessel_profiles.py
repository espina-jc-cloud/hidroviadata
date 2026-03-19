"""
build_vessel_profiles.py
Derives vessel_profiles.json from the existing cleaned dataset (output/data.json).
Run: python3 build_vessel_profiles.py
Output: output/vessel_profiles.json
"""

import json
from collections import defaultdict
from datetime import datetime

# ── Same fertilizer set used by the dashboard ─────────────────────────────────
FERTILIZER_SET = {
    'UREA','MAP','DAP','MOP','TSP','NPS','UAN','AMSUL','NP','NPK',
    'SSP','STP','GMOP','FERTILIZANTE','NPS+ZN','MAP 10-50','NP 1240',
    'NPS 1240','NPS 840','NP 840','NITRODOBLE','NITROBOR','NITROCOMPLEX',
    'GSSP','CAN','AMIDAS','NITRATO DE AMONIO','MAP10-50',
}

def is_fert(material):
    m = (material or '').strip().upper()
    return m in FERTILIZER_SET or 'FERTILIZ' in m or 'NITRO' in m

def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    if len(s) >= 10 and s[4] == '-':
        try:
            return datetime.strptime(s[:10], '%Y-%m-%d')
        except ValueError:
            return None
    return None

def top_by_count(counter):
    """Return the key with the highest count, or None."""
    return max(counter, key=counter.get) if counter else None

def top_by_value(counter):
    """Return the key with the highest summed value, or None."""
    return max(counter, key=counter.get) if counter else None


# ── Load data ─────────────────────────────────────────────────────────────────
with open('output/data.json', encoding='utf-8') as f:
    records = json.load(f)

# ── Aggregate per vessel ──────────────────────────────────────────────────────
vessels = defaultdict(lambda: {
    'all_records':      [],
    'fert_records':     [],
    'dates':            [],
    'material_count':   defaultdict(int),
    'material_tons':    defaultdict(float),
    'origin_count':     defaultdict(int),
    'importer_tons':    defaultdict(float),
    'muelle_count':     defaultdict(int),
    'month_count':      defaultdict(int),   # key: "MM" 01-12
    'tons_list':        [],
    'field_hits':       0,   # non-empty key fields across all records
    'total_records':    0,
})

KEY_FIELDS = ['eta', 'material', 'cliente', 'tons', 'operador', 'origen']

for r in records:
    name = (r.get('buque') or '').strip()
    if not name:
        continue

    v = vessels[name]
    v['all_records'].append(r)
    v['total_records'] += 1

    # Date
    d = parse_date(r.get('eta'))
    if d:
        v['dates'].append(d)
        v['month_count'][d.strftime('%m')] += 1

    # Fertilizer
    fert = is_fert(r.get('material'))
    if fert:
        v['fert_records'].append(r)

    # Material
    mat = (r.get('material') or '').strip()
    if mat:
        v['material_count'][mat] += 1
        v['material_tons'][mat] += float(r.get('tons') or 0)

    # Origin
    orig = (r.get('origen') or '').strip()
    if orig:
        v['origin_count'][orig] += 1

    # Importer
    imp = (r.get('cliente') or '').strip()
    if imp:
        tons = float(r.get('tons') or 0)
        v['importer_tons'][imp] += tons

    # Muelle
    muelle = (r.get('muelle') or '').strip()
    if muelle:
        v['muelle_count'][muelle] += 1

    # Tons list (all records)
    if r.get('tons'):
        v['tons_list'].append(float(r['tons']))

    # Confidence: count non-empty key fields
    v['field_hits'] += sum(1 for f in KEY_FIELDS if r.get(f))

# ── Build profiles ────────────────────────────────────────────────────────────
profiles = []

for name, v in sorted(vessels.items()):
    dates      = sorted(v['dates'])
    tons_list  = v['tons_list']
    n_all      = v['total_records']
    n_fert     = len(v['fert_records'])
    n_key_max  = n_all * len(KEY_FIELDS)     # maximum possible field hits

    # Seasonality: month label → visit count
    seasonality = {
        f"{int(m):02d}": v['month_count'][m]
        for m in sorted(v['month_count'])
    }

    # Main ports: list sorted by frequency
    main_ports = sorted(v['muelle_count'], key=v['muelle_count'].get, reverse=True)

    profile = {
        'vessel_name':               name,
        'visits_to_argentina':       n_all,
        'fertilizer_visits':         n_fert,
        'first_seen_date':           dates[0].strftime('%Y-%m-%d') if dates else None,
        'last_seen_date':            dates[-1].strftime('%Y-%m-%d') if dates else None,
        'dominant_product':          top_by_count(v['material_count']),
        'dominant_origin':           top_by_count(v['origin_count']),
        'dominant_importer':         top_by_value(v['importer_tons']),
        'avg_tonnage':               round(sum(tons_list) / len(tons_list)) if tons_list else None,
        'min_tonnage':               int(min(tons_list)) if tons_list else None,
        'max_tonnage':               int(max(tons_list)) if tons_list else None,
        'main_ports_in_argentina':   main_ports,
        'seasonality_by_month':      seasonality,
        'confidence_inputs_available': round(v['field_hits'] / n_key_max, 3) if n_key_max else 0,
    }
    profiles.append(profile)

# ── Save ──────────────────────────────────────────────────────────────────────
out_path = 'output/vessel_profiles.json'
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(profiles, f, ensure_ascii=False, indent=2)

print(f"vessel_profiles.json written — {len(profiles)} profiles")

# ── Preview: top vessel by visits ─────────────────────────────────────────────
top = max(profiles, key=lambda p: p['visits_to_argentina'])
print("\nExample profile (most active vessel):")
print(json.dumps(top, indent=2, ensure_ascii=False))
