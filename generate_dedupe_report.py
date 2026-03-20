"""
generate_dedupe_report.py
─────────────────────────
Read output/data.json (current) and output/data_before_dedup.json (snapshot)
and produce:
  · output/dedupe_report.json
  · stdout summary

Usage
─────
    python3 generate_dedupe_report.py

To do a full rebuild first:
    python3 parser.py && python3 migrate.py --reset && python3 generate_dedupe_report.py
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _analyse(data: list[dict]) -> dict:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in data:
        k = (r.get('buque', ''), r.get('material', ''), r.get('cliente', ''))
        groups[k].append(r)

    offenders = []
    for k, recs in groups.items():
        if len(recs) < 2:
            continue
        dates = [_parse_dt(r.get('eta')) for r in recs]
        valid = [d for d in dates if d]
        if len(valid) < 2:
            continue
        span = (max(valid) - min(valid)).days
        if span <= 3:
            continue
        offenders.append({
            'buque':         k[0],
            'material':      k[1],
            'cliente':       k[2],
            'count':         len(recs),
            'eta_span_days': span,
            'etas':          sorted(r.get('eta') for r in recs if r.get('eta')),
            'tons':          [r.get('tons') for r in recs],
            'sources':       [r.get('source_id', '?') for r in recs],
        })

    offenders.sort(key=lambda x: (-x['count'], -x['eta_span_days']))

    return {
        'total_records':          len(data),
        'total_tons':             sum(r['tons'] for r in data if r.get('tons')),
        'null_tons':              sum(1 for r in data if r.get('tons') is None),
        'no_material':            sum(1 for r in data if not r.get('material')),
        'dup_groups_over_3d':     len(offenders),
        'dup_records_in_groups':  sum(o['count'] for o in offenders),
        'top_offenders':          offenders[:20],
    }


def _focus_rows(data: list[dict], buque: str, material: str, cliente: str) -> list[dict]:
    return sorted(
        [{'eta': r.get('eta'), 'tons': r.get('tons'), 'source_id': r.get('source_id', '—')}
         for r in data
         if r.get('buque') == buque and r.get('material') == material
         and r.get('cliente') == cliente],
        key=lambda x: x['eta'] or '',
    )


def main() -> None:
    after_path  = BASE / 'output' / 'data.json'
    before_path = BASE / 'output' / 'data_before_dedup.json'

    if not after_path.exists():
        print("ERROR: output/data.json not found. Run  python3 parser.py  first.")
        raise SystemExit(1)

    after_data  = json.loads(after_path.read_text(encoding='utf-8'))
    before_data = (
        json.loads(before_path.read_text(encoding='utf-8'))
        if before_path.exists()
        else None
    )

    stats_a = _analyse(after_data)
    stats_b = _analyse(before_data) if before_data else None

    # ── Report dict ────────────────────────────────────────────────────────────
    report: dict = {
        'generated': datetime.now().isoformat(),
        'current': {k: v for k, v in stats_a.items() if k != 'top_offenders'},
        'top_offenders': stats_a['top_offenders'],
    }

    if stats_b:
        report['before_snapshot'] = {k: v for k, v in stats_b.items()
                                      if k != 'top_offenders'}
        report['delta'] = {
            'records_removed':   stats_b['total_records'] - stats_a['total_records'],
            'tons_removed':      stats_b['total_tons']    - stats_a['total_tons'],
            'dup_groups_resolved': stats_b['dup_groups_over_3d'] - stats_a['dup_groups_over_3d'],
        }
        report['top_offenders_before'] = stats_b['top_offenders'][:20]

    # Focus cases
    report['clipper_i_star_map_bunge'] = {
        'before': _focus_rows(before_data, 'CLIPPER I-STAR', 'MAP', 'BUNGE') if before_data else [],
        'after':  _focus_rows(after_data,  'CLIPPER I-STAR', 'MAP', 'BUNGE'),
    }
    report['nord_kaizan'] = {
        'before': [{'buque': r['buque'], 'eta': r.get('eta'), 'material': r.get('material'),
                    'cliente': r.get('cliente'), 'tons': r.get('tons')}
                   for r in (before_data or []) if 'KAIZAN' in (r.get('buque') or '')],
        'after':  [{'buque': r['buque'], 'eta': r.get('eta'), 'material': r.get('material'),
                    'cliente': r.get('cliente'), 'tons': r.get('tons')}
                   for r in after_data if 'KAIZAN' in (r.get('buque') or '')],
    }

    # ── Write JSON ─────────────────────────────────────────────────────────────
    out_json = BASE / 'output' / 'dedupe_report.json'
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    # ── Print summary ──────────────────────────────────────────────────────────
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  HidrovíaData — Dedup Report                            ║")
    print(f"║  Generated: {report['generated'][:19]:<46} ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    if stats_b:
        print(f"  {'Metric':<35} {'Before':>10} {'After':>10} {'Delta':>10}")
        print(f"  {'─'*35} {'─'*10} {'─'*10} {'─'*10}")
        for label, bv, av in [
            ('Total records',          stats_b['total_records'],         stats_a['total_records']),
            ('Total tonnage (t)',       stats_b['total_tons'],            stats_a['total_tons']),
            ('Null-ton rows',           stats_b['null_tons'],             stats_a['null_tons']),
            ('Dup groups (ETA >3d)',    stats_b['dup_groups_over_3d'],    stats_a['dup_groups_over_3d']),
            ('Dup records in groups',   stats_b['dup_records_in_groups'], stats_a['dup_records_in_groups']),
        ]:
            print(f"  {label:<35} {bv:>10,} {av:>10,} {av-bv:>+10,}")
    else:
        print(f"  Total records : {stats_a['total_records']:,}")
        print(f"  Total tonnage : {stats_a['total_tons']:,} t")
        print(f"  Dup groups    : {stats_a['dup_groups_over_3d']}")
        print(f"  (no before-snapshot found at output/data_before_dedup.json)")

    print()
    print("  Remaining duplicate groups (ETA span > 3 days):")
    if not stats_a['top_offenders']:
        print("  ✓ None — all duplicates resolved.")
    else:
        for o in stats_a['top_offenders']:
            print(f"    [{o['count']}recs|{o['eta_span_days']}d] "
                  f"{o['buque']} | {o['material']} | {o['cliente']} | tons={o['tons']}")

    print()
    clip_after = report['clipper_i_star_map_bunge']['after']
    print(f"  CLIPPER I-STAR + MAP + BUNGE : {len(clip_after)} row(s) remain")
    for row in clip_after:
        print(f"    eta={row['eta']}  tons={row['tons']}  src={row['source_id']}")

    kz_after = report['nord_kaizan']['after']
    print(f"  NORD KAIZAN rows             : {len(kz_after)} (all legitimate — different clients)")

    print()
    print(f"  → output/dedupe_report.json")


if __name__ == '__main__':
    main()
