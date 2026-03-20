"""
verify_candidates.py
────────────────────
Compare vessel_candidates against the shipments DB to confirm or expire
predictions using only the lineup data — no paid AIS required.

Logic
─────
For each candidate with prediction_status = 'predicted':

  1. Search shipments for a row where:
       buque (normalised) matches vessel_name (normalised)
       AND shipment.eta is within [eta_estimated − 30 days, eta_estimated + 60 days]
       (if eta_estimated is missing, use [created_at, created_at + 60 days])

  2. If match found:
       → prediction_status = 'confirmed'
       → confirmed_eta = matching shipment.eta
       → confirmed_match_reason = short description

  3. If no match AND candidate is older than 60 days from today (or from eta_estimated):
       → prediction_status = 'expired'

Produces
────────
  output/track_record.md   — human-readable summary
  stdout                   — live progress

Usage
─────
    python3 verify_candidates.py          # update DB + print report
    python3 verify_candidates.py --dry-run  # print only, no DB changes
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE    = Path(__file__).parent
DB_PATH = BASE / 'hidroviadata.db'
OUT_MD  = BASE / 'output' / 'track_record.md'
DRY_RUN = '--dry-run' in sys.argv

WINDOW_BEFORE_DAYS = 30
WINDOW_AFTER_DAYS  = 60
EXPIRE_AFTER_DAYS  = 60    # mark expired if no match after this many days past ETA/created_at


def _norm(s: str | None) -> str:
    """Normalise vessel name for comparison: uppercase, collapse whitespace."""
    if not s:
        return ''
    import re
    return re.sub(r'\s+', ' ', s.strip().upper())


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except ValueError:
            continue
    return None


def _ensure_columns(con: sqlite3.Connection) -> None:
    """Add confirmed_eta and confirmed_match_reason if they don't exist yet (safe migration)."""
    existing = {row[1] for row in con.execute("PRAGMA table_info(vessel_candidates)")}
    for col, typedef in [
        ('confirmed_eta',          'TEXT'),
        ('confirmed_match_reason', 'TEXT'),
    ]:
        if col not in existing:
            con.execute(f'ALTER TABLE vessel_candidates ADD COLUMN {col} {typedef}')
    con.commit()


def _verify_one(candidate: dict, shipments: list[dict]) -> tuple[str, str | None, str | None]:
    """
    Returns (new_status, confirmed_eta, confirmed_match_reason).
    new_status is one of: 'predicted' (no change), 'confirmed', 'expired'.
    """
    name_norm = _norm(candidate['vessel_name'])

    # Determine the reference date and expiry horizon
    ref_dt = _parse_date(candidate.get('eta_estimated')) \
          or _parse_date(candidate.get('created_at')) \
          or datetime.now()

    window_start = ref_dt - timedelta(days=WINDOW_BEFORE_DAYS)
    window_end   = ref_dt + timedelta(days=WINDOW_AFTER_DAYS)
    expire_after = ref_dt + timedelta(days=EXPIRE_AFTER_DAYS)

    # Search shipments for a match
    for s in shipments:
        if _norm(s.get('buque')) != name_norm:
            continue
        s_dt = _parse_date(s.get('eta'))
        if s_dt and window_start <= s_dt <= window_end:
            reason = (
                f"{s.get('material','?')} for {s.get('cliente','?')} "
                f"ETA {s.get('eta','?')} src={s.get('source_id','?')}"
            )
            return 'confirmed', s.get('eta'), reason

    # No match — check if we should expire
    if datetime.now() > expire_after:
        return 'expired', None, None

    return 'predicted', None, None


def main() -> None:
    if not DB_PATH.exists():
        print(f'ERROR: {DB_PATH} not found. Run  python3 migrate.py  first.')
        raise SystemExit(1)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    _ensure_columns(con)

    # Load all candidates that are currently 'predicted'
    candidates = [dict(r) for r in con.execute(
        "SELECT * FROM vessel_candidates WHERE prediction_status = 'predicted'"
    ).fetchall()]

    # Load all shipments (vessel name + eta is enough; load full row for reason text)
    shipments = [dict(r) for r in con.execute(
        "SELECT buque, eta, material, cliente, source_id FROM shipments"
    ).fetchall()]

    # Also load already-confirmed/expired for the report
    all_candidates = [dict(r) for r in con.execute(
        "SELECT * FROM vessel_candidates"
    ).fetchall()]

    now_str = datetime.now().strftime('%Y-%m-%d')
    results = {'confirmed': [], 'expired': [], 'unchanged': []}

    for c in candidates:
        new_status, conf_eta, conf_reason = _verify_one(c, shipments)
        if new_status == 'predicted':
            results['unchanged'].append(c)
            continue

        results[new_status].append({**c, 'confirmed_eta': conf_eta, 'confirmed_match_reason': conf_reason})

        if not DRY_RUN:
            con.execute(
                """UPDATE vessel_candidates
                   SET prediction_status = ?, confirmed_eta = ?, confirmed_match_reason = ?
                   WHERE id = ?""",
                (new_status, conf_eta, conf_reason, c['id']),
            )

    if not DRY_RUN:
        con.commit()

    # ── Track-record stats ─────────────────────────────────────────────────────
    # Re-read all candidates for complete picture
    all_cands = [dict(r) for r in con.execute("SELECT * FROM vessel_candidates").fetchall()]
    con.close()

    total      = len(all_cands)
    n_pred     = sum(1 for c in all_cands if c['prediction_status'] == 'predicted')
    n_conf     = sum(1 for c in all_cands if c['prediction_status'] == 'confirmed')
    n_exp      = sum(1 for c in all_cands if c['prediction_status'] == 'expired')

    # Confirmation rate by level (only on resolved = confirmed + expired)
    rate_by_level: dict[str, dict] = {}
    for level in ('high', 'medium', 'low'):
        level_cands = [c for c in all_cands
                       if c.get('probability_level') == level
                       and c['prediction_status'] in ('confirmed', 'expired')]
        n_conf_lv = sum(1 for c in level_cands if c['prediction_status'] == 'confirmed')
        rate_by_level[level] = {
            'resolved': len(level_cands),
            'confirmed': n_conf_lv,
            'rate': round(n_conf_lv / len(level_cands) * 100, 1) if level_cands else None,
        }

    # Top 10 false positives (expired + high confidence)
    false_positives = sorted(
        [c for c in all_cands
         if c['prediction_status'] == 'expired' and c.get('probability_level') == 'high'],
        key=lambda c: -(c.get('probability_score') or 0),
    )[:10]

    # ── Print to stdout ────────────────────────────────────────────────────────
    print('╔══════════════════════════════════════════════════════╗')
    print('║  HidrovíaData — Prediction Track Record              ║')
    print(f'║  {now_str:<52} ║')
    print('╚══════════════════════════════════════════════════════╝')
    print()
    print(f'  Total candidates : {total}')
    print(f'  Predicted        : {n_pred}')
    print(f'  Confirmed        : {n_conf}')
    print(f'  Expired          : {n_exp}')
    print()
    print('  Confirmation rate by probability level:')
    for level, s in rate_by_level.items():
        if s['resolved'] == 0:
            print(f'    {level:<8} : no resolved predictions yet')
        else:
            print(f'    {level:<8} : {s["confirmed"]}/{s["resolved"]} confirmed  ({s["rate"]}%)')
    print()
    if results['confirmed']:
        print('  Newly confirmed this run:')
        for c in results['confirmed']:
            print(f'    ✓ {c["vessel_name"]} — ETA {c["confirmed_eta"]}  ({c["confirmed_match_reason"]})')
    if results['expired']:
        print('  Newly expired this run:')
        for c in results['expired']:
            print(f'    ✗ {c["vessel_name"]} (score={c.get("probability_score")}) — no match found')
    if DRY_RUN:
        print()
        print('  (dry run — no DB changes written)')

    # ── Write markdown ─────────────────────────────────────────────────────────
    md_lines = [
        '# HidrovíaData — Prediction Track Record',
        '',
        f'_Generated: {now_str}_',
        '',
        '## Summary',
        '',
        f'| Status | Count |',
        f'|---|---|',
        f'| Predicted (open) | {n_pred} |',
        f'| Confirmed | {n_conf} |',
        f'| Expired | {n_exp} |',
        f'| **Total** | **{total}** |',
        '',
        '## Confirmation Rate by Probability Level',
        '',
        '| Level | Resolved | Confirmed | Rate |',
        '|---|---|---|---|',
    ]
    for level, s in rate_by_level.items():
        rate_str = f"{s['rate']}%" if s['rate'] is not None else 'N/A'
        md_lines.append(f"| {level} | {s['resolved']} | {s['confirmed']} | {rate_str} |")

    md_lines += [
        '',
        '## All Candidates',
        '',
        '| Vessel | Score | Level | Status | ETA Est | Confirmed ETA | Match Reason |',
        '|---|---|---|---|---|---|---|',
    ]
    for c in sorted(all_cands, key=lambda x: -(x.get('probability_score') or 0)):
        md_lines.append(
            f"| {c.get('vessel_name','?')} "
            f"| {c.get('probability_score','?')} "
            f"| {c.get('probability_level','?')} "
            f"| {c.get('prediction_status','?')} "
            f"| {c.get('eta_estimated','—')} "
            f"| {c.get('confirmed_eta') or '—'} "
            f"| {c.get('confirmed_match_reason') or '—'} |"
        )

    if false_positives:
        md_lines += [
            '',
            '## Top False Positives (Expired + High Confidence)',
            '',
            '| Vessel | Score | ETA Est | Created |',
            '|---|---|---|---|',
        ]
        for c in false_positives:
            md_lines.append(
                f"| {c.get('vessel_name','?')} "
                f"| {c.get('probability_score','?')} "
                f"| {c.get('eta_estimated','—')} "
                f"| {c.get('created_at','—')} |"
            )

    OUT_MD.parent.mkdir(exist_ok=True)
    OUT_MD.write_text('\n'.join(md_lines) + '\n', encoding='utf-8')
    print(f'\n  → {OUT_MD}')


if __name__ == '__main__':
    main()
