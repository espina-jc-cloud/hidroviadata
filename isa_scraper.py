"""
isa_scraper.py
──────────────
Scrapes the ISA Agents lineup page and feeds FERTILIZERS / DISCH rows
directly into the shipments table — the same table used by the PDF pipeline.

Usage
─────
    python3 isa_scraper.py              # fetch + print sample, no DB write
    python3 isa_scraper.py --insert     # fetch → dedup → insert into shipments
    python3 isa_scraper.py --insert --dry-run  # parse only, skip DB

Design notes
────────────
• Stdlib only — no new dependencies (urllib, html.parser).
• Target table  : shipments  (same as PDF pipeline, no schema change).
• source_id     : 'ISA_LINEUP'  (stable, clearly identifies ISA rows).
• source_date   : YYYY-MM-DD of the scrape (today).
• Dedup         : vessel (normalised exact match) + material (normalised) +
                  ETA ±5 days.  If ISA row has no ETA, match on
                  vessel+material when any shipments row exists with the
                  same keys and source_date within the last 14 days.
• ISA Port      : stored in the `sector` column (reuses existing column;
                  PDF sector values are zone labels like "NORTE"/"SUR",
                  ISA sector values are port city names like "SAN NICOLAS").
• ISA Berth     : stored in `muelle`.
• Extra ISA fields (etb, ets, remarks) are not stored — no schema change.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import ssl
import sys
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

BASE_DIR     = Path(__file__).parent
DB_PATH      = Path(os.environ.get('DB_PATH', str(BASE_DIR / 'hidroviadata.db')))
ALIASES_FILE = BASE_DIR / 'aliases.json'

ISA_URL   = 'https://www.isa-agents.com.ar/info/line_up_mndrn.php?lang=es'
SOURCE_ID = 'ISA_LINEUP'   # stable identifier for all ISA-sourced rows

# ── Spanish month abbreviations ───────────────────────────────────────────────
_MONTHS_ES: dict[str, int] = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
}


# ── Vessel aliases (reuse aliases.json from the PDF pipeline) ─────────────────

def _load_vessel_aliases() -> dict[str, str]:
    try:
        data = json.loads(ALIASES_FILE.read_text(encoding='utf-8'))
        return {k.upper().strip(): v.upper().strip()
                for k, v in data.get('vessels', {}).items()}
    except Exception:
        return {}


_VESSEL_ALIASES: dict[str, str] = _load_vessel_aliases()


def _norm_vessel(name: str | None) -> str:
    """Uppercase, collapse hyphens/underscores/dots to spaces, strip non-alphanum.
    Identical to the function in migrate.py — kept in sync manually."""
    if not name:
        return ''
    s = name.upper()
    s = re.sub(r'[-_.]', ' ', s)
    s = re.sub(r'[^A-Z0-9 ]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def _norm_material(name: str | None) -> str:
    """Normalise material name for dedup comparison."""
    return (name or '').strip().upper()


def _apply_vessel_alias(name: str) -> str:
    """Return canonical vessel name from aliases.json when available."""
    return _VESSEL_ALIASES.get(name.upper().strip(), name.strip())


# ── ISA date parser ───────────────────────────────────────────────────────────

def _parse_isa_date(text: str | None) -> str | None:
    """
    Convert ISA date string (e.g. "26-abr", "3-may") to ISO YYYY-MM-DD.

    Year inference: if the candidate date is more than 60 days in the past,
    bump the year by 1 (handles December→January rollovers).
    Returns None for TBC / TBA / empty / unrecognised formats.
    """
    if not text:
        return None
    text = text.strip().lower()
    if text in ('tbc', 'tba', '-', '', 'n/a'):
        return None
    m = re.match(r'^(\d{1,2})-([a-z]{3})$', text)
    if not m:
        return None
    day = int(m.group(1))
    mon = _MONTHS_ES.get(m.group(2))
    if not mon:
        return None
    today = datetime.now()
    year  = today.year
    try:
        candidate = datetime(year, mon, day)
        if (today - candidate).days > 60:
            year += 1
        return datetime(year, mon, day).strftime('%Y-%m-%d')
    except ValueError:
        return None


# ── HTML table parser (stdlib only) ───────────────────────────────────────────

class _TableParser(HTMLParser):
    """Extract the first HTML table as a list of string-lists."""

    def __init__(self) -> None:
        super().__init__()
        self.rows:  list[list[str]] = []
        self._in_t  = False
        self._in_r  = False
        self._in_c  = False
        self._cur_r: list[str] = []
        self._cur_c = ''

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == 'table':
            self._in_t = True
        elif tag == 'tr' and self._in_t:
            self._in_r = True
            self._cur_r = []
        elif tag in ('td', 'th') and self._in_r:
            self._in_c = True
            self._cur_c = ''

    def handle_endtag(self, tag: str) -> None:
        if tag == 'table':
            self._in_t = False
        elif tag == 'tr' and self._in_r:
            self._in_r = False
            if self._cur_r:
                self.rows.append(self._cur_r[:])
        elif tag in ('td', 'th') and self._in_c:
            self._in_c = False
            self._cur_r.append(self._cur_c.strip())

    def handle_data(self, data: str) -> None:
        if self._in_c:
            self._cur_c += data


# ── Core scrape function ───────────────────────────────────────────────────────

def scrape_isa() -> list[dict]:
    """
    Fetch the ISA lineup page and return all FERTILIZERS + DISCH rows as
    dicts ready for INSERT INTO shipments.

    Returned keys match shipments columns exactly:
        buque, agencia, eta, material, cliente, tons,
        operador, operacion, muelle, sector, origen,
        source_id, source_date

    Raises: URLError, ValueError on fetch/parse failure.
    """
    req = Request(ISA_URL, headers={'User-Agent': 'HidroviaData/1.0 (scraper)'})

    # ISA's server is slow — allow 60 s.  On macOS the system SSL root certs
    # may not cover ISA's CA; fall back to an unverified context (dev only —
    # Railway/Linux verifies correctly with the system bundle).
    _NO_VERIFY_CTX = ssl.create_default_context()
    _NO_VERIFY_CTX.check_hostname = False
    _NO_VERIFY_CTX.verify_mode    = ssl.CERT_NONE

    def _fetch(ctx=None) -> str:
        kw = {'context': ctx} if ctx is not None else {}
        with urlopen(req, timeout=60, **kw) as resp:
            return resp.read().decode('utf-8', errors='replace')

    html: str = ''
    last_exc: Exception | None = None
    for _attempt in range(2):
        try:
            html = _fetch()
            last_exc = None
            break
        except URLError as exc:
            last_exc = exc
            if isinstance(getattr(exc, 'reason', None), ssl.SSLError):
                print('[isa_scraper] WARNING: SSL cert verify failed — retrying without '
                      'verification (dev only; Railway is fine)', flush=True)
                try:
                    html = _fetch(_NO_VERIFY_CTX)
                    last_exc = None
                    break
                except Exception as exc2:
                    last_exc = exc2
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc

    parser = _TableParser()
    parser.feed(html)

    if not parser.rows:
        raise ValueError('No table found on ISA lineup page.')

    header = parser.rows[0]
    if len(header) < 14 or header[0].strip().lower() not in ('port', 'puerto'):
        raise ValueError(f'Unexpected ISA table header: {header}')

    today_iso = datetime.now().strftime('%Y-%m-%d')

    result: list[dict] = []
    for raw_row in parser.rows[1:]:
        if len(raw_row) < 14:
            continue

        (port, berth, vessel_raw, ops, cat,
         cargo, qty, dest_orig, _area, shipper,
         eta_raw, _etb, _ets, _remarks) = raw_row[:14]

        if cat.strip().upper() != 'FERTILIZERS':
            continue
        if ops.strip().upper() != 'DISCH':
            continue

        vessel_clean = _apply_vessel_alias(vessel_raw.strip())

        try:
            tons: float | None = float(qty.replace(',', '').strip())
        except (ValueError, TypeError):
            tons = None

        result.append({
            'buque':       vessel_clean,
            'agencia':     'ISA',
            'eta':         _parse_isa_date(eta_raw),
            'material':    cargo.strip().upper() or 'UNKNOWN',
            'cliente':     shipper.strip().upper() or '',
            'tons':        tons,
            'operador':    '',
            'operacion':   'DESCARGA',
            'muelle':      berth.strip().upper(),
            'sector':      port.strip().upper(),   # ISA Port city → sector column
            'origen':      dest_orig.strip().upper() or '',
            'source_id':   SOURCE_ID,
            'source_date': today_iso,
        })

    return result


# ── Deduplication against shipments table ────────────────────────────────────

def _is_dup_in_shipments(row: dict, existing: list[dict], today: datetime) -> bool:
    """
    Return True if row already exists in the existing shipments rows.

    Match criteria:
      • Normalised vessel name  (exact match after _norm_vessel)
      • Normalised material     (exact match after _norm_material)
      • If new row has ETA: existing row ETA within ±5 days
      • If new row has no ETA: any existing row with same vessel+material
        whose source_date is within the last 14 days
    """
    norm_v = _norm_vessel(row.get('buque'))
    norm_m = _norm_material(row.get('material'))

    try:
        eta_new = datetime.fromisoformat(row['eta'][:10]).date() if row.get('eta') else None
    except (ValueError, TypeError):
        eta_new = None

    cutoff_14 = (today - timedelta(days=14)).date()

    for ex in existing:
        if _norm_vessel(ex.get('buque')) != norm_v:
            continue
        if _norm_material(ex.get('material')) != norm_m:
            continue

        if eta_new is not None:
            # Both must have ETA within ±5 days
            try:
                eta_ex = datetime.fromisoformat(str(ex.get('eta') or '')[:10]).date() if ex.get('eta') else None
            except (ValueError, TypeError):
                eta_ex = None
            if eta_ex is not None and abs((eta_new - eta_ex).days) <= 5:
                return True
        else:
            # No ETA — match if existing row is recent (≤14 days old)
            try:
                sd = datetime.fromisoformat(str(ex.get('source_date') or '')[:10]).date() if ex.get('source_date') else None
            except (ValueError, TypeError):
                sd = None
            if sd is not None and sd >= cutoff_14:
                return True

    return False


def dedup_against_shipments(
    rows: list[dict],
    con: sqlite3.Connection,
) -> tuple[list[dict], int]:
    """
    Filter out rows already present in shipments (from any source: PDF or ISA).
    Returns (new_rows, duplicates_count).
    """
    existing: list[dict] = []
    try:
        for r in con.execute(
            'SELECT buque, material, eta, source_date FROM shipments'
        ).fetchall():
            existing.append({
                'buque':       r[0],
                'material':    r[1],
                'eta':         r[2],
                'source_date': r[3],
            })
    except sqlite3.OperationalError:
        pass   # empty or missing table — all rows are new

    today = datetime.now()
    new_rows: list[dict] = []
    duplicates = 0

    for row in rows:
        if _is_dup_in_shipments(row, existing, today):
            duplicates += 1
        else:
            new_rows.append(row)
            # Add to in-memory set so intra-batch dedup works correctly.
            # Same vessel can appear with multiple cargoes (TSP + MAP) — those
            # are distinct records and both should be inserted.
            existing.append({
                'buque':       row['buque'],
                'material':    row['material'],
                'eta':         row['eta'],
                'source_date': row['source_date'],
            })

    return new_rows, duplicates


# ── Insert into shipments ─────────────────────────────────────────────────────

def insert_into_shipments(rows: list[dict], con: sqlite3.Connection) -> int:
    """Insert rows into shipments table. Returns count of rows inserted."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO shipments
            (buque, agencia, eta, material, cliente, tons,
             operador, operacion, muelle, sector, origen,
             source_id, source_date)
        VALUES
            (:buque, :agencia, :eta, :material, :cliente, :tons,
             :operador, :operacion, :muelle, :sector, :origen,
             :source_id, :source_date)
        """,
        rows,
    )
    con.commit()
    return len(rows)


# ── Full pipeline (used by app.py) ────────────────────────────────────────────

def run_scrape(db_path: Path = DB_PATH) -> dict:
    """
    Fetch → parse → dedup against shipments → insert into shipments.
    Returns a stats dict matching the /api/admin/scrape_isa response shape.
    """
    rows      = scrape_isa()
    today_iso = datetime.now().strftime('%Y-%m-%d')
    total     = len(rows)
    ports_all = sorted({r['sector'] for r in rows})

    con = sqlite3.connect(str(db_path))
    new_rows, duplicates = dedup_against_shipments(rows, con)
    inserted = insert_into_shipments(new_rows, con)
    con.close()

    return {
        'status':                'ok',
        'new_rows':              inserted,
        'duplicates':            duplicates,
        'ports_covered':         sorted({r['sector'] for r in new_rows}) if new_rows else [],
        'total_fertilizer_rows': total,
        'source_id':             SOURCE_ID,
        'as_of':                 today_iso,   # scrape date (source_date stored in DB)
    }


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == '__main__':
    _insert  = '--insert'  in sys.argv
    _dry_run = '--dry-run' in sys.argv

    print(f'[isa_scraper] Fetching {ISA_URL} …', flush=True)
    try:
        _rows = scrape_isa()
    except Exception as exc:
        print(f'[isa_scraper] FETCH FAILED: {exc}')
        sys.exit(1)

    _ports = sorted({r['sector'] for r in _rows})
    print(f'[isa_scraper] Found {len(_rows)} FERTILIZERS+DISCH rows  ports={_ports}')

    for r in _rows[:5]:
        print(
            f"  {r['sector']:20s} | {r['buque']:28s} | ETA={r['eta']} "
            f"| {r['material']:8s} | {(r['tons'] or 0):>8.0f} t | {r['cliente']}"
        )
    if len(_rows) > 5:
        print(f'  … {len(_rows) - 5} more rows')

    if not _insert:
        print('\n[isa_scraper] Tip: pass --insert to write to DB (shipments table).')
        sys.exit(0)

    if _dry_run:
        print('\n[isa_scraper] --dry-run: skipping DB write.')
        sys.exit(0)

    _con = sqlite3.connect(str(DB_PATH))
    _new, _dup = dedup_against_shipments(_rows, _con)
    _n = insert_into_shipments(_new, _con)
    _con.close()
    print(f'\n[isa_scraper] Done — inserted={_n}  duplicates={_dup}  '
          f'source_id={SOURCE_ID}  db={DB_PATH}')
