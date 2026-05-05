"""
isa_scraper.py
──────────────
Scrapes the ISA Agents lineup page and imports FERTILIZERS / DISCH rows
into the isa_shipments table.  Can be imported by app.py or run standalone.

Usage
─────
    python3 isa_scraper.py              # scrape → print stats (no DB write)
    python3 isa_scraper.py --insert     # scrape → dedup → insert into DB
    python3 isa_scraper.py --insert --dry-run  # parse only, skip DB

Design notes
────────────
• No third-party deps — uses stdlib only (urllib, html.parser).
• Dedup is against the isa_shipments table only (not the PDF shipments table).
  Same scraper run twice in a day inserts 0 new rows.
• isa_shipments table is created on first scrape; it survives app restarts
  but is wiped when migrate.py --reset rebuilds the DB from scratch.
  Re-run the scraper after each publish to restore ISA data.
• source_id  = "isa_scraper_YYYY-MM-DD"  (today's scrape date)
• source      = "isa_scraper"             (distinguishes from "pdf_lineup")
"""

from __future__ import annotations

import difflib
import json
import os
import re
import sqlite3
import ssl
import sys
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

BASE_DIR     = Path(__file__).parent
DB_PATH      = Path(os.environ.get('DB_PATH', str(BASE_DIR / 'hidroviadata.db')))
ALIASES_FILE = BASE_DIR / 'aliases.json'

ISA_URL = 'https://www.isa-agents.com.ar/info/line_up_mndrn.php?lang=es'

# ── Spanish month abbreviations used by ISA ───────────────────────────────────
_MONTHS_ES: dict[str, int] = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
}


# ── Vessel aliases (reuse aliases.json from the PDF pipeline) ─────────────────

def _load_vessel_aliases() -> dict[str, str]:
    try:
        data = json.loads(ALIASES_FILE.read_text(encoding='utf-8'))
        # Upper-case both keys and values for case-insensitive lookup
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


def _apply_vessel_alias(name: str) -> str:
    """Return canonical vessel name from aliases.json when available."""
    return _VESSEL_ALIASES.get(name.upper().strip(), name.strip())


# ── ISA date parser ───────────────────────────────────────────────────────────

def _parse_isa_date(text: str | None) -> str | None:
    """
    Convert ISA date string (e.g. "26-abr", "3-may") to ISO format YYYY-MM-DD.

    Year inference: if the candidate date is more than 60 days in the past,
    bump the year by 1 (handles December → January rollovers etc.).
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
        self.rows:   list[list[str]] = []
        self._in_t   = False
        self._in_r   = False
        self._in_c   = False
        self._cur_r: list[str] = []
        self._cur_c  = ''

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
    pipeline-compatible dicts.

    Pipeline-compatible keys (match shipments table + extra ISA fields):
        buque, buque_raw, agencia, eta, material, cliente, tons,
        operador, operacion, muelle, sector, origen, source_id, source_date,
        source,   ← 'isa_scraper'
        etb, ets, remarks, isa_area

    Raises: urllib.error.URLError, ValueError on fetch/parse failure.
    """
    req = Request(ISA_URL, headers={'User-Agent': 'HidroviaData/1.0 (scraper)'})
    # ISA's server is slow — allow up to 60 s.  On macOS the system SSL root
    # certs may not cover ISA's CA; fall back to unverified context (dev only).
    _NO_VERIFY_CTX = ssl.create_default_context()
    _NO_VERIFY_CTX.check_hostname = False
    _NO_VERIFY_CTX.verify_mode    = ssl.CERT_NONE

    def _fetch(ctx=None) -> str:
        kw = {'context': ctx} if ctx is not None else {}
        with urlopen(req, timeout=60, **kw) as resp:
            return resp.read().decode('utf-8', errors='replace')

    html: str = ''
    last_exc: Exception | None = None
    for _attempt in range(2):          # up to 2 attempts
        try:
            html = _fetch()
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
            # non-SSL URLError or second attempt — retry
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc

    parser = _TableParser()
    parser.feed(html)

    if not parser.rows:
        raise ValueError('No table found on ISA lineup page.')

    header = parser.rows[0]
    # Sanity-check the header
    if len(header) < 14 or header[0].strip().lower() not in ('port', 'puerto'):
        raise ValueError(f'Unexpected ISA table header: {header}')

    today_iso = datetime.now().strftime('%Y-%m-%d')
    source_id = f'isa_scraper_{today_iso}'

    result: list[dict] = []
    for raw_row in parser.rows[1:]:
        if len(raw_row) < 14:
            continue

        (port, berth, vessel_raw, ops, cat,
         cargo, qty, dest_orig, area, shipper,
         eta_raw, etb_raw, ets_raw, remarks) = raw_row[:14]

        # Filter: FERTILIZERS + DISCH only
        if cat.strip().upper() != 'FERTILIZERS':
            continue
        if ops.strip().upper() != 'DISCH':
            continue

        # Vessel: apply alias then keep raw for audit
        vessel_clean = _apply_vessel_alias(vessel_raw.strip())

        # Quantity → tons (strip commas, dots as decimal separator is '.')
        try:
            tons: float | None = float(qty.replace(',', '').strip())
        except (ValueError, TypeError):
            tons = None

        result.append({
            # ── Pipeline-compatible keys ───────────────────────────────────
            'buque':       vessel_clean,
            'buque_raw':   vessel_raw.strip(),
            'agencia':     'ISA',
            'eta':         _parse_isa_date(eta_raw),
            'material':    cargo.strip().upper() or 'UNKNOWN',
            'cliente':     shipper.strip().upper() or '',
            'tons':        tons,
            'operador':    '',
            'operacion':   'DESCARGA',
            'muelle':      berth.strip().upper(),
            'sector':      port.strip().upper(),    # ISA Port → stored in sector
            'origen':      dest_orig.strip().upper() or '',
            'source_id':   source_id,
            'source_date': today_iso,
            # ── ISA-specific extras ────────────────────────────────────────
            'source':      'isa_scraper',
            'etb':         _parse_isa_date(etb_raw),
            'ets':         _parse_isa_date(ets_raw),
            'remarks':     remarks.strip(),
            'isa_area':    area.strip().upper(),
        })

    return result


# ── DB table management ────────────────────────────────────────────────────────

_ISA_DDL = """
CREATE TABLE IF NOT EXISTS isa_shipments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    buque       TEXT,
    buque_raw   TEXT,
    agencia     TEXT DEFAULT 'ISA',
    eta         TEXT,
    etb         TEXT,
    ets         TEXT,
    material    TEXT,
    cliente     TEXT,
    tons        REAL,
    operacion   TEXT DEFAULT 'DESCARGA',
    muelle      TEXT,
    sector      TEXT,
    origen      TEXT,
    isa_area    TEXT,
    remarks     TEXT,
    source      TEXT DEFAULT 'isa_scraper',
    source_id   TEXT,
    source_date TEXT,
    scraped_at  TEXT DEFAULT (datetime('now'))
);
"""


def ensure_isa_table(con: sqlite3.Connection) -> None:
    """Create isa_shipments table if it doesn't exist."""
    con.executescript(_ISA_DDL)
    con.commit()


# ── Deduplication ──────────────────────────────────────────────────────────────

def _is_duplicate(row: dict, existing: list[dict]) -> bool:
    """
    Return True if row matches any entry in existing:
      - Same sector (port), AND
      - Vessel name similarity ≥ 0.85 (after normalisation), AND
      - ETA within ± 5 days (or both null).
    """
    norm_new = _norm_vessel(row.get('buque'))
    port_new = (row.get('sector') or '').strip().upper()
    try:
        eta_new = datetime.fromisoformat(row['eta'][:10]) if row.get('eta') else None
    except (ValueError, TypeError):
        eta_new = None

    for ex in existing:
        port_ex = (ex.get('sector') or '').strip().upper()
        if port_new != port_ex:
            continue

        try:
            eta_ex = datetime.fromisoformat(str(ex.get('eta') or '')[:10]) if ex.get('eta') else None
        except (ValueError, TypeError):
            eta_ex = None

        if eta_new and eta_ex:
            if abs((eta_new - eta_ex).days) > 5:
                continue
        elif eta_new or eta_ex:
            # One has ETA and the other doesn't — treat as different records
            continue

        norm_ex = _norm_vessel(ex.get('buque'))
        if norm_new == norm_ex:
            return True
        if difflib.SequenceMatcher(None, norm_new, norm_ex).ratio() >= 0.85:
            return True

    return False


def dedup_rows(
    rows: list[dict],
    con: sqlite3.Connection,
) -> tuple[list[dict], int]:
    """
    Filter rows that are already in isa_shipments (dedup across repeated scrapes).
    Returns (new_rows, skipped_count).
    """
    # Load existing ISA rows for comparison
    existing: list[dict] = []
    try:
        for r in con.execute('SELECT buque, sector, eta FROM isa_shipments').fetchall():
            existing.append({'buque': r[0], 'sector': r[1], 'eta': r[2]})
    except sqlite3.OperationalError:
        pass   # table not yet created — all rows are new

    new_rows: list[dict] = []
    skipped = 0
    for row in rows:
        if _is_duplicate(row, existing):
            skipped += 1
        else:
            new_rows.append(row)
            # Register in-memory so intra-batch duplicates (same vessel, two cargo
            # lines for the same call) are NOT skipped — they are distinct records.
            # We intentionally do NOT add to existing here because the same vessel
            # loading multiple products is valid and all should be inserted.

    return new_rows, skipped


def insert_rows(rows: list[dict], con: sqlite3.Connection) -> int:
    """Insert rows into isa_shipments. Returns count of rows inserted."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO isa_shipments
            (buque, buque_raw, agencia, eta, etb, ets,
             material, cliente, tons, operacion,
             muelle, sector, origen, isa_area, remarks,
             source, source_id, source_date)
        VALUES
            (:buque, :buque_raw, :agencia, :eta, :etb, :ets,
             :material, :cliente, :tons, :operacion,
             :muelle, :sector, :origen, :isa_area, :remarks,
             :source, :source_id, :source_date)
        """,
        rows,
    )
    con.commit()
    return len(rows)


# ── Convenience: run a full scrape+dedup+insert cycle ─────────────────────────

def run_scrape(
    db_path: Path = DB_PATH,
    dry_run: bool = False,
) -> dict:
    """
    Full pipeline: fetch → parse → dedup → insert.
    Returns a stats dict (same shape as the /api/admin/scrape_isa response).
    """
    rows = scrape_isa()
    ports_all = sorted({r['sector'] for r in rows})
    total     = len(rows)

    if dry_run or not rows:
        return {
            'status':               'ok',
            'dry_run':              dry_run,
            'new_rows':             0,
            'skipped_rows':         0,
            'ports_covered':        ports_all,
            'total_fertilizer_rows': total,
        }

    con = sqlite3.connect(str(db_path))
    ensure_isa_table(con)
    new_rows, skipped = dedup_rows(rows, con)
    inserted = insert_rows(new_rows, con)
    con.close()

    return {
        'status':                'ok',
        'dry_run':               False,
        'new_rows':              inserted,
        'skipped_rows':          skipped,
        'ports_covered':         sorted({r['sector'] for r in new_rows}) if new_rows else [],
        'total_fertilizer_rows': total,
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

    # Always print a sample
    for r in _rows[:5]:
        print(
            f"  {r['sector']:20s} | {r['buque']:28s} | ETA={r['eta']} "
            f"| {r['material']:8s} | {(r['tons'] or 0):>8.0f} t | {r['cliente']}"
        )
    if len(_rows) > 5:
        print(f'  … {len(_rows) - 5} more rows')

    if not _insert:
        print('\n[isa_scraper] Tip: pass --insert to write to DB.')
        sys.exit(0)

    if _dry_run:
        print('\n[isa_scraper] --dry-run: skipping DB write.')
        sys.exit(0)

    _con = sqlite3.connect(str(DB_PATH))
    ensure_isa_table(_con)
    _new, _skip = dedup_rows(_rows, _con)
    _n = insert_rows(_new, _con)
    _con.close()
    print(f'\n[isa_scraper] Done — inserted={_n}  skipped(dup)={_skip}  db={DB_PATH}')
