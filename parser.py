"""
HidrovíaData — Parser de Lineups Portuarios
Puerto San Nicolás · PDFs semanales

Extrae, limpia y consolida registros de buques con fertilizantes.
"""

import json
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

import pdfplumber

# ─── PATHS ────────────────────────────────────────────────────────────────────

PDF_DIR = Path("pdfs")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ─── LIMPIEZA ─────────────────────────────────────────────────────────────────

# Client name corrections applied after X-artifact removal
_CLIENT_CORRECTIONS: dict[str, str] = {
    'CARGIL': 'CARGILL',
}

# ETA text tokens that represent an unknown/unparseable date → return None
_ETA_NULL_TOKENS: frozenset[str] = frozenset({
    'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'JUNIO',
    'ENERO', 'ENEXRO', 'FIN',
})


def clean_vessel_name(raw: str) -> str:
    """
    'ARGENMAR MISTRAL\n170 Mts.'  → 'ARGENMAR MISTRAL'
    'X\nBEATRICE 180 MTS. TYS MTR' → 'BEATRICE'
    'XUNIGALAXY\n180 Mts.'        → 'UNIGALAXY'
    'REM. DOÑA CARMEN\nBarc. ...'  → 'REM. DOÑA CARMEN'
    'oSk\nSCARABE\n180 Mts.'      → 'SCARABE'   (line-selection skips artifact line)
    'oSk SCARABE'                 → 'SCARABE'   (Case A: space-separated artifact ≤3 chars)
    'oSkSCARABE'                  → 'SCARABE'   (Case B: glued, first [A-Z]{2} at pos 3)
    'oNkNORD KAIZAN'              → 'NORD KAIZAN'  (Case B: first [A-Z]{2} at pos 3)
    'oNkORD KAIZAN'               → 'NORD KAIZAN'  (Case B + correction dict)
    """
    if not raw:
        return ""
    # Strip leading X-separator prefix (with or without newline: 'X\nNAME' or 'XNAME')
    raw = re.sub(r"^X\s*\n?", "", raw.strip())

    # ── Line selection: first line that looks like a real vessel name ──────────
    # Criteria: starts with uppercase, ≥ 4 chars, not a dimension-only line.
    # Skips artifact-only lines like 'oSk', 'oNk', '190 Mts.'.
    name = ""
    for line in raw.split("\n"):
        line = line.strip()
        if not line or re.match(r"^[Xx]+$", line):
            continue
        if is_dimension_only(line):
            continue
        if line[0].isupper() and len(line) >= 4:
            name = line
            break
    if not name:
        # Fallback: first non-empty, non-pure-X line (may be lowercase — cleaned below)
        for line in raw.split("\n"):
            line = line.strip()
            if line and not re.match(r"^[Xx]+$", line):
                name = line
                break
    if not name:
        name = raw.split("\n")[0].strip()

    # Remove trailing dimension / unit / draft suffixes.
    # Unit tokens (TYS, MTR) are stripped first because they sometimes follow
    # the dimension number: 'BEATRICE 180 MTS. TYS MTR' → remove ' TYS MTR' first,
    # then ' 180 MTS.' — the order matters.
    name = re.sub(r"(\s+(TYS|MTR\.?))+\s*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+\d+\s*[Mm][Tt][Ss]?\.?\s*$", "", name)
    name = re.sub(r"\s+CALADO\s+[\d,\.]+\s*$", "", name, flags=re.IGNORECASE)

    # ── Artifact prefix cleanup (only triggers when name still starts lowercase) ─

    # Case A — space-separated artifact of ≤ 3 non-space chars before the name:
    #   'oNk NORD KAIZAN' → strip 'oNk ' → 'NORD KAIZAN'
    #   'oSk SCARABE'     → strip 'oSk ' → 'SCARABE'
    #   The {0,2} cap (1 lead char + at most 2 more) prevents greedy over-stripping
    #   that the old \S* caused: 'oNkNORD KAIZAN' no longer matches (no space at pos 3).
    name = re.sub(r"^[a-z]\S{0,2}\s+", "", name)

    # Case B — artifact glued directly to the real name, no separating space:
    #   Find the first run of ≥ 2 consecutive uppercase letters — that marks the
    #   start of the real vessel name.  Strip the lowercase-start prefix before it,
    #   but only when that prefix is ≤ 3 characters (otherwise the name is likely fine).
    #   'oNkNORD KAIZAN' → [A-Z]{2} at pos 3 → strip 3 → 'NORD KAIZAN'
    #   'oNkORD KAIZAN'  → [A-Z]{2} at pos 3 → strip 3 → 'ORD KAIZAN' (→ correction dict)
    if name and name[0].islower():
        m = re.search(r"[A-Z]{2}", name)
        if m and m.start() <= 3:
            name = name[m.start():]

    # Fix mixed-case OCR artifacts remaining in the name body
    name = re.sub(r"\boSk\b", "OSK", name)
    name = re.sub(r"\boNk\b", "ONK", name)

    # ── Correction dictionary: final safety net for known bad outputs ──────────
    _CORRECTIONS: dict[str, str] = {
        'CARABE':     'SCARABE',      # oSk consumed leading S of SCARABE
        'ARABE':      'SCARABE',      # extreme truncation of SCARABE
        'ORD KAIZAN': 'NORD KAIZAN',  # oNk consumed N, leaving ORD KAIZAN
        'KAIZAN':     'NORD KAIZAN',  # oNk consumed NORD entirely
        'ORD ANTHEM': 'NORD ANTHEM',  # same oNk pattern on NORD ANTHEM
    }
    name = _CORRECTIONS.get(name.strip(), name)

    # Final guard: a valid vessel name must start with an uppercase letter
    name = name.strip()
    if not name or not name[0].isupper():
        return ""
    return name


def is_dimension_only(val: str) -> bool:
    """Return True if val is only a dimension string: '180 Mts.' or '199 Mts.'"""
    return bool(re.match(r"^\d+\s*[Mm]ts\.?\s*$", val.strip()))


def clean_agency(raw: str) -> str:
    """'ALPEMAR 12' → 'ALPEMAR'  |  'X\nMARSA' → 'MARSA'"""
    if not raw:
        return ""
    val = re.sub(r"^X\s*\n", "", raw.strip())
    val = val.split("\n")[0].strip()
    # Drop trailing numbers (e.g. agency codes)
    val = re.sub(r"\s+\d+\s*$", "", val)
    return val.strip()


def clean_operador(raw: str) -> str:
    """Remove leading X-prefix and all embedded X artifacts from operator names.
    'CXASPORT' → 'CASPORT'  |  'PXTP' → 'PTP'  |  'TXYS' → 'TYS'
    'PXAMSA'   → 'PAMSA'    |  'MXTR' → 'MTR'
    """
    if not raw:
        return ""
    val = re.sub(r"^X\s*\n?", "", raw.strip())
    val = val.split("\n")[0].strip()
    val = re.sub(r"X", "", val).strip()
    return val


def parse_date(raw: str) -> str | None:
    """
    '13/6/2025'        → '2025-06-13'
    '28/7/2025\n21:40' → '2025-07-28'
    '10/3X/2026'       → '2026-03-10'  (X artifact removed)
    'JULIO'            → 'JULIO'        (kept as-is for reference)
    ''                 → None
    """
    if not raw:
        return None
    # Strip X-prefix and take first line
    val = re.sub(r"^X\s*\n?", "", raw.strip())
    val = val.split("\n")[0].strip()
    if not val or val in ("X", ""):
        return None
    # Remove embedded X artifacts from date strings (e.g. '10/3X/2026')
    val_clean = re.sub(r"X", "", val)
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(val_clean, fmt).date().isoformat()
        except ValueError:
            pass
    # Text tokens that represent unknown ETAs → null instead of keeping the text
    val_upper = val.upper()
    if any(val_upper == tok or val_upper.startswith(tok + ' ')
           for tok in _ETA_NULL_TOKENS):
        return None
    # Other non-date strings kept for reference
    return val


def parse_tons(raw: str) -> int | None:
    """'5.000' → 5000  |  '5,000' → 5000  |  '4.0X00' → 4000  |  '' → None"""
    if not raw:
        return None
    val = re.sub(r"^X\s*\n?", "", raw.strip())
    val = val.split("\n")[0].strip()
    # Remove embedded X artifacts before numeric parsing
    val = re.sub(r"X", "", val)
    # Argentine thousands separator is '.' — remove it; also handle ','
    val = re.sub(r"[.,](\d{3})", r"\1", val)
    val = re.sub(r"[^0-9]", "", val)
    return int(val) if val else None


def split_clients(raw: str) -> list[str]:
    """
    'YPF / COFCO'  → ['YPF', 'COFCO']
    'CXARGIL'      → ['CARGIL']   (embedded X artifact removed)
    'TOTAL'        → []
    '5.000'        → []           (tonnage leaked into client col → discard)
    """
    if not raw:
        return []
    val = re.sub(r"^X\s*\n?", "", raw.strip())
    val = val.split("\n")[0].strip()
    if val.upper() == "TOTAL" or not val:
        return []
    # Remove X artifacts embedded between uppercase letters/digits
    val = re.sub(r"(?<=[A-Z0-9])X(?=[A-Z0-9])", "", val)
    clients = [c.strip() for c in re.split(r"\s*/\s*", val) if c.strip()]
    # Filter out values that are clearly numeric (column shift artifact)
    clients = [c for c in clients if not re.match(r"^\d[\d.,]*$", c)]
    # Apply known client name corrections
    clients = [_CLIENT_CORRECTIONS.get(c, c) for c in clients]
    return clients


def normalize_operacion(raw: str) -> str:
    """
    Normalize operation values, removing X separator artifacts.
    'XDESCARGAR' → 'DESCARGAR'
    'DXESCARGA'  → 'DESCARGA'
    'TXRASBORDO' → 'TRASBORDO'
    'DXESCARGA\nTRASBORDO' → 'DESCARGA'  (take first line after clean)
    '14/10/2025' → ''  (date leaked in due to column shift → discard)
    """
    if not raw:
        return ""
    val = re.sub(r"^X\s*\n?", "", raw.strip())
    val = val.split("\n")[0].strip()
    # Remove all embedded X artifacts from operation names
    val = re.sub(r"X", "", val).strip()
    # Discard if it looks like a date (column alignment issue in some PDFs)
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", val):
        return ""
    return val


def clean_cell(raw) -> str:
    """General cell cleaner: strip X-prefix, take first line."""
    if raw is None:
        return ""
    val = str(raw).strip()
    val = re.sub(r"^X\s*\n?", "", val)
    return val.split("\n")[0].strip()


# ── CANONICALIZATION (dedup keys + normalisation) ─────────────────────────────
# These functions produce stable, comparable keys. They are NOT used to rewrite
# stored fields — the DB keeps the original cleaned values. They only control
# which records are treated as "same voyage".

_MATERIAL_ALIASES: dict[str, str] = {
    'SULFATO DE AMONIO':   'AMSUL',
    'SULPHATE OF AMMONIA': 'AMSUL',
    'S.A.':                'AMSUL',
    'NITRATO DE AMONIO':   'NITRODOBLE',
    'NITRATO AMONIO':      'NITRODOBLE',
    'FOSFATO DIAMONICO':   'DAP',
    'FOSFATO MONOAMONICO': 'MAP',
    'CLORURO DE POTASIO':  'MOP',
    'UREA GRANULADA':      'UREA',
    'SULFATO SIMPLE':      'SSP',
}

_CLIENT_NORM: dict[str, str] = {
    'CARGIL':        'CARGILL',
    'DREYFUS':       'LDC',
    'LOUIS DREYFUS': 'LDC',
}

# Origin typo / alias map — applied by norm_origin() when writing source data
# AND used in canon keys so that e.g. "RUSSIA" and "RUSIA" collapse together.
_ORIGIN_NORM: dict[str, str] = {
    'MARRECOS':     'MARRUECOS',
    'MARROCO':      'MARRUECOS',
    'RUSSIA':       'RUSIA',
    'EEUU':         'ESTADOS UNIDOS',
    'EE.UU.':       'ESTADOS UNIDOS',
    'ARABIA':       'ARABIA SAUDITA',
    'ARAB SAUDI':   'ARABIA SAUDITA',
    'SAUDI ARABIA': 'ARABIA SAUDITA',
}


def canon_vessel_name(name: str) -> str:
    """Dedup key for vessel name: uppercase, single spaces, normalised REM prefix."""
    if not name:
        return ""
    n = name.upper().strip()
    n = re.sub(r"\s+", " ", n)
    # 'REMOLCADOR X' → 'REM. X'   |   'REM X' → 'REM. X'
    n = re.sub(r"^REMOLCADOR\.?\s+", "REM. ", n)
    n = re.sub(r"^REM\.?\s+", "REM. ", n)
    return n


def canon_material(raw: str) -> str:
    """Dedup key for material: alias expansion, empty → UNKNOWN."""
    if not raw or not raw.strip():
        return "UNKNOWN"
    return _MATERIAL_ALIASES.get(raw.upper().strip(), raw.upper().strip())


def canon_cliente(raw: str) -> str:
    """Dedup key for client: uppercase, single spaces, PTP variant collapse."""
    if not raw:
        return ""
    c = raw.upper().strip()
    c = re.sub(r"\s+", " ", c)
    # 'PTP (ZONA FRANCA)' / 'PTP ZONA FRANCA' / 'PTP-ZF' → 'PTP'
    c = re.sub(r"^PTP\b.*", "PTP", c)
    return _CLIENT_NORM.get(c, c)


def canon_operacion(raw: str) -> str:
    """Dedup key for operation: map to DESCARGAR / CARGA / TRASBORDO / UNKNOWN."""
    if not raw:
        return "UNKNOWN"
    v = raw.upper().strip()
    if "DESCARGAR" in v or "DESCARGA" in v:
        return "DESCARGAR"
    if "CARGA" in v and "TRANS" not in v:
        return "CARGA"
    if "TRASBORDO" in v or "TRANSBORDO" in v:
        return "TRASBORDO"
    return "UNKNOWN"


def norm_origin(raw: str) -> str:
    """Normalise known origin typos and aliases to canonical country name."""
    if not raw:
        return raw
    return _ORIGIN_NORM.get(raw.upper().strip(), raw)


def extract_origin(obs: str) -> str:
    """
    'ETA RECALADA - MARRUECOS'   → 'MARRUECOS'
    'ETC NUEVA PALMIRA - CHINA'  → 'CHINA'
    'ETA SAN NICOLAS - EXPO A PARAGUAY' → 'EXPO A PARAGUAY'
    'X\nARABIA SAUDITA'          → 'ARABIA SAUDITA'  (no-dash direct format)
    'XARABIA SAUDITA'            → 'ARABIA SAUDITA'  (no-dash, X-glued)
    """
    if not obs:
        return ""
    obs = re.sub(r"^X\s*\n?", "", obs.strip())
    obs = obs.replace("\n", " ").strip()
    if not obs:
        return ""
    match = re.search(r"-\s*(.+)$", obs)
    if match:
        return match.group(1).strip()
    # No dash: some PDFs put the origin country directly in the cell without
    # the "ETA LOCATION - " prefix.  Return the cleaned string as-is, but
    # skip purely numeric values (tonnage bleed from an adjacent column).
    if re.match(r"^\d[\d.,\s]*$", obs):
        return ""
    return obs


# ─── ROW CLASSIFICATION ───────────────────────────────────────────────────────

def is_separator_row(row: list) -> bool:
    """'X X X X X X X X X X X X' in any single cell → separator."""
    for cell in row:
        if cell and re.match(r"^(X\s*)+$", str(cell).strip()):
            non_null = sum(1 for c in row if c and str(c).strip() not in ("", "X"))
            if non_null == 0:
                return True
    return False


def is_column_header_row(row: list) -> bool:
    return str(row[0] or "").strip() in (
        "BUQUE", "MUELLE CARGA GENERAL", "MUELLE ELEVADOR", "MUELLE AES", "REFERENCIAS"
    )


def is_total_row(row: list) -> bool:
    for cell in row:
        if cell and "TOTAL" in str(cell).upper():
            return True
    return False


def is_empty_row(row: list) -> bool:
    return all(not c or not str(c).strip() for c in row)


# ─── PDF DATE EXTRACTION ──────────────────────────────────────────────────────

def extract_pdf_date(full_text: str) -> str | None:
    """Find the issue date in the PDF header text."""
    matches = re.findall(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", full_text)
    for m in matches:
        try:
            return datetime.strptime(m, "%d/%m/%Y").date().isoformat()
        except ValueError:
            pass
    return None


# ─── MUELLE DETECTION ────────────────────────────────────────────────────────

def detect_muelle(cell_text: str) -> str | None:
    """Return muelle name if this cell looks like a section header."""
    val = str(cell_text or "").strip()
    m = re.match(r"MUELLE\s+(.+)", val, re.IGNORECASE)
    if m:
        return m.group(1).strip().split("\n")[0]
    return None


# ─── CORE PARSER ─────────────────────────────────────────────────────────────

def is_lineup_pdf(full_text: str) -> bool:
    """Check if the PDF is a San Nicolás lineup (not some other document)."""
    return "LINE UP PUERTO SAN NICOLAS" in full_text.upper() or "MUELLE" in full_text.upper()


def parse_pdf(pdf_path: Path) -> tuple[str | None, list[dict]]:
    """
    Returns (pdf_date_iso, list_of_records).
    Each record has keys: buque, eta, material, cliente, tons, operador,
                          operacion, muelle, agencia, origen,
                          _pdf_date, _pdf_file
    """
    records: list[dict] = []
    pdf_date: str | None = None
    current_muelle = "CARGA GENERAL"  # fallback

    with pdfplumber.open(pdf_path) as pdf:
        # Extract full text once for date detection + format validation
        full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)

        if not is_lineup_pdf(full_text):
            return None, []  # Not a lineup PDF, skip silently

        pdf_date = extract_pdf_date(full_text)

        # vessel_ctx persists across ALL tables and pages within this PDF
        # (handles page-boundary splits where vessel name ends on one page
        #  and dimensions row starts the next)
        vessel_ctx: dict = {}

        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                if not table:
                    continue

                # ── Check for muelle section header in first cell ──────────
                first_cell = str(table[0][0] or "").strip()
                muelle_candidate = detect_muelle(first_cell)
                if muelle_candidate:
                    current_muelle = muelle_candidate

                for row in table:
                    # Skip structural noise
                    if (is_separator_row(row)
                            or is_column_header_row(row)
                            or is_total_row(row)
                            or is_empty_row(row)):
                        continue

                    col0_raw = str(row[0] or "").strip()
                    col0 = re.sub(r"^X\s*\n?", "", col0_raw).strip()

                    # ── New vessel? (col 0 non-empty after stripping X) ────
                    if col0 and col0 not in ("", "X"):
                        # Skip dimension-only rows (page-boundary artifact)
                        # e.g. '180 Mts.' appearing as first row on next page
                        if is_dimension_only(col0):
                            pass  # keep existing vessel_ctx, don't update
                        else:
                            vessel_name = clean_vessel_name(col0)
                            if vessel_name:
                                vessel_ctx = {
                                    "buque":         vessel_name,
                                    "agencia":       clean_agency(str(row[1] or "")),
                                    "operacion":     normalize_operacion(clean_cell(row[6])),
                                    "eta":           parse_date(clean_cell(row[7])),
                                    "sector":        clean_cell(row[8]).split("\n")[0],
                                    "etb":           parse_date(clean_cell(row[9])),
                                    # Pass raw cell (with newlines) so 'BAHIA\nBLANCA' → 'BAHIA BLANCA'
                                    "origen":        extract_origin(str(row[10] or "")),
                                    "muelle":        current_muelle,
                                    "last_material": None,   # Bug 2: forward-fill seed
                                }

                    # ── No vessel context yet, skip ────────────────────────
                    if not vessel_ctx:
                        continue

                    # ── Extract material / client / tons for this line ─────
                    material  = clean_cell(row[2])
                    # Bug 2 fix: forward-fill material within the same vessel block.
                    # In the PDF, only the first importer row carries the material;
                    # continuation rows leave that cell blank.
                    if not material and vessel_ctx.get("last_material"):
                        material = vessel_ctx["last_material"]
                    elif material:
                        vessel_ctx["last_material"] = material

                    # Origin forward-fill: the OBSERVACION column (col 10) sometimes
                    # carries the origin on the first importer row rather than on the
                    # vessel-name row.  Update vessel_ctx["origen"] whenever we find a
                    # non-empty origin so all subsequent rows in the block inherit it.
                    row_origin = extract_origin(str(row[10] or ""))
                    if row_origin:
                        vessel_ctx["origen"] = row_origin

                    raw_cli   = clean_cell(row[3])
                    raw_tons  = clean_cell(row[4])
                    operador  = clean_operador(str(row[5] or ""))
                    # Use this row's OPERACIÓN if available (normalized), else inherit
                    operacion = normalize_operacion(clean_cell(row[6])) or vessel_ctx["operacion"]

                    clients = split_clients(raw_cli)
                    # Fallback: col3 (cliente) is empty but material is present.
                    # Some PDF layouts put the agency entity in col1 and leave col3
                    # blank when the agent == consignee (e.g. PTP importing AMSUL).
                    # Use col1 of this row first; fall back to the vessel's inherited
                    # agencia.  Do NOT trigger when material is missing (aggregate rows).
                    if not clients and material:
                        row_agency = clean_agency(str(row[1] or ""))
                        fallback_cli = row_agency or vessel_ctx.get("agencia", "")
                        if fallback_cli:
                            clients = [fallback_cli]
                    if not clients:
                        continue

                    tons = parse_tons(raw_tons)

                    for cliente in clients:
                        records.append({
                            "buque":       vessel_ctx["buque"],
                            "agencia":     vessel_ctx["agencia"],
                            "eta":         vessel_ctx["eta"],
                            "material":    material,
                            "cliente":     cliente,
                            "tons":        tons,
                            "operador":    operador or "",
                            "operacion":   operacion,
                            "muelle":      vessel_ctx["muelle"],
                            "sector":      vessel_ctx["sector"],
                            "origen":      norm_origin(vessel_ctx["origen"]),
                            # Provenance — kept in output (not underscore-prefixed)
                            "source_id":   pdf_path.name,
                            "source_date": pdf_date,
                            # Internal fields for dedup sort (stripped before JSON output)
                            "_pdf_date":   pdf_date,
                            "_pdf_file":   pdf_path.name,
                        })

    return pdf_date, records


# ─── CONSOLIDATION ────────────────────────────────────────────────────────────

def eta_to_date(eta: str | None) -> datetime | None:
    """Parse ISO date string to datetime, return None if unparseable."""
    if not eta:
        return None
    try:
        return datetime.fromisoformat(eta)
    except ValueError:
        return None


def consolidate_within_pdf(records: list[dict]) -> list[dict]:
    """
    Within a single PDF: deduplicate on (buque, eta, material, cliente).
    Keep the first occurrence — do NOT sum tons. Repeated rows within the
    same PDF are parser/PDF-layout artifacts, not genuinely separate parcels.
    (Summing created inflated values, e.g. 2×1425 = 2850 for the same slot.)
    """
    seen: set[tuple] = set()
    result: list[dict] = []
    for r in records:
        key = (r["buque"], r["eta"], r["material"], r["cliente"])
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def consolidate_across_pdfs(all_records: list[dict]) -> list[dict]:
    """
    Across all PDFs: collapse same-voyage records into one (best snapshot wins).

    Voyage identity uses a canonical base_key:
        (canon_vessel_name, canon_material, canon_cliente, canon_operacion, muelle_upper)

    Two records are treated as the same voyage when base_key matches AND any of:
      1. ETA-null yields to ETA-present (early draft vs confirmed arrival)
      2. ETA difference ≤ 21 days  (weekly lineup drift window)
      3. Tons identical / both null (strong override — definitively same snapshot)

    Best-snapshot priority (applied via sort order before dedup loop):
      • ETA-present records before ETA-null records for the same base_key
      • Among ETA-present: most recent source_date wins
      This means the first occurrence in the loop is always the best snapshot.
    """
    # Step 1 — most-recent PDF first (source_date descending)
    all_records.sort(key=lambda r: r.get("_pdf_date") or "", reverse=True)
    # Step 2 — ETA-present records before ETA-null (stable sort preserves step 1 order)
    all_records.sort(key=lambda r: 0 if r.get("eta") else 1)

    # Pre-compute canonical keys once per record (avoids repeated calls in O(n²) loop)
    keyed: list[tuple[tuple, dict]] = []
    for r in all_records:
        k = (
            canon_vessel_name(r.get("buque")     or ""),
            canon_material(   r.get("material")  or ""),
            canon_cliente(    r.get("cliente")   or ""),
            canon_operacion(  r.get("operacion") or ""),
            (r.get("muelle") or "").strip().upper(),
        )
        keyed.append((k, r))

    final_records: list[dict]  = []
    final_keys:    list[tuple] = []

    for bkey, record in keyed:
        eta_dt   = eta_to_date(record.get("eta"))
        rec_tons = record.get("tons")

        matched = False
        for ekey, existing in zip(final_keys, final_records):
            if bkey != ekey:
                continue

            ex_dt   = eta_to_date(existing.get("eta"))
            ex_tons = existing.get("tons")

            # Rule 1: ETA-null record always yields to any ETA-present record.
            # The ETA-null was an early "draft" sighting; the ETA-present is the
            # definitive arrival snapshot.  Because the sort puts ETA-present
            # records first, by the time we reach an ETA-null candidate the
            # ETA-present record is already in final → we safely drop the draft.
            if eta_dt is None and ex_dt is not None:
                matched = True
                break

            # Strong override: identical tons (or both null) → always same voyage
            if rec_tons == ex_tons or (rec_tons is None and ex_tons is None):
                matched = True
                break

            # Normal window: ETA within 21 days
            if eta_dt and ex_dt:
                if abs((eta_dt - ex_dt).days) <= 21:
                    matched = True
                    break
            elif record.get("eta") == existing.get("eta"):
                # Both non-parseable strings — exact match
                matched = True
                break

        if not matched:
            final_records.append(record)
            final_keys.append(bkey)

    return final_records


def strip_internal(r: dict) -> dict:
    """Remove _-prefixed internal fields from output."""
    return {k: v for k, v in r.items() if not k.startswith("_")}


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    pdf_files = sorted(PDF_DIR.glob("*.pdf")) + sorted(PDF_DIR.glob("*.PDF"))

    if not pdf_files:
        print(f"No se encontraron PDFs en {PDF_DIR.resolve()}")
        return

    print(f"╔══════════════════════════════════════════════╗")
    print(f"║  HidrovíaData Parser — {len(pdf_files)} PDFs encontrados")
    print(f"╚══════════════════════════════════════════════╝\n")

    all_records: list[dict] = []
    pdf_stats: list[dict]   = []
    raw_total = 0

    for pdf_path in pdf_files:
        try:
            pdf_date, records = parse_pdf(pdf_path)
            before = len(records)
            records = consolidate_within_pdf(records)
            after  = len(records)
            raw_total += before
            all_records.extend(records)
            pdf_stats.append({
                "archivo":   pdf_path.name,
                "fecha":     pdf_date,
                "filas_raw": before,
                "registros": after,
            })
            print(f"  ✓ {pdf_path.name:<55} fecha:{pdf_date}  "
                  f"raw:{before:>4} → intra:{after:>4}")
        except Exception as e:
            print(f"  ✗ {pdf_path.name}: ERROR — {e}")
            pdf_stats.append({"archivo": pdf_path.name, "error": str(e)})

    print(f"\n  Registros acumulados (pre inter-PDF): {len(all_records)}")

    # ── Consolidate across PDFs ───────────────────────────────────────────────
    final_records = consolidate_across_pdfs(all_records)
    dupes_removed = len(all_records) - len(final_records)
    print(f"  Duplicados inter-PDF eliminados:     {dupes_removed}")
    print(f"  Registros finales:                   {len(final_records)}\n")

    # ── Clean and sort output ─────────────────────────────────────────────────
    output_data = sorted(
        [strip_internal(r) for r in final_records],
        key=lambda r: (r.get("eta") or "9999", r.get("buque") or "")
    )

    with open(OUTPUT_DIR / "data.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    # ── Statistics ────────────────────────────────────────────────────────────
    ops       = defaultdict(int)
    mats      = defaultdict(int)
    clis      = defaultdict(int)
    muelles   = defaultdict(int)
    buques    = defaultdict(int)
    total_ton = 0

    for r in final_records:
        ops[r.get("operacion") or "—"] += 1
        mats[r.get("material") or "—"] += 1
        clis[r.get("cliente")  or "—"] += 1
        muelles[r.get("muelle") or "—"] += 1
        buques[r.get("buque")  or "—"] += 1
        if r.get("tons"):
            total_ton += r["tons"]

    with open(OUTPUT_DIR / "resumen.txt", "w", encoding="utf-8") as f:
        def w(line=""):
            f.write(line + "\n")

        w("═══════════════════════════════════════════════════════")
        w("  RESUMEN — HidrovíaData Parser")
        w(f"  Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        w("═══════════════════════════════════════════════════════")
        w()
        w(f"PDFs procesados:                  {len(pdf_stats)}")
        w(f"Registros raw (suma todos PDFs):  {raw_total}")
        w(f"Tras consolidación intra-PDF:     {len(all_records)}")
        w(f"Duplicados inter-PDF eliminados:  {dupes_removed}")
        w(f"REGISTROS FINALES:                {len(final_records)}")
        w(f"Toneladas totales:                {total_ton:,}")
        w()

        w("─── Por operación ───────────────────────────────────")
        for op, cnt in sorted(ops.items(), key=lambda x: -x[1]):
            w(f"  {op:<30} {cnt:>5}")

        w()
        w("─── Por material (top 20) ───────────────────────────")
        for mat, cnt in sorted(mats.items(), key=lambda x: -x[1])[:20]:
            w(f"  {mat:<30} {cnt:>5}")

        w()
        w("─── Por cliente (top 25) ────────────────────────────")
        for cli, cnt in sorted(clis.items(), key=lambda x: -x[1])[:25]:
            w(f"  {cli:<30} {cnt:>5}")

        w()
        w("─── Por muelle ──────────────────────────────────────")
        for mu, cnt in sorted(muelles.items(), key=lambda x: -x[1]):
            w(f"  {mu:<30} {cnt:>5}")

        w()
        w("─── Buques únicos (top 30) ──────────────────────────")
        for bu, cnt in sorted(buques.items(), key=lambda x: -x[1])[:30]:
            w(f"  {bu:<35} {cnt:>5}")

        w()
        w("─── Detalle por PDF ─────────────────────────────────")
        for s in pdf_stats:
            if "error" in s:
                w(f"  ✗ {s['archivo']}: ERROR — {s['error']}")
            else:
                w(f"  ✓ {s['archivo']:<55} ({s['fecha']})  "
                  f"raw:{s['filas_raw']:>4}  intra:{s['registros']:>4}")

    print(f"  → {OUTPUT_DIR}/data.json")
    print(f"  → {OUTPUT_DIR}/resumen.txt")
    print(f"\n✓ Listo.")


if __name__ == "__main__":
    main()
