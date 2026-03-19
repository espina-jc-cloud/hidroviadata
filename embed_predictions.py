"""
embed_predictions.py
────────────────────
Patch dashboard.html to include the latest buques_en_ruta predictions.

Reads  output/buques_en_ruta.json  and replaces the
  const BUQUES_EN_RUTA_DATA = [...];
line in dashboard.html with the live prediction data.

Usage
─────
    python3 predict_vessels.py      # generate / update predictions first
    python3 embed_predictions.py    # then embed into dashboard

The dashboard's "Buques en Ruta" tab will read BUQUES_EN_RUTA_DATA
and render all entries automatically — no further code changes needed.
"""

import json
import re
from pathlib import Path

DASHBOARD = Path(__file__).parent / "dashboard.html"
DATA_FILE = Path(__file__).parent / "output" / "buques_en_ruta.json"

# Matches both the original 'const' form and the current 'let' form,
# including any trailing inline comment on the same line.
PATTERN = re.compile(r'(?:let|const) BUQUES_EN_RUTA_DATA\s*=\s*\[.*?\];[^\n]*', re.DOTALL)


def embed() -> None:
    if not DATA_FILE.exists():
        print("ERROR: output/buques_en_ruta.json not found.")
        print("       Run  python3 predict_vessels.py  first.")
        raise SystemExit(1)

    if not DASHBOARD.exists():
        print(f"ERROR: {DASHBOARD} not found.")
        raise SystemExit(1)

    with open(DATA_FILE, encoding="utf-8") as f:
        data: list[dict] = json.load(f)

    html = DASHBOARD.read_text(encoding="utf-8")

    if not PATTERN.search(html):
        print("ERROR: BUQUES_EN_RUTA_DATA placeholder not found in dashboard.html.")
        print("       Expected:  const BUQUES_EN_RUTA_DATA = [];")
        raise SystemExit(1)

    minified    = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    replacement = f"let BUQUES_EN_RUTA_DATA = {minified};  // pre-embedded; overwritten by API in init()"
    patched     = PATTERN.sub(replacement, html)

    DASHBOARD.write_text(patched, encoding="utf-8")

    print(f"dashboard.html updated — {len(data)} prediction(s) embedded")
    print()
    for e in data:
        lvl   = e.get("probability_level", "?").upper().ljust(6)
        name  = e.get("vessel_name", "?")[:25].ljust(25)
        score = e.get("probability_score", 0)
        prod  = (e.get("probable_product") or "—").ljust(6)
        eta   = e.get("eta_estimated", "—")
        print(f"  [{lvl}]  {name}  score={score:3}  product={prod}  ETA={eta}")

    print()
    print("Open dashboard.html — 'Buques en Ruta' tab now shows live predictions.")


if __name__ == "__main__":
    embed()
