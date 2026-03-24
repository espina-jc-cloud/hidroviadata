# HidrovíaData — Instrucciones maestras para Claude Code

## Objetivo del producto
1) Lineup confirmado (del último PDF importado) debe mostrarse como "SEGUROS".
2) Predichos por AIS (vessel_candidates) debe mostrarse como "PREDICHOS" y ser alimentado manualmente por el usuario o por script.
3) Track record: verify_candidates.py debe confirmar/expirar y mostrar lead_time.

## Regla de oro de trabajo
- NO inventar datos demo.
- NO hardcodear candidatos.
- Todo lo que se ve en el dashboard debe venir de la DB o endpoints /api/*.

## Flujo diario esperado
1) Agrego PDFs nuevos → `python3 parser.py` + `python3 migrate.py --reset`
2) Veo lineup confirmado en dashboard (último PDF)
3) Cargo 1-5 buques AIS manualmente (form) o JSON → `python3 detect_candidates.py observations.json`
4) Semanal: `python3 verify_candidates.py`

## Endpoints obligatorios (prod y local)
- GET /health -> "ok"
- GET /api/status -> json con counts + latest_source_date/source_id
- GET /api/shipments -> json array no vacío
- GET /api/lineup_confirmed -> datos del último PDF importado
- GET /api/vessel_candidates -> candidatos AIS
- GET /api/debug -> git_sha + DB stats

## Restricciones
- No refactorizar sin razón.
- Cambios mínimos, verificables.
- Siempre entregar comandos de verificación (curl o queries).