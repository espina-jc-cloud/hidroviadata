# /project:daily

Objetivo: actualizar lineup y cargar candidatos AIS manuales.

Pasos:
1) Ingesta PDFs:
- python3 parser.py
- python3 migrate.py --reset

2) Chequeo:
- python3 db_status.py

3) Agregar candidatos AIS (manual):
- usar UI del dashboard (form) o `detect_candidates.py` con un JSON si aplica

4) Verificación semanal:
- python3 verify_candidates.py
