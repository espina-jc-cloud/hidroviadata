# /project:debug

1) Mostrar estado del repo
- git log -1 --oneline
- git status

2) DB status
- python3 db_status.py
- Mostrar tabla counts (shipments, vessel_candidates)

3) Verificar endpoints local (sin modificar)
- python3 -c "import requests; print('skip if no server')"
Si server local está corriendo: probar /health /api/status