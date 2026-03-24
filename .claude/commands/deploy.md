# /project:deploy

Objetivo: asegurar que Railway está corriendo la versión nueva.

Pasos:
1) Verificar Procfile / nixpacks.toml / requirements.txt
2) Confirmar endpoints existen:
- /health
- /api/status
- /api/shipments

3) Si producción muestra “viejo”:
- agregar indicador de versión en /api/debug (git sha)
- headers no-store en '/'
- asegurar bootstrap DB si falta
4) Commit + push