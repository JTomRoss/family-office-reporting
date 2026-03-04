# FO Reporting System

Sistema de reporting financiero interno para Family Office.

## Quick Start

```bash
# 1. Clonar e instalar
git clone <repo-url>
cd fo-reporting
pip install -e ".[dev]"

# 2. Inicializar BD
python -m backend.db.init_db

# 3. Levantar app (backend + frontend, sin --reload)
.\scripts\start.ps1

# 4. Detener app
.\scripts\stop.ps1
```

## Docker

```bash
docker-compose up --build
```

## Estructura

Ver [ARCHITECTURE.md](ARCHITECTURE.md) para documentación completa.

```
backend/          → FastAPI + lógica de negocio
frontend/         → Streamlit UI (solo presentación)
parsers/          → Plugins de parsing por banco/tipo
calculations/     → Fórmulas financieras aisladas
data/             → Raw docs, BD, cache
scripts/          → Freeze, restore, utilidades
tests/            → Tests unitarios e integración
```

## Comandos Críticos

```bash
# Freeze (snapshot completo)
python scripts/freeze.py --label "cierre_enero_2026"

# Restore
python scripts/restore.py --tag 20260226_120000_cierre_enero_2026

# Tests
pytest tests/ -v --cov=backend --cov=calculations --cov=parsers
```
