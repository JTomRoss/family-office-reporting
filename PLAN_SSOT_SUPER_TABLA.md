# PLAN SSOT SUPER TABLA (monthly_metrics_normalized)

Last update: 2026-03-30  
Owner: JTROSS + Codex

## 1) Objetivo final (reglas cerradas)
- Una sola tabla de lectura para reporting: `monthly_metrics_normalized`.
- Ningún gráfico/tabla debe leer `monthly_closings`, `etf_compositions`, `parsed_statements` ni PDF en runtime de reporting.
- No tocar motores de lectura PDF ni romper su aislamiento por banco/tipo de cuenta.
- Dos diccionarios:
  - Diccionario de cartolas bancarias: ya existente en Excel (`Documentos/Excel/Diccionario de instrumentos.xlsx`).
  - Diccionario de reportes de mandato: nuevo archivo dedicado (no Excel), separado del de cartolas.
- Desglose canónico para cualquier tipo de cuenta:
  - `Cash`
  - `% Investment Grade Fixed Income`
  - `% High Yield Fixed Income`
  - `% US Equities`
  - `% Non US Equities`
  - `% Private Equity`
  - `% Real Estate`
- Métricas adicionales persistidas en super tabla:
  - `fixed_income_yield`
  - `fixed_income_duration`
- Derivados persistidos (no calculados ad-hoc en frontend/routers):
  - `Fixed Income = IG + HY`
  - `Equities = US + Non US`
  - `Non US Equities = todo lo no-US`
  - `Global Equities = 2/3 US + 1/3 Non-US`
  - `Alternativos = PE + RE`

## 2) Gap actual (por qué hoy no cumple al 100%)
- Reporting aún tiene fallback/lecturas mixtas en `backend/routers/data.py`:
  - `_query_closing_rows()` consulta `monthly_closings` + join normalized.
  - `_resolve_asset_allocation_json()` cae a `monthly_closings` si falta normalized.
  - Endpoint ETF (`/data/etf`) arma tablas desde `etf_compositions`.
  - Mandatos/Personal aún tienen interpretación de labels en runtime.
- La tabla `monthly_metrics_normalized` hoy no contiene de forma uniforme:
  - breakdown canónico completo para todas las cuentas,
  - breakdown por instrumento requerido por ETF,
  - métricas FI (`yield`, `duration`) en formato canónico común para todas las vistas.

## 3) Paquetes de cambio con trazabilidad

### PKG-01: Esquema canónico de super tabla
**Objetivo**: definir payload único de reporting dentro de `monthly_metrics_normalized`.

Archivos:
- `backend/db/models.py`
- `alembic/versions/<new>_ssot_super_tabla.py`

Cambios:
- Agregar campos canónicos (si se decide columna por campo) o estandarizar `asset_allocation_json` + `metrics_json` + `instrument_breakdown_json`.
- Incluir explícitamente `%` y monto por bucket canónico.
- Incluir `fixed_income_yield` y `fixed_income_duration` en estructura estable.

Pruebas:
- `tests/test_loader_contracts.py`
- `tests/test_data_loading_operational.py`

---

### PKG-02: Diccionario Mandato separado (nuevo)
**Objetivo**: separar semántica de reportes de mandato del diccionario de cartolas.

Archivos:
- `mandate_report_dictionary.json` (nuevo)
- `mandate_taxonomy.py` (nuevo helper cargador del diccionario)
- `backend/services/data_loading_service.py` (usar taxonomy de mandato en normalización)

Cambios:
- Definir mapping de labels de mandato -> buckets canónicos.
- Reglas explícitas para US/Non-US, IG/HY, PE/RE, cash y fallback.
- Reglas para `Global Equities` y `Non US Equities`.

Pruebas:
- nuevas regresiones unitarias de taxonomy mandato.

---

### PKG-03: Normalización unificada por tipo de cuenta (sin tocar parsers)
**Objetivo**: que loader escriba SIEMPRE super payload canónico en `monthly_metrics_normalized`.

Archivos:
- `backend/services/data_loading_service.py`

Cambios:
- Centralizar una función única de construcción canónica por cuenta/mes.
- Usar como input solo datos ya persistidos/resultados parseados existentes (sin releer PDF).
- Incluir:
  - Mandato (split + métricas FI),
  - ETF (composición por instrumento + buckets),
  - Brokerage/Bonds/otros account types al mismo esquema canónico.

Pruebas:
- `tests/test_loader_contracts.py`
- `tests/test_data_loading_operational.py`
- `tests/test_normalized_reporting_layer.py`

---

### PKG-04: Backfill sin reprocesar PDFs
**Objetivo**: reconstruir super tabla desde datos ya cargados.

Archivos:
- `scripts/backfill_super_tabla_normalized.py` (nuevo)
- `backend/services/data_loading_service.py` (métodos reutilizables de backfill)

Cambios:
- Backfill por cuenta/año/mes usando:
  - `monthly_closings` histórico,
  - `etf_compositions`,
  - `raw_documents` metadatos ya persistidos,
  - sin volver a parsear PDFs.

Pruebas:
- test de integración de backfill sobre DB de test.

---

### PKG-05: Routers reporting solo SSOT
**Objetivo**: eliminar interpretadores y lecturas paralelas en runtime.

Archivos:
- `backend/routers/data.py`

Cambios:
- `/summary`, `/mandates`, `/etf`, `/personal` leyendo solo `monthly_metrics_normalized` (+ `accounts` para metadata de filtro/labels).
- Eliminar fallback a `monthly_closings` y derivaciones desde `etf_compositions` dentro del reporting.
- Conservar `monthly_closings` solo histórico/operacional, no fuente de lectura de vistas.

Pruebas:
- `tests/test_normalized_reporting_layer.py`
- `tests/test_summary_returns.py`
- `tests/test_api.py`

---

### PKG-06: Frontend presentación pura (sin reglas financieras)
**Objetivo**: frontend consume payload final de backend sin inferencia.

Archivos:
- `frontend/pages/summary.py`
- `frontend/pages/mandates.py`
- `frontend/pages/etf.py`
- `frontend/pages/personal.py`
- `frontend/api_client.py` (si cambia contrato)

Cambios:
- Eliminar lógica de fallback/heurística de instrumentos/buckets en frontend.
- Mostrar solo filas con dato cuando aplique (visual).
- Todo cálculo de agregados viene pre-armado desde backend.

Pruebas:
- validación manual + tests de contrato API.

---

### PKG-07: Contrato API SSOT v2
**Objetivo**: payloads explícitos, versionables y trazables.

Archivos:
- `backend/schemas.py`
- `backend/routers/data.py`
- tests de contrato

Cambios:
- Definir estructura estable:
  - `asset_breakdown_canonical`
  - `instrument_breakdown` (ETF)
  - `fi_metrics` (`yield`, `duration`)
  - `derived_breakdown` (FI, Equities, Global, Alternativos)

---

### PKG-08: Salud BD / auditoría SSOT
**Objetivo**: auditoría valida solo super tabla y consistencia interna.

Archivos:
- `backend/routers/data.py` (health endpoints)
- `frontend/pages/operational.py`

Cambios:
- checks de completitud canónica por cuenta/mes
- checks de identidad + coherencia de derivadas persistidas.

---

### PKG-09: Documentación y contexto
**Objetivo**: dejar trazabilidad operacional explícita.

Archivos:
- `AGENT_CONTEXT.md`
- `SESSION_STATE.md`
- `DEEP_CONTEXT.md` (si aplica)

Cambios:
- registrar decisiones finales de SSOT v2.

---

### PKG-10: Rollback y control de riesgo
**Objetivo**: rollback rápido ante desvíos.

Archivos:
- `scripts/backup.py` / scripts de restore (uso operativo)
- documento de runbook SSOT

Cambios:
- checkpoint DB pre-migración/backfill.
- switch controlado de lectura (flag temporal) si se requiere despliegue gradual.

## 4) Orden recomendado de ejecución
1. PKG-01 (schema)
2. PKG-02 (diccionario mandato)
3. PKG-03 (normalizador único)
4. PKG-04 (backfill sin PDF)
5. PKG-05 (routers solo SSOT)
6. PKG-06 (frontend puro)
7. PKG-07/08/09/10 (contratos, auditoría, docs, rollback)

## 5) Criterios de aceptación (DoD)
- Todas las vistas (`Summary`, `Mandates`, `ETF`, `Personal`) leen exclusivamente `monthly_metrics_normalized`.
- No existe fallback de reporting a `monthly_closings` ni a `etf_compositions`.
- Los 7 buckets canónicos + métricas FI existen en cada cuenta/mes aplicable.
- Derivados (`FI`, `Equities`, `Global`, `Alternativos`) salen de campos persistidos en super tabla.
- Cero reproceso PDF para habilitar el cambio (solo backfill de datos ya cargados).
- Pruebas de regresión en verde.

