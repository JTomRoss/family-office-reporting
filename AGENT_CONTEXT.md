# AGENT_CONTEXT — Family Office Reporting System

> **Propósito**: Este archivo es el SSOT de contexto para cualquier agente AI que trabaje en este proyecto. Léelo COMPLETO antes de hacer cualquier cambio.
> **Última actualización**: 2026-03-04 (dashboards negocio + hardening parsers JPM/UBS/BBH + regla UBS trimestral)

---

## 1. QUÉ ES ESTA APLICACIÓN

Sistema de **reporting financiero interno** para un Family Office chileno. La aplicación:
- Recibe cartolas bancarias (PDF) y datos operativos (Excel/CSV) de múltiples bancos internacionales.
- Parsea, clasifica, concilia y presenta la información en dashboards consolidados.
- Es de **uso interno exclusivo**, no es un producto comercial. El usuario NO es programador.

---

## 2. STACK TECNOLÓGICO (no negociable)

| Componente | Tecnología | Versión mínima |
|---|---|---|
| **Lenguaje** | Python | 3.12.8 |
| **Backend API** | FastAPI + Uvicorn | 0.104+ |
| **Frontend UI** | Streamlit | 1.29+ |
| **ORM / BD** | SQLAlchemy 2.0 + SQLite WAL | 2.0+ |
| **Migraciones** | Alembic | 1.13+ |
| **PDF parsing** | pdfplumber (principal) + PyMuPDF/fitz (fallback GS) | — |
| **Excel parsing** | pandas + openpyxl | — |
| **HTTP client** | httpx | 0.25+ |
| **Charts** | Plotly | 5.18+ |
| **Schemas** | Pydantic v2 | 2.5+ |
| **Tests** | pytest + hypothesis | — |
| **Moneda en BD** | `Numeric(20,4)` — NUNCA Float | — |

### Entorno local
- **venv**: `.venv/` (activar con `.\.venv\Scripts\Activate.ps1` en Windows)
- **Instalar**: `pip install -e ".[dev]"`
- **Iniciar app**: `.\scripts\start.ps1` (levanta backend 8000 + frontend 8501, SIN --reload)
- **Detener app**: `.\scripts\stop.ps1`
- **Reiniciar tras cambios**: `.\scripts\stop.ps1 ; .\scripts\start.ps1`
- **Tests**: `python -m pytest tests/ -x -q`
- **Git**: repo `JTomRoss/family-office-reporting` (privado), branch `master`
- ⚠️ **NUNCA** usar `--reload` ni `--log-level debug`. Ver sección 10.

---

## 3. ARQUITECTURA — REGLAS NO NEGOCIABLES

### 3.1 Separación estricta de capas

```
frontend/          → SOLO presentación. CERO lógica de negocio.
  api_client.py    → TODA comunicación UI→Backend pasa por aquí (httpx).
  pages/           → Una página por pestaña (upload, summary, etf, etc.)

backend/           → API REST. Toda lógica de negocio vive aquí.
  main.py          → Único entrypoint FastAPI. Registra routers.
  config.py        → Rutas, settings, constantes.
  schemas.py       → Pydantic schemas (contrato API).
  routers/         → health, documents, accounts, data
  services/        → document_service, account_service, data_loading_service, cache_service
  db/              → models.py (SQLAlchemy ORM), session.py, init_db.py

parsers/           → Sistema de plugins aislados. Un parser = un archivo.
  base.py          → ABC BaseParser, ParseResult, ParsedRow
  registry.py      → Auto-discovery + registro dinámico
  <banco>/         → Carpeta por banco (jpmorgan/, ubs/, goldman_sachs/, etc.)
    <tipo>.py      → Un archivo por tipo de cuenta

calculations/      → Cálculos financieros puros (sin I/O).
  allocation.py    → Asset allocation
  profit.py        → Profit/return
  reconciliation.py → Conciliación diaria vs mensual

tests/             → 119 tests (unit + contracts + parsers + cálculos + arquitectura)
data/              → Archivos: raw/, cache/, snapshots/, db/
```

### 3.2 Reglas de hierro

1. **La UI NUNCA importa nada de `backend/` ni `parsers/`**. Solo usa `api_client.py`.
2. **Los parsers son islas**: cada parser es un archivo autocontenido. NO comparten helpers entre bancos distintos (excepción: `goldman_sachs/_gs_common.py` para los 2 parsers GS que usan PyMuPDF).
3. **Idempotencia**: todo upload verifica SHA-256. Mismo archivo = mismo resultado.
4. **Moneda SIEMPRE en `Numeric(20,4)`**, nunca Float.
5. **UTC everywhere**: usar `datetime.now(timezone.utc)`, nunca `datetime.utcnow()`.
6. **Versionado de parsers**: cada parser tiene `VERSION = "X.Y.Z"`. El hash del código fuente se registra en BD para trazabilidad.
7. **`safe_parse()` en vez de `parse()`**: el wrapper automático valida el contrato.
8. **La cartola bancaria (PDF) es la VERDAD** para conciliación mensual. Los Excel son datos operativos diarios.
9. **Auto-detección determinista**: en empate de parsers, gana el orden alfabético por nombre.

---

## 4. BASE DE DATOS — 12 MODELOS

| # | Tabla | Propósito |
|---|---|---|
| 1 | `accounts` | Maestro de cuentas (SSOT desde Excel maestro) |
| 2 | `raw_documents` | Archivo fuente original (PDF/Excel). SHA-256 unique. |
| 3 | `parser_versions` | Versión + hash del parser que procesó cada documento |
| 4 | `parsed_statements` | Resultado de parsing de cartolas (intermedio) |
| 5 | `daily_positions` | Posiciones diarias (desde Excel) |
| 6 | `daily_movements` | Movimientos/transacciones diarias (desde Excel) |
| 7 | `daily_prices` | Precios FX + activos (desde Excel) |
| 8 | `monthly_closings` | Cierre mensual oficial (desde cartola = VERDAD) |
| 9 | `reconciliations` | Resultado de conciliación diaria vs mensual |
| 10 | `validation_logs` | Audit trail completo del sistema |
| 11 | `etf_compositions` | Composición ETFs por instrumento |
| 12 | `cache_metadata` | Control de cache Parquet pre-calculados |

### Enums definidos en `backend/db/models.py`:
- `AccountType`: custody, current, savings, investment, etf
- `EntityType`: sociedad, persona
- `MandateType`: discretionary, advisory, execution_only
- `DocumentStatus`: uploaded, processing, parsed, validated, error
- `FileType`: pdf_cartola, pdf_report, excel_positions, excel_movements, excel_prices, excel_master, csv
- `MovementType`: buy, sell, dividend, interest, fee, transfer_in, transfer_out, fx, coupon, other

### Relaciones clave con cascade:
- `RawDocument.parsed_statements` → `cascade="all, delete-orphan"`
- `RawDocument.validation_logs` → `cascade="all, delete-orphan"`

---

## 5. PARSERS — INVENTARIO COMPLETO (14 parsers)

### 5.1 PDF parsers (10, todos v2.0.0)

| Banco | Tipo | Clase | Lib PDF | Notas |
|---|---|---|---|---|
| jpmorgan | etf | `JPMorganEtfParser` | pdfplumber | Multi-cuenta: si >1 cuenta → `account_number="Varios"` |
| jpmorgan | brokerage | `JPMorganBrokerageParser` | pdfplumber | Idem ETF (Consolidated Statement) |
| jpmorgan | custody | `JPMorganCustodyParser` | pdfplumber | Investment Management format |
| jpmorgan | bonds | `JPMorganBondsParser` | pdfplumber | Fixed income con maturity breakdown |
| bbh | custody | `BBHCustodyParser` | pdfplumber | — |
| bice | brokerage | `BICEBrokerageParser` | pdfplumber | — |
| ubs | custody | `UBSSuizaCustodyParser` | pdfplumber | UBS Suiza |
| ubs_miami | custody | `UBSMiamiCustodyParser` | pdfplumber | UBS Miami |
| goldman_sachs | etf | `GoldmanSachsEtfParser` | **PyMuPDF (fitz)** | pdfplumber no puede leer GS |
| goldman_sachs | custody | `GoldmanSachsCustodyParser` | **PyMuPDF (fitz)** | Comparte `_gs_common.py` |

### 5.2 Excel parsers (4, todos v1.0.0)

| Key | Clase | Propósito |
|---|---|---|
| system.master_accounts | `MasterAccountsParser` | Maestro de cuentas (SSOT) |
| system.daily_positions | `DailyPositionsParser` | Posiciones diarias |
| system.daily_movements | `DailyMovementsParser` | Movimientos diarios |
| system.daily_prices | `DailyPricesParser` | Precios FX + activos |

### 5.3 Contrato de parser (`ParseResult`)

Campos clave del dataclass:
- `status`: SUCCESS / PARTIAL / ERROR
- `account_number`: str (o "Varios" si multi-cuenta)
- `account_numbers`: list[str] (sub-cuentas si multi-cuenta)
- `rows`: list[ParsedRow] (cada row tiene `.data: dict`, `.confidence: float`)
- `balances`: dict (saldos estructurados)
- `qualitative_data`: dict (asset allocation, etc.)
- `period_start`, `period_end`, `statement_date`, `currency`
- `opening_balance`, `closing_balance`

Validación automática en `validate_contract()`:
- SUCCESS requiere `account_number` o `account_numbers` + `currency` + (`statement_date` o filas)
- Cada fila debe tener `data` no vacío

---

## 6. FLUJO DE DATOS — CÓMO FUNCIONA

### 6.1 Carga de maestro de cuentas
```
UI (Excel/CSV tab) → POST /documents/upload-and-process
  → DocumentService.upload_document() (guarda raw, SHA-256)
  → DocumentService.process_document()
    → MasterAccountsParser.safe_parse()
    → AccountService.upsert_from_master(rows) → tabla accounts
```

### 6.2 Carga de cartola PDF
```
UI (PDFs tab) → POST /documents/upload-and-process
  → Metadata manual: banco, sociedad, dígito verificador (auto-fill)
  → Auto-fill: GET /accounts/auto-fill?identification_number=&bank_code=&entity_name=
  → Solo 3 campos requeridos: banco, sociedad, dígito verificador
  → Detección de duplicados con opciones: Reclasificar / Omitir
  → DocumentService.upload_document() → RawDocument en BD
  → DocumentService.process_document()
    → Parser.safe_parse() → ParseResult
    → DataLoadingService.load_parse_result()
      → parsed_statements, monthly_closings, etf_compositions
```

### 6.3 Endpoints principales

| Método | Ruta | Propósito |
|---|---|---|
| GET | /health | Health check |
| POST | /documents/upload | Upload con clasificación |
| POST | /documents/upload-and-process | Upload + proceso inmediato (maestro) |
| POST | /documents/upload-batch | Carga masiva |
| POST | /documents/{id}/process | Procesar con parser |
| POST | /documents/{id}/reclassify | Reclasificar metadata |
| GET | /documents/ | Listar documentos (filtros) |
| DELETE | /documents/{id} | Eliminar documento |
| DELETE | /documents/ | Eliminar todos |
| GET | /accounts/ | Listar cuentas maestro |
| GET | /accounts/filter-options | Opciones de filtro para UI |
| GET | /accounts/{number}/auto-fill | Auto-completar metadata |
| POST | /data/summary | **Funcional** — filas verticales + chart_data consolidado (fecha/ending/mov/profit/rent%), con fórmula rent basada en movimientos |
| POST | /data/mandates | **Funcional** — KPIs/tablas mandatos, allocation y retornos con datos reales |
| GET | /data/etf-dates | **Funcional** — fechas YYYY-MM disponibles con datos ETF |
| POST | /data/etf | **Funcional** — instruments×societies (montos+pesos%), composición pies, society montos×meses, society returns (mensual+YTD) |
| POST | /data/personal | **Funcional** — consolidado USD/CLP, caja, pie charts y tablas |
| POST | /data/reconciliation | **Funcional** — conciliación diario vs mensual |
| GET | /data/parser-quality | **Funcional** — control parser-vs-cierre cargado |
| POST | /data/asset-allocation-report | **Funcional** — vista reportes de asset allocation PDF |

---

## 7. FRONTEND — 7 PÁGINAS

| Página | Archivo | Estado |
|---|---|---|
| 🏠 Inicio | `pages/home.py` | Funcional |
| 📁 Carga | `pages/upload.py` | **Funcional** — 3 tabs: PDFs, Excel, Docs cargados. Upload+process automático. Botón "Procesar pendientes". |
| 📋 Resumen | `pages/summary.py` | **Funcional** — tabla vertical + gráficos (ending/profit/rent%) desde chart_data, rango personalizado consolidado, detalle cartolas, right-align |
| 📑 Mandatos | `pages/mandates.py` | **Funcional** — consume `/data/mandates` y renderiza payload real |
| 📈 ETF | `pages/etf.py` | **Funcional** — filtros (Fecha/Banco/Sociedad/ConSinCaja), instrumentos×sociedades (montos+pesos%), 2 tortas+rango, society montos×meses, retornos Mensual/YTD |
| 👤 Personal | `pages/personal.py` | **Funcional** — consume `/data/personal` y muestra consolidado/tablas |
| ⚙️ Operacional | `pages/operational.py` | **Funcional** — incluye parser quality + asset allocation report |

### Campos de la página de carga PDF:
- Fila 1: Tipo de documento (cartola / reporte) | Banco * (jpmorgan, ubs, ubs_miami, goldman_sachs, bbh, bice)
- Fila 2: Sociedad * | Dígito verificador * | Botón Auto-llenar
- Solo 3 campos requeridos: banco, sociedad, dígito verificador
- Auto-fill: busca en maestro por identification_number + bank_code + entity_name
- Campos auto-llenados (deshabilitados): cuenta, tipo cuenta, moneda, tipo entidad, persona, código interno
- **Año y mes NO se piden** — el parser los extrae del PDF
- PDFs se procesan automáticamente al subir (upload-and-process)

---

## 8. TESTS — 118 PASSING (+1 skipped)

| Archivo | Tests | Qué valida |
|---|---|---|
| test_allocation.py | 11 | Cálculos de asset allocation |
| test_api.py | 16 | API schemas y endpoints |
| test_architecture_rules.py | 2 | Reglas de arquitectura frontend/backend |
| test_data_loading_operational.py | 4 | Carga de daily positions/movements/prices + report PDF |
| test_json_schemas.py | 16 | JSON schemas de BD |
| test_loader_contracts.py | 3 | Contratos de carga (incluye UBS histórico) |
| test_parser_contracts.py | 8 | Contrato BaseParser |
| test_parsers.py | 12 | Parsers individuales |
| test_profit.py | 25 | Cálculos de profit/return |
| test_reconciliation.py | 8 | Conciliación |
| test_specific_cartola_extraction.py | 10 (+1 skipped) | Casos reales complejos por banco |
| test_summary_returns.py | 3 | Fórmulas de rentabilidad resumen (con/sin caja) |

**REGLA**: Cualquier cambio debe mantener suite estable (actual: 118 passed, 1 skipped). No borrar tests existentes.

---

## 9. ESTADO ACTUAL Y PENDIENTES

### ✅ Completado
- Scaffolding completo (40+ archivos)
- 12 hardening fixes (audit completo)
- 14 parsers funcionales (10 PDF v2.0.0 + 4 Excel v1.0.0, master_accounts v3.0.0)
- Goldman Sachs resuelto con PyMuPDF fallback
- Página de carga simplificada (3 campos requeridos + auto-fill por dígito verificador)
- Auto-fill por `identification_number` + banco + sociedad
- Multi-cuenta ("Varios") con sub-cuentas
- Upload + proceso automático del maestro Y de PDFs (upload-and-process)
- Detección de duplicados con interacción de usuario (Reclasificar/Omitir)
- Tabla maestro visible tras carga de Excel
- Eliminación de documentos con cascade correcto + multi-select checkbox
- **Data pipeline completo**: ParseResult → DataLoadingService → parsed_statements + monthly_closings + etf_compositions
- **Endpoints `/data/summary`, `/data/mandates`, `/data/personal`, `/data/etf` funcionales** con queries reales a BD
- **Summary redesign**: tabla vertical (meses en filas), columnas fijas (Fecha, Sociedad, Banco, ID, Moneda, Ending Value, Movimientos, Profit, Rent. Mensual %, Rent. Mensual sin Caja %). Gráficos usan `chart_data` consolidado (NO diffs de totals). Rango Personalizado = tabla consolidada con selectores año/mes. Detalle Cartolas = tabla por cartola.
- **ETF redesign v2**: filtro Fecha (YYYY-MM), Banco, Sociedad, Con/Sin Caja. Tablas con datos alineados a la derecha.
  - Tabla 1: Instrumentos × Sociedades (montos), solo Fecha
  - Tabla 2: Instrumentos × Sociedades (pesos %), sin caja excluye Money Market
  - 2 tortas + Rango Personalizado en tercios
  - Tabla Sociedades × Meses (montos)
  - Tabla Rentabilidad × Sociedad con toggle Mensual/YTD
- **Diccionario de instrumentos ETF** (consolidación de nombres, en `INSTRUMENT_NAME_MAP` de data.py):
  - IWDA = ISHARES CORE MSCI WORLD
  - IEMA = ISHARES MSCI EM-ACC
  - IHYA = ISHARES USD HY CORP USD ACC = ISHARES USD HIGH YIELD CORP BOND
  - VDCA = VAND USDCP1-3 USDA
  - VDPA = VANG USDCPBD USDA
  - Money Market = sweep, liquidity, cash, depósito
  - Orden fijo: IWDA, IEMA, VDCA, VDPA, IHYA, Money Market
- Society mapping via `SOCIETY_MAPPING` en data.py.
- Filtros UI: BANK_DISPLAY_NAMES, filtros reducidos por pestaña, `render_fecha_filter` para ETF
- 43 cuentas en maestro, campo `identification_number` (dígito verificador, no unique)
- Botón "Procesar pendientes" en tab documentos
- 118 tests passing (+1 skipped)

### 🔲 Pendiente
- **Hardening adicional UBS Suiza**: seguir validando edge-cases de quarterly backfill con cartolas nuevas
- **Cobertura de controles YTD por banco**: extender validaciones y alertas por parser
- **Consolidar backlog de QA funcional UI** (resumen/mandatos/personal) con checklists de negocio
- **Cache Parquet** — la infraestructura existe pero no se usa aún
- **Alembic** — configurado pero sin migraciones ejecutadas formalmente
- **Observabilidad**: mejorar métricas/alertas sobre `validation_logs`

### 🔧 Estado Git
- Worktree con cambios locales en progreso (no asumir limpio/commiteado).
- Antes de cualquier release/respaldo: verificar `git status`, correr tests y luego definir commit/tag.

---

## 9.1 ACTUALIZACIÓN OPERATIVA ACUMULADA (2026-03-04)

### ✅ Avances implementados en esta iteración

- **Dashboards de negocio conectados a datos reales**:
  - `/data/mandates` implementado con datos reales de `monthly_closings` y `asset_allocation_json`.
  - `/data/personal` implementado con consolidados USD/CLP, cash y tablas.
  - `frontend/pages/mandates.py` y `frontend/pages/personal.py` conectadas a backend real.
- **Ingesta operativa diaria habilitada**:
  - `DataLoadingService.load_operational_result()` para `excel_positions`, `excel_movements`, `excel_prices`.
  - Upload de Excel en UI pasa por `upload-and-process` (procesamiento inmediato).
- **Bloque asset allocation PDF**:
  - Nuevo parser `parsers/system/report_asset_allocation.py`.
  - Nuevo flujo `load_asset_allocation_report()` en loader.
  - Endpoint `/data/asset-allocation-report` + vista operacional.
- **Calidad operacional/parsers**:
  - Endpoint `/data/parser-quality` (parser vs cierre cargado).
  - Test de arquitectura para impedir imports backend/parsers desde frontend.

### ✅ Correcciones críticas por banco (con pruebas)

- **JPMorgan Mandato (`parsers/jpmorgan/custody.py`)**
  - Corrección de extracción de `Net Cash Contributions / Withdrawals` con signos/paréntesis.
  - Soporte de captura YTD (`net_contributions_ytd`, `utilidad_ytd`) para control.
  - Caso validado: cuenta `1412600` en 2025-02 con salida `-3,000,000`.
- **BBH Mandato (`parsers/bbh/custody.py`)**
  - Captura explícita de `Prior period adjustments` (current + YTD).
  - Se expone en `account_monthly_activity` para ajuste contable posterior.
- **UBS Suiza (`parsers/ubs/custody.py` + loader)**
  - Corrección de parsing de montos con separador de miles por espacio (ej. `54 185`).
  - Corrección de movimiento marzo 2025 para reflejar `-1,435` (no `-1,428`) cuando existe inflow técnico pequeño.
  - Regla específica UBS Suiza implementada en loader:
    1. En cartola trimestral, para meses previos solo se toma movimiento (`change_in_value`) desde historial.
    2. No se sobreescribe `net_value` de meses previos con backfill trimestral.
    3. Para meses no trimestrales (1,2,4,5,7,8,10,11), la utilidad se recalcula por identidad:
       - `utilidad = valor_activo_mes - movimientos - valor_activo_mes_anterior`
    4. Meses de cierre trimestral (3,6,9,12) mantienen utilidad reportada por cartola trimestral.

### ✅ Cambios en fórmula de rentabilidad (Resumen)

- `backend/routers/data.py` actualizado:
  - Rentabilidad mensual: `(valor_mes - movimientos) / valor_mes_anterior - 1`
  - Rentabilidad mensual sin caja: misma fórmula sobre base sin caja (`ending_sin_caja` y `movimientos_sin_caja`).
- Se elimina dependencia de `utilidad/ending_value` como fórmula principal.

### ✅ Control de consistencia YTD

- En `DataLoadingService` se agregó control de consistencia mensual acumulada vs YTD de:
  - caja (`change_investment_ytd`)
  - utilidad (`income_ytd`)
- Si hay diferencias materiales, se registra warning en `validation_logs`.
- Para BBH, si aplica ajuste retroactivo, se corrige mes anterior para alinear la suma mensual con YTD.

### ✅ Estado real verificado en BD (post reprocesamiento)

- UBS Boatview `206-560552-02`:
  - `2025-01 net_value = 54,185` (ya no `81,861,001`).
  - `2025-03 change_in_value = -1,435`.
- La diferencia detectada de `163,719,166` provenía de suma de dos cuentas UBS mandato en enero:
  - Telmar `206-560402-01` + Boatview `206-560552-02`.

### ✅ Tests agregados/ajustados y ejecutados

- Nuevos/ajustados:
  - `tests/test_data_loading_operational.py`
  - `tests/test_architecture_rules.py`
  - `tests/test_loader_contracts.py`
  - `tests/test_specific_cartola_extraction.py`
  - `tests/test_api.py`
  - `tests/test_summary_returns.py`
- Resultado en esta etapa:
  - tests de parsers/summary/loader relevantes en verde.
  - regla: mantener suite completa verde antes de cerrar cada bloque.

### 🔒 Regla permanente adicional (datos confiables UBS)

- SOLO para UBS Suiza:
  - Si existe cartola mensual del mes, ese `net_value` manda.
  - El trimestral se usa para completar/controlar movimientos históricos y utilidad por identidad en no-trimestre.
  - No mezclar esta lógica con otros bancos ni otros tipos de cuenta.

---

## 9.2 ACTUALIZACION CAPA NORMALIZADA DE REPORTING (2026-03-05)

### ✅ Cambio estructural implementado

- Se incorpora una nueva tabla canonica mensual: `monthly_metrics_normalized`.
- Esta capa es **interna/invisible para UI** y concentra metricas interpretadas por cuenta/mes:
  - `ending_value_with_accrual`
  - `ending_value_without_accrual`
  - `accrual_ending`
  - `cash_value`
  - `movements_net`
  - `profit_period`
  - `currency`
- Modelo ORM agregado en `backend/db/models.py`:
  - `MonthlyMetricNormalized`
  - relacion `Account.normalized_monthly_metrics`.
- Migracion Alembic creada:
  - `alembic/versions/20260305_0002_add_monthly_metrics_normalized.py`
  - unique key por `account_id + year + month`.

### ✅ Carga y sincronizacion de la capa normalizada

- `DataLoadingService` ahora persiste y mantiene esta capa:
  - `_upsert_monthly_metric_normalized(...)` en carga de cartolas.
  - `_refresh_normalized_activity_from_monthly_closings(...)` despues de ajustes YTD/prior-period y tambien tras carga de asset allocation.
  - `sync_normalized_for_account_year(...)` como punto publico para backfill/resync.
- Script operativo de backfill incorporado:
  - `scripts/backfill_normalized_metrics.py`
  - objetivo: poblar/reconciliar historico desde `monthly_closings`.

### ✅ Consumo en reportes (regla de lectura)

- `backend/routers/data.py` centraliza lectura con join controlado:
  - helper `_query_closing_rows(...)` hace `outer join` entre `monthly_closings` y `monthly_metrics_normalized`.
- Endpoints de reporting (`summary`, `mandates`, `etf`, `personal`) consumen primero capa normalizada con fallback seguro.
- Helpers canonicos de resolucion incorporados:
  - `_resolve_ending_with_accrual(...)`
  - `_resolve_ending_without_accrual(...)`
  - `_resolve_cash_value(...)`
  - `_resolve_movements_and_profit(...)`
- Regla funcional:
  1. Si existe dato normalizado, ese valor manda para reportes.
  2. Si falta fila/campo normalizado, se usa `monthly_closings` como fallback.

### ✅ Observabilidad y pruebas de regresion

- Nuevo endpoint de diagnostico:
  - `/api/v1/data/normalization-quality`
  - entrega cobertura (`normalized_rows` vs `monthly_closings`), faltantes y ejemplos de mismatch.
- Pruebas agregadas/extendidas para validar persistencia + consumo + calidad:
  - `tests/test_normalized_reporting_layer.py`
  - `tests/test_loader_contracts.py`
  - `tests/test_data_loading_operational.py`

### 🔒 Regla permanente de arquitectura (no negociable)

- La interpretacion financiera mensual de cartolas debe vivir en la capa normalizada (backend), no en UI.
- Las tablas/pestañas de reporte deben leer la capa normalizada como fuente primaria.
- `monthly_closings` queda como respaldo/fallback y fuente historica base.

---

## 10. PROTOCOLO DE OPERACIÓN — REGLA FUNDAMENTAL

### ⛔ PROHIBICIONES ABSOLUTAS (causa problemas reales)
1. **NUNCA usar `--reload`** en uvicorn. El reloader vigila archivos y reinicia el servidor automáticamente, causando procesos zombis, código viejo en memoria, y puertos ocupados.
2. **NUNCA usar `--log-level debug`** en producción/testing.
3. **NUNCA levantar un segundo proceso** sin verificar que el puerto está libre.
4. **NUNCA asumir que matar un proceso libera el puerto instantáneamente** — siempre esperar 2 segundos y verificar.

### Scripts de gestión (usar SIEMPRE):
```powershell
# DETENER la aplicación:
.\scripts\stop.ps1

# INICIAR la aplicación (detiene primero, luego levanta ambos):
.\scripts\start.ps1

# REINICIAR tras hacer cambios de código:
.\scripts\stop.ps1 ; .\scripts\start.ps1
```

### Flujo OBLIGATORIO para el agente al hacer cambios:

**ANTES de cualquier cambio:**
1. Lee este archivo completo.
2. Lee los archivos que vas a modificar.
3. Corre los tests: `python -m pytest tests/ -x -q` — deben dar suite verde (referencia actual: 118 passed, 1 skipped).

**DESPUÉS de cada cambio:**
1. Corre los tests para verificar que no rompiste nada.
2. **Detener con**: `.\scripts\stop.ps1`
3. **Esperar** 2 segundos.
4. **Levantar con**: `.\scripts\start.ps1`
5. *NO HAY ALTERNATIVA*. No uses comandos manuales de uvicorn ni streamlit.
6. Indica al usuario que **refresque el navegador**.

### Reinicio automático (sin pedir confirmación)
1. Si un cambio requiere reiniciar para verse, el agente debe ejecutar reinicio completo **sin preguntar**:
   - `.\scripts\stop.ps1`
   - esperar 2 segundos
   - verificar puertos 8000/8501
   - `.\scripts\start.ps1`
2. Tras reiniciar, el agente debe avisar: **"app reiniciada, ya puedes revisar"**.
3. Si no requiere reinicio y solo basta refrescar UI, el agente debe avisar al final: **"solo refresca navegador"**.
4. Regla permanente: **nunca debe haber varias versiones/procesos de la app corriendo al mismo tiempo**.

### Protocolo de respaldo (cuando el usuario diga "guardar", "respaldar" o similar)
1. El agente debe **explicar primero** qué hará antes de ejecutar el respaldo.
2. Verificar estado de app y evitar concurrencia:
   - detener con `.\scripts\stop.ps1`
   - verificar puertos libres (8000/8501)
3. Verificar integridad de código:
   - `git status` limpio o dejar commit explícito
   - ejecutar tests (`python -m pytest tests/ -x -q`)
4. Respaldo de datos:
   - ejecutar `python scripts/freeze.py --label "<etiqueta>"`
   - esto guarda snapshot de DB + raw + cache y actualiza `LATEST_VALID_BACKUP.txt`
5. Respaldo de código:
   - asegurar commit en `master`
   - push de branch y push de tags (`git push origin master --tags`)
6. Entrega obligatoria al usuario:
   - tag generado
   - hash commit
   - ruta snapshot
   - confirmación de cómo restaurar (`python scripts/restore.py --tag <tag>` o `--latest`)
7. Al terminar respaldo, dejar app en estado operativo con `.\scripts\start.ps1`.

### Prioridades de calidad (orden estricto)
1. **Confianza de datos y cálculos** (exactitud y trazabilidad) es prioridad #1.
2. **Estabilidad operativa** (# procesos y puertos bajo control) es prioridad #2.
3. **Performance/experiencia de uso** es prioridad #3, sin sacrificar exactitud.

### Comandos de backend y frontend (referencia, NO usar directamente):
```powershell
# Estos comandos son los que usan los scripts internamente.
# El agente SIEMPRE debe usar .\scripts\start.ps1 y .\scripts\stop.ps1
# Backend: puerto FIJO 8000, SIN --reload
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

# Frontend: puerto FIJO 8501
python -m streamlit run frontend/app.py --server.port 8501 --server.headless true
```

### Git:
- Repo: `JTomRoss/family-office-reporting` (privado)
- Branch: `master`
- Commits en español con prefijo convencional (`feat:`, `fix:`, `refactor:`)
- No pushear sin verificar tests

---

## 11. BANCOS Y PARTICULARIDADES

| Banco | Código | Formatos PDF | Notas |
|---|---|---|---|
| JP Morgan | `jpmorgan` | Consolidated Statement (ETF, Brokerage), Investment Mgmt (Custody, Bonds) | Mandatos agrupa múltiples cuentas (ej: 2600, 3400, 9200) → "Varios" |
| UBS Suiza | `ubs` | Custody | — |
| UBS Miami | `ubs_miami` | Custody | Formato diferente a UBS Suiza |
| Goldman Sachs | `goldman_sachs` | ETF, Custody | **pdfplumber no funciona** → usar PyMuPDF (fitz) |
| BBH | `bbh` | Custody | Brown Brothers Harriman |
| BICE | `bice` | Brokerage | Banco chileno |

---

## 12. ARCHIVOS CLAVE — REFERENCIA RÁPIDA

| Archivo | Qué contiene | Líneas aprox |
|---|---|---|
| `backend/db/models.py` | 12 modelos ORM + 8 enums | ~670 |
| `backend/schemas.py` | Pydantic contracts (API) | ~210 |
| `backend/services/document_service.py` | Upload, process (+DataLoadingService), list, delete, reclassify | ~430 |
| `backend/services/data_loading_service.py` | ParseResult → parsed_statements, monthly_closings, etf_compositions + carga daily_* + reglas UBS/BBH | ~1080 |
| `backend/services/account_service.py` | Maestro: upsert, auto-fill, filter options | ~160 |
| `backend/routers/documents.py` | CRUD documentos + upload-and-process | ~190 |
| `backend/routers/data.py` | summary/mandates/personal/etf/reconciliation/parser-quality/asset-allocation-report | ~1200 |
| `frontend/pages/upload.py` | Página de carga (3 tabs) — upload-and-process, multi-select delete | ~610 |
| `frontend/api_client.py` | HTTP client UI→Backend | ~68 |
| `parsers/base.py` | BaseParser ABC + ParseResult + ParsedRow | ~255 |
| `parsers/registry.py` | Auto-discovery de plugins | ~193 |
| `parsers/excel/master_accounts.py` | Parser maestro cuentas | ~147 |
| `pyproject.toml` | Deps + config pytest/ruff | ~65 |
