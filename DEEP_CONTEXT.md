# DEEP_CONTEXT — Family Office Reporting System

> **Propósito**: Este archivo es el SSOT de contexto para cualquier agente AI que trabaje en este proyecto. Léelo COMPLETO antes de hacer cualquier cambio.
> **Última actualización**: 2026-03-13 (normalized SSOT + Salud BD + OCR GS + hardening UBS)

---

## 0. CIERRE TÉCNICO RECIENTE (2026-03-12 / 2026-03-13)

### Arquitectura de reporting consolidada
- `monthly_metrics_normalized` quedó como la **capa canónica mensual** para reporting.
- La regla vigente es:
  - parsers interpretan la cartola;
  - `DataLoadingService` persiste la interpretación mensual en tablas;
  - `backend/routers/data.py` y la UI **consumen** datos persistidos, pero no deben reinterpretar mensualidades desde identidad o YTD.
- En otras palabras: no deben existir más "interpretadores paralelos" de datos mensuales en UI/endpoints.

### Estado funcional ya implementado
- `Summary`, `Mandates`, `ETF` y `Personal` consumen datos reales desde backend con prioridad de lectura en capa normalizada.
- `Salud BD` está activa como superficie read-only de auditoría:
  - incumplimientos de identidad mensual;
  - componentes faltantes;
  - diferencias YTD;
  - nota/filtro para casos donde el `beginning value` de la cartola actual no coincide con `prev_ending_value` y prevalece el ending value auditado.

### Reglas estables vigentes
- Identidad mensual:
  `valor_final_mes_actual - movimientos_mes - utilidad_mes = valor_final_mes_anterior`
- YTD es **solo control**.
- Si identidad o YTD fallan, el sistema debe alertar; no debe mutar datos silenciosamente para hacerlos cuadrar.
- Las tablas y gráficos del reporting deben leer de la capa normalizada / BD, no del PDF.

### Hardening específico ya incorporado en código
- **Goldman Sachs**:
  - fallback OCR automático para PDFs con texto ilegible/garbled;
  - tolerancia a spacing OCR en extracción de período y overview.
- **JPMorgan ETF / brokerage**:
  - los valores mensuales en blanco ya no toman YTD como si fueran mensual;
  - YTD queda solo como control;
  - en `brokerage`, la utilidad no debe duplicar caja cuando `Change In Investment Value` replica `Net Contributions/Withdrawals`.
- **JPMorgan custody**:
  - `Net Security Contributions` cuenta como parte de movimientos cuando corresponde.
- **UBS Miami**:
  - lectura robusta de `Change in value of accrued interest` aunque el label venga partido en varias líneas.
- **UBS Suiza**:
  - cuando una cartola trae múltiples portafolios, la cuenta `...-01` / `...-02` debe usar el portafolio específico referenciado por ese sufijo;
  - no debe usarse el total combinado si el portafolio puntual es identificable;
  - los `ending value` negativos son válidos y deben persistirse;
  - en vistas UBS puras, si la posición actual o previa es negativa, la rentabilidad mensual mostrada se fuerza a `0%`.

### Reproceso / auditoría relevante ya cerrada
- Se ejecutó un barrido enfocado sobre `UBS Suiza`:
  - auditó `238` filas/documentos candidatos;
  - encontró `33` discrepancias reales entre BD y `selected_portfolio.net_assets`;
  - reprocesó esas `33`;
  - verificación final: `remaining = 0`.

### Estado de raw PDFs
- Los PDFs ya no se consultan en tiempo real para poblar tablas/gráficos.
- Pero **siguen siendo necesarios** para:
  - reprocesos históricos;
  - fixes de parsers;
  - trazabilidad y auditoría.
- Por eso, hoy no es seguro asumir "ya procesado = ya se puede borrar el PDF".

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

tests/             → 151 tests recolectados (150 passing + 1 skipped en baseline reciente)
data/              → Archivos: raw/, cache/, snapshots/, db/
```

### 3.2 Reglas de hierro

1. **La UI NUNCA importa nada de `backend/` ni `parsers/`**. Solo usa `api_client.py`.
2. **Los parsers son islas**: cada parser es un archivo autocontenido. **Aislamiento obligatorio por banco y por tipo de cuenta**: cambios en un motor de lectura PDF no pueden afectar a otros. NO comparten helpers entre bancos distintos (excepción: `goldman_sachs/_gs_common.py` solo entre los 2 parsers GS que usan PyMuPDF).
3. **Idempotencia**: todo upload verifica SHA-256. Mismo archivo = mismo resultado.
4. **Moneda SIEMPRE en `Numeric(20,4)`**, nunca Float.
5. **UTC everywhere**: usar `datetime.now(timezone.utc)`, nunca `datetime.utcnow()`.
6. **Versionado de parsers**: cada parser tiene `VERSION = "X.Y.Z"`. El hash del código fuente se registra en BD para trazabilidad.
7. **`safe_parse()` en vez de `parse()`**: el wrapper automático valida el contrato.
8. **La cartola bancaria (PDF) es la VERDAD** para conciliación mensual. Los Excel son datos operativos diarios.
9. **Auto-detección determinista**: en empate de parsers, gana el orden alfabético por nombre.
10. **Identidad mensual obligatoria**: `valor_final_mes_actual - movimientos_mes - utilidad_mes = valor_final_mes_anterior`.
11. **YTD es solo control**: YTD se usa para auditar la consistencia de los datos mensuales, nunca para sobrescribir ni completar movimientos/utilidad mensuales.
12. **No forzar identidad**: si la identidad mensual o el control YTD fallan, el sistema debe alertar; no debe mutar datos silenciosamente para hacerlos cuadrar.

### 3.3 Reglas de trabajo con el agente (no negociables)

1. **Aislamiento de motores PDF**: Se debe mantener siempre el aislamiento de los motores de lectura de PDF por banco y por tipo de cuenta. Cambios en un parser no pueden afectar a otros.
2. **Motores de lectura PDF ya optimizados**: Los procesos de lectura de PDF están optimizados y funcionando bien. Los datos de reporting deben obtenerse de la capa persistida / normalizada (`monthly_metrics_normalized` como fuente primaria, `monthly_closings` como fallback) y **no** leer directamente los PDF. **Si el agente va a cambiar algo en un motor de parsing (parser), debe avisar primero y no hacer el cambio sin tu OK.**
3. **Instancia de prueba antes de oficial**: Los cambios se hacen en la instancia de prueba (preview); solo después de tu OK pasan a la app oficial.
4. **Reinicio y aviso tras cada cambio**: Después de cada cambio el agente reinicia la app como corresponde y te avisa, o te indica que solo refresques el navegador si eso es suficiente.

---

## 4. BASE DE DATOS — 13 MODELOS

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
| 9 | `monthly_metrics_normalized` | Capa canónica mensual para reporting |
| 10 | `reconciliations` | Resultado de conciliación diaria vs mensual |
| 11 | `validation_logs` | Audit trail completo del sistema |
| 12 | `etf_compositions` | Composición ETFs por instrumento |
| 13 | `cache_metadata` | Control de cache Parquet pre-calculados |

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

### 5.1 PDF parsers (10, versiones activas en código al 2026-03-13)

| Banco | Tipo | Clase | Lib PDF | Notas |
|---|---|---|---|---|
| jpmorgan | etf | `JPMorganEtfParser` (`2.2.0`) | pdfplumber | Multi-cuenta: si >1 cuenta → `account_number="Varios"` |
| jpmorgan | brokerage | `JPMorganBrokerageParser` (`2.1.0`) | pdfplumber | Idem ETF (Consolidated Statement) |
| jpmorgan | custody | `JPMorganCustodyParser` (`2.1.0`) | pdfplumber | Investment Management format |
| jpmorgan | bonds | `JPMorganBondsParser` (`2.0.0`) | pdfplumber | Fixed income con maturity breakdown |
| bbh | custody | `BBHCustodyParser` (`2.1.0`) | pdfplumber | — |
| bice | brokerage | `BICEBrokerageParser` (`2.0.0`) | pdfplumber | — |
| ubs | custody | `UBSSwitzerlandCustodyParser` (`2.3.2`) | pdfplumber | UBS Suiza; portafolio específico por sufijo |
| ubs_miami | custody | `UBSMiamiCustodyParser` (`2.1.2`) | pdfplumber | UBS Miami |
| goldman_sachs | etf | `GoldmanSachsEtfParser` (`2.1.0`) | **PyMuPDF (fitz)** | pdfplumber no puede leer GS; OCR fallback disponible |
| goldman_sachs | custody | `GoldmanSachsCustodyParser` (`2.1.0`) | **PyMuPDF (fitz)** | Comparte `_gs_common.py` |

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

## 8. TESTS — BASELINE RECIENTE `150 passed, 1 skipped`

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

**REGLA**: Cualquier cambio debe mantener suite estable (baseline reciente: `150 passed, 1 skipped`). No borrar tests existentes.

---

## 9. ESTADO ACTUAL Y PENDIENTES

### ✅ Completado
- Arquitectura de reporting consolidada con `monthly_metrics_normalized` como SSOT mensual.
- Endpoints de reporting activos y funcionales (`summary`, `mandates`, `personal`, `etf`) con prioridad de lectura en capa normalizada y fallback controlado a `monthly_closings`.
- `Salud BD` operativo como superficie read-only de auditoría.
- Upload + procesamiento automático de maestro, PDFs y documentos operativos; duplicados controlados por SHA-256.
- `Goldman Sachs` con fallback OCR automático.
- `JPMorgan ETF/brokerage` con YTD solo como control y monthly blanks tratados correctamente.
- `JPMorgan custody` contando `Net Security Contributions` en movimientos cuando aplica.
- `UBS Suiza` endurecido para:
  - portafolio específico por sufijo `-01` / `-02`;
  - negativos válidos;
  - retorno `0%` en vistas UBS puras cuando current/previous position es negativa.
- Reproceso enfocado de `UBS Suiza` completado con `33` discrepancias corregidas y verificación final `remaining = 0`.
- Baseline reciente de suite completa: `150 passed, 1 skipped`.

### 🔲 Pendiente
- **QA visual** post-reproceso, especialmente `UBS Suiza` en `Salud BD`.
- **Respaldo oficial/checkpoint** solo después de aprobación visual de datos.
- **Cobertura de controles YTD por banco**: extender validaciones y alertas por parser.
- **Consolidar backlog de QA funcional UI** (resumen/mandatos/personal) con checklists de negocio.
- **Cache Parquet**: la infraestructura existe pero no se usa aún.
- **Observabilidad**: mejorar métricas/alertas sobre `validation_logs`.
- **Política futura de PDFs raw**: evaluar archivado/retención solo cuando los datos auditados queden estables.

### 🔧 Estado Git
- Worktree con cambios locales en progreso (no asumir limpio/commiteado).
- Antes de cualquier release/respaldo: verificar `git status`, correr tests y luego definir commit/tag.

---

> **Nota de lectura importante**:
> Las subsecciones `9.1` a `9.4` son **historial técnico útil**.
> Pueden contener detalles puntuales válidos, pero **no reemplazan** el estado vigente resumido en `§0` y `§9`.
> Si alguna nota histórica contradice el bloque superior, prevalece `§0` / `§9` / el código actual.

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
  - `movements_ytd`
  - `profit_ytd`
  - `asset_allocation_json`
  - `currency`
- Modelo ORM agregado en `backend/db/models.py`:
  - `MonthlyMetricNormalized`
  - relacion `Account.normalized_monthly_metrics`.
- Migracion Alembic creada:
  - `alembic/versions/20260305_0002_add_monthly_metrics_normalized.py` (creación de tabla)
  - `alembic/versions/20260312_0003_add_ytd_and_asset_alloc_to_normalized.py` (YTD + asset allocation JSON)
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
- La capa API ya no debe depender de helpers que "completen" mensualidades faltantes a partir de identidad o YTD; esa interpretación se fija abajo, en parsing/loading.
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
- `Salud BD` debe auditar valores persistidos; no debe convertirse en un nuevo interpretador financiero mensual.

---

## 9.3 Cierre bloque Eliminar seleccionados (2026-03-10)

**Problema:** En la pestaña Documentos, al marcar una o más filas y pulsar "Eliminar seleccionados" no ocurría nada ni se mostraba mensaje en verde.

**Causa:** Los IDs a eliminar se obtenían por alineación de índices entre `edited_df` y `df_docs` (`df_docs.iloc[selected_indices]["id"]`). Con filtros, orden o estado del data_editor, los índices podían no coincidir con las filas mostradas, dando `selected_ids` vacío o incorrecto. Además las excepciones del DELETE se tragaban con `except: pass`.

**Solución (frontend/pages/upload.py):**
- Obtener los IDs desde la columna **"ID"** del mismo dataframe que devuelve el data_editor (`edited_df.loc[selected_mask, "ID"].tolist()`), garantizando que se eliminan exactamente las filas que el usuario ve seleccionadas.
- Mostrar `st.success("✅ Su selección ha sido eliminada.")` cuando se elimina al menos un documento; luego `st.rerun()`.
- Mostrar `st.error` por cada fallo del DELETE (ya no `pass`).
- Mostrar `st.warning` si había filas seleccionadas pero no se eliminó ninguna y no hubo errores.

No se modificó ninguna regla de arquitectura; solo UX y robustez del flujo de borrado.

---

## 9.4 JPMorgan Brokerage - caja / YTD / accruals (2026-03-11)

### Problema detectado

En varias cartolas `JPMorgan brokerage` con perfil de caja o actividad muy simple se observaron dos layouts/problematicas:

1. **Fila mensual en blanco con YTD visible**:
   - la cartola muestra dos columnas (`Current Period Value`, `Year-to-Date Value`);
   - en algunas filas el valor mensual viene visualmente en blanco, pero el YTD sí viene poblado;
   - el parser anterior capturaba ese único número como si fuera mensual.

2. **`Change In Investment Value` duplicando caja**:
   - en ciertos meses la fila `Change In Investment Value` trae exactamente el mismo monto que `Net Contributions/Withdrawals` (misma magnitud, signo opuesto o mismo número visual);
   - si se suma a utilidad, se duplica conceptualmente un movimiento de caja y se infla `profit`.

### Regla funcional adoptada

Para `parsers.jpmorgan.brokerage`:

1. **Si una fila de Portfolio Activity trae un solo monto, se interpreta como YTD**.
   - El valor mensual en blanco se trata como `0`.
   - El YTD se conserva solo como dato de control/trazabilidad.

2. **`account_ytd` no se usa para rellenar mensual en brokerage**.
   - Puede servir para control/auditoría.
   - No debe completar `income` ni `change_investment` mensuales.

3. **`Change In Investment Value` se excluye de utilidad cuando duplica caja**.
   - Heurística: si `abs(Change In Investment Value) == abs(Net Contributions/Withdrawals)` y no es cero, se considera duplicación operativa de caja.
   - En ese caso, se deja fuera del cálculo de utilidad mensual.

4. **Fórmula mensual para utilidad en estos casos**:
   - base: `Income & Distributions mensual`
   - más: `delta_accrual = accrual_ending - accrual_beginning`
   - más: `Change In Investment Value` **solo si no duplica caja**

5. **Trazabilidad**:
   - cuando se aplica alguna heurística, se guarda una nota en `qualitative_data.account_monthly_activity[].interpretation_notes`;
   - el loader registra la nota en `validation_logs`.

### Casos observados

- **Armel Canada (`5000`)**
  - `2025-05`: `Change In Investment Value = 5,894.95` duplica en magnitud a `Net Contributions/Withdrawals = (5,894.95)`.
  - La utilidad correcta pasa a ser `12.11 + (12.40 - 12.06) = 12.45`.
  - `2025-02`: no aparece la línea `Change In Investment Value`; la utilidad se interpreta con `Income & Distributions + delta accruals`.

- **La Guardia (`1008`)**
  - `2025-11` y `2025-12`: `Income & Distributions` mensual viene en blanco y solo aparece el YTD (`5.91`).
  - Regla: mensual = `0`, no `5.91`.

- **Mi Investments (`1000`)**
  - mismo patrón de YTD tomado erróneamente como mensual en varios meses recientes;
  - además hay meses donde `Change In Investment Value` replica caja y debe excluirse de utilidad.

- **Ecoterra RE (`2008`)**
  - comparte el patrón `brokerage` con actividad simple/caja dominante;
  - aplicar la misma interpretación: mensual real + delta accruals; no llenar mensual desde YTD.

### Ecoterra Internacional JPMorgan Bonds - histórico pendiente

Revisión de `parsed_statements.parsed_data_json` para `1530900` y `1531100` mostró:

- En varios meses históricos (`2024-12` a `2025-03`, y análogos anteriores) sí existe `portfolio_activity` con:
  - `net_cash_contributions.current_period`
  - `income_distributions.current_period`
  - `change_investment.current_period`
  - `ending_market_value.current_period`
- Sin embargo, en `monthly_closings` muchos de esos meses siguen con `change_in_value = None` e `income = None`.

Conclusión:

- el problema histórico pendiente de `Ecoterra Internacional` para JPM `bonds` no es que falte la materia prima en el parseo;
- el dato existe en `parsed_data_json` y la corrección requiere **reproceso dirigido** con la lógica nueva del loader;
- en la BD consultada no apareció una cuenta `Ecoterra Internacional / jpmorgan / mandato`, ni documentos `Ect Intl ... Mandato` en `raw_documents`, por lo que el bloque pendiente identificado en esta revisión queda acotado principalmente a `bonds` (`0900`, `1100`) salvo que existan documentos fuera de esta BD.

---

## 10. PROTOCOLO DE OPERACIÓN — REGLA FUNDAMENTAL

Ver también **§3.3 Reglas de trabajo con el agente** (aislamiento parsers, no tocar motores sin OK, cambios en instancia de prueba, reinicio/aviso).

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
3. Corre los tests: `python -m pytest tests/ -x -q` — deben dar suite verde (referencia reciente: `150 passed, 1 skipped`).

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
| `backend/db/models.py` | 13 modelos ORM + enums/tablas de soporte | ~700 |
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
