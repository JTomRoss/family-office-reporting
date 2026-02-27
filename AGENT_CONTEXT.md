# AGENT_CONTEXT — Family Office Reporting System

> **Propósito**: Este archivo es el SSOT de contexto para cualquier agente AI que trabaje en este proyecto. Léelo COMPLETO antes de hacer cualquier cambio.
> **Última actualización**: 2026-02-27

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
- **Backend**: puerto **8000** → `python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload`
- **Frontend**: puerto **8501** → `python -m streamlit run frontend/app.py --server.port 8501`
- **Tests**: `python -m pytest tests/ -x -q`
- **Git**: repo `JTomRoss/family-office-reporting` (privado), branch `master`

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
  services/        → document_service, account_service, cache_service
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

tests/             → 93 tests (unit + contracts + parsers + calculations)
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
UI (PDFs tab) → POST /documents/upload
  → Metadata manual: banco, cuenta, sociedad, tipo cuenta,
    moneda, portafolio/personal, nombre persona, código interno
  → Auto-fill disponible: GET /accounts/{account_number}/auto-fill
  → Soporte multi-cuenta (checkbox "Varios" + sub-cuentas)
  → Detección de duplicados con opciones: Reclasificar / Omitir
  → DocumentService.upload_document() → RawDocument en BD
  → (procesamiento posterior vía POST /documents/{id}/process)
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
| POST | /data/summary | **STUB** — datos pestaña Resumen |
| POST | /data/mandates | **STUB** — datos pestaña Mandatos |
| POST | /data/etf | **STUB** — datos pestaña ETF |
| POST | /data/personal | **STUB** — datos pestaña Personal |

---

## 7. FRONTEND — 7 PÁGINAS

| Página | Archivo | Estado |
|---|---|---|
| 🏠 Inicio | `pages/home.py` | Funcional |
| 📁 Carga | `pages/upload.py` | **Funcional** — 3 tabs: PDFs, Excel, Docs cargados |
| 📋 Resumen | `pages/summary.py` | Scaffold (espera /data/summary) |
| 📑 Mandatos | `pages/mandates.py` | Scaffold (espera /data/mandates) |
| 📈 ETF | `pages/etf.py` | Scaffold (espera /data/etf) |
| 👤 Personal | `pages/personal.py` | Scaffold (espera /data/personal) |
| ⚙️ Operacional | `pages/operational.py` | Scaffold |

### Campos de la página de carga PDF:
- Tipo de documento (cartola / reporte)
- Banco * (jpmorgan, ubs, ubs_miami, goldman_sachs, bbh, bice)
- Número de cuenta * (o checkbox "Varios" + sub-cuentas para multi-cuenta)
- Botón Auto-llenar (desde maestro)
- Sociedad / Nombre entidad *
- Código interno
- Tipo de cuenta * (custody, current, savings, investment, etf)
- Moneda * (USD, EUR, CHF, CLP, etc.)
- Portafolio o Personal * (sociedad / persona)
- Nombre persona * (condicional: solo si es "persona")
- **Año y mes NO se piden** — el parser los extrae del PDF

---

## 8. TESTS — 93 PASSING

| Archivo | Tests | Qué valida |
|---|---|---|
| test_allocation.py | 11 | Cálculos de asset allocation |
| test_api.py | 13 | API schemas y endpoints |
| test_json_schemas.py | 16 | JSON schemas de BD |
| test_parser_contracts.py | 8 | Contrato BaseParser |
| test_parsers.py | 12 | Parsers individuales |
| test_profit.py | 25 | Cálculos de profit/return |
| test_reconciliation.py | 8 | Conciliación |

**REGLA**: Cualquier cambio debe mantener 93+ tests passing. No borrar tests existentes.

---

## 9. ESTADO ACTUAL Y PENDIENTES

### ✅ Completado
- Scaffolding completo (40+ archivos)
- 12 hardening fixes (audit completo)
- 14 parsers funcionales (10 PDF v2.0.0 + 4 Excel v1.0.0)
- Goldman Sachs resuelto con PyMuPDF fallback
- Página de carga con todos los campos de clasificación
- Auto-fill desde maestro de cuentas
- Multi-cuenta ("Varios") con sub-cuentas
- Upload + proceso automático del maestro
- Detección de duplicados con interacción de usuario (Reclasificar/Omitir)
- Tabla maestro visible tras carga de Excel
- Eliminación de documentos con cascade correcto

### 🔲 Pendiente
- **Endpoints `/data/*` son STUBS** — necesitan implementación real
- **Proceso de cartolas PDF** — los PDFs se suben pero el flujo completo (parse → monthly_closings → reconciliación) no está conectado end-to-end
- **Cargas masivas Excel** (posiciones, movimientos, precios) → no se alimentan las tablas diarias
- **Cálculos de profit, allocation, reconciliación** — la lógica existe en `calculations/` pero no está wired a los endpoints
- **Dashboards** — las páginas de Resumen, Mandatos, ETF, Personal están en scaffold
- **Cache Parquet** — la infraestructura existe pero no se usa aún
- **Alembic** — configurado pero sin migraciones ejecutadas formalmente

### 🔧 Cambios no commiteados
Los siguientes archivos tienen cambios desde el último commit (`c0215c9`):
- `backend/db/models.py` — cascade en relationships
- `backend/routers/documents.py` — nuevos campos, reclassify, upload-and-process
- `backend/schemas.py` — existing_metadata en DocumentUploadResponse
- `backend/services/document_service.py` — proceso maestro, fix parser lookup
- `frontend/pages/upload.py` — rediseño completo con todos los campos
- `parsers/base.py` — account_numbers list, contract validation
- `parsers/jpmorgan/brokerage.py` — multi-cuenta Varios
- `parsers/jpmorgan/etf.py` — multi-cuenta Varios

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
3. Corre los tests: `python -m pytest tests/ -x -q` — deben dar 93+ passed.

**DESPUÉS de cada cambio:**
1. Corre los tests para verificar que no rompiste nada.
2. **Detener con**: `.\scripts\stop.ps1`
3. **Esperar** 2 segundos.
4. **Levantar con**: `.\scripts\start.ps1`
5. *NO HAY ALTERNATIVA*. No uses comandos manuales de uvicorn ni streamlit.
6. Indica al usuario que **refresque el navegador**.

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
| `backend/services/document_service.py` | Upload, process, list, delete, reclassify | ~360 |
| `backend/services/account_service.py` | Maestro: upsert, auto-fill, filter options | ~160 |
| `backend/routers/documents.py` | CRUD documentos + upload-and-process | ~190 |
| `backend/routers/data.py` | **STUBS** — summary, mandates, etf, personal | ~160 |
| `frontend/pages/upload.py` | Página de carga (3 tabs) | ~450 |
| `frontend/api_client.py` | HTTP client UI→Backend | ~68 |
| `parsers/base.py` | BaseParser ABC + ParseResult + ParsedRow | ~255 |
| `parsers/registry.py` | Auto-discovery de plugins | ~193 |
| `parsers/excel/master_accounts.py` | Parser maestro cuentas | ~147 |
| `pyproject.toml` | Deps + config pytest/ruff | ~65 |
