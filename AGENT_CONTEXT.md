# AGENT_CONTEXT - Quick Context (SSOT Lite)

Last updated: 2026-04-24 (Reporting APP nueva + endpoints /api/v1/{master,dictionary,reporting,parity,quality,sources} + filtro Ámbito Internacional/BICE con bice_currency CLP/USD)

## 1) Scope
Internal financial reporting system for a family office.
Stack: FastAPI + Streamlit + HTML/CSS/JS vanilla (frontend nuevo) + SQLAlchemy + SQLite + Alembic + pytest.

This file is intentionally short. Long historical detail should live in `DEEP_CONTEXT.md`.

## 2) Startup Protocol (mandatory)
In new chats, load only:
1. `AGENT_CONTEXT.md`
2. `SESSION_STATE.md`
3. `git status --short` and only relevant files

Do not scan the full repository unless needed.

## 3) Runtime / Ops Rules
- Use scripts only, never manual uvicorn/streamlit commands.
- Main app (backend + Streamlit legacy):
  - Start: `./scripts/start.ps1`
  - Stop: `./scripts/stop.ps1`
  - Ports: backend `8000`, Streamlit frontend `8501`
- Reporting APP nueva (frontend HTML/CSS/JS vanilla, sin build):
  - Start: `./scripts/start_new_frontend.ps1`
  - Stop: `./scripts/stop_new_frontend.ps1`
  - Port: `8701` (HTML estático servido con `python -m http.server`)
  - Root: `Reporting APP/` (index.html + assets/{mock.js, api.js, dataSource.js, app.js, styles.css})
- Preview app:
  - Start: `./scripts/start_preview.ps1`
  - Stop: `./scripts/stop_preview.ps1`
  - Sync DB: `./scripts/sync_preview_db.ps1`
  - Ports: backend `8100`, Streamlit `8601`
- Never run with `--reload`.
- **Port guard** en `start.ps1`: detecta procesos ajenos al FO en 8000/8501 (ej. otros proyectos Claude Code corriendo uvicorn simultáneo) y los mata antes de arrancar, para evitar conflictos de puerto silenciosos.

## 4) Architecture Guardrails
- `frontend/` (Streamlit) is presentation only. No business logic.
- `Reporting APP/` (HTML/CSS/JS vanilla) is presentation only. No business logic. Consume REST via `assets/api.js` → `window.API`.
- Frontend Streamlit accesses backend only via `frontend/api_client.py`.
- `backend/` owns business logic and API endpoints.
- `parsers/` are isolated plugins by bank and account type.
- Monetary DB persistence must use `Numeric(20,4)` (no float persistence).
- Use timezone-aware UTC datetimes.
- Reporting endpoints are read-only consumers of persisted normalized data.
- Reporting surfaces must not infer missing financial data at runtime from raw payloads.

## 4.1) Normalized tables (SSOT para reporting) — 3 tablas, 2 mundos
El frontend (Streamlit y Reporting APP) lee SOLO de estas tablas. Nunca recalcula cartola en runtime.

### Mundo Internacional (USD)
| Tabla | Rol | Qué guarda |
|---|---|---|
| `monthly_metrics_normalized` | **SSOT primaria** | Fila por (account_id, year, month) con 6 campos canónicos §1: `ending_value_with_accrual`, `ending_value_without_accrual`, `accrual_ending`, `cash_value`, `movements_net`, `profit_period` + `asset_allocation_json`. Poblada por `DataLoadingService` después de cada parser. |
| `monthly_closings` | **Fallback histórico auditable** | Tabla original pre-normalized. Usada cuando `monthly_metrics_normalized` no tiene fila para una cuenta-mes. Contiene `net_value`, `total_assets`, `income`, `change_in_value`, `asset_allocation_json`. |

Cubre: JPM (brokerage, bonds, custody, ETF, mandato), UBS Suiza, UBS Miami, Goldman Sachs, BBH, Wellington. Todas USD.

### Mundo Nacional (CLP + USD separados)
| Tabla | Rol | Qué guarda |
|---|---|---|
| `bice_monthly_snapshot` | **SSOT dedicada BICE** | Saldos y movimientos con columnas paralelas CLP / USD: `ending_*`, `caja_*`, `renta_fija_*`, `equities_*`, `aportes_*`, `retiros_*`, `dividendos_*`, `profit_*`. Los dos mundos son independientes: **no se convierten ni mezclan** entre sí (no hay FX oficial). |

Cubre: BICE Inversiones Corredores de Bolsa (`bice_inversiones`) + BICE Asesorías / Altos Patrimonios (`bice_asesorias`).

### Regla de consumo (no negociable)
1. Routers para scope=international leen primero `monthly_metrics_normalized`; si falta → fallback a `monthly_closings`.
2. Routers para scope=national leen `bice_monthly_snapshot` con query param `bice_currency` ∈ {CLP, USD}.
3. UI nunca hace cálculos financieros; solo renderiza.

## 4.2) Endpoints del frontend nuevo (Reporting APP)
Read-only. No tocan `data.py` (usado por Streamlit legacy).

| Endpoint | Descripción |
|---|---|
| `GET /api/v1/master/{accounts,societies,banks,parsers}` | Maestro de referencia. |
| `GET /api/v1/dictionary/{buckets,etf,mandates}` | Diccionarios canónicos §6. |
| `GET /api/v1/reporting/dashboard?period=&scope=&bice_currency=` | KPIs + serie 13m + allocation por bucket §6.1 + by-society/bank/currency. |
| `GET /api/v1/reporting/positions?period=` | Foto de posiciones diarias (hoy vacío; pendiente Excel diario). |
| `GET /api/v1/reporting/normalized?period=&scope=&bice_currency=` | Tabla canónica para auditoría. Fila por cuenta×mes. |
| `GET /api/v1/reporting/returns?period=&scope=` | TWR consolidado + por sociedad. Scope=intl only (v1). |
| `GET /api/v1/reporting/alternatives?period=` | PE/RE/VC desde cuentas con `bank_code='alternativos'`. |
| `GET /api/v1/reporting/audit-log?limit=` | `ValidationLog` transformado para UI. |
| `GET /api/v1/reporting/files?limit=&bank_code=&status=&file_type=` | Lista de `raw_documents` procesados. |
| `GET /api/v1/reporting/coverage?months=&scope=` | Matriz cuenta × mes. Celda cubierta si existe fila en normalized/closings/bice_snapshot. |
| `GET /api/v1/quality/alerts?period=&scope=&limit=` | Alertas combinadas (ValidationLog + heurísticas on-the-fly). |
| `GET /api/v1/sources/{document_id}` | Metadata de doc fuente para drawer "Ver fuente" (trazabilidad §1.5). |
| `GET /api/v1/parity/dashboard?period=&scope=&tolerance_usd=` | Auditor de paridad: compara `reporting/dashboard` contra lectura independiente de la BD. Aplica `__reporting_value_exclusion` §5.4. |

## 4.3) Filtro Ámbito (frontend nuevo)
- `state.filters.scope` ∈ {`international`, `national`} → chip "Ámbito" en filter bar (primer chip, azul).
- `international` (default): USD, cuentas no-BICE.
- `national`: BICE con toggle USD/CLP dentro del Dashboard (botones abajo del tab "Por entidad"). Persistido en `localStorage` como `ecoterra.scope` y `ecoterra.biceCcy`.
- Cambiar scope o biceCurrency dispara `window.swapMockWithScope(scope, ccy)` que re-fetch completo al backend y repinta.
- Los demás filtros (sociedad, banco, cuenta, tipo, persona) filtran en memoria tras apretar **Aplicar**.

## 5) Data / Reporting Rules
- Bank statements (`pdf_cartola`) are auditable source of monthly closing values.
- Loader path: parser output -> `DataLoadingService` -> reporting tables.
- `monthly_metrics_normalized` is SSOT for monthly reporting.
- `monthly_closings` is historical source + allowed fallback only.
- If normalized data exists, normalized wins over fallback.
- `Alternativos.xlsx` is an independent source and loads only into `monthly_metrics_normalized`.
- Alternativos parser excludes any non-USD column (EUR, GBP, etc.) that has a USD counterpart with the same nemo+entity; this prevents ghost accounts and double-counting.
- Alternativos loader reads `Documentos/Excel/Excel Cuentas Contables.xlsx` to resolve `person_name` per entity (fuzzy match, cutoff 0.85) — no code change needed when new persons are added, only update the Excel.
- `excel_alternatives` upload always reprocesses (never blocks as duplicate), and auto-deletes prior raw_documents for `bank_code=alternativos` after successful load.
- Identity control is mandatory: `ending_current - movements - profit = ending_previous`.
- YTD is control-only, never used to auto-fill monthly movements or profit.
- If identity/YTD controls fail, reporting must alert and must not mutate persisted values.
- Raw PDFs remain operationally required for reprocess, parser fixes, and traceability.

## 5.1) Mandato Contract (stable)
- PDF engines must stay strictly isolated by bank, account type, and mandate report parser:
  - `parsers/*/report_mandato.py` per bank.
  - No cross-bank shared mandate-report engine.
- Mandato macro USD totals come only from cartola:
  - `Cash, Deposits & Money Market`
  - `Fixed Income`
  - `Equities`
- `report_mandato` must never overwrite mandate macro USD totals.
- `report_mandato` parsers may emit only complement data:
  - `Investment Grade Fixed Income`
  - `High Yield Fixed Income`
  - `US Equities`
  - `Non US Equities`
  - `Global Equity` (if present)
  - FI metrics (`duration`, `yield`)
- Split rules are centralized in backend merge/normalization:
  - `Fixed Income = IG + HY`
  - `Equities = US + Non-US`
  - `Global Equity` split: `2/3 US + 1/3 Non-US`
  - `Emerging FI` contributes to HY where bank rule says so (UBS Miami)
  - `Alternativos = Private Equity + Real Estate`
- Reprocessing a mandate cartola must refresh audited macros while preserving valid report enrichments for that month.
- **Salvaguarda permanente en `_upsert_monthly_closing`**: si el documento entrante es `pdf_report` y el registro existente ya tiene `net_value IS NOT NULL`, los campos financieros (`net_value`, `total_assets`, `income`, `change_in_value`, `accrual`, `source_document_id`) se preservan del registro existente. Solo `asset_allocation_json` puede enriquecerse con los sub-splits del reporte. Esta misma protección aplica al `source_document_id` en `monthly_metrics_normalized`.

## 5.2) Canonical Breakdown for Detail View
`Detalle > Detalle por Activo` must use canonical categories:
- `Cash`
- `IG Fixed income`
- `HY Fixed income`
- `US equities`
- `Non-US equities`
- `PE`
- `RE`
- `Other investments`

Mapping stays centralized in backend/taxonomy (not in frontend).

## 5.3) Wellington Rules
- `parsers/wellington/custody.py` (`WellingtonCustodyParser` v1.0.0): multi-fund PDF, sums `Closing Balance` across all pages via regex on plain text (pdfplumber returns no tables for this format).
- Detection normalizes to lowercase+no-spaces to handle pdfplumber word-collapsing.
- Account must exist in master accounts (Excel Cuentas Contables, `banco=wellington`) before loading cartolas.
- Completely isolated: no shared code with any other parser.

## 5.3c) BICE Rules (stable)
- **Parser isolation**: `parsers/bice/brokerage.py` (BICE Inversiones) and `parsers/bice_asesorias/wealth_management.py` (BICE Asesorías) are completely isolated — no shared code, no cross-imports.
- **Never modify** `parsers/bice/brokerage.py` without explicit user approval (DAP vencimiento issue is unresolved, stand-by).
- **BICE Asesorías parser v1.2.0** (`BANK_CODE = "bice_asesorias"`, `ACCOUNT_TYPE = "wealth_management"`):
  - Aportes/retiros source: table "FLUJO PATRIMONIAL (Últimos Movimientos)" on page 2. Filter only rows whose date falls within the statement month/year.
  - Individual flow rows exposed as `qualitative_data["transactions"]` (format: `fecha`, `operacion`, `instrumento`, `monto`, `monto_raw`, `moneda`, `categoria_auto`).
  - Page 13 transactions retained in `qualitative_data["transactions_p13"]` as reference only (not used for totals).
  - Account identifier format: `C0000-XXXX` (extracted from cover page).
- **`scripts/reprocess_bice.py`**: only deletes/recreates snapshots for the `bank_code` being reprocessed (`bice`/`bice_inversiones`). Does NOT touch other bank snapshots (e.g. `bice_asesorias`). Each bank's reprocess is independent.
- **BICE snapshot storage**: both BICE Inversiones and BICE Asesorías use `bice_monthly_snapshot` (shared table). Reprocess scripts must filter by `account_id IN (accounts of target bank_code)`, never delete all rows blindly.

## 5.3b) Stable Bank Rules (selected)
- JPM `brokerage/etf`: blank current-period values remain monthly `0`/`None`; YTD remains control-only.
- JPM `brokerage` (v2.1.3): skip Table-of-Contents pages that reference "Portfolio Activity" as a page number (detected by absence of real data markers: `Ending Market Value`, `Ending Cash Balance`, etc.). Cash-only accounts (e.g. E74997009) use "Ending Cash Balance" as the ending value and have no `net_contributions`/`income_distributions`; parser returns data if any ending value is found.
- JPM `custody`: `Net Security Contributions` counts in monthly movements when present.
- JPM bonds account `1531100`:
  - cash in `Cash, Deposits & Short Term` comes from `Total Cash Holdings` only.
  - `Short Term Investments` contributes to `Fixed Income` (IG) for canonical breakdown.
- JPM `report_mandato`:
  - `Investment Review` contributes only `US Equities` / `Non US Equities`.
  - `Complementario` contributes `Investment Grade Fixed Income` / `High Yield Fixed Income` + FI metrics.
  - If the JPM mandate report resolves to `Varios`, apply it across sibling mandato accounts that share the same monthly cartola source document.
- ETF taxonomy compatibility aliases must include `NON-US EQUITY`/`NON US EQUITY` and `Emerging Market Equities` variants as `RV EM`.
- GS: OCR fallback is parser-level backup only.
- GS legacy custody wraps may place the primary `Overview` on page 4+; parser must detect
  the audited overview dynamically and must not double-count the main overview as a sub-portfolio.
- GS Telmar legacy cartolas can expose `Other Investments` / `Hedge Funds` / `Miscellaneous`;
  preserve them in normalized payload as reporting category `Other Investments` and exclude only
  explicit duplicated `Private Equity` from reporting totals.
- UBS Suiza: selected portfolio value prevails over combined relation total.
- UBS Suiza `Total assets`: ignore chart-axis noise (`10/0/-10`) and keep the explicit `Total`
  column when rows compress as `market / accrued / total`.
- UBS Suiza only: previous audited month-end ending prevails over next beginning mismatch; `profit` absorbs identity adjustment.
- Document reclassification must reconcile `raw_documents.account_id`, purge derived outputs, and reprocess immediately.

## 5.4) Reporting Exclusion Rules (dedupe vs Alternativos)
- Keep auditable raw month-end values in `monthly_closings`.
- Apply reporting-only exclusions in `monthly_metrics_normalized.asset_allocation_json` under
  `__reporting_value_exclusion` (no frontend logic).
- Current stable exclusions:
  - `Telmar | goldman_sachs | mandato | 097-4`: exclude Private Equity duplicated in `Alternativos.xlsx`.
  - `Telmar | jpmorgan | brokerage | B43459001`: exclude Alternative Assets duplicated in `Alternativos.xlsx`.
- All reporting totals must consume this exclusion consistently via backend helpers.

## 5.5) Temas Pendientes (stand-by)

- **Mi Investments UBS Suiza — NAVs inconsistentes (pendiente resolución con banco)**:
  Las cartolas de `Mi Investments | ubs | brokerage | 206-579943-01` para varios meses históricos
  reportan movimientos que no cuadran con los NAVs mensuales. Los datos se están leyendo
  correctamente por el parser; el problema es de calidad de la información en las cartolas
  originales del banco. En stand-by hasta que el banco proporcione cartolas corregidas o
  aclaraciones. No hacer cambios al parser ni a la BD para esta cuenta hasta resolver con el banco.

- **UBS Miami Boatview — Cartolas custodia 2021-04 a 2023-09 pendientes de carga**:
  La cuenta P2 (3J 00432 P2, `account_id=77`) no tiene cartolas de custodia para ese período.
  Los archivos "BOATVIEW LIMITED - fecha.pdf" ya cargados son **performance reviews** (pdf_report),
  no cartolas mensuales — son leídos por `report_mandato.py`, no por `custody.py`.
  El usuario debe localizar y subir las cartolas mensuales de custodia (pdf_cartola) para ese período.

- **Isabel Izquierdo — Saldos 2022 (Jan-Oct)**:
  Confirmado por el usuario como correcto (período anterior al traspaso a JPM). No requiere acción.

## 6) Key Paths
- Backend entrypoint: `backend/main.py`
- Reporting router: `backend/routers/data.py`
- Audit router: `backend/routers/audit.py`
- Loader: `backend/services/data_loading_service.py`
- Document processing: `backend/services/document_service.py`
- DB models: `backend/db/models.py`
- Taxonomy: `asset_taxonomy.py`, `asset_bucket_dictionary.json`, `mandate_taxonomy.py`, `mandate_report_dictionary.json`
- Frontend entrypoint: `frontend/app.py`

## 7) Current Baseline
- Worktree can be dirty; do not assume clean state.
- Run targeted tests for touched modules; run broader suite when needed.

## 8) Context File Policy
- `AGENT_CONTEXT.md`: stable architecture/rules only.
- `SESSION_STATE.md`: current block status, recent decisions, next actions.
- `DEEP_CONTEXT.md`: long-form historical memory.

## 9) Update Rules
After major work:
- Update `SESSION_STATE.md` with what changed now.
- Update `AGENT_CONTEXT.md` only when a stable rule changes.
- Update `DEEP_CONTEXT.md` only for deep historical notes.
