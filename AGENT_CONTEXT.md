# AGENT_CONTEXT - Quick Context (SSOT Lite)

Last updated: 2026-03-24 (JPM/BBH reclassification consistency for duplicate mandate reports)

## 1) Scope
Internal financial reporting system for a family office.
Stack: FastAPI + Streamlit + SQLAlchemy + SQLite + Alembic + pytest.

This file is intentionally short. Detailed history and deep notes live in `DEEP_CONTEXT.md`.

## 2) Startup Protocol (mandatory for new chats)
Only load these first:
1. `AGENT_CONTEXT.md`
2. `SESSION_STATE.md`
3. `git status --short` and only relevant changed files

Do NOT read the full repo or `DEEP_CONTEXT.md` unless explicitly needed.

## 3) Runtime / Ops Rules
- Use scripts, never manual uvicorn/streamlit commands.
- Main app:
  - Start: `./scripts/start.ps1`
  - Stop: `./scripts/stop.ps1`
  - Ports: backend `8000`, frontend `8501`
- Preview app:
  - Start: `./scripts/start_preview.ps1`
  - Stop: `./scripts/stop_preview.ps1`
  - Sync DB: `./scripts/sync_preview_db.ps1`
  - Ports: backend `8100`, frontend `8601`
- Never use `--reload`.

## 4) Architecture Guardrails
- `frontend/` is presentation only. No business logic.
- UI must call backend only through `frontend/api_client.py`.
- `backend/` contains business logic and API endpoints.
- `parsers/` are isolated plugins (one parser file per bank/account type).
- Monetary values in DB must be `Numeric(20,4)` (no float persistence).
- Use timezone-aware UTC datetimes.
- Reporting endpoints must be read-only consumers of normalized data; they must not "complete" missing data by reinterpreting raw payloads.
- Reporting views such as `Detalle`, `Mandatos` and `ETF` must consume backend-prepared payloads. Frontend must not aggregate, normalize or infer financial metrics on its own.

## 5) Data / Reporting Rules
- Bank statements (PDF cartolas) are truth for monthly closings.
- Parser output loads via `DataLoadingService` into reporting tables.
- `monthly_metrics_normalized` is the canonical monthly reporting layer.
- Tables and charts in reporting (`Summary`, `Mandates`, `ETF`, `Personal`) must read monthly values from `monthly_metrics_normalized`.
- `monthly_closings` is historical source + fallback; if normalized data exists, it wins.
- `Alternativos.xlsx` is an independent Excel statement source. It loads only into `monthly_metrics_normalized` and is exposed in reporting as synthetic bank `alternativos` / `Alternativos`.
- Identity control is mandatory: `ending_current - movements - profit = ending_previous`.
- YTD is control-only. Never use YTD to auto-fill, overwrite, or "force" monthly movements/profit.
- If identity or YTD controls fail, reporting must alert; it must not silently mutate values to make them match.
- `Salud BD` is an audit/read-only surface: it should alert from normalized/historical persisted values, not create a third interpreter of monthly data.
- Operacional has two tabs: Salud BD and Revisión. The Revisión tab runs an **LLM-only** audit: PDF text is sent to OpenAI with **engine rules** (bank/account-type context) so extraction aligns with parser/normalization logic; results are compared to `monthly_metrics_normalized`. Read-only, isolated under `backend/services/audit/`, runs only when the user clicks Revisar; requires `OPENAI_API_KEY`. It does not modify the database or loading flows.
- Raw PDFs are still operationally important for reprocesos, parser fixes, and audit traceability; do not assume processed PDFs can be deleted safely.
- ETF / active-class taxonomy is centralized in `asset_taxonomy.py`: the official instrument dictionary now comes from `Documentos/Excel/Diccionario de instrumentos.xlsx`, while `asset_bucket_dictionary.json` remains only for visual order/colors. Do not create local copies of that mapping in pages or routers.
- `Alternativos` must reuse canonical society names already used across the app (`Ecoterra Internacional`, `Ecoterra RE`, `Ecoterra RE II`, `Ecoterra RE III`) so consolidated scopes/presets match across banks.

## 5.1) Stable Parser / Bank Rules Already In Code
- JPMorgan `brokerage` / `etf`: blank current-period fields stay `0`/`None` as monthly values; YTD remains control-only.
- JPMorgan `brokerage` / `etf`: if old statements omit `asset_allocation` but holdings include cash-like sweep / money-market rows, `DataLoadingService` may derive `cash_value` into `monthly_metrics_normalized` during normalization/backfill. Reporting must not read `ParsedStatement` to infer cash.
- JPMorgan `brokerage`: `asset_allocation_json` must be bucketized from persisted holdings + centralized taxonomy inside loader/normalization (not in parser, not in frontend) so `Detalle > Tipo de activos` can read normalized/fallback persisted buckets.
- JPMorgan `custody`: `Net Security Contributions` must count as part of monthly movements when present.
- JPMorgan `bonds` (`1531100` Ecoterra Internacional): in `Cash, Deposits & Short Term`, cash must come from `Total Cash Holdings` only (exclude `Short Term Investments` from cash). This is isolated to that account parser case.
- Goldman Sachs: automatic OCR fallback exists for garbled PDFs; OCR is a parser-level backup, not a UI concern.
- UBS Suiza: for multi-portfolio statements, the account suffix (`-01`, `-02`) selects the portfolio-specific `net_assets`; never use the combined total when a specific portfolio is identifiable.
- UBS Suiza: negative positions are valid; in UBS-only reporting views, monthly return shown to the user is forced to `0%` when current or previous position is negative.
- UBS Suiza only: the previous audited month-end `ending value` prevails over the next statement's `beginning value` when they differ. Quarterly performance tables may refine prior-month `movements`, but must not overwrite auditable month-end balances. `profit` is the adjustment variable and must be recomputed from identity against the previous audited ending.
- Document reclassification rule: when bank/account classification changes, `raw_documents.account_id` must be reconciled with `bank_code`, document-derived outputs (`monthly_closings`, `monthly_metrics_normalized`, `etf_compositions`, `parsed_statements`, related `reconciliations`) must be purged, and the document must be reprocesado immediately (no stale `uploaded` state left by UI flow).

## 6) Key Paths
- Backend entrypoint: `backend/main.py`
- Reporting router: `backend/routers/data.py`
- Audit Revisión router: `backend/routers/audit.py` (POST `/data/audit-revision-run`)
- Loader: `backend/services/data_loading_service.py`
- Document processing: `backend/services/document_service.py`
- DB models: `backend/db/models.py`
- Asset taxonomy: `asset_bucket_dictionary.json`, `asset_taxonomy.py`
- Frontend entrypoint: `frontend/app.py`
- Main pages: `frontend/pages/summary.py`, `mandates.py`, `etf.py`, `personal.py`, `upload.py`
- Current backup metadata: `LATEST_VALID_BACKUP.txt`

## 7) Current Baseline
- Latest known full-suite checkpoint in this line of work: `186 passed, 1 skipped`.
- Current worktree may be ahead of that checkpoint with additional UI/reporting changes; run targeted/full tests as needed.
- Worktree can be dirty; do not assume clean state.

## 8) Context File Policy
- `AGENT_CONTEXT.md`: stable rules and quick architecture (this file).
- `SESSION_STATE.md`: current sprint/session state, decisions, next tasks.
- `DEEP_CONTEXT.md`: full historical context, long notes, old iterations.

## 9) Update Rules
After major work, update only:
- `SESSION_STATE.md` (what changed now)
- `AGENT_CONTEXT.md` only if a stable rule changed
- `DEEP_CONTEXT.md` only for deep historical notes
 
