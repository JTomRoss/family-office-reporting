# AGENT_CONTEXT - Quick Context (SSOT Lite)

Last updated: 2026-03-16 (normalized SSOT + UBS Switzerland identity policy + JPM cash normalization)

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

## 5) Data / Reporting Rules
- Bank statements (PDF cartolas) are truth for monthly closings.
- Parser output loads via `DataLoadingService` into reporting tables.
- `monthly_metrics_normalized` is the canonical monthly reporting layer.
- Tables and charts in reporting (`Summary`, `Mandates`, `ETF`, `Personal`) must read monthly values from `monthly_metrics_normalized`.
- `monthly_closings` is historical source + fallback; if normalized data exists, it wins.
- Identity control is mandatory: `ending_current - movements - profit = ending_previous`.
- YTD is control-only. Never use YTD to auto-fill, overwrite, or "force" monthly movements/profit.
- If identity or YTD controls fail, reporting must alert; it must not silently mutate values to make them match.
- `Salud BD` is an audit/read-only surface: it should alert from normalized/historical persisted values, not create a third interpreter of monthly data.
- Raw PDFs are still operationally important for reprocesos, parser fixes, and audit traceability; do not assume processed PDFs can be deleted safely.

## 5.1) Stable Parser / Bank Rules Already In Code
- JPMorgan `brokerage` / `etf`: blank current-period fields stay `0`/`None` as monthly values; YTD remains control-only.
- JPMorgan `brokerage` / `etf`: if old statements omit `asset_allocation` but holdings include cash-like sweep / money-market rows, `DataLoadingService` may derive `cash_value` into `monthly_metrics_normalized` during normalization/backfill. Reporting must not read `ParsedStatement` to infer cash.
- JPMorgan `custody`: `Net Security Contributions` must count as part of monthly movements when present.
- Goldman Sachs: automatic OCR fallback exists for garbled PDFs; OCR is a parser-level backup, not a UI concern.
- UBS Suiza: for multi-portfolio statements, the account suffix (`-01`, `-02`) selects the portfolio-specific `net_assets`; never use the combined total when a specific portfolio is identifiable.
- UBS Suiza: negative positions are valid; in UBS-only reporting views, monthly return shown to the user is forced to `0%` when current or previous position is negative.
- UBS Suiza only: the previous audited month-end `ending value` prevails over the next statement's `beginning value` when they differ. Quarterly performance tables may refine prior-month `movements`, but must not overwrite auditable month-end balances. `profit` is the adjustment variable and must be recomputed from identity against the previous audited ending.

## 6) Key Paths
- Backend entrypoint: `backend/main.py`
- Reporting router: `backend/routers/data.py`
- Loader: `backend/services/data_loading_service.py`
- Document processing: `backend/services/document_service.py`
- DB models: `backend/db/models.py`
- Frontend entrypoint: `frontend/app.py`
- Main pages: `frontend/pages/summary.py`, `mandates.py`, `etf.py`, `personal.py`, `upload.py`

## 7) Current Baseline
- Tests baseline after latest hardening: `169 passed, 1 skipped`.
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
