# AGENT_CONTEXT - Quick Context (SSOT Lite)

Last updated: 2026-03-09

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

## 5) Data / Reporting Rules
- Bank statements (PDF cartolas) are truth for monthly closings.
- Parser output loads via `DataLoadingService` into reporting tables.
- `monthly_metrics_normalized` is primary reporting layer.
- `monthly_closings` is historical source + fallback.

## 6) Key Paths
- Backend entrypoint: `backend/main.py`
- Reporting router: `backend/routers/data.py`
- Loader: `backend/services/data_loading_service.py`
- Document processing: `backend/services/document_service.py`
- DB models: `backend/db/models.py`
- Frontend entrypoint: `frontend/app.py`
- Main pages: `frontend/pages/summary.py`, `mandates.py`, `etf.py`, `personal.py`, `upload.py`

## 7) Current Baseline
- Tests baseline from project context: 131 passed, 1 skipped.
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
