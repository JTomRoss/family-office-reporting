# SESSION_STATE - Current Working State

Last updated: 2026-03-09
Owner: JTROSS + Codex
Branch: master

## 1) Current Product Status
- App promoted to official main environment.
- Main endpoints for Summary, Mandates, ETF, Personal are functional with real DB queries.
- Preview/staging local flow is available (`8100/8601`) with separate DB.
- Normalized reporting layer (`monthly_metrics_normalized`) is active and used by reporting endpoints.
- Mandate asset-allocation normalization (Cash / Fixed Income / Equities) is implemented.

## 2) Current Known Priorities
1. UBS Switzerland additional hardening for new quarterly edge cases.
2. Expand YTD controls and parser quality alerts by bank.
3. Functional QA checklist consolidation for Summary/Mandates/Personal.
4. Optional: activate parquet cache usage in reporting paths.

## 3) Operational Rules (quick)
- Restart main app after backend/frontend code changes:
  - `./scripts/stop.ps1`
  - wait 2 seconds
  - `./scripts/start.ps1`
- For visual validation before main promotion, use preview scripts.

## 4) What to Load in New Chat
Minimum context load:
1. `AGENT_CONTEXT.md`
2. `SESSION_STATE.md`
3. `git status --short`
4. Only files directly related to requested task

Optional deep load:
- `DEEP_CONTEXT.md` only if historical decisions are needed.

## 5) Active Worktree Snapshot Guidance
Worktree may contain WIP changes in backend/frontend/parsers/tests.
Do not assume a clean tree.
Do not revert unrelated changes.

## 6) Session Log Template
Use this short format when closing a work block:
- Date:
- Goal:
- Files changed:
- Decisions made:
- Tests run + result:
- Pending next actions:

## 7) Next Action Template (for user prompting Codex)
"Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status.
Luego trabaja solo en [ruta/feature concreta]."
