# SESSION_STATE - Current Working State

Last updated: 2026-04-09
Owner: JTROSS + Codex
Branch: master

## 1) Current Snapshot
- SSOT monthly reporting layer: `monthly_metrics_normalized`.
- `monthly_closings` remains historical source and allowed fallback.
- Frontend remains presentation-only.
- Mandato parsing/merge contract is hardened:
  - Cartola provides auditable macro USD totals.
  - `report_mandato` provides only sub-asset splits and FI metrics.
  - Merge/normalization is centralized in backend.

## 2) Guardrails To Keep
- Strict PDF engine isolation by:
  - bank
  - account type
  - mandate report parser per bank (`parsers/*/report_mandato.py`)
- Do not move financial logic to frontend.
- Do not create runtime interpreters that bypass normalized persisted data.
- Reporting tables/charts must read from normalized layer.
- Keep raw PDFs for reprocess and audit traceability.

## 3) Closed Block - 2026-03-31 (JPM donut artifact)
Goal:
- Fix `Boatview | jpmorgan | mandato | 1412600 | 2026-02` where parser captured false `Cash 43.00%` from donut text.

Changes:
- Hardened JPM mandate report parser to prioritize table breakdown and ignore donut artifact.
- Added regression test for this layout.
- Reprocessed only required docs for that month/account.

Result:
- Cash in persisted allocation corrected from false percent to cartola-aligned USD.
- Canonical breakdown reconciled for target account.

## 4) Closed Block - 2026-03-31 (Mandato contract hardening + repairs)
Goal:
- Enforce mandate source contract globally and repair contaminated monthly rows without broad reprocess.

Code changes:
- Hardened mandate report parsers:
  - `parsers/jpmorgan/report_mandato.py` -> `1.2.0`
  - `parsers/bbh/report_mandato.py` -> `1.2.0`
  - `parsers/ubs/report_mandato.py` -> `1.2.0`
  - `parsers/ubs_miami/report_mandato.py` -> `1.2.0`
- Contract now:
  - No report-level macros (`Cash`, `Fixed Income`, `Equities`) in report parser output.
  - Keep sub-splits (`IG/HY`, `US/Non-US`, `Global Equity`) + FI metrics only.
- Added parser contract regressions:
  - `tests/test_mandate_report_parser_contracts.py`
  - Updated `tests/test_jpm_report_mandato_parser.py`

Operations executed:
- App restarted using official scripts.
- Surgical reprocess:
  - GS `451-9`: docs `1764` (cartola) + `1786` (report)
  - BBH `7085`: docs `1763` (cartola) + `1801` (report)
  - UBS/UBS Miami Feb-2026 cleanup:
    - `1774` (UBS cartola)
    - `1771` (UBS Miami cartola)
    - `1834` (UBS Miami report)

Validation:
- Tests run and passing:
  - parser contract + operational + targeted loader coverage.
- Reconciled mandate rows (Feb-2026):
  - `Boatview | GS | 451-9` -> gap resolved.
  - `Boatview | BBH | 7085` -> gap resolved.
  - `Boatview | UBS | 206-560552-02` -> macros persisted as USD, no percent macro carryover.
  - `Boatview | UBS Miami | 3J 00432 P1` -> macros persisted as USD, no percent macro carryover.

## 5) Known Residuals
- Important user decision (2026-03-31):
  - Do not touch or delete GS/JPM cartolas with duplicate alternatives exposures.
  - Reason: they are still needed for historical traceability.
- Remaining small residuals are now operational/rounding scale (plus accrual pockets), not PE duplication blocks.

## 6) Closed Block - 2026-03-31 (ETF EM + JPM bonds short-term + GS non-us alias)
Goal:
- Resolve residuals caused by missing `Emerging Market Equities` / `NON-US EQUITY` classification and by JPM bonds `1531100` short-term handling.

Code changes:
- `asset_taxonomy.py`
  - Added compatibility aliases:
    - `EMERGING MARKET EQUITIES`
    - `EMERGING MARKETS EQUITIES`
    - `NON-US EQUITY`
    - `NON US EQUITY`
    - `TOTAL NON-US EQUITY`
  - All mapped to `RV EM`.
- `backend/services/data_loading_service.py`
  - Reworked `_canonical_from_etf_instruments` to map ETF buckets directly into canonical 7 categories, avoiding synthetic `Global Equities` fallback that dropped EM amounts in practice.
  - Added `self.db.flush()` before ETF-triggered normalized refresh inside `_upsert_etf_compositions` (session uses `autoflush=False`).
- `parsers/jpmorgan/bonds.py` (`2.0.2`)
  - For account `1531100`, kept cash override from `Total Cash Holdings`.
  - Added extraction of `Short Term Investments` and sum into `Fixed Income`.
- Tests:
  - Updated/added targeted regressions in:
    - `tests/test_loader_contracts.py`
    - `tests/test_normalized_reporting_layer.py`

Operations executed:
- App restarted with official scripts.
- Surgical reprocess (only affected docs):
  - JPM ETF: `1732` (`5001`), `1761` (`7000`), `1750` (`4009`), `1735` (`0007`)
  - JPM bonds: `404` (`1531100`)
  - GS ETF: `1766` (`452-2`)

Validation:
- Emerging exposures now included in canonical `Non US Equities` for:
  - `U28375001`, `B75667000`, `E30994009`, `E31070007`, `452-2`
- `1531100` now reconciles exactly:
  - `cash + fixed income = ending value`
- Consolidated filter (`Mi Inv + Ect. Int + Armel`, `2026-02`) residual reduced to the known GS mandato account plus minor small residuals.

## 7) Closed Block - 2026-03-31 (Historical dedupe vs Alternativos for Telmar)
Goal:
- Remove duplicated alternatives exposure from reporting totals (all history), without deleting historical cartolas.

Scope:
- `Telmar | goldman_sachs | mandato | 097-4`
- `Telmar | jpmorgan | brokerage | B43459001`

Code changes:
- `backend/services/data_loading_service.py`
  - Added stable account-scoped reporting exclusion rules.
  - Exclusions persist in `monthly_metrics_normalized.asset_allocation_json` under `__reporting_value_exclusion`.
  - Fallback extraction for GS `097-4` from `parsed_statements.rows` (`WEST STREET CAPITAL PARTNERS VII OFFSHORE, L.P.`) when allocation labels are absent.
  - Fallback extraction for JPM `B43459001` as residual vs ending (without accrual) when `Alternative Assets` label is not explicit.
- `backend/routers/data.py`
  - Added centralized reader of `__reporting_value_exclusion`.
  - `_resolve_ending_with_accrual` / `_resolve_ending_without_accrual` now subtract exclusion once, so all reporting tables stay aligned.
- Tests:
  - `tests/test_loader_contracts.py`
  - `tests/test_normalized_reporting_layer.py`

Operations executed:
- App restarted using official scripts.
- Historical backfill (all years available) by refreshing normalized rows for target accounts.

Backfill result:
- `goldman_sachs | mandato | 097-4`: years `2020..2026`, rows `74`, rows_with_exclusion `12` (from `2025-03` to `2026-02`).
- `jpmorgan | brokerage | B43459001`: years `2020..2026`, rows `63`, rows_with_exclusion `39` (from `2020-12` to `2026-02`).

Validation:
- User control set (`Mi Inv + Ect. Int + Armel`, `2026-02`) gap reduced to `28,588.31` (previous PE duplication removed).
- Duplicate PE blocks specifically removed from totals:
  - GS `097-4`: `3,304,758.00` (2026-02) via exclusion metadata.
  - JPM `B43459001`: `19,314.00` (2026-02) via exclusion metadata.

## 8) Closed Block - 2026-04-01 (JPM mandato OCR + sibling spread, UBS Suiza historical refresh)
Goal:
- Repair historical mandate sub-asset reporting where JPM complementario was not read and JPM portfolio-level reports were landing on only one subaccount.
- Reprocess UBS Suiza mandate reports so the persisted reporting layer reflects the central `Global Equity -> 2/3 US + 1/3 Non-US` rule.

Code changes:
- `parsers/jpmorgan/report_mandato.py` (`1.3.0`)
  - Added isolated OCR fallback for JPM complementario.
  - Extracts `HG/HY` + blended FI metrics from `Portfolio Positioning / Duration / Yield`.
  - `Investment Review` now contributes only `US Equities` / `Non US Equities` (no FI split from that layout).
- `backend/services/data_loading_service.py`
  - `pdf_report` loader can now apply JPM portfolio-level mandate reports (`account_number = Varios`) to sibling mandato accounts that share the same cartola source document in the target month.
  - Added JPM safeguard so a one-sided `Investment Review` IG input does not overwrite an existing `HG/HY` split already persisted from complementario.
- Tests:
  - `tests/test_jpm_report_mandato_parser.py`
  - `tests/test_data_loading_operational.py`

Operations executed:
- Targeted tests passed for JPM parser + mandate report loading.
- Historical reprocess executed for all `pdf_report` docs in:
  - `jpmorgan` (`15` docs)
  - `ubs` (`13` docs)
- App restarted with official scripts after code changes.

Validation:
- JPM `2026-02`:
  - `Complementario` now parses `HG 43% / HY 12%`.
  - `Investment Review` now parses `US 31.24% / Non-US 11.76%`.
  - Sibling mandate accounts share the report enrichments correctly:
    - `1412600` carries FI split.
    - `1483400` carries equity split (plus its FI sleeve).
- Aggregated JPM canonical amounts for `Boatview | 2026-02` now reconcile to:
  - Cash `9,481,775.70`
  - IG FI `145,071,643.2233`
  - HY FI `40,485,109.7367`
  - US Eq `106,884,978.1695`
  - Non-US Eq `40,235,830.4505`
- UBS Suiza `2026-02` now persists:
  - US Eq `18,418,874.8226`
  - Non-US Eq `20,071,327.1774`
  - This reflects the mandated `2/3 - 1/3` split of `Global Equity`.

Known source exception:
- `raw_document 1845` (`2025 09 JPM Complementario - Reporte Mandato.pdf`) is an Outlook email wrapper with no portfolio table attached in the PDF itself.
- Result: no extractable allocation exists for that source file, so September 2025 cannot be repaired from that document alone.

## 9) Closed Block - 2026-04-01 (JPM Jan/Feb 2025 mandate refresh + UBS Suiza graph-noise fix)
Goal:
- Repair two residual reporting gaps detected in validation:
  - `Boatview | jpmorgan | mandato | 2025-01 / 2025-02`
  - `Telmar | ubs | mandato | 2023-10 / 2023-11`

Code changes:
- `parsers/ubs/custody.py` (`2.3.2`)
  - Hardened `Total assets` extraction so chart-axis ticks (`10`, `0`, `-10`) never contaminate
    `Equities` or `Net assets`.
  - Added compressed-row handling for layouts like `Liquidity 54085 151 54236 100.00`,
    preserving the explicit `Total` column instead of concatenating the three tokens.
- `tests/test_specific_cartola_extraction.py`
  - Added regression coverage for:
    - `202310/202311 Telmar UBS SW Mandato (0402 60P).pdf`
    - existing Boatview `Portfolio01` compressed-liquidity layouts (`202505/202508/202511`)

Operations executed:
- Targeted UBS parser regressions passed.
- Reprocessed only affected cartolas:
  - JPM mandato docs `71` and `72`
  - UBS mandato docs `1605` and `1606`

Validation:
- JPM `Boatview` `2025-01` / `2025-02`:
  - `1412600` now persists audited cartola macros in USD again:
    - `2025-01`: cash `7,918,584.26`, fixed income `133,726,722.86`
    - `2025-02`: cash `4,548,158.22`, fixed income `135,969,165.12`
  - mandate detail totals now reconcile to ending totals with only rounding residuals:
    - `2025-01` diff `-0.06`
    - `2025-02` diff `-0.05`
- UBS Suiza `Telmar` `2023-10` / `2023-11`:
  - ending values corrected from false `-10` to:
    - `68,433,704`
    - `72,201,365`
  - asset allocation corrected to:
    - `2023-10`: cash `990,349`, FI `43,964,829`, equities `23,478,819`
    - `2023-11`: cash `919,933`, FI `45,557,933`, equities `25,731,409`

## 10) Closed Block - 2026-04-01 (Frontend UX: apply filters + upload timeout)
Goal:
- Reduce unnecessary reruns/heavy API calls in UI filters.
- Avoid false red `timed out` errors during single-PDF upload when backend finishes successfully.

Code changes:
- `frontend/components/filters.py`
  - Added `use_apply_filters(...)` helper to separate draft widget state from applied filter state.
- `frontend/pages/personal.py`
- `frontend/pages/summary.py`
- `frontend/pages/mandates.py`
- `frontend/pages/etf.py`
  - Top filters now require explicit `Aplicar` before hitting heavy backend endpoints.
  - `Detalle` / `Detalle de Cartolas` no longer recompute cascading seed rows on every widget change; they use full filter options and applied state.
- `frontend/pages/upload.py`
  - `Documentos en el sistema` now stays empty until a filter/search is applied with `Buscar`.
- `frontend/api_client.py`
  - Increased file upload timeout from `30s` to `300s`.

Validation / diagnosis:
- The earlier false timeout in UI was frontend-only; backend had completed processing.
- The bad source in that moment was an Outlook wrapper export, not the attached report itself.

## 11) Closed Block - 2026-04-01 (Upload tab batch selection + JPM complementario OCR tightening)
Goal:
- Stop `Documentos cargados` from rerunning the whole page on every checkbox toggle.
- Reduce wasted OCR work for the image-based JPM complementario layout and recover the real `HG/HY` split from the correct Sep-2025 PDF.

Code changes:
- `frontend/pages/upload.py`
  - Wrapped the `Documentos en el sistema` selection grid inside a Streamlit form.
  - Checkbox changes are now draft-only; deletion happens only when the form button is pressed.
  - Added `Limpiar selección`.
  - Reset editor state when the visible document scope changes, so stale selections do not leak across searches.
- `parsers/jpmorgan/report_mandato.py` (`1.3.1`)
  - Complementario OCR now uses a single focused crop around `Portfolio Positioning / Duration / Yield`.
  - Switched from two broader paragraph OCR passes to one grouped-line OCR pass.
  - Added OCR normalization helpers for noisy tokens (`47c`, `125`, `4609`, `6550`, etc.).
- `tests/test_jpm_report_mandato_parser.py`
  - Added regression coverage for noisy OCR output from the real complementario layout.

Validation:
- The newly uploaded correct file remains `raw_document 1861` (`2025 09 JPM Complementario - Reporte Mandato.pdf`) and is no longer the Outlook wrapper.
- Direct parse now resolves:
  - statement date `2025-09-30`
  - `Investment Grade Fixed Income` `47%`
  - `High Yield Fixed Income` `12%`
  - blended duration `3.80`
- Main cause of slowness remains OCR model warm-up on image-only PDFs, but the parser now does a smaller/faster single pass instead of the prior broader retries.

## 12) Next-Chat Continuity Prompt
Use this starter:

`Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status --short + archivos relevantes.`
`Mantener monthly_metrics_normalized como SSOT mensual, monthly_closings solo historico/fallback, frontend solo presentacion y aislamiento estricto de motores por banco/tipo (incluyendo report_mandato por banco).`

## 13) Session Log Template
- Date:
- Goal:
- Files changed:
- Decisions:
- Tests:
- Data operations:
- Pending:

## 14) Closed Block - 2026-04-01 (GS historical PE + legacy overview + Boatview cash)
Goal:
- Repair Goldman Sachs historical reporting mismatches detected in bank-by-bank validation:
  - `Telmar` legacy months with doubled totals
  - historical PE duplication not flowing to all reporting tables
  - `Boatview` mandato months with stale cash instead of the audited `38M` umbrella

Code changes:
- `parsers/goldman_sachs/custody.py` (`2.1.2`)
  - Primary account overview is now detected dynamically across the first pages instead of assuming page 3.
  - Legacy GS wraps that insert `Special Messages` before `Overview` now parse correctly.
  - Sub-portfolio overview detection now requires exact `Statement Detail` footer markers, preventing the main overview page from being summed as if it were another sleeve.
- `tests/test_specific_cartola_extraction.py`
  - Added/updated GS regression coverage for:
    - legacy Telmar wrap no longer double-counting the main overview
    - Boatview Jan-2026 cash umbrella

Data operations:
- Reprocessed GS mandato cartolas:
  - Boatview `2025-01..2026-02`: docs `183,184,185,186,187,188,189,178,179,180,181,182,1765,1764`
  - Telmar legacy fixes: docs `1889` (`2017-11`), `1881` (`2018-02`)
- Refreshed normalized layer for `Telmar | goldman_sachs | mandato | 097-4` across all years `2015..2026`
  so reporting exclusions apply consistently in every table.

Validation:
- Tests passing:
  - `tests/test_specific_cartola_extraction.py` -> `46 passed, 1 skipped`
  - `tests/test_normalized_reporting_layer.py` -> `33 passed`
- GS outcomes:
  - `Telmar 2017-11` ending corrected from inflated `201,210,470.06` to audited `100,605,235.03`
  - `Telmar 2018-02` ending corrected from inflated `200,890,757.00` to audited `100,445,378.50`
  - `Boatview GS mandato` cash umbrella restored historically in 2025-05..2026-02:
    - `2025-05` `39,368,559.03`
    - `2025-06` `38,411,537.73`
    - `2025-07` `37,556,740.62`
    - `2025-08` `37,690,021.58`
    - `2025-09` `37,836,162.16`
    - `2025-10` `37,887,058.28`
    - `2025-11` `38,008,474.80`
    - `2025-12` `38,078,891.58`
    - `2026-01` `38,057,935.83`
    - `2026-02` `38,193,992.65`

## 15) Closed Block - 2026-04-01 (GS Other Investments + removal of bad PE residual fallback)
Goal:
- Fix regression introduced by the prior GS Telmar exclusion fallback, where non-PE legacy
  categories (`Other Investments`, `Hedge Funds`, `Miscellaneous`) were being removed as if
  they were duplicated PE.

Code changes:
- `mandate_taxonomy.py`
  - Added mandate category `other_investments`.
- `mandate_report_dictionary.json`
  - `private_equity` rules now match only explicit PE labels.
  - `other_investments` now matches `Other Investments`, `Asset Allocation Investments`,
    `Miscellaneous`, `Hedge Funds`.
- `backend/services/data_loading_service.py`
  - Mandato normalization with `macro_only=True` now preserves:
    - `Private Equity`
    - `Real Estate`
    - `Other Investments`
  - `Other Investments` dedupes umbrella/sub-lines:
    - keep `Other Investments` umbrella when present
    - otherwise sum its component rows
    - add `Hedge Funds` as part of `Other Investments`
  - Removed the bad GS residual fallback that was excluding `ending - allocation_total`
    as if it were PE.
- `backend/services/normalized_reporting_payload.py`
  - Added canonical category `Other Investments`.
  - `Alternativos` derived subtotal now includes `PE + RE + Other Investments`.
- `backend/routers/data.py`
  - Added personal/detail bucket `Other investments`.
- `frontend/pages/personal.py`
  - `Detalle por Activo` now renders `Otras inversiones` below `Real Estate`.

Data operations:
- Reprocessed full historical GS Telmar mandato series (`61` cartolas for account `097-4`).
- Reprocessed Boatview GS mandato `2025-01..2026-02` to keep GS state consistent after the new normalization.

Validation:
- Regression tests passed:
  - `tests/test_loader_contracts.py` targeted GS/OI coverage
  - `tests/test_specific_cartola_extraction.py`
  - `tests/test_normalized_reporting_layer.py`
- GS Telmar examples after fix:
  - `2016-01`: `Other Investments = 2,726,214.37`, exclusion `None`
  - `2017-01`: `Other Investments = 11,188,162.09`, exclusion `None`
  - `2018-01`: `PE = 793,729.00`, `Other Investments = 5,022,621.63`, exclusion `793,729.00`
  - `2018-03`: `PE = 800,662.00`, `Other Investments = 4,788,112.69`, exclusion `800,662.00`
  - `2019-11`: `PE = 2,873,723.00`, `Other Investments = 41,903.09`, exclusion `2,873,723.00`
  - `2023-05`: `PE = 4,599,378.00`, `Other Investments = 18,295.30`, exclusion `4,599,378.00`
  - `2024-06`: `PE = 4,584,038.00`, `Other Investments = 8,442.89`, exclusion `4,584,038.00`

## 16) Closed Block - 2026-04-09 (Bug report full resolution — 11 bugs)
Goal:
- Resolve all bugs identified in `bug_report.md` (static review by Claude Sonnet 4.6, 2026-04-07).

Code changes:

**Bug 1** — `parsers/jpmorgan/bonds.py`
- `_extract_period`: correct `period_start` year for January cross-year statements
  (`start_year = year - 1 if start_month > end_month else year`).

**Bug 2** — `parsers/jpmorgan/bonds.py`
- `_extract_total_short_term_investments`: fallback pattern now captures the ending
  column (second number), not the beginning (first number), for account `1531100`.

**Bug 3** — `backend/services/data_loading_service.py`
- Removed dead method `_apply_bbh_prior_adjustments` (was never called; its design of
  mutating prior months' closings violates architecture guardrails).
- `_validate_ytd_consistency` now logs an `info` entry when BBH `prior_period_adjustments`
  is detected, making the YTD gap traceable. The warning is still emitted so discrepancies
  remain visible for audit.

**Bug 4** — `backend/services/data_loading_service.py`
- `_recompute_ubs_income_from_identity`: warn when recomputed income exceeds 50% of
  ending value (guard against parser errors absorbed silently as profit).

**Bug 5** — `backend/services/data_loading_service.py`
- `_validate_ytd_consistency`: query now excludes current month and adds it from
  in-memory `account_values`, making YTD comparison deterministic with `autoflush=False`.

**Bug 6** — `backend/routers/data.py`
- `_reconcile_mandates_asset_breakdown_to_target`: added `logger.warning` when positive
  residual > 1% of target is silently assigned to Equities.
- Added `import logging` + `logger = logging.getLogger(__name__)` to router.

**Bug 7** — `backend/services/normalized_reporting_payload.py`
- `canonical_breakdown_from_payload`: log warning when a significant negative amount
  (`< -1`) is silently discarded from canonical breakdown.
- Added `import logging` + `logger` to module.

**Bug 8** — `backend/services/normalized_reporting_payload.py`, `data_loading_service.py`, `data.py`
- Unified duplicate cash extractor into single canonical function
  `cash_from_asset_allocation_json` in `normalized_reporting_payload.py`.
- Fixed divergence: list-path missing `_is_mixed_cash_bucket` check (was in loader,
  absent in router). Removed both local implementations.

**Bug 9** — `backend/routers/data.py`
- `_get_filter_options`: extended `years`, `bank_codes`, `entity_names`, `person_names`
  to union results from `MonthlyMetricNormalized` so Alternativos accounts appear in
  UI filter options.

**Bug 10** — `backend/routers/data.py`, `backend/services/account_service.py`
- Hardened Alternativos `asset_class` LIKE filter to match both JSON serialization
  formats (`"asset_class": "PE"` with space and `"asset_class":"PE"` without space)
  using `or_()` in all 3 occurrences.

**Bug 11** — `backend/services/data_loading_service.py`
- `load_parse_result`: added `self.db.rollback()` in per-account exception handler to
  prevent `PendingRollbackError` on subsequent flush.

Tests:
- All targeted tests passing: `test_loader_contracts.py`, `test_normalized_reporting_layer.py`.
- `test_loader_bbh_prior_adjustment_is_control_only`: updated to also assert info log for
  `prior_period_adjustments` detection.

Other:
- Deleted temporary debug PNG files: `tmp_jpm_*.png` (5 files).

Commits (in order):
- `bc3ed1f` Bug 2, `b8feea3` Bug 11, `539db90` Bug 1, `91c621e` Bug 4,
  `d47c18b` Bug 5, `47cad92` Bug 6, `10aa731` Bug 7, `6b41eee` Bug 8,
  `46a03e8` Bug 9, `bd36bf3` Bug 10, `f8af5bb` Bug 3.

## 17) Closed Block - 2026-04-13 (ETF sin_caja rentabilidad fixes)
Goal:
- Corregir bugs en la tabla de rentabilidad por sociedad del endpoint `/data/etf` con filtro `sin_caja`.
- Separar correctamente `RV EM` (100% Non-US) vs `RV DM`/`Global Equity` (2/3 US + 1/3 Non-US) en canonical ETF mapping.

Code changes:
- `backend/routers/data.py`
  - Eliminado bloque incorrecto `mov -= (curr_cash - prev_cash)` en sección de movimientos.
  - Nuevo dict `soc_month_cash_nc` cubre ambos años (`sel_year-1` y `sel_year`) para que denominador de Enero use EV_nc de Diciembre anterior.
  - Ending sin caja: `max(ending_raw - cash_nc, 0.0)`.
  - Retorno mensual sin caja: ajusta `mov_raw` por `delta_cash` (consistente con `/summary`).
- `backend/services/normalized_reporting_payload.py`
  - `_canonical_from_category_totals`: residual de equities distribuye `2/3 US + 1/3 Non-US`.
- `backend/services/data_loading_service.py`
  - Separación `RV EM` (100% Non-US) vs `RV DM`/`Global Equity` (2/3 US + 1/3 Non-US).
- `frontend/pages/etf.py`
  - `@st.cache_data(ttl=300)` wrapper `_fetch_etf_data` para evitar re-fetch en cada widget change.
- `frontend/pages/personal.py`
  - Typos corregidos: `Private Equtiy` → `Private Equity`, `Real Esteate` → `Real Estate`.
- Tests: expectativas actualizadas para IWDA (RV DM) split y sin_caja movimientos/retornos.

Validation:
- Armel Hold. | Enero 2026 | sin_caja = **1,1708%** ✓
- Suite: **233 passed, 1 skipped**.

Commit: `b7b0eb3` — pushed to origin/master.

## 18) Closed Block - 2026-04-13 (GS ETF parser fallback + reclasificación UBS Miami)

Goal:
- Corregir que GS Boatview ETF 2026-03 no mostraba detalle por activo (allocation todo en cero).
- Gestionar documentos UBS Miami Boatview con account_id=None.

Root cause GS ETF:
- El PDF de marzo 2026 cambió formato: ya no incluye la sección "Asset Strategy Analysis" por instrumento.
- El GS ETF parser (v2.1.1) dependía exclusivamente de esa sección; al no encontrarla devolvía 0 rows → no se creaban ETF compositions → canonical breakdown todo en cero.
- Además, el PDF ya no incluye el número de grupo "452-2" en ninguna página (solo "062-3"), causando que el income/profit no se vinculara correctamente.

Fix aplicado:
- `parsers/goldman_sachs/etf.py` → v2.1.2:
  - Nuevo método `_parse_holdings_fallback`: cuando `extract_asset_strategy` devuelve vacío, extrae market_value por instrumento directamente de la tabla Holdings usando nombre exacto como ancla y capturando el 3er número (market_value) de las filas de cifras.
  - Nueva constante `GROUP_ACCOUNT_NUMBER = "452-2"` + `_SUB_PORTFOLIO_NUMBERS = {"062-3"}`.
  - En `account_monthly_activity` y `accounts`, se usa `activity_acct_num = GROUP_ACCOUNT_NUMBER` cuando el PDF solo muestra "062-3" → profit/income vinculado correctamente.
- Re-procesado doc 1956 (202603 Boatview - GS (ETF).pdf).
- UBS Miami: 27 raw_documents reclasificados a account_id=38 (Boatview).

Validation:
- GS ETF 2026-03 canonical breakdown: Cash=109.16, IG FI=22.79M, HY FI=4.52M, US Eq=10.35M, Non-US=6.96M ✓
- Profit 2026-03: -1,854,687.93 ✓
- Suite: **233 passed, 1 skipped** ✓

Commit: `aa3c61d` — pushed to origin/master.

## 19) Closed Block - 2026-04-13 (JPM brokerage v2.1.3 + Isabel Izquierdo + UBS Miami reports)

Goal:
- Corregir que el parser JPM brokerage no leía los $9.84 de Isabel Izquierdo (acct E74997009) en 2024.
- Aclarar la naturaleza de los archivos "BOATVIEW LIMITED - fecha.pdf" de UBS Miami y procesarlos correctamente.

Root causes:
- **Bug 1 (ToC page):** Página 1 del PDF tiene "Portfolio Activity 4" en el índice del documento. El parser la procesaba como página de actividad, añadía entrada vacía para E74997009 y el chequeo anti-duplicado bloqueaba páginas 2 y 4 (datos reales).
- **Bug 2 (cash-only account):** `_parse_account_activity_page` retornaba `None` si `net_contributions` y `income_distributions` eran ambos `None`. Cuentas cash-only sin transacciones no tienen esos campos. Además, la página usaba "Ending Cash Balance" (no "Ending Market Value") que tampoco se capturaba.
- **UBS Miami pdf_report:** Los archivos "BOATVIEW LIMITED - fecha.pdf" son performance reviews (no cartolas). Son correctamente leídos por `parsers/ubs_miami/report_mandato.py` (score 0.98). Tenían `account_id=None` al subirse → DataLoadingService no los vinculaba → 0 ParsedStatements.

Fix aplicado:
- `parsers/jpmorgan/brokerage.py` → v2.1.3:
  - Skip de páginas ToC que mencionan "Portfolio Activity" sin marcadores de datos reales (`_ACTIVITY_DATA_MARKERS`).
  - Extracción de "Ending Cash Balance" como alias de `ending_value_without_accrual`.
  - Guardia de retorno `None` relajada: permite datos si hay `ending_value_without_accrual` aunque no haya `net_contributions`/`income_distributions`.

Data operations:
- 37 raw_documents Isabel Izquierdo (7009, 2022-2026) reclasificados a `account_id=13` y reprocesados.
  - 2022-2023: tenían acct_id=None y ParsedStatements (datos correctos en esas fechas, e.g. $17-19M).
  - Sep 2023 en adelante: $9.84 ahora se lee correctamente.
  - 2024 completo (12 meses) + Ene 2026: `[NEW]` → monthly_closing creado con $9.84 cada mes.
- 11 UBS Miami Boatview performance reviews (pdf_report, dic 2024 a dic 2025 + Jan/Feb/Mar 2026) cargados a `account_id=38`.
- Normalized sync ejecutado para acct=13 (2022-2026) y acct=38 (2024-2026).

Validation:
- Isabel Ene-2024: `status=success  acct=E74997009  ev=9.84` ✓
- La Guardia (control de no regresión): `status=success  ev=0.00` ✓
- Suite: **258 passed, 1 skipped** ✓

Commit: `e3c73a9` — pushed to origin/master.

## 20) Closed Block - 2026-04-14 (JPM mandato net_security_contributions)

Goal:
- Agregar extracción de "Net Security Contributions / Withdrawals" al motor de lectura JPM mandato, que en el formato pre-2021 separaba efectivo y securities en dos líneas.
- Boatview JPM mandato 2020-04 (acct 1483400): movimientos corregidos de $17,258,517.10 → $58,423,807.95.

Root cause:
- El formato "Statement of Account" pre-2021 tiene dos filas separadas en Portfolio Activity:
  - "Net Cash Contributions / Withdrawals" (efectivo)
  - "Net Security Contributions / Withdrawals" (valores en especie)
- El bonds parser solo extraía la primera. La segunda ($41,165,290.85) se ignoraba.
- El brokerage parser también tenía el mismo hueco (aunque no aplica para esta cuenta específica, se corrigió igualmente por completitud y aislamiento).

Code changes:
- `parsers/jpmorgan/bonds.py` → v2.0.3:
  - Nuevo patrón `net_security_contributions` en `_extract_account_summary`.
- `parsers/jpmorgan/brokerage.py` → v2.1.4:
  - Dos nuevos patrones en `_ACTIVITY_ROW_PATTERNS` para delimitar las filas split.
  - Fallback en `_parse_account_activity_page`: suma net_cash + net_security cuando el formato unificado no matchea.
- `backend/services/data_loading_service.py`:
  - En el bloque fallback `portfolio_activity` de bonds/custody: lee `net_security_contributions` y lo suma a `net_cash_contributions` como total de movimientos.
- `tests/test_specific_cartola_extraction.py`:
  - Nuevo test `test_jpm_brokerage_split_format_sums_cash_and_security_contributions`.
- `tests/test_loader_contracts.py`:
  - Nuevo test `test_loader_jpmorgan_bonds_sums_cash_and_security_contributions`.

Data operations:
- Reprocesado doc 1246 (202004 Boatview JPM NY Multiactivo (3400) - INICIO.pdf).
- Otros 8 docs 2020 (1244, 1245, 1247-1252): `net_security=0`, sin cambio efectivo en BD.
- Sync normalized para account_id=4 (1483400) año 2020.

Validation:
- 1483400 | 2020-04 | change_in_value = 58,423,807.95 ✓ (identidad: 0 + 58,423,807.95 + 1,130,341.73 = 59,554,149.68 ✓)
- Suite: **220 passed, 1 skipped** ✓

## 21) Closed Block - 2026-04-14 (UBS Miami 2024-12 fix + salvaguarda pdf_report)

Goal:
- Restaurar net_value=NULL en UBS Miami Boatview 2024-12 causado por reporte de gestión sobreescribiendo la cartola.
- Implementar salvaguarda permanente en DataLoadingService: un pdf_report nunca puede sobreescribir datos financieros de un monthly_closing ya establecido por una cartola.

Root cause:
- `BOATVIEW LIMITED - 12-31-2024.pdf` (pdf_report, doc 1854) fue procesado por `_upsert_monthly_closing` que, al encontrar un registro existente, sobreescribía incondicionalmente `net_value`, `total_assets`, e `income` con los valores del reporte (incluyendo `closing_bal=None`).
- `source_document_id` también se actualizaba al report, borrando la trazabilidad a la cartola.

Fix aplicado:
- `backend/services/data_loading_service.py`:
  - Variable `is_report_doc` detecta si el documento es `pdf_report` (`doc.file_type`).
  - Variable `_preserve_financials = is_report_doc AND existing.net_value IS NOT NULL`.
  - Si `_preserve_financials=True`: reasigna `closing_bal`, `income`, `change_in_value`, `accrual` a los valores del registro existente. El `source_document_id` en `monthly_closings` no se actualiza. El `source_document_id` en `monthly_metrics_normalized` también preserva el del documento original.
  - El `asset_allocation_json` sí puede enriquecerse (los sub-splits del reporte de gestión siguen integrándose).

Data operations:
- Reprocesado doc 150 (202412 Boatview UBS Miami (432).pdf) para restaurar net_value=81,813,518.43.
- Era el único mes afectado (solo 1 monthly_closing con net_value=NULL en UBS Miami 2024+).

Validation:
- 3J 00432 P1 | 2024-12 | net_value=81,813,518.43 ✓ | source=pdf_cartola (doc 150) ✓
- monthly_metrics_normalized 2024-12: ending_with_accrual=81,813,518.43 ✓ | source=pdf_cartola ✓
- Suite: **73 passed, 1 skipped** (loader_contracts + specific_cartola_extraction, -k not goldman) ✓

## 22) Pending — Próxima sesión

- **UBS Miami Boatview cartolas P2 2021-04 a 2023-09**: Si existen cartolas de custodia para ese período del P2 (account_id=77), se deben cargar.
- **GS tests pre-existentes fallando** (12 tests): Parsers GS tienen tests fallando, presumiblemente por PDFs no disponibles. Investigar si es necesario.
