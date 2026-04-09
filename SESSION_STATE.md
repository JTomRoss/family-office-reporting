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
