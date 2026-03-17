# SESSION_STATE - Current Working State

Last updated: 2026-03-17
Owner: JTROSS + Codex
Branch: master

## 0) Handoff (2026-03-17 - Alternativos Excel)
- **Objetivo del bloque:** Integrar el Excel `Alternativos.xlsx` como motor independiente de carga dentro de `Carga > Excel / CSV`, persistiendo solo en `monthly_metrics_normalized` y exponiendolo solo en `Resumen` y `Detalle`.
- **Decision estructural:** `Alternativos` se trata como una cartola adicional con parser propio (`parsers/excel/alternatives.py`). Para reporting se materializa como banco sintetico `alternativos` / `Alternativos`, con subcuentas `investment` agregadas por `sociedad + clase de activo + estrategia + moneda`.
- **Reglas de parseo cerradas:** se excluyen `Ecoterra` y `El Faro`; si existe columna `EUR` seguida por la misma posicion en `USD`, se toma solo `USD`; la hoja `Movimientos` se invierte a signo de contribucion al activo (capital call/subscripcion positivo para reporting, distribucion/retiro negativo).
- **Persistencia / reporting:** `backend/services/data_loading_service.py` ahora crea/actualiza cuentas sinteticas `alternativos`, limpia y recarga sus filas en `monthly_metrics_normalized`, y no crea `monthly_closings`. `backend/routers/data.py` extiende `Summary` y `Personal` para incluir filas `normalized-only` sin cierre historico, manteniendo `monthly_closings` como fallback permitido.
- **UI cerrada:** `frontend/pages/upload.py` agrega `excel_alternatives` en `Excel / CSV`, envia `bank_code=alternativos`, muestra mensaje explicativo y confirma filas normalizadas + subcuentas creadas/actualizadas. El filtro de documentos tambien reconoce `excel_alternatives`.
- **Etiquetas / visual ordenado:** las subcuentas de `Alternativos` preservan `detail_label = sociedad | clase | estrategia | moneda` como metadata, pero `Detalle por Cuenta` agrupa visualmente solo como `Sociedad-ALT-PE` o `Sociedad-ALT-RE`.
- **Normalizacion adicional cerrada:** el parser canoniza nombres de sociedad para que los consolidadores de la app cierren con el resto de bancos: `Ect Intl -> Ecoterra Internacional`, `Ect RE -> Ecoterra RE`, `Ect RE II -> Ecoterra RE II`, `Ect RE III -> Ecoterra RE III`.
- **Presentacion cerrada:** `Detalle por Activo` ya muestra `PE` y `RE`; `Resumen` y `Detalle` exponen filtros pseudo `pe` / `re` (`Private Equity (PE)` y `Real Estate (RE)`), y `ALT` / `PE` / `RE` usan tonos verdes en los graficos vinculados a Alternativos.
- **IDs / cuentas:** el `identification_number` de cada cuenta sintetica de `Alternativos` usa el `NEMO` sin espacios, truncado a 5 caracteres. El banco sintetico sigue siendo `Alternativos`; las subcuentas continúan agregadas por `sociedad + clase + estrategia + moneda`.
- **Presets de consolidado alineados:** `Mi Investments` incluye `Boatview`, `Telmar`, `White Alaska`, `Ecoterra RE`, `Ecoterra RE II`, `Ecoterra RE III`. `Mi Inv + Ect. Int` y `Mi Inv + Ect. Int+ Armel` ahora heredan tambien esas tres sociedades RE, ademas de `Ecoterra Internacional` y, cuando corresponde, `Armel Holdings`.
- **Validacion numerica cerrada:** tras reprocesar `Alternativos.xlsx`, `Detalle` / `Resumen` para `2025-12` muestran `Alternativos = 149.612.606,14` bajo el scope `Boatview + Telmar + White Alaska + Ecoterra Internacional + Ecoterra RE + Ecoterra RE II + Ecoterra RE III`. El valor `128.884.707,18` era solo el subconjunto sin las tres sociedades RE.
- **Tests corridos:** `.\.venv\Scripts\python.exe -m pytest tests/test_excel_alternatives_parser.py tests/test_loader_contracts.py tests/test_normalized_reporting_layer.py tests/test_asset_taxonomy.py -q` -> `49 passed`.
- **Validacion operativa:** parser corrido contra `Documentos/Excel/Alternativos.xlsx` -> `success`, `2185` filas. Reproceso real del documento `Alternativos.xlsx` ya cargado: `2185` filas normalizadas, sin errores. Reinicio real con `./scripts/stop.ps1` + espera 2s + `./scripts/start.ps1`; backend `200` en `http://localhost:8000/api/v1/health`; frontend `200` en `http://localhost:8501`.
- **Pendiente inmediato recomendado:** QA visual final de `Resumen` y `Detalle` con presets `Mi Investments` y `Mi Inv + Ect. Int+ Armel`, y luego respaldo formal del estado ya consolidado.

## 0) Handoff (2026-03-17)
- **Estado general:** La app principal esta operativa y el criterio arquitectonico se mantiene: `monthly_metrics_normalized` es la unica SSOT mensual de reporting; `monthly_closings` queda como historico/fallback permitido; frontend sigue siendo presentacion solamente.
- **Respaldo formal vigente:** branch `master`, commit `2ffd74b9013e478f1c7fa902a166bcda984f44a9`, tag `20260316_170000_reporting_visual_checkpoint_20260316`, snapshot `data/snapshots/20260316_170000_reporting_visual_checkpoint_20260316`, snapshot hash `87982dfbf14a7479b0ea6085714cdf0ab657ddb2a3f36bfc9748db15f28141bd`. Restore exacto: `python scripts/restore.py --tag 20260316_170000_reporting_visual_checkpoint_20260316`.
- **Cambios recientes de reporting / UI ya cerrados:**
  - Navegacion principal: `Detalle`, `Mandatos`, `ETF`, `Resumen`, `Carga`, `Operacional`; en `Operacional` quedo solo `Salud BD`.
  - `Detalle` consume payload backend-driven (`returns_panel` + `detail_views`) y ya no hace calculos financieros arriba.
  - `Detalle` ahora tiene filtros dependientes, tabla de movimientos 12M, secciones por `Banco`, `Cuenta`, `Sociedad` y `Activo`, y se elimino el warning amarillo de Streamlit por `Session State`.
  - `Detalle`: `Mi Investments` ahora incluye `Boatview`, `Telmar`, `White Alaska`, `Ecoterra RE`, `Ecoterra RE II` y `Ecoterra RE III`.
  - `Detalle por Cuenta` usa abreviacion `sociedad-banco-cuenta-id` y la columna visible es `Mov mes`.
  - `Detalle por Activo` existe debajo de `Detalle por Sociedad` y por ahora usa solo composicion ETF.
  - `Mandatos` tiene filtro global `Con Caja / Sin Caja`, ETF queda siempre `Sin Personal`, la tabla principal muestra `Monto` sin caja + `Caja` + `Monto total`, y el grafico `% por tipo de activo` incluye `Portafolio ETF` y linea horizontal al `60%`.
  - `ETF` ya dejo consistente el primer grafico con la tabla final para `Sin Caja`, invirtio ejes del primer grafico (mensual izquierda / YTD derecha) y usa el desglose de activos centralizado para `% por tipo de activo en cada banco`.
  - `Salud BD` anota `YTD BBH incluye prior adjustments` cuando el desfase de `movements_ytd` cuadra con `prior_period_adjustments`; esto es solo nota de auditoria, no mutacion de data.
- **Cambios recientes de parser/loader ya cerrados:**
  - JPM old `brokerage/etf`: la derivacion de caja desde holdings cash-like quedo aislada en `backend/services/data_loading_service.py` solo para poblar/backfillear `cash_value`.
  - UBS Suiza `Boatview brokerage 206-560552-01`: se corrigio el parser para meses `2025-05`, `2025-08`, `2025-11` con `Liquidity` mal concatenado y luego se revisaron todas las cartolas cargadas de esa cuenta sin encontrar mas casos del mismo patron.
  - Goldman Sachs mandatos antiguos: el parser ahora consolida `sub_portfolio_overviews` cuando falta el overview top-level, cerrando faltantes visibles en `Salud BD` para `097-4` y `214-9`.
- **Taxonomia de activos vigente:**
  - Archivo canonico: `asset_bucket_dictionary.json`
  - Helper de carga: `asset_taxonomy.py`
  - Buckets: `RV DM`, `RV EM`, `RF IG Short`, `RF IG Long`, `HY`, `Alternativos`, `Real Estate`, `Caja`
  - Mapping explicito: `IWDA -> RV DM`, `IEMA -> RV EM`, `VDCA -> RF IG Short`, `VDPA -> RF IG Long`, `IHYA -> HY`, `ALT -> Alternativos`, `ALT RE -> Real Estate`, aliases de money market / deposits / cash / caja -> `Caja`
  - Regla: no duplicar este diccionario en paginas ni routers; se reutiliza desde el archivo central.
- **Tests mas recientes corridos en este bloque:** `.\.venv\Scripts\python.exe -m pytest tests/test_normalized_reporting_layer.py -q` -> `22 passed`.
- **Estado operativo actual:** backend health OK en `http://localhost:8000/api/v1/health`; frontend principal OK en `http://localhost:8501`.
- **Pendiente inmediato recomendado:** QA visual puntual de `Detalle` (alineaciones finas tabla/graficos) y decidir si la taxonomia central de activos debe extenderse luego a mandatos/brokerage a nivel de datos persistidos, o mantenerse temporalmente ETF-only.

## 0) Handoff (2026-03-16)
- **Objetivo del bloque:** Revisar por quÃ© la app todavÃ­a podÃ­a mostrar `54.185` en `Boatview UBS Suiza 206-560552-02 2025-01` si la tabla canÃ³nica ya estaba en `0`.
- **Hallazgo:** No habÃ­a una lectura residual de `parsed_data_json`, `parsed closing` ni `total agregado` saltÃ¡ndose `monthly_metrics_normalized` en reporting. `Summary`, `Mandates`, `Personal` y `Salud BD` ya devolvÃ­an `0` para el mandato `206-560552-02`. El `54.185` visible venÃ­a de una cuenta hermana real del mismo `Boatview + UBS`: `206-560552-01` (`brokerage`), que la UI estaba agregando por banco/sociedad sin desglosar cuentas.
- **Cambios aplicados:** `backend/routers/data.py`: `Personal` ahora expone `id/account_number` por fila en `entities_table`. `frontend/pages/personal.py`: `Detalle por Banco` muestra columna `Cuentas visibles` para desambiguar agregados multi-cuenta. `frontend/pages/summary.py`: aviso cuando el consolidado combina varias cuentas visibles del mismo `Banco + Sociedad`. `tests/test_normalized_reporting_layer.py`: regresiÃ³n `test_personal_exposes_sibling_accounts_separately_when_one_normalized_value_is_zero`.
- **Tests corridos:** `tests/test_normalized_reporting_layer.py -q -k "personal_exposes_sibling_accounts_separately_when_one_normalized_value_is_zero or summary_prefers_normalized_monthly_metrics or summary_zeroes_negative_ubs_return" -p no:cacheprovider` â†’ `3 passed`.
- **Estado operativo:** App principal levantada y verificada OK en `http://localhost:8501`; backend health OK en `http://localhost:8000/api/v1/health`.
- **Pendiente inmediato:** ValidaciÃ³n visual del usuario en `Resumen` y `Detalle`, confirmando que el `54.185` solo aparezca cuando corresponda a `206-560552-01` y no al mandato `206-560552-02`.

## 0.1) Cleanup pass (2026-03-16)
- **Objetivo:** Reordenar reporting para que vuelva a apoyarse primero en la capa normalizada, sin tocar reglas especiales de loaders/parsers por banco.
- **Cambios aplicados:** `backend/routers/data.py`: `cash_value` normalizado ahora prevalece sobre `asset_allocation_json` historico y otros fallbacks; `asset_allocation_json` normalizado ahora prevalece en `Mandates` y en `/data/asset-allocation-report`; el fallback a `ParsedStatement` para caja JPM se mantiene solo como ultimo recurso. `frontend/pages/mandates.py`: el KPI usa `*_ytd` ya entregado por backend en vez de recomputarlo en frontend, y el pin visual de `Total` queda delegado al renderer comun. `scripts/start.ps1`: ahora escribe logs runtime y muestra `stdout/stderr` cuando el proceso no responde.
- **Decisiones:** No se tocaron reglas UBS/JPM/GS especificas del loader. No se elimino por completo el fallback raw de caja porque en la BD local todavia hay muchos `cash_value = NULL` en `JPM brokerage/ETF`; quitarlo de golpe podia mover demasiados casos reales. Se degradÃ³ a ultimo recurso, pero no se mantiene por delante de la capa normalizada.
- **Tests:** Suite completa `166 passed, 1 skipped`. Reinicio real con `./scripts/start.ps1` verificado OK; backend `200`, frontend `200`.
- **Pendiente:** Si se quiere cerrar totalmente la desviacion arquitectonica de caja, primero hay que completar/backfillear `cash_value` normalizado en las filas donde hoy sigue nulo.

## 0.2) JPM cash normalization closure (2026-03-16)
- **Objetivo:** Sacar la ultima interpretacion raw de caja fuera de reporting y moverla a la normalizacion JPM antigua, manteniendo el principio SSOT en `monthly_metrics_normalized`.
- **Cambios aplicados:** `backend/services/data_loading_service.py`: nueva derivacion loader-side de `cash_value` para `jpmorgan brokerage/etf` cuando falta `asset_allocation_json` pero existen holdings cash-like persistidos (`deposit sweep`, `liquidity sweep`, `LI-LIQ`, `prime MM`, `pending sales`, etc.). Esa derivacion corre tanto en carga nueva como en `_refresh_normalized_activity_from_monthly_closings`. `backend/routers/data.py`: se elimino el fallback de caja que leia `ParsedStatement` directamente desde reporting; `Summary`/`Personal` vuelven a consumir solo normalized + fallback historico permitido. Tests nuevos en `tests/test_loader_contracts.py` y `tests/test_summary_returns.py`.
- **Decisiones:** La interpretacion de caja queda aislada en loader/backfill JPM, no en endpoints. Si una cartola antigua no trae ni `asset_allocation` ni holdings cash-like parseados, `cash_value` sigue `NULL` y eso se trata como falta de dato de origen, no como permiso para reinterpretar raw desde reporting.
- **BD local:** Se backfillearon `91` grupos cuenta/año JPM (`brokerage` + `etf`) con la rutina normalizada. Los `cash_value = NULL` bajaron de `729` a `13`. Los `13` remanentes corresponden a meses donde el `ParsedStatement` no trae filas de holdings o solo trae instrumentos de renta fija, sin una linea de caja pura identificable.
- **Tests:** Focalizados `4 passed`; suite completa `169 passed, 1 skipped`.
- **Pendiente:** Si se quiere cerrar los `13` remanentes, el ajuste debe ir al parser JPM especifico de esas cartolas antiguas o a un reproceso focalizado, nunca a reporting.

## 1) Current Product Status
- App promoted to official main environment.
- Main endpoints for `Summary`, `Mandates`, `ETF`, `Personal` are functional with real DB queries.
- Preview/staging local flow is available (`8100/8601`) with separate DB.
- `monthly_metrics_normalized` is the canonical monthly reporting layer and is already used by reporting endpoints.
- Reporting endpoints are expected to be read-only over persisted monthly data; identity/YTD stay as controls, not data completion rules.
- Mandate asset-allocation normalization (Cash / Fixed Income / Equities) is implemented.
- `Salud BD` is active in main UI and includes identity, missing-components, and YTD control surfaces.
- Raw PDFs are still stored and remain operationally necessary for reprocesos, parser hardening, and audit traceability.

## 2) Current Known Priorities
1. Visual QA of the current `Detalle`, `Mandatos` and `ETF` layouts after the latest reporting polish.
2. Decide if the centralized asset taxonomy should next populate normalized monthly data for non-ETF accounts, or remain ETF-only until parser/loader work is ready.
3. Keep `Salud BD` as audit-only and continue closing real parser/loader gaps instead of adjusting reporting.
4. Define later the archive/retention strategy for raw PDFs once data is stable and approved.

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

## 6) Current Architecture / Reporting Decisions
- Single source of truth for monthly reporting: `monthly_metrics_normalized`.
- `monthly_closings` remains historical source + fallback, but if normalized data exists it must drive reporting.
- `backend/routers/data.py` should not recreate a second/third interpreter of monthly data from raw JSON or YTD.
- `Salud BD` must alert on persisted data inconsistencies; it should not silently mutate values to make identity pass.
- Tables/graphs in reporting should consume normalized monthly values; parser logic and loader logic live below the API layer.

## 7) High-Signal Change Digest (what is already in code)
- Normalized layer hardening:
  - `monthly_metrics_normalized` now carries explicit monthly fields needed by reporting, including YTD controls and asset allocation JSON.
  - Reporting endpoints were refactored away from helper-based reinterpretation of movements/profit/YTD and toward persisted monthly values.
- `Salud BD` / UI:
  - Added the `Nota` field/filter for identity issues where statement beginning value mismatches previous ending value but audited ending value prevails.
  - `YTD BBH incluye prior adjustments` note exists for BBH movement-YTD mismatches explained by prior-period adjustments in the statement.
  - UI displays `ubs` as `ubs_suiza` in health tables.
  - `Personal` page no longer auto-loads broad data without scope filters.
  - `Operacional` UI now keeps only the `Salud BD` subtab.
- ETF / society fixes:
  - Added `Con Personal / Sin Personal` behavior around `Raíces LP`.
  - ETF total return row is computed in backend from aggregated values instead of ad hoc UI recomputation.
  - ETF asset breakdown now uses the centralized taxonomy file and bank-level percentage payload from backend.
  - `Raíces LP` coverage was relinked/reprocessed so reporting starts from historical documents again.
- Detail / mandates visual layer:
  - `Detalle` consumes backend-prepared `returns_panel` and `detail_views`, including `Detalle por Activo` from ETF composition only.
  - `Detalle por Cuenta` uses abbreviated labels `sociedad-banco-cuenta-id`.
  - `Mandatos` has a global `Con Caja / Sin Caja` mode and keeps ETF permanently `Sin Personal`.
- Goldman Sachs:
  - OCR fallback exists for garbled PDFs, including period/overview tolerance for OCR spacing issues.
  - Legacy mandate statements can consolidate `sub_portfolio_overviews` when a top-level overview is missing.
- JPMorgan:
  - `ETF`: blank monthly `Income & Distributions` / `Change In Investment Value` fields no longer inherit YTD as monthly.
  - `Custody`: `Net Security Contributions` is included in movements where applicable.
- UBS Miami:
  - `Change in value of accrued interest` regex was hardened for multiline labels.
- UBS Suiza:
  - Multi-portfolio statements use the portfolio selected by account suffix (`-01`, `-02`) instead of summed totals.
  - Negative ending values are valid and now parsed/persisted.
  - In UBS-only reporting views, monthly return shows `0%` when current or previous position is negative.
  - A focused UBS audit/reproceso later found `33` persisted portfolio mismatches and corrected them with final verification `remaining = 0`.
  - From `2026-03-15`, UBS-only identity policy is stricter: auditable `ending value` always prevails over a mismatching next-month `beginning value`; quarterly tables may refine prior-month `movements`; `profit` must absorb any continuity gap via identity recomputation. This is intentionally isolated to UBS Suiza and must not bleed into other banks.

## 8) Session Log Template
Use this short format when closing a work block:
- Date:
- Goal:
- Files changed:
- Decisions made:
- Tests run + result:
- Pending next actions:

## 9) Cierre de bloque (2026-03-10)
- **Objetivo:** Que al eliminar documentos seleccionados (tab Documentos) se muestre mensaje en verde ("Su selección ha sido eliminada") y que los errores del DELETE no se traguen.
- **Cambios:** `frontend/pages/upload.py`: obtener IDs desde la columna "ID" de `edited_df` (tabla que ve el usuario) en lugar de por índice con `df_docs`; mostrar `st.success("✅ Su selección ha sido eliminada.")` cuando `deleted > 0`; mostrar `st.error` por cada DELETE fallido; `st.warning` si había selección pero no se eliminó ninguno.
- **Decisiones:** Usar la misma tabla que el usuario edita (`edited_df`) para leer los IDs a eliminar, evitando desalineación entre índices y filas (causa de que no pasara nada al pulsar "Eliminar seleccionados").
- **Tests:** No se añadieron tests nuevos; suite existente. Regresión visual en tab Documentos.
- **Pendientes:** Validar en entorno del usuario que el mensaje en verde y el flujo de eliminación se ven correctamente tras reinicio del frontend.

## 10) Cierre de bloque (2026-03-10)
- **Objetivo:** Auditoría read-only de salud BD + alertas de identidad/YTD + corregir carga mensual JPMorgan bonds sin usar YTD para forzar datos.
- **Cambios:** `backend/routers/data.py`: nuevo endpoint `/data/health-report` read-only con controles de identidad mensual, faltantes y diferencias YTD. `frontend/pages/operational.py`: nueva pestaña `Salud BD`. `frontend/pages/summary.py`, `mandates.py`, `etf.py`, `personal.py`: alertas visibles cuando los filtros activos muestran inconsistencias. `frontend/components/data_health.py`: helper compartido. `backend/services/data_loading_service.py`: fallback para `parsers.jpmorgan.bonds` y `parsers.jpmorgan.custody` usando `portfolio_activity` cuando falta `account_monthly_activity`; controles YTD pasan a warning-only (sin sobrescribir movimientos/utilidad).
- **Decisiones:** La identidad mensual `valor_final - movimientos - utilidad = valor_final_anterior` queda como control obligatorio. YTD se usa solo como control; no se usa para completar ni corregir datos mensuales. No se tocó UBS Suiza.
- **Tests:** `135 passed, 1 skipped` con `.venv`.
- **Pendientes:** Validar en preview la nueva pestaña `Salud BD` y las alertas en tablas. Siguiente paso sugerido: revisar y corregir, por separado, los faltantes históricos de `JPMorgan mandato` (principalmente 2020-2021) sin mezclarlo con `bonds` ni con otros bancos.

## 11) Cierre de bloque (2026-03-11)
- **Objetivo:** Corregir interpretación mensual de `JPMorgan brokerage` en casos caja-only / layout inconsistente, evitando tomar YTD como mensual y evitando duplicar utilidad cuando `Change In Investment Value` replica caja.
- **Cambios:** `parsers/jpmorgan/brokerage.py`: nueva extracción por línea de `Portfolio Activity` con soporte a filas que traen solo YTD; si la fila mensual viene en blanco, se interpreta como `0` y se conserva YTD solo como control. `Change In Investment Value` se excluye de `utilidad` cuando duplica `Net Contributions/Withdrawals` en valor absoluto. `backend/services/data_loading_service.py`: `account_ytd` deja de rellenar `income` / `change_investment` mensuales para `parsers.jpmorgan.brokerage`; además se registran notas de heurística en `validation_logs`. Tests nuevos/ajustados en `tests/test_specific_cartola_extraction.py` y `tests/test_loader_contracts.py`.
- **Casos cubiertos:** `Armel Canada` (`5000`), `La Guardia` (`1008`), `Mi Investments` (`1000`) y `Ecoterra RE` (`2008`) bajo el patrón `brokerage` con caja predominante o filas mensuales vacías.
- **Decisiones:** En `brokerage`, los valores YTD se conservan solo como referencia/control. La utilidad mensual se interpreta como `Income & Distributions` mensual + `delta accruals`, y solo suma `Change In Investment Value` si no duplica caja. Si el mensual viene en blanco pero el YTD sí aparece, mensual = `0`.
- **Tests:** Suite base previa `135 passed, 1 skipped`. Tests focalizados posteriores: `25 passed, 1 skipped`.
- **Pendientes:** Reprocesar en preview los documentos históricos afectados para que la BD de prueba refleje la nueva lógica. Revisar después en `Salud BD` si quedan meses históricos faltantes en `Ecoterra Internacional` bonds (`0900`, `1100`) y si requieren reproceso adicional o solo auditoría.

## 12) Cierre de bloque (2026-03-11)
- **Objetivo:** Corregir `JPMorgan brokerage` cuando `Net Contributions/Withdrawals` viene con signo `-` o partido en varias líneas, y ajustar `Armel Canada 2025-05` para conservar `Change In Investment Value` dentro de utilidad.
- **Cambios:** `parsers/jpmorgan/brokerage.py`: `_ACTIVITY_VALUE_RE` ahora acepta montos negativos con signo; la extracción de `Portfolio Activity` dejó de depender de una sola línea y ahora toma el bloque entre labels, permitiendo leer movimientos que `pdfplumber` separa en línea siguiente. También se eliminó la exclusión automática de `Change In Investment Value` en utilidad cuando coincide en magnitud con `Net Contributions/Withdrawals`, porque puede reflejar transferencias de securities y no duplicación espuria. `tests/test_specific_cartola_extraction.py`: nuevas regresiones para signo negativo, bloque partido y utilidad con transferencias. Preview reprocesada para `Armel Canada` (`2025-04/05/07`), `La Guardia` (`2025-03`), `Mi Investments` (`2025-05/10`) y `Ecoterra RE` (`2025-03/04/05/07/10`).
- **Decisiones:** La causa raíz no era `Salud BD`; los `parsed_statements` ya venían mal desde el parser. El problema era de extracción textual del bloque `Portfolio Activity` en JPMorgan brokerage. Para estos casos, los movimientos correctos quedaron cargados desde la cartola, sin usar YTD para rellenar mensual.
- **Tests:** `141 passed, 1 skipped` con `.venv`; tests focalizados de `test_specific_cartola_extraction.py`: `17 passed, 1 skipped`.
- **Pendientes:** En preview quedan 2 incumplimientos de identidad de `JPMorgan brokerage 2025`, pero ya no son los casos corregidos aquí: `Los Misioneros Int.` (`E92755009`, 2025-07) y `Rengiroa` (`E99087000`, 2025-12`).

## 13) Cierre de bloque (2026-03-11)
- **Objetivo:** Hacer que `Salud BD` no marque como faltante un `movements=None` cuando la identidad mensual demuestra que el movimiento implícito es `0`.
- **Cambios:** `backend/routers/data.py`: nueva interpretación read-only `_resolve_audit_movements()` para auditoría; si falta `movements` pero `ending_value - previous_ending - profit` da aproximadamente `0`, la auditoría trata movimientos como `0.0` solo para el reporte, sin mutar BD. `tests/test_normalized_reporting_layer.py`: dos regresiones nuevas, una para “None pero implícitamente 0” y otra para “None con movimiento no-cero sigue faltante”.
- **Operación preview:** se reinició preview con scripts (`stop_preview.ps1` + `start_preview.ps1`), lo que resincronizó la DB preview desde la oficial; después se reprocesaron todas las cartolas `JPMorgan brokerage 2025` de `Armel Canada`, `La Guardia`, `Mi Investments` y `Ecoterra RE` para restaurar el estado corregido.
- **Resultado en preview:** filtro `JPMorgan + 2025 + brokerage` queda con `identity_mismatch_count = 0` y `missing_components_count = 0`. Persisten solo alertas YTD (`36` movimientos, `36` utilidad).
- **Tests:** suite completa `143 passed, 1 skipped`.

## 14) Cierre de bloque (2026-03-12)
- **Objetivo:** Corregir la familia `JPMorgan bonds` (`Ecoterra Internacional 0900/1100` y `North Harbor 4700`) donde `Salud BD` mostraba `None` en movimientos/utilidad y `Ecoterra 0900` abril-2025 empezó a caer con incumplimiento de identidad.
- **Cambios:** `backend/services/document_service.py`: el ruteo de `pdf_cartola` JPMorgan ahora reconoce filenames tipo `BO` / `bond` / `bono` como `parsers.jpmorgan.bonds`, y el fallback por `statements-XXXX` ya puede inferir `account_type = bonds` desde el maestro de cuentas. No se cambió el parser `bonds`; el problema principal era de selección de motor y de estado cargado en preview. Preview reiniciada con scripts (`stop_preview.ps1` + `start_preview.ps1`) y luego reprocesadas las 36 cartolas `bonds 2025` de `1530900`, `1531100` y `1584700`.
- **Decisiones:** La línea `Portfolio Activity` sí era legible en estos PDFs. En `0900`, desde abril el autodetect estaba mandando archivos `BO` a `parsers.jpmorgan.custody`, lo que alteró la interpretación mensual; además, varios `None` visibles en preview eran arrastre de datos antiguos resincronizados desde la DB oficial y no de una imposibilidad actual de lectura del PDF. La corrección fue forzar el ruteo correcto a `bonds` y reprocesar.
- **Resultado en preview:** `1530900` abril-diciembre quedó nuevamente bajo `parsers.jpmorgan.bonds`; las tres cuentas objetivo (`1530900`, `1531100`, `1584700`) quedaron sin `change_in_value` / `income` nulos en `monthly_closings 2025`. La identidad mensual de esas tres cuentas quedó cuadrando con diferencias de redondeo de centavos (`<= 0.04`).
- **Tests:** suite completa `143 passed, 1 skipped`.
- **Pendientes:** Si el usuario sigue viendo filas faltantes al filtrar `JPMorgan + 2025`, revisar los otros `NULL` restantes fuera de este subgrupo `bonds`, porque todavía existen casos en otras cuentas JPMorgan no tocadas en este bloque.

## 15) Cierre de bloque (2026-03-12 / 2026-03-13)
- **Objetivo:** Consolidar la refactorización de reporting para que la app consuma la capa normalizada, corregir tablas/controles visibles en `Salud BD`, y cerrar edge cases grandes en GS, JPM y UBS.
- **Cambios:** `backend/db/models.py` + migración Alembic: columnas YTD y `asset_allocation_json` para `monthly_metrics_normalized`. `backend/services/data_loading_service.py`: carga completa de normalized layer, persistencia explícita de UBS, priorización de portafolio seleccionado, y refresco consistente hacia reporting. `backend/routers/data.py`: endpoints de reporting/health más lectores y menos interpretativos; nota/filtro para beginning vs prev ending mismatch; filtros ETF `Con Personal / Sin Personal`; `Raíces LP` incluido en sociedades; total ETF calculado en backend. `frontend/pages/etf.py`, `personal.py`, `operational.py`: filtro personal, columnas dinámicas, guardas de scope y mejoras de `Salud BD`. `parsers/goldman_sachs/_gs_common.py`: OCR fallback. `parsers/jpmorgan/etf.py` / `custody.py`: fixes de YTD blank y movimientos con securities contributions. `parsers/ubs_miami/custody.py`: regex multiline. `parsers/ubs/custody.py`: soporte a negativos, layouts adicionales, selección obligatoria de portafolio por sufijo y retorno `0%` en vistas UBS negativas. Tests extendidos en `tests/test_loader_contracts.py`, `tests/test_normalized_reporting_layer.py`, `tests/test_specific_cartola_extraction.py`, `tests/test_summary_returns.py`.
- **Casos/sociedades cubiertos:** `Raíces LP`, `Boatview JPM mandato 3400`, `Boatview UBS Suiza 206-560552-02`, `Telmar UBS Suiza 206-560402-02`, `Armel Canada UBS Suiza 206-579852-01`, `Mi Investments UBS Suiza 206-579943-01`, varios JPM ETF/brokerage y GS `Telmar` con OCR.
- **Decisiones:** `monthly_metrics_normalized` queda como SSOT mensual para reporting. No deben existir más interpretadores paralelos en UI/endpoints a partir de identidad o YTD. `monthly_closings` se mantiene como base histórica/fallback y `raw_documents`/PDFs se conservan porque todavía se requieren para reprocesos y auditoría.
- **Resultado:** Reproceso focalizado de `UBS Suiza` auditó `238` filas, encontró `33` discrepancias entre BD y `selected_portfolio`, reprocesó esas `33` y terminó con verificación `remaining = 0`.
- **Tests:** regresiones UBS/reporting `44 passed, 1 skipped`; suite completa posterior `150 passed, 1 skipped`.
- **Pendientes:** Validación visual del usuario en app oficial, especialmente `Salud BD` para `UBS Suiza`; luego decidir respaldo oficial y, más adelante, estrategia de archivado de PDFs.

## 16) Next Action Template (for user prompting Codex)
"Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status.
Luego trabaja solo en [ruta/feature concreta]."

## 16.1) Current Prompt Starter
"Contextualizate solo con `AGENT_CONTEXT.md` + `SESSION_STATE.md` + `git status --short` + archivos relevantes. Mantén intactas las reglas: `monthly_metrics_normalized` como unica SSOT mensual de reporting, `monthly_closings` solo como historico/fallback permitido, frontend solo presentacion, sin interpretadores nuevos de datos arriba. Ademas reutiliza la taxonomia central de activos en `asset_bucket_dictionary.json` / `asset_taxonomy.py` y no la dupliques localmente. Luego trabaja solo en [tarea concreta]."

## 17) Cierre de bloque (2026-03-15)
- **Objetivo:** Aislar en `UBS Suiza` la regla de continuidad donde el `ending value` auditado del mes anterior prevalece sobre el `beginning value` de la cartola siguiente, dejando `profit` como variable de ajuste y aprovechando movimientos de tablas trimestrales solo dentro del motor UBS.
- **Cambios:** `backend/services/data_loading_service.py`: el backfill trimestral UBS ahora puede refinar `change_in_value` de meses previos no cierre de trimestre aun si ya existe cartola directa, sin tocar `net_value` auditado; `income`/`profit` UBS se recalcula por identidad contra el `prev ending` auditado en todos los meses donde hay `movements` y mes previo disponible. `tests/test_loader_contracts.py`: nuevas regresiones para continuidad con mismatch `beginning vs prev ending` y para refinamiento trimestral de `movements` sobre meses directos no trimestrales.
- **Decisiones:** Esto es **solo para UBS Suiza** (`bank_code = ubs`). No se generaliza a JPM, GS, BBH ni UBS Miami. En UBS, `ending` nunca se fuerza, `movements` vienen de cartola/tabla trimestral UBS, y `profit` absorbe la diferencia de identidad.
- **Tests:** Focalizados UBS/reporting: `32 passed, 25 deselected`. Loader UBS nuevo: `5 passed`.
- **Pendientes:** Reproceso focalizado de cartolas UBS afectadas en la BD local/oficial y validación visual en `Salud BD`, especialmente `Boatview 206-560552-02 2025` y `Mi Investments 206-579943-01`.
