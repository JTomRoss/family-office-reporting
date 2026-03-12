# SESSION_STATE - Current Working State

Last updated: 2026-03-12
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

## 8) Cierre de bloque (2026-03-10)
- **Objetivo:** Que al eliminar documentos seleccionados (tab Documentos) se muestre mensaje en verde ("Su selección ha sido eliminada") y que los errores del DELETE no se traguen.
- **Cambios:** `frontend/pages/upload.py`: obtener IDs desde la columna "ID" de `edited_df` (tabla que ve el usuario) en lugar de por índice con `df_docs`; mostrar `st.success("✅ Su selección ha sido eliminada.")` cuando `deleted > 0`; mostrar `st.error` por cada DELETE fallido; `st.warning` si había selección pero no se eliminó ninguno.
- **Decisiones:** Usar la misma tabla que el usuario edita (`edited_df`) para leer los IDs a eliminar, evitando desalineación entre índices y filas (causa de que no pasara nada al pulsar "Eliminar seleccionados").
- **Tests:** No se añadieron tests nuevos; suite existente. Regresión visual en tab Documentos.
- **Pendientes:** Validar en entorno del usuario que el mensaje en verde y el flujo de eliminación se ven correctamente tras reinicio del frontend.

## 9) Cierre de bloque (2026-03-10)
- **Objetivo:** Auditoría read-only de salud BD + alertas de identidad/YTD + corregir carga mensual JPMorgan bonds sin usar YTD para forzar datos.
- **Cambios:** `backend/routers/data.py`: nuevo endpoint `/data/health-report` read-only con controles de identidad mensual, faltantes y diferencias YTD. `frontend/pages/operational.py`: nueva pestaña `Salud BD`. `frontend/pages/summary.py`, `mandates.py`, `etf.py`, `personal.py`: alertas visibles cuando los filtros activos muestran inconsistencias. `frontend/components/data_health.py`: helper compartido. `backend/services/data_loading_service.py`: fallback para `parsers.jpmorgan.bonds` y `parsers.jpmorgan.custody` usando `portfolio_activity` cuando falta `account_monthly_activity`; controles YTD pasan a warning-only (sin sobrescribir movimientos/utilidad).
- **Decisiones:** La identidad mensual `valor_final - movimientos - utilidad = valor_final_anterior` queda como control obligatorio. YTD se usa solo como control; no se usa para completar ni corregir datos mensuales. No se tocó UBS Suiza.
- **Tests:** `135 passed, 1 skipped` con `.venv`.
- **Pendientes:** Validar en preview la nueva pestaña `Salud BD` y las alertas en tablas. Siguiente paso sugerido: revisar y corregir, por separado, los faltantes históricos de `JPMorgan mandato` (principalmente 2020-2021) sin mezclarlo con `bonds` ni con otros bancos.

## 10) Cierre de bloque (2026-03-11)
- **Objetivo:** Corregir interpretación mensual de `JPMorgan brokerage` en casos caja-only / layout inconsistente, evitando tomar YTD como mensual y evitando duplicar utilidad cuando `Change In Investment Value` replica caja.
- **Cambios:** `parsers/jpmorgan/brokerage.py`: nueva extracción por línea de `Portfolio Activity` con soporte a filas que traen solo YTD; si la fila mensual viene en blanco, se interpreta como `0` y se conserva YTD solo como control. `Change In Investment Value` se excluye de `utilidad` cuando duplica `Net Contributions/Withdrawals` en valor absoluto. `backend/services/data_loading_service.py`: `account_ytd` deja de rellenar `income` / `change_investment` mensuales para `parsers.jpmorgan.brokerage`; además se registran notas de heurística en `validation_logs`. Tests nuevos/ajustados en `tests/test_specific_cartola_extraction.py` y `tests/test_loader_contracts.py`.
- **Casos cubiertos:** `Armel Canada` (`5000`), `La Guardia` (`1008`), `Mi Investments` (`1000`) y `Ecoterra RE` (`2008`) bajo el patrón `brokerage` con caja predominante o filas mensuales vacías.
- **Decisiones:** En `brokerage`, los valores YTD se conservan solo como referencia/control. La utilidad mensual se interpreta como `Income & Distributions` mensual + `delta accruals`, y solo suma `Change In Investment Value` si no duplica caja. Si el mensual viene en blanco pero el YTD sí aparece, mensual = `0`.
- **Tests:** Suite base previa `135 passed, 1 skipped`. Tests focalizados posteriores: `25 passed, 1 skipped`.
- **Pendientes:** Reprocesar en preview los documentos históricos afectados para que la BD de prueba refleje la nueva lógica. Revisar después en `Salud BD` si quedan meses históricos faltantes en `Ecoterra Internacional` bonds (`0900`, `1100`) y si requieren reproceso adicional o solo auditoría.

## 11) Cierre de bloque (2026-03-11)
- **Objetivo:** Corregir `JPMorgan brokerage` cuando `Net Contributions/Withdrawals` viene con signo `-` o partido en varias líneas, y ajustar `Armel Canada 2025-05` para conservar `Change In Investment Value` dentro de utilidad.
- **Cambios:** `parsers/jpmorgan/brokerage.py`: `_ACTIVITY_VALUE_RE` ahora acepta montos negativos con signo; la extracción de `Portfolio Activity` dejó de depender de una sola línea y ahora toma el bloque entre labels, permitiendo leer movimientos que `pdfplumber` separa en línea siguiente. También se eliminó la exclusión automática de `Change In Investment Value` en utilidad cuando coincide en magnitud con `Net Contributions/Withdrawals`, porque puede reflejar transferencias de securities y no duplicación espuria. `tests/test_specific_cartola_extraction.py`: nuevas regresiones para signo negativo, bloque partido y utilidad con transferencias. Preview reprocesada para `Armel Canada` (`2025-04/05/07`), `La Guardia` (`2025-03`), `Mi Investments` (`2025-05/10`) y `Ecoterra RE` (`2025-03/04/05/07/10`).
- **Decisiones:** La causa raíz no era `Salud BD`; los `parsed_statements` ya venían mal desde el parser. El problema era de extracción textual del bloque `Portfolio Activity` en JPMorgan brokerage. Para estos casos, los movimientos correctos quedaron cargados desde la cartola, sin usar YTD para rellenar mensual.
- **Tests:** `141 passed, 1 skipped` con `.venv`; tests focalizados de `test_specific_cartola_extraction.py`: `17 passed, 1 skipped`.
- **Pendientes:** En preview quedan 2 incumplimientos de identidad de `JPMorgan brokerage 2025`, pero ya no son los casos corregidos aquí: `Los Misioneros Int.` (`E92755009`, 2025-07) y `Rengiroa` (`E99087000`, 2025-12`).

## 12) Cierre de bloque (2026-03-11)
- **Objetivo:** Hacer que `Salud BD` no marque como faltante un `movements=None` cuando la identidad mensual demuestra que el movimiento implícito es `0`.
- **Cambios:** `backend/routers/data.py`: nueva interpretación read-only `_resolve_audit_movements()` para auditoría; si falta `movements` pero `ending_value - previous_ending - profit` da aproximadamente `0`, la auditoría trata movimientos como `0.0` solo para el reporte, sin mutar BD. `tests/test_normalized_reporting_layer.py`: dos regresiones nuevas, una para “None pero implícitamente 0” y otra para “None con movimiento no-cero sigue faltante”.
- **Operación preview:** se reinició preview con scripts (`stop_preview.ps1` + `start_preview.ps1`), lo que resincronizó la DB preview desde la oficial; después se reprocesaron todas las cartolas `JPMorgan brokerage 2025` de `Armel Canada`, `La Guardia`, `Mi Investments` y `Ecoterra RE` para restaurar el estado corregido.
- **Resultado en preview:** filtro `JPMorgan + 2025 + brokerage` queda con `identity_mismatch_count = 0` y `missing_components_count = 0`. Persisten solo alertas YTD (`36` movimientos, `36` utilidad).
- **Tests:** suite completa `143 passed, 1 skipped`.

## 13) Cierre de bloque (2026-03-12)
- **Objetivo:** Corregir la familia `JPMorgan bonds` (`Ecoterra Internacional 0900/1100` y `North Harbor 4700`) donde `Salud BD` mostraba `None` en movimientos/utilidad y `Ecoterra 0900` abril-2025 empezó a caer con incumplimiento de identidad.
- **Cambios:** `backend/services/document_service.py`: el ruteo de `pdf_cartola` JPMorgan ahora reconoce filenames tipo `BO` / `bond` / `bono` como `parsers.jpmorgan.bonds`, y el fallback por `statements-XXXX` ya puede inferir `account_type = bonds` desde el maestro de cuentas. No se cambió el parser `bonds`; el problema principal era de selección de motor y de estado cargado en preview. Preview reiniciada con scripts (`stop_preview.ps1` + `start_preview.ps1`) y luego reprocesadas las 36 cartolas `bonds 2025` de `1530900`, `1531100` y `1584700`.
- **Decisiones:** La línea `Portfolio Activity` sí era legible en estos PDFs. En `0900`, desde abril el autodetect estaba mandando archivos `BO` a `parsers.jpmorgan.custody`, lo que alteró la interpretación mensual; además, varios `None` visibles en preview eran arrastre de datos antiguos resincronizados desde la DB oficial y no de una imposibilidad actual de lectura del PDF. La corrección fue forzar el ruteo correcto a `bonds` y reprocesar.
- **Resultado en preview:** `1530900` abril-diciembre quedó nuevamente bajo `parsers.jpmorgan.bonds`; las tres cuentas objetivo (`1530900`, `1531100`, `1584700`) quedaron sin `change_in_value` / `income` nulos en `monthly_closings 2025`. La identidad mensual de esas tres cuentas quedó cuadrando con diferencias de redondeo de centavos (`<= 0.04`).
- **Tests:** suite completa `143 passed, 1 skipped`.
- **Pendientes:** Si el usuario sigue viendo filas faltantes al filtrar `JPMorgan + 2025`, revisar los otros `NULL` restantes fuera de este subgrupo `bonds`, porque todavía existen casos en otras cuentas JPMorgan no tocadas en este bloque.

## 7) Next Action Template (for user prompting Codex)
"Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status.
Luego trabaja solo en [ruta/feature concreta]."
