# CHAT_START_PROMPT - Copy/Paste for New Chats

Use one of these prompts at the start of a new chat.

## A) Standard start (recommended)
Contextualizate SOLO con:
1) AGENT_CONTEXT.md
2) SESSION_STATE.md
3) git status --short

No leas DEEP_CONTEXT.md ni todo el repo salvo que haga falta para la tarea.
Despues, trabaja en: [describe aqui la tarea].

## B) Focused start by scope
Contextualizate SOLO con:
- AGENT_CONTEXT.md
- SESSION_STATE.md
- git status --short
- estos archivos: [ruta1], [ruta2], [ruta3]

No hagas barrido completo del repo.

## C) Deep historical start (only if needed)
Contextualizate con:
- AGENT_CONTEXT.md
- SESSION_STATE.md
- DEEP_CONTEXT.md (solo secciones relevantes para [tema])
- git status --short

Luego trabaja en: [tarea].

## D) Continuity start (Mandato/ETF SSOT)
Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status --short + archivos relevantes.
Reglas obligatorias:
1. No tocar nada del agente LLM / Revision (proyecto paralelo).
2. Mantener monthly_metrics_normalized como SSOT mensual de reporting.
3. monthly_closings solo historico/fallback permitido.
4. Frontend solo presentacion (sin logica financiera nueva).
5. Mantener aislamiento estricto de motores de lectura PDF por banco y tipo de cuenta (incluyendo report_mandato por banco).

No leer cartolas/PDF en este ejercicio: ya estan extraidas y correctas.
Enfocate en [describe aqui la tarea].
Despues de cada cambio, reinicia la app si corresponde y avisa; si no, indica que solo refresque.

## E) Continuity start (Boatview 2600 - Cash 43 artifact)
Contextualizate solo con AGENT_CONTEXT.md + SESSION_STATE.md + git status --short + archivos relevantes.
Reglas obligatorias:
1. No tocar nada del agente LLM / Revision (proyecto paralelo).
2. Mantener monthly_metrics_normalized como SSOT mensual de reporting.
3. monthly_closings solo historico/fallback permitido.
4. Frontend solo presentacion (sin logica financiera nueva).
5. Mantener aislamiento estricto de motores de lectura PDF por banco y tipo de cuenta (incluyendo report_mandato por banco).

Contexto puntual:
- Issue abierto en `Boatview | jpmorgan | mandato | 1412600 | 2026-02`.
- El `43.00%` corresponde a Equity en el grafico del Investment Review, pero `pdfplumber` genera una linea artefacto `Cash 43.00%` y el parser JPM report lo captura.
- Eso deja `asset_allocation_json` contaminado y sobre-infla `Detalle > Tipo de activos` (cash de tabla activos > cash de otras tablas).

No hagas cambios de alcance amplio. Enfocate en [fix puntual o validacion puntual].
Despues de cada cambio, reinicia la app si corresponde y avisa; si no, indica que solo refresque.
