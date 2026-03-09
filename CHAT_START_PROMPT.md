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
