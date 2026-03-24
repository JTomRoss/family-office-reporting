"""Extracción de montos con OpenAI usando reglas de motores (prompt enriquecido)."""

from __future__ import annotations

import json
import os
import re
from decimal import Decimal
from typing import Any, Optional

from backend.services.audit.audit_llm_rules import build_engine_rules_prompt


def _focus_user_instruction(focus: str) -> str:
    return {
        "valor_cierre": "Extrae el valor de cierre / patrimonio neto al FINAL del periodo mensual de la cartola (USD).",
        "movimientos_netos": "Extrae el movimiento NETO del MES (aportes menos retiros netos, o equivalente en la sección de actividad del mes; no uses YTD salvo que sea lo único explícito y entonces indícalo en short_note).",
        "caja": "Extrae el monto de caja / liquidez / sweep relevante según las reglas del banco (USD).",
        "instrumentos": "Extrae el total de instrumentos / cartera de inversión coherente con totales de la cartola (USD), evitando doble conteo de totales y subtotales.",
        "aportes": "Si existe, total de aportes o contribuciones del periodo (USD).",
        "retiros": "Si existe, total de retiros o distribuciones del periodo (USD).",
    }.get(focus, "Extrae el monto principal solicitado (USD).")


def _parse_json_object(text: str) -> Optional[dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return None


def extract_audit_amount_with_engine_rules(
    *,
    focus: str,
    pdf_text: str,
    bank_code: str,
    account_type: str,
    account_number: str,
    identification_short: str,
    elemento_bd_label: str,
    statement_date_iso: str,
    model: str = "gpt-4o-mini",
) -> tuple[Optional[Decimal], str, bool]:
    """
    Llama al modelo con reglas de negocio de los motores.

    Returns:
        (amount_usd, note_for_user, could_not_extract)
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or not (pdf_text or "").strip():
        return None, "Sin texto del PDF o sin OPENAI_API_KEY.", True

    try:
        from openai import OpenAI
    except ImportError:
        return None, "Paquete openai no instalado.", True

    rules = build_engine_rules_prompt(
        bank_code=bank_code,
        account_type=account_type,
        account_number=account_number,
        identification_short=identification_short,
    )

    user_block = (
        f"### Foco de auditoría\n"
        f"- Elemento: **{elemento_bd_label}** (foco interno: `{focus}`)\n"
        f"- Fecha de cartola (referencia): {statement_date_iso}\n\n"
        f"### Instrucción\n{_focus_user_instruction(focus)}\n\n"
        f"### Texto extraído del PDF (fragmento)\n{pdf_text[:12000]}"
    )

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": rules},
            {"role": "user", "content": user_block},
        ],
    )
    raw = (resp.choices[0].message.content or "").strip()
    data = _parse_json_object(raw)
    if not isinstance(data, dict):
        return None, f"Respuesta del modelo no es JSON válido: {raw[:200]}", True

    could_not = bool(data.get("could_not_extract"))
    note = str(data.get("short_note") or "").strip()
    amt_raw = data.get("amount_usd")

    if could_not or amt_raw is None:
        return None, note or "No se pudo extraer un monto único.", True

    if isinstance(amt_raw, (int, float)):
        try:
            return Decimal(str(amt_raw)), note, False
        except Exception:
            return None, note or "Monto no numérico.", True

    if isinstance(amt_raw, str):
        cleaned = re.sub(r"[^\d.\-]", "", amt_raw.replace(",", ""))
        try:
            return Decimal(cleaned), note, False
        except Exception:
            return None, note or "Monto no parseable.", True

    return None, note or "Formato de amount_usd inesperado.", True
