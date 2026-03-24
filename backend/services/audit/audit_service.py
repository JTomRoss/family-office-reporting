"""Orquestador del agente de auditoría Revisión (solo LLM + reglas de motores, solo lectura)."""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from backend.db.models import ParsedStatement
from backend.schemas import (
    AuditRevisionHallazgo,
    AuditRevisionParams,
    AuditRevisionResponse,
)
from backend.services.audit.audit_business_rules import enrich_hallazgo, note_beginning_if_relevant
from backend.services.audit.audit_comparator import (
    difference_pct,
    to_decimal,
    within_tolerance,
)
from backend.services.audit.audit_llm_extractor import extract_audit_amount_with_engine_rules
from backend.services.audit.audit_normalized_values import elemento_label_for_focus, get_bd_value_for_focus
from backend.services.audit.audit_pdf_reader import extract_pdf_text, resolve_raw_path
from backend.services.audit.audit_priority import classify_level, compute_priority
from backend.services.audit.audit_report_builder import build_summary
from backend.services.audit.audit_sampling import sample_rows
from backend.services.audit.audit_universe import AuditUniverseRow, fetch_previous_normalized_ending, fetch_universe

FOCUS_EXPAND_TODOS = ("valor_cierre", "movimientos_netos", "caja", "instrumentos")


def _bank_display(acct) -> str:
    return acct.bank_name or acct.bank_code.replace("_", " ").title()


def _account_id_display(acct) -> str:
    return (acct.identification_number or acct.account_number or "").strip()


def _hallazgo_common(
    row: AuditUniverseRow,
    stmt: ParsedStatement,
    acct,
    *,
    elemento_revisado: str,
    monto_agente: Optional[float],
    monto_bd: Optional[float],
    diferencia: float,
    diferencia_pct: Optional[float],
    nivel: str,
    nota: str,
    prioridad: float,
) -> AuditRevisionHallazgo:
    return AuditRevisionHallazgo(
        sociedad=acct.entity_name,
        banco=_bank_display(acct),
        tipo_cuenta=acct.account_type,
        id_cuenta=_account_id_display(acct),
        fecha_cartola=stmt.statement_date,
        documento_id=row.raw_document.id,
        archivo=row.raw_document.filename or "",
        elemento_revisado=elemento_revisado,
        monto_agente=monto_agente,
        monto_bd=monto_bd,
        diferencia=diferencia,
        diferencia_pct=diferencia_pct,
        nivel=nivel,
        nota=nota,
        prioridad=prioridad,
        parser=row.parser_name,
    )


def _row_period(row: AuditUniverseRow) -> tuple[int, int]:
    raw = row.raw_document
    stmt = row.parsed_statement
    if raw.period_year and raw.period_month:
        return int(raw.period_year), int(raw.period_month)
    d = stmt.statement_date
    return d.year, d.month


def _build_one_finding(
    *,
    row: AuditUniverseRow,
    focus: str,
    stmt: ParsedStatement,
    db: Session,
) -> Optional[AuditRevisionHallazgo]:
    norm = row.normalized
    acct = row.account
    year, month = _row_period(row)

    prev = fetch_previous_normalized_ending(db, account_id=acct.id, year=year, month=month)
    prev_end = to_decimal(prev.ending_value_with_accrual) if prev else None

    elem_label = elemento_label_for_focus(focus)

    if focus in ("aportes", "retiros"):
        return _hallazgo_common(
            row,
            stmt,
            acct,
            elemento_revisado=elem_label,
            monto_agente=None,
            monto_bd=None,
            diferencia=0.0,
            diferencia_pct=None,
            nivel="no_auditable",
            nota="No hay columnas separadas de aportes/retiros en la capa normalizada para comparar.",
            prioridad=-1.0,
        )

    bd_val, elem_label = get_bd_value_for_focus(focus, norm)
    if norm is None:
        return _hallazgo_common(
            row,
            stmt,
            acct,
            elemento_revisado=elem_label,
            monto_agente=None,
            monto_bd=None,
            diferencia=0.0,
            diferencia_pct=None,
            nivel="no_auditable",
            nota="Sin fila en monthly_metrics_normalized para esta cuenta y periodo.",
            prioridad=-1.0,
        )

    path = resolve_raw_path(row.raw_document.filepath)
    pdf_text = extract_pdf_text(path)

    llm_val, llm_note, could_not = extract_audit_amount_with_engine_rules(
        focus=focus,
        pdf_text=pdf_text,
        bank_code=acct.bank_code,
        account_type=acct.account_type,
        account_number=acct.account_number,
        identification_short=_account_id_display(acct),
        elemento_bd_label=elem_label,
        statement_date_iso=stmt.statement_date.isoformat(),
    )

    nota_begin = note_beginning_if_relevant(
        parsed_data_json=stmt.parsed_data_json,
        opening_statement=to_decimal(stmt.opening_balance),
        prev_ending_normalized=prev_end,
    )

    if could_not or llm_val is None:
        extra = f" {nota_begin}" if nota_begin else ""
        return _hallazgo_common(
            row,
            stmt,
            acct,
            elemento_revisado=elem_label,
            monto_agente=None,
            monto_bd=float(bd_val) if bd_val is not None else None,
            diferencia=0.0,
            diferencia_pct=None,
            nivel="ambiguo",
            nota=f"LLM: {llm_note}{extra}",
            prioridad=0.5,
        )

    if bd_val is None:
        return _hallazgo_common(
            row,
            stmt,
            acct,
            elemento_revisado=elem_label,
            monto_agente=float(llm_val),
            monto_bd=None,
            diferencia=0.0,
            diferencia_pct=None,
            nivel="no_auditable",
            nota=f"LLM extrajo valor pero no hay monto comparable en BD ({elem_label}). {llm_note}",
            prioridad=-1.0,
        )

    if within_tolerance(llm_val, bd_val):
        return None

    diff = llm_val - bd_val
    d_pct = difference_pct(llm_val, bd_val)
    nivel = classify_level(
        diferencia=diff,
        diferencia_pct=d_pct,
        ambiguous=False,
        comparable=True,
    )
    nota = (
        f"LLM: {llm_note} · Diferencia frente a BD: {float(diff):,.2f} USD"
        + (f" ({d_pct:.4f}%)" if d_pct is not None else "")
        + "."
    )
    if nota_begin:
        nota = f"{nota} {nota_begin}"

    norm_ytd = to_decimal(norm.movements_ytd) if norm else None
    nota, nivel = enrich_hallazgo(
        bank_code=acct.bank_code,
        elemento_revisado=elem_label,
        parsed_data_json=stmt.parsed_data_json,
        nota=nota,
        nivel=nivel,
        norm_movements_ytd=norm_ytd,
    )

    prio = compute_priority(
        nivel=nivel,
        diferencia=float(diff),
        diferencia_pct=d_pct,
    )

    return _hallazgo_common(
        row,
        stmt,
        acct,
        elemento_revisado=elem_label,
        monto_agente=float(llm_val),
        monto_bd=float(bd_val),
        diferencia=float(diff),
        diferencia_pct=d_pct,
        nivel=nivel,
        nota=nota.strip(),
        prioridad=prio,
    )


def run_audit_revision(db: Session, params: AuditRevisionParams) -> AuditRevisionResponse:
    universe = fetch_universe(
        db,
        bank_codes=params.bank_codes,
        entity_names=params.entity_names,
        account_types=params.account_types,
        year_start=params.year_start,
        year_end=params.year_end,
        month_start=params.month_start,
        month_end=params.month_end,
    )
    total = len(universe)
    sampled = sample_rows(
        universe,
        sample_pct=params.sample_pct,
        max_docs=params.max_docs,
        sample_mode=params.sample_mode,
    )

    if not sampled:
        return AuditRevisionResponse(
            total_candidatos=total,
            revisados=0,
            resumen=build_summary(documentos_revisados=0, hallazgos=[]),
            hallazgos=[],
        )

    if not os.getenv("OPENAI_API_KEY"):
        raise HTTPException(
            status_code=400,
            detail="Se requiere OPENAI_API_KEY para la auditoría por LLM (solo lectura).",
        )

    subfocuses: tuple[str, ...] = (
        FOCUS_EXPAND_TODOS if params.focus == "todos" else (params.focus,)
    )

    hallazgos: list[AuditRevisionHallazgo] = []
    for row in sampled:
        for sf in subfocuses:
            h = _build_one_finding(
                row=row,
                focus=sf,
                stmt=row.parsed_statement,
                db=db,
            )
            if h is not None:
                hallazgos.append(h)

    max_prio = max(
        (h.prioridad for h in hallazgos if h.nivel not in ("no_auditable", "ambiguo")),
        default=1.0,
    )
    scaled: list[AuditRevisionHallazgo] = []
    for h in hallazgos:
        if h.nivel == "ambiguo":
            scaled.append(
                h.model_copy(update={"prioridad": 0.5 * float(max_prio)})
            )
        else:
            scaled.append(h)

    scaled.sort(key=lambda x: x.prioridad, reverse=True)

    resumen = build_summary(documentos_revisados=len(sampled), hallazgos=scaled)
    return AuditRevisionResponse(
        total_candidatos=total,
        revisados=len(sampled),
        resumen=resumen,
        hallazgos=scaled,
    )
