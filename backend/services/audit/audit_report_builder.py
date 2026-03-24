"""Resumen ejecutivo y agregación de estadísticas."""

from __future__ import annotations

from collections import Counter

from backend.schemas import AuditRevisionHallazgo, AuditRevisionSummary


def build_summary(
    *,
    documentos_revisados: int,
    hallazgos: list[AuditRevisionHallazgo],
) -> AuditRevisionSummary:
    if documentos_revisados <= 0:
        return AuditRevisionSummary(
            documentos_revisados=0,
            pct_con_diferencias=0.0,
            pct_ambiguos=0.0,
            pct_no_auditables=0.0,
            top_bancos_incidencias=[],
            top_parsers_incidencias=[],
        )

    if not hallazgos:
        return AuditRevisionSummary(
            documentos_revisados=documentos_revisados,
            pct_con_diferencias=0.0,
            pct_ambiguos=0.0,
            pct_no_auditables=0.0,
            top_bancos_incidencias=[],
            top_parsers_incidencias=[],
        )

    # Porcentajes sobre el total de FILAS de hallazgo (puede haber varias por documento con foco Todos).
    n = len(hallazgos)
    n_diff = sum(1 for h in hallazgos if h.nivel not in ("ambiguo", "no_auditable"))
    n_amb = sum(1 for h in hallazgos if h.nivel == "ambiguo")
    n_na = sum(1 for h in hallazgos if h.nivel == "no_auditable")

    by_bank = Counter(h.banco for h in hallazgos)
    by_parser = Counter(h.parser for h in hallazgos if h.parser)

    return AuditRevisionSummary(
        documentos_revisados=documentos_revisados,
        pct_con_diferencias=round(100.0 * n_diff / n, 2),
        pct_ambiguos=round(100.0 * n_amb / n, 2),
        pct_no_auditables=round(100.0 * n_na / n, 2),
        top_bancos_incidencias=[{"banco": b, "cantidad": c} for b, c in by_bank.most_common(8)],
        top_parsers_incidencias=[{"parser": p, "cantidad": c} for p, c in by_parser.most_common(8)],
    )
