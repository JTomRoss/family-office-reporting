"""Helpers de auditoría read-only para salud de datos."""

from __future__ import annotations

import streamlit as st

from frontend import api_client


def fetch_health_report(payload: dict | None = None, *, limit: int = 50) -> dict:
    body = dict(payload or {})
    body["limit"] = limit
    return api_client.post("/data/health-report", json=body)


def render_health_warning(
    payload: dict | None = None,
    *,
    label: str = "esta vista",
    limit: int = 20,
) -> dict | None:
    """Muestra alerta corta si los datos visibles tienen inconsistencias."""
    try:
        report = fetch_health_report(payload, limit=limit)
    except Exception:
        return None

    summary = report.get("summary", {})
    alert_count = int(summary.get("alert_count", 0) or 0)
    if alert_count <= 0:
        return report

    parts = []
    identity_count = int(summary.get("identity_mismatch_count", 0) or 0)
    missing_count = int(summary.get("missing_components_count", 0) or 0)
    ytd_mov_count = int(summary.get("ytd_movement_mismatch_count", 0) or 0)
    ytd_profit_count = int(summary.get("ytd_profit_mismatch_count", 0) or 0)
    if identity_count:
        parts.append(f"{identity_count} incumplimientos de identidad")
    if missing_count:
        parts.append(f"{missing_count} filas con movimientos/utilidad faltantes")
    if ytd_mov_count or ytd_profit_count:
        parts.append(
            f"{ytd_mov_count + ytd_profit_count} diferencias YTD"
        )

    st.warning(
        f"Se detectaron inconsistencias en {label}: " + ", ".join(parts) + ". "
        "Revísalas en Operacional > Salud BD."
    )
    return report
