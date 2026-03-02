"""
Página Resumen – Vista consolidada con filtros multi-selección.

Estructura:
- Filtros multi-selección (banco, sociedad, tipo cuenta, año)
- 3 gráficos (Total Assets, Profit, Rentabilidad)
- Tabla resumen VERTICAL (meses en filas, columnas fijas)
- Tabla rango personalizado (mismo formato, filtrado por fecha)
- Tabla detalle cartolas (mismo formato, por cartola individual)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.filters import (
    render_filters,
    render_date_range_filter,
    BANK_DISPLAY_NAMES,
)


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def _fmt_number(val):
    """Format a number for display."""
    if val is None:
        return ""
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return ""


def _fmt_pct(val):
    """Format a percentage for display."""
    if val is None:
        return ""
    try:
        return f"{float(val):.2f}%"
    except (ValueError, TypeError):
        return ""


def _fmt_bank(code):
    """Format bank code for display."""
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _build_table(rows):
    """Build a display DataFrame from summary rows."""
    if not rows:
        return pd.DataFrame()

    table_data = []
    for r in rows:
        table_data.append({
            "Fecha": r["fecha"],
            "Sociedad": r["sociedad"],
            "Banco": _fmt_bank(r["banco"]),
            "ID": r["id"],
            "Moneda": r["moneda"],
            "Ending Value": _fmt_number(r["ending_value"]),
            "Movimientos": _fmt_number(r["movimientos"]),
            "Profit": _fmt_number(r["profit"]),
            "Rent. Mensual (%)": _fmt_pct(r["rent_mensual_pct"]),
            "Rent. Mensual sin Caja (%)": _fmt_pct(r["rent_mensual_sin_caja_pct"]),
        })
    return pd.DataFrame(table_data)


def render():
    st.title("📋 Resumen")
    st.markdown("---")

    # ── Obtener opciones de filtro ───────────────────────────────
    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {
            "bank_codes": [],
            "entity_names": [],
            "account_types": [],
        }

    filter_opts = {
        "bank_codes": filter_opts.get("bank_codes", []),
        "entity_names": filter_opts.get("entity_names", []),
        "account_types": filter_opts.get("account_types", []),
        "years": [str(y) for y in range(2020, 2027)],
    }

    # ── Renderizar filtros ───────────────────────────────────────
    selections = render_filters(filter_opts, key_prefix="summary")

    st.markdown("---")

    # ── Obtener datos ────────────────────────────────────────────
    try:
        data = api_client.post("/data/summary", json={
            "years": [int(y) for y in selections.get("years", [])],
            "bank_codes": selections.get("bank_codes", []),
            "entity_names": selections.get("entity_names", []),
            "account_types": selections.get("account_types", []),
        })
    except Exception as e:
        data = {"rows": [], "totals": {}}
        st.error(f"Error: {e}")

    rows = data.get("rows", [])
    totals = data.get("totals", {})

    # Meses ordenados
    sorted_months = sorted(totals.keys()) if totals else []

    # ── Gráficos (3 gráficos) ───────────────────────────────────
    st.subheader("📊 Evolución 12 meses")

    chart_months = []
    chart_values = []
    for mk in sorted_months:
        parts = mk.split("-")
        month_idx = int(parts[1]) - 1
        chart_months.append(f"{MONTHS[month_idx]} {parts[0][-2:]}")
        chart_values.append(float(totals.get(mk, 0)))

    if not chart_months:
        chart_months = MONTHS
        chart_values = [0] * 12

    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=chart_values,
            name="Total Assets",
            marker_color="steelblue",
        ))
        fig.update_layout(
            title="Total Assets por Mes",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        profit_values = [
            (chart_values[i] - chart_values[i - 1]) if i > 0 else 0
            for i in range(len(chart_values))
        ]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=profit_values,
            name="Profit",
            marker_color="mediumseagreen",
        ))
        fig.update_layout(
            title="Profit Mensual",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col3:
        ret_values = [
            (((chart_values[i] - chart_values[i - 1]) / chart_values[i - 1]) * 100)
            if i > 0 and chart_values[i - 1] != 0 else 0
            for i in range(len(chart_values))
        ]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_months, y=ret_values,
            mode="lines+markers",
            name="Rentabilidad",
            line=dict(color="coral"),
        ))
        fig.update_layout(
            title="Rentabilidad Mensual %",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Tabla Resumen (VERTICAL) ─────────────────────────────────
    st.subheader("📋 Tabla Resumen")
    st.caption(
        "Formato vertical: un registro por cuenta por mes. "
        "Columnas: Fecha · Sociedad · Banco · ID · Moneda · "
        "Ending Value · Movimientos · Profit · Rent. Mensual (%) · "
        "Rent. Mensual sin Caja (%)"
    )

    df_summary = _build_table(rows)
    if not df_summary.empty:
        st.dataframe(df_summary, use_container_width=True, height=400)
    else:
        st.info("Sin datos. Cargue documentos y aplique filtros.")

    st.markdown("---")

    # ── Rango Personalizado ──────────────────────────────────────
    st.subheader("📅 Rango Personalizado")
    y_start, m_start, y_end, m_end = render_date_range_filter(
        key_prefix="summary_range"
    )
    st.info(
        f"Rango: {MONTHS[m_start - 1]} {y_start} → {MONTHS[m_end - 1]} {y_end}"
    )

    # Filtrar rows por rango de fechas
    range_start = f"{y_start}-{m_start:02d}"
    range_end = f"{y_end}-{m_end:02d}"
    range_rows = [
        r for r in rows
        if range_start <= r["fecha"] <= range_end
    ]

    df_range = _build_table(range_rows)
    if not df_range.empty:
        st.dataframe(df_range, use_container_width=True, height=300)
    else:
        st.info("Sin datos en el rango seleccionado.")

    st.markdown("---")

    # ── Detalle Cartolas ─────────────────────────────────────────
    st.subheader("📄 Detalle Cartolas")
    st.caption("Detalle individual de cada cartola (cuenta/período).")

    if not df_summary.empty:
        st.dataframe(df_summary, use_container_width=True, height=300)
    else:
        st.info("Sin datos. Cargue cartolas para ver el detalle.")
