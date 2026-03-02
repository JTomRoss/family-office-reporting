"""
Página Resumen – Vista consolidada con filtros multi-selección.

Estructura:
- Filtros multi-selección (banco, sociedad, tipo cuenta, año)
- 3 gráficos (Total Assets, Profit, Rentabilidad) consolidados
- Tabla resumen VERTICAL (meses en filas, columnas fijas)
- Tabla rango personalizado (selectores año/mes inicio y fin)
- Tabla detalle cartolas (mismo formato vertical, por cartola)
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


# Columnas numéricas que se alinean a la derecha
_NUM_COLS = [
    "Ending Value", "Movimientos", "Profit",
    "Rent. Mensual (%)", "Rent. Mensual sin Caja (%)",
]


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


def _right_align_config(df):
    """Return column_config dict to right-align numeric columns."""
    cfg = {}
    for col in df.columns:
        if col in _NUM_COLS:
            cfg[col] = st.column_config.TextColumn(col, width="medium")
    return cfg


def _style_right(df):
    """Return a Styler that right-aligns numeric columns."""
    styles = []
    for col in df.columns:
        if col in _NUM_COLS:
            styles.append({"selector": f"td.col{df.columns.get_loc(col)}", "props": "text-align: right"})
    return df.style.set_table_styles(styles, overwrite=False).format(
        {c: "{}" for c in df.columns}
    )


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
        data = {"rows": [], "chart_data": []}
        st.error(f"Error: {e}")

    rows = data.get("rows", [])
    chart_data = data.get("chart_data", [])

    # ── Gráficos (3 gráficos — datos consolidados) ──────────────
    st.subheader("📊 Evolución 12 meses")

    chart_months = []
    chart_ev = []
    chart_profit = []
    chart_ret = []
    for cd in chart_data:
        parts = cd["fecha"].split("-")
        chart_months.append(f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}")
        chart_ev.append(cd["ending_value"])
        chart_profit.append(cd["profit"])
        chart_ret.append(cd["rent_pct"] if cd["rent_pct"] is not None else 0)

    if not chart_months:
        chart_months = MONTHS
        chart_ev = [0] * 12
        chart_profit = [0] * 12
        chart_ret = [0] * 12

    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=chart_ev,
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
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=chart_profit,
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
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_months, y=chart_ret,
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

    # ── Tablas: 60% Resumen | 40% Rango ─────────────────────────
    col_table1, col_table2 = st.columns([6, 4])

    with col_table1:
        st.subheader("📋 Tabla Resumen")
        df_summary = _build_table(rows)
        if not df_summary.empty:
            st.dataframe(
                _style_right(df_summary),
                use_container_width=True,
                height=400,
            )
        else:
            st.info("Sin datos. Cargue documentos y aplique filtros.")

    with col_table2:
        st.subheader("📅 Rango Personalizado")
        y_start, m_start, y_end, m_end = render_date_range_filter(
            key_prefix="summary_range"
        )
        st.info(
            f"Rango: {MONTHS[m_start - 1]} {y_start} → {MONTHS[m_end - 1]} {y_end}"
        )

        # Consolidar por mes dentro del rango
        range_start = f"{y_start}-{m_start:02d}"
        range_end = f"{y_end}-{m_end:02d}"
        range_cd = [
            cd for cd in chart_data
            if range_start <= cd["fecha"] <= range_end
        ]

        if range_cd:
            range_table = []
            for cd in range_cd:
                parts = cd["fecha"].split("-")
                range_table.append({
                    "Mes": f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}",
                    "Ending Value": _fmt_number(cd["ending_value"]),
                    "Movimientos": _fmt_number(cd["movimientos"]),
                    "Profit": _fmt_number(cd["profit"]),
                    "Rent. (%)": _fmt_pct(cd["rent_pct"]),
                })
            df_range = pd.DataFrame(range_table)
            # Right-align numeric cols
            ncols = ["Ending Value", "Movimientos", "Profit", "Rent. (%)"]
            st.dataframe(
                df_range.style.format({c: "{}" for c in df_range.columns}),
                use_container_width=True,
                height=300,
            )
        else:
            st.info("Sin datos en el rango seleccionado.")

    st.markdown("---")

    # ── Detalle Cartolas ─────────────────────────────────────────
    st.subheader("📄 Detalle Cartolas")
    st.caption("Detalle individual de cada cartola (cuenta/período).")

    if not df_summary.empty:
        st.dataframe(
            _style_right(df_summary),
            use_container_width=True,
            height=300,
        )
    else:
        st.info("Sin datos. Cargue cartolas para ver el detalle.")
