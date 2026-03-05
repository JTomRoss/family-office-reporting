"""
Página Personal.

Estructura:
- Filtro persona + fecha
- Saldo consolidado USD/CLP + caja
- Gráficos torta
- Tabla sociedades
- Tabla resumen vertical
- Tabla rango personalizado
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.table_utils import render_table
from frontend.components.filters import render_date_range_filter, render_filters


def render():
    st.title("👤 Personal")
    st.markdown("---")

    # ── Filtros persona + fecha ──────────────────────────────────
    try:
        opts = api_client.get("/accounts/filter-options")
    except Exception:
        opts = {"entity_names": [], "years": []}

    filter_opts = {
        "entity_names": opts.get("entity_names", []),
        "years": [str(y) for y in opts.get("years", [])],
    }
    selections = render_filters(filter_opts, key_prefix="personal")
    years = [int(y) for y in selections.get("years", [])]
    people = selections.get("entity_names", [])
    if not people:
        st.info("Seleccione al menos una persona/sociedad para ver su información financiera.")
        return
    person = ", ".join(people[:2]) + (f" (+{len(people)-2})" if len(people) > 2 else "")

    data = api_client.post(
        "/data/personal",
        json={
            "entity_names": people,
            "years": years,
        },
    )

    st.markdown("---")

    # ── Saldo consolidado ────────────────────────────────────────
    st.subheader(f"💰 Saldo Consolidado – {person}")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total USD", f"${data.get('consolidated_usd', 0):,.2f}")
    with col2:
        st.metric("Total CLP", f"${data.get('consolidated_clp', 0):,.0f}")
    with col3:
        st.metric("Caja", f"${data.get('cash', 0):,.2f}")

    st.markdown("---")

    # ── Gráficos torta ───────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribución por Banco")
        bank_rows = data.get("pie_charts", {}).get("by_bank", [])
        labels = [r.get("label") for r in bank_rows] or ["Sin datos"]
        values = [r.get("value") for r in bank_rows] or [1]
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Distribución por Tipo")
        type_rows = data.get("pie_charts", {}).get("by_type", [])
        labels = [r.get("label") for r in type_rows] or ["Sin datos"]
        values = [r.get("value") for r in type_rows] or [1]
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Tabla sociedades ─────────────────────────────────────────
    st.subheader("Sociedades")
    render_table(pd.DataFrame(data.get("entities_table", [])))

    st.markdown("---")

    # ── Tabla resumen vertical ───────────────────────────────────
    st.subheader("Resumen Vertical")
    render_table(pd.DataFrame(data.get("summary_table", [])))

    st.markdown("---")

    # ── Rango personalizado ──────────────────────────────────────
    st.subheader("Rango Personalizado")
    y_start, m_start, y_end, m_end = render_date_range_filter(key_prefix="personal_range")
    range_rows = pd.DataFrame(data.get("range_table", []))
    if not range_rows.empty and "fecha" in range_rows.columns:
        start_key = f"{int(y_start)}-{int(m_start):02d}"
        end_key = f"{int(y_end)}-{int(m_end):02d}"
        range_rows = range_rows[
            (range_rows["fecha"] >= start_key) & (range_rows["fecha"] <= end_key)
        ]
    render_table(range_rows)


