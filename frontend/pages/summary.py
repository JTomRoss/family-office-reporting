"""
Página Resumen – Vista consolidada con filtros multi-selección.

Estructura:
- Filtros multi-selección NO cascada destructiva
- 3 gráficos (12 meses siempre visibles)
- Tabla resumen 60%
- Tabla rango personalizado 40%
- Tabla detalle cartolas
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.filters import render_filters, render_date_range_filter


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


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
            "currencies": [],
        }

    # Agregar año
    filter_opts["years"] = [str(y) for y in range(2020, 2027)]

    # ── Renderizar filtros ───────────────────────────────────────
    selections = render_filters(filter_opts, key_prefix="summary")

    st.markdown("---")

    # ── Obtener datos ────────────────────────────────────────────
    try:
        data = api_client.post("/data/summary", json={
            "years": [int(y) for y in selections.get("years", [])],
            "bank_codes": selections.get("bank_codes", []),
            "entity_names": selections.get("entity_names", []),
        })
    except Exception as e:
        data = {"rows": [], "message": f"Error: {e}"}

    # ── Gráficos (3 gráficos, 12 meses siempre visibles) ────────
    st.subheader("📊 Evolución 12 meses")

    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=MONTHS, y=[0] * 12,
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
            x=MONTHS, y=[0] * 12,
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
            x=MONTHS, y=[0] * 12,
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

    # ── Tablas ───────────────────────────────────────────────────
    col_table1, col_table2 = st.columns([6, 4])

    with col_table1:
        st.subheader("Tabla Resumen")
        st.caption("Diciembre año previo → Diciembre actual. Dic previo rellena columnas excepto rentabilidad.")

        if data.get("rows"):
            df = pd.DataFrame(data["rows"])
            st.dataframe(df, use_container_width=True, height=400)
        else:
            st.info("Sin datos. Cargue documentos y aplique filtros.")

    with col_table2:
        st.subheader("Rango Personalizado")
        y_start, m_start, y_end, m_end = render_date_range_filter(key_prefix="summary_range")
        st.info(
            f"Rango: {MONTHS[m_start - 1]} {y_start} → {MONTHS[m_end - 1]} {y_end}"
        )
        st.dataframe(pd.DataFrame(), use_container_width=True, height=300)

    st.markdown("---")

    # ── Detalle cartolas ─────────────────────────────────────────
    st.subheader("📄 Detalle Cartolas")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=300)
