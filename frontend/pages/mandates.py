"""
Página Mandatos.

Estructura:
- % Mandatos 12m
- Asset allocation 12m
- AA por banco (0-100%)
- Tabla bancos x meses
- Tabla rentabilidad mensual / YTD
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.filters import render_filters


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def render():
    st.title("📑 Mandatos")
    st.markdown("---")

    # ── Filtros ──────────────────────────────────────────────────
    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {"bank_codes": [], "entity_names": []}

    # Solo banco, sociedad y año (tipo cuenta/moneda/país no aplican)
    filter_opts = {
        "bank_codes": filter_opts.get("bank_codes", []),
        "entity_names": filter_opts.get("entity_names", []),
        "years": [str(y) for y in range(2020, 2027)],
    }
    selections = render_filters(filter_opts, key_prefix="mandates")

    st.markdown("---")

    # ── % Mandatos 12 meses ──────────────────────────────────────
    st.subheader("% Mandatos (12 meses)")
    fig = go.Figure()
    fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Discretionary"))
    fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Advisory"))
    fig.update_layout(barmode="stack", height=350)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Asset Allocation 12 meses ────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Asset Allocation (12 meses)")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Equity"))
        fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Fixed Income"))
        fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Alternatives"))
        fig.add_trace(go.Bar(x=MONTHS, y=[0] * 12, name="Cash"))
        fig.update_layout(barmode="stack", height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("AA por Banco (0-100%)")
        st.info("Pendiente: barras apiladas por banco mostrando composición AA")
        st.dataframe(pd.DataFrame(), use_container_width=True, height=300)

    st.markdown("---")

    # ── Tabla bancos x meses ─────────────────────────────────────
    st.subheader("Bancos × Meses")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=300)

    st.markdown("---")

    # ── Rentabilidad mensual / YTD ───────────────────────────────
    st.subheader("Rentabilidad Mensual / YTD")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=300)
