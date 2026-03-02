"""
Página ETF.

Estructura:
- Bancos x sociedades (totales)
- Composición instrumentos %
- Composición instrumentos montos
- Evolución mensual
- Rentabilidad mensual / YTD
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.filters import render_filters


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

# ETFs válidos
VALID_ETFS = [
    "IWDA – iShares Core MSCI World",
    "IEMA – iShares MSCI EM-ACC",
    "IHYA – iShares USD HY Corp Bond",
    "VDCA – Vanguard USD Corp 1-3",
    "VDPA – Vanguard USD Corp Bond",
]


def render():
    st.title("📈 ETF")
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
    selections = render_filters(filter_opts, key_prefix="etf")

    st.markdown("---")

    # ── Bancos × Sociedades (totales) ────────────────────────────
    st.subheader("Bancos × Sociedades (Totales)")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=250)

    st.markdown("---")

    # ── Composición instrumentos ─────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Composición ETF (%)")
        fig = go.Figure(data=[go.Pie(
            labels=[etf.split(" – ")[0] for etf in VALID_ETFS],
            values=[0] * len(VALID_ETFS),
            hole=0.4,
        )])
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Composición ETF (Montos)")
        fig = go.Figure()
        for etf in VALID_ETFS:
            code = etf.split(" – ")[0]
            fig.add_trace(go.Bar(name=code, x=MONTHS, y=[0] * 12))
        fig.update_layout(barmode="stack", height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Evolución mensual ────────────────────────────────────────
    st.subheader("Evolución Mensual")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=MONTHS, y=[0] * 12,
        mode="lines+markers",
        name="Total ETF",
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Rentabilidad mensual / YTD ───────────────────────────────
    st.subheader("Rentabilidad Mensual / YTD")
    st.caption("Submotores independientes: JPMorgan y Goldman Sachs")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=300)

    # ── Info ETFs válidos ────────────────────────────────────────
    with st.expander("ℹ️ ETFs válidos"):
        for etf in VALID_ETFS:
            st.markdown(f"- {etf}")
