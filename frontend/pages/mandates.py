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
from frontend.components.table_utils import render_table
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

    payload = {
        "years": [int(y) for y in selections.get("years", [])],
        "bank_codes": selections.get("bank_codes", []),
        "entity_names": selections.get("entity_names", []),
    }
    data = api_client.post("/data/mandates", json=payload)

    # ── % Mandatos 12 meses ──────────────────────────────────────
    st.subheader("% Mandatos (12 meses)")
    mandate_rows = data.get("mandate_pcts", [])
    if mandate_rows:
        mdf = pd.DataFrame(mandate_rows)
        fig = go.Figure()
        for col, label in [
            ("discretionary", "Discretionary"),
            ("advisory", "Advisory"),
            ("execution_only", "Execution Only"),
            ("other", "Other"),
        ]:
            if col in mdf.columns:
                fig.add_trace(go.Bar(x=mdf["fecha"], y=mdf[col], name=label))
        fig.update_layout(barmode="stack", height=350, yaxis_title="%")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos para % de mandatos.")

    st.markdown("---")

    # ── Asset Allocation 12 meses ────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Asset Allocation (12 meses)")
        aa_rows = data.get("asset_allocation", [])
        if aa_rows:
            adf = pd.DataFrame(aa_rows)
            fig = go.Figure()
            for col in [c for c in adf.columns if c != "fecha"]:
                fig.add_trace(go.Bar(x=adf["fecha"], y=adf[col], name=col))
            fig.update_layout(barmode="stack", height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de asset allocation.")

    with col2:
        st.subheader("AA por Banco (0-100%)")
        aa_bank = data.get("aa_by_bank", {})
        if aa_bank:
            bdf = pd.DataFrame.from_dict(aa_bank, orient="index").fillna(0).round(4)
            bdf.index.name = "bank_code"
            render_table(bdf.reset_index())
        else:
            st.info("Sin datos de AA por banco.")

    st.markdown("---")

    # ── Tabla bancos x meses ─────────────────────────────────────
    st.subheader("Bancos × Meses")
    banks_rows = data.get("banks_by_month", [])
    if banks_rows:
        render_table(pd.DataFrame(banks_rows))
    else:
        st.info("Sin datos en bancos por mes.")

    st.markdown("---")

    # ── Rentabilidad mensual / YTD ───────────────────────────────
    st.subheader("Rentabilidad Mensual / YTD")
    rt = data.get("returns_table", [])
    if rt:
        render_table(pd.DataFrame(rt))
    else:
        st.info("Sin datos de rentabilidad.")


