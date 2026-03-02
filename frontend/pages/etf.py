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

    # ── Obtener datos del backend ────────────────────────────────
    try:
        data = api_client.post("/data/etf", json={
            "years": [int(y) for y in selections.get("years", [])],
            "bank_codes": selections.get("bank_codes", []),
            "entity_names": selections.get("entity_names", []),
        })
    except Exception as e:
        data = {}
        st.error(f"Error obteniendo datos: {e}")

    bank_entity_totals = data.get("bank_entity_totals", [])
    composition_pct = data.get("composition_pct", [])
    composition_amounts = data.get("composition_amounts", [])
    monthly_evolution = data.get("monthly_evolution", [])
    returns_table = data.get("returns_table", [])

    # ── Bancos × Sociedades (totales) ────────────────────────────
    st.subheader("Bancos × Sociedades (Totales)")
    if bank_entity_totals:
        df_totals = pd.DataFrame(bank_entity_totals)
        # Formatear valores
        if "net_value" in df_totals.columns:
            df_totals["net_value"] = df_totals["net_value"].apply(
                lambda x: f"{float(x):,.2f}" if x else ""
            )
        df_totals.columns = [
            c.replace("_", " ").title() for c in df_totals.columns
        ]
        st.dataframe(df_totals, use_container_width=True, height=250)
    else:
        st.info("Sin datos. Cargue cartolas ETF y aplique filtros.")

    st.markdown("---")

    # ── Composición instrumentos ─────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Composición ETF (%)")
        if composition_pct:
            labels = [c["etf_name"][:30] for c in composition_pct]
            values = [float(c["weight_pct"]) for c in composition_pct]
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=0.4,
            )])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            fig = go.Figure(data=[go.Pie(
                labels=["Sin datos"],
                values=[1],
                hole=0.4,
            )])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Composición ETF (Montos)")
        if composition_amounts:
            df_comp = pd.DataFrame(composition_amounts)
            if "market_value" in df_comp.columns:
                df_comp["market_value"] = df_comp["market_value"].apply(
                    lambda x: f"{float(x):,.2f}" if x else ""
                )
            df_comp.columns = [
                c.replace("_", " ").title() for c in df_comp.columns
            ]
            st.dataframe(df_comp, use_container_width=True, height=350)
        else:
            st.info("Sin composiciones")

    st.markdown("---")

    # ── Evolución mensual ────────────────────────────────────────
    st.subheader("Evolución Mensual")
    if monthly_evolution:
        evo_months = [
            f"{MONTHS[e['month'] - 1]} {str(e['year'])[-2:]}"
            for e in monthly_evolution
        ]
        evo_values = [float(e["total_value"]) for e in monthly_evolution]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=evo_months, y=evo_values,
            mode="lines+markers",
            name="Total ETF",
            line=dict(color="steelblue", width=2),
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
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
    if returns_table:
        df_ret = pd.DataFrame(returns_table)
        # Renombrar columnas
        col_map = {
            "bank_code": "Banco",
            "entity_name": "Sociedad",
            "year": "Año",
            "month": "Mes",
            "net_value": "Valor Neto",
            "monthly_return_pct": "Rent. Mensual %",
        }
        df_ret = df_ret.rename(columns=col_map)
        if "Valor Neto" in df_ret.columns:
            df_ret["Valor Neto"] = df_ret["Valor Neto"].apply(
                lambda x: f"{float(x):,.2f}" if x else ""
            )
        st.dataframe(df_ret, use_container_width=True, height=300)
    else:
        st.dataframe(pd.DataFrame(), use_container_width=True, height=300)
