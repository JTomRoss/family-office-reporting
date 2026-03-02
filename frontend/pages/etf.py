"""
Página ETF.

Estructura:
- Filtro Fecha (YYYY-MM), Banco, Sociedad
- Tabla Bancos × Sociedades (solo filtro Fecha)
- 2 gráficos torta: distribución por sociedades + por instrumentos (solo Fecha)
- Tabla ETF montos: instrumentos × meses (Fecha + Banco + Sociedad)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.filters import (
    render_fecha_filter,
    BANK_DISPLAY_NAMES,
)


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

SOCIETY_COLUMNS = [
    "Boatview JPM", "Boatview GS", "Telmar",
    "Armel Holdings", "Ect Internacional", "Total",
]


def _fmt_bank(code):
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def render():
    st.title("📈 ETF")
    st.markdown("---")

    # ── Obtener opciones de filtro ───────────────────────────────
    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {"bank_codes": [], "entity_names": []}

    # Obtener fechas disponibles para ETF
    try:
        etf_dates = api_client.get("/data/etf-dates")
    except Exception:
        etf_dates = {"dates": []}

    available_dates = etf_dates.get("dates", [])

    # ── Renderizar filtros ───────────────────────────────────────
    st.markdown("### 🔍 Filtros")

    fcol1, fcol2, fcol3 = st.columns(3)

    with fcol1:
        fecha = render_fecha_filter(available_dates, key_prefix="etf")

    with fcol2:
        bank_options = filter_opts.get("bank_codes", [])
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            format_func=_fmt_bank,
            key="etf_banco_filter",
        )

    with fcol3:
        entity_options = filter_opts.get("entity_names", [])
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            key="etf_sociedad_filter",
        )

    st.markdown("---")

    # ── Obtener datos del backend ────────────────────────────────
    try:
        data = api_client.post("/data/etf", json={
            "fecha": fecha,
            "bank_codes": selected_banks,
            "entity_names": selected_entities,
        })
    except Exception as e:
        data = {}
        st.error(f"Error obteniendo datos: {e}")

    bank_society_table = data.get("bank_society_table", {})
    society_totals = data.get("society_totals", {})
    composition_by_society = data.get("composition_by_society", [])
    composition_by_instrument = data.get("composition_by_instrument", [])
    montos_table = data.get("montos_table", [])
    selected_year = data.get("selected_year")

    # ── Bancos × Sociedades ──────────────────────────────────────
    st.subheader("Bancos × Sociedades")
    st.caption("Solo afectado por el filtro Fecha.")

    if bank_society_table:
        table_rows = []
        for banco, society_vals in bank_society_table.items():
            row = {"Banco": _fmt_bank(banco)}
            for col in SOCIETY_COLUMNS:
                val = society_vals.get(col, 0)
                row[col] = f"{val:,.2f}" if val else ""
            table_rows.append(row)

        # Fila total
        total_row = {"Banco": "TOTAL"}
        grand_total = 0.0
        for col in SOCIETY_COLUMNS[:-1]:
            val = society_totals.get(col, 0)
            total_row[col] = f"{val:,.2f}" if val else ""
            grand_total += float(val or 0)
        total_row["Total"] = f"{grand_total:,.2f}"
        table_rows.append(total_row)

        df_bs = pd.DataFrame(table_rows, columns=["Banco"] + SOCIETY_COLUMNS)
        st.dataframe(df_bs, use_container_width=True, height=250)
    else:
        st.info("Sin datos. Seleccione una fecha con datos ETF.")

    st.markdown("---")

    # ── Gráficos de torta ────────────────────────────────────────
    st.subheader("Distribución")
    st.caption("Solo afectado por el filtro Fecha.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Por Sociedades**")
        if composition_by_society:
            labels = [c["label"] for c in composition_by_society]
            values = [float(c["value"]) for c in composition_by_society]
            fig = go.Figure(data=[go.Pie(
                labels=labels, values=values, hole=0.4,
            )])
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de composición por sociedades.")

    with col2:
        st.markdown("**Por Instrumentos**")
        if composition_by_instrument:
            labels = [c["label"][:35] for c in composition_by_instrument]
            values = [float(c["value"]) for c in composition_by_instrument]
            fig = go.Figure(data=[go.Pie(
                labels=labels, values=values, hole=0.4,
            )])
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de composición por instrumentos.")

    st.markdown("---")

    # ── ETF Montos (instrumentos × meses) ────────────────────────
    year_label = str(selected_year) if selected_year else ""
    st.subheader(f"ETF Montos {year_label}")
    st.caption("Afectado por filtros Fecha, Banco y Sociedad. Columnas = meses del año.")

    if montos_table:
        df_montos = pd.DataFrame(montos_table)

        # Renombrar columnas de meses a Ene, Feb, etc.
        col_rename = {"instrumento": "Instrumento"}
        for m in range(1, 13):
            mk = f"{m:02d}"
            if mk in df_montos.columns:
                suffix = f" {str(selected_year)[-2:]}" if selected_year else ""
                col_rename[mk] = f"{MONTHS[m - 1]}{suffix}"
        df_montos = df_montos.rename(columns=col_rename)

        # Formatear columnas numéricas
        for col in df_montos.columns:
            if col != "Instrumento":
                df_montos[col] = df_montos[col].apply(
                    lambda x: f"{float(x):,.2f}" if x and float(x) != 0 else ""
                )

        st.dataframe(df_montos, use_container_width=True, height=400)
    else:
        st.info("Sin datos de montos ETF.")
