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
        }

    # Solo banco, sociedad, tipo cuenta y año (moneda/país no aplican)
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
        data = {"rows": [], "totals": {}, "message": f"Error: {e}"}

    rows = data.get("rows", [])
    totals = data.get("totals", {})

    # Determinar meses con datos
    all_months_keys = set()
    for r in rows:
        all_months_keys.update(r.get("month_values", {}).keys())
    sorted_months = sorted(all_months_keys)

    # ── Gráficos (3 gráficos, 12 meses siempre visibles) ────────
    st.subheader("📊 Evolución 12 meses")

    # Preparar datos de gráficos desde totals
    chart_months = []
    chart_values = []
    for mk in sorted_months:
        parts = mk.split("-")
        month_idx = int(parts[1]) - 1
        chart_months.append(MONTHS[month_idx])
        chart_values.append(float(totals.get(mk, 0)))

    # Si no hay datos, mostrar 12 meses vacíos
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
        # Profit mensual = diferencias entre meses consecutivos
        profit_values = []
        for i, v in enumerate(chart_values):
            if i == 0:
                profit_values.append(0)
            else:
                profit_values.append(v - chart_values[i - 1])
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
        # Rentabilidad % mensual
        ret_values = []
        for i, v in enumerate(chart_values):
            if i == 0 or chart_values[i - 1] == 0:
                ret_values.append(0)
            else:
                ret_values.append(
                    ((v - chart_values[i - 1]) / chart_values[i - 1]) * 100
                )
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

    # ── Tablas ───────────────────────────────────────────────────
    col_table1, col_table2 = st.columns([6, 4])

    with col_table1:
        st.subheader("Tabla Resumen")
        st.caption("Diciembre año previo → Diciembre actual. Dic previo rellena columnas excepto rentabilidad.")

        if rows:
            # Construir DataFrame con columnas: Sociedad, Banco, Cuenta, Moneda, + mes columns
            table_data = []
            for r in rows:
                row_dict = {
                    "Sociedad": r["entity_name"],
                    "Banco": r["bank_code"],
                    "ID": r.get("identification_number", ""),
                    "Tipo": r["account_type"],
                    "Moneda": r["currency"],
                }
                for mk in sorted_months:
                    parts = mk.split("-")
                    month_label = f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}"
                    val = r.get("month_values", {}).get(mk)
                    row_dict[month_label] = (
                        f"{float(val):,.2f}" if val else ""
                    )
                table_data.append(row_dict)
            df = pd.DataFrame(table_data)
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
