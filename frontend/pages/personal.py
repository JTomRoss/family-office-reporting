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
from frontend.components.filters import render_date_range_filter


def render():
    st.title("👤 Personal")
    st.markdown("---")

    # ── Filtros persona + fecha ──────────────────────────────────
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Obtener lista de personas
        try:
            opts = api_client.get("/accounts/filter-options")
            persons = opts.get("entity_names", [])
        except Exception:
            persons = []

        person = st.selectbox("Persona", [""] + persons, key="personal_person")

    with col2:
        year = st.number_input("Año", min_value=2015, max_value=2030, value=2025)
    with col3:
        month = st.number_input("Mes", min_value=1, max_value=12, value=1)

    if not person:
        st.info("Seleccione una persona para ver su información financiera.")
        return

    st.markdown("---")

    # ── Saldo consolidado ────────────────────────────────────────
    st.subheader(f"💰 Saldo Consolidado – {person}")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total USD", "$0.00")
    with col2:
        st.metric("Total CLP", "$0")
    with col3:
        st.metric("Caja", "$0.00")

    st.markdown("---")

    # ── Gráficos torta ───────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribución por Banco")
        fig = go.Figure(data=[go.Pie(
            labels=["Sin datos"],
            values=[1],
            hole=0.4,
        )])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Distribución por Tipo")
        fig = go.Figure(data=[go.Pie(
            labels=["Sin datos"],
            values=[1],
            hole=0.4,
        )])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Tabla sociedades ─────────────────────────────────────────
    st.subheader("Sociedades")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=250)

    st.markdown("---")

    # ── Tabla resumen vertical ───────────────────────────────────
    st.subheader("Resumen Vertical")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=300)

    st.markdown("---")

    # ── Rango personalizado ──────────────────────────────────────
    st.subheader("Rango Personalizado")
    y_start, m_start, y_end, m_end = render_date_range_filter(key_prefix="personal_range")
    st.dataframe(pd.DataFrame(), use_container_width=True, height=250)
