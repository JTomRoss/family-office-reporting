"""
Pagina Operacional: solo salud de datos.
"""

import pandas as pd
import streamlit as st

from frontend import api_client
from frontend.components.data_health import fetch_health_report
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.table_utils import render_table


def _display_health_bank_code(value: str | None) -> str | None:
    if value == "ubs":
        return "ubs_suiza"
    return value


def _render_health_tab() -> None:
    st.subheader("Auditoria Read-Only de Salud de Datos")
    st.caption(
        "Revisa identidad mensual, faltantes y diferencias YTD. "
        "Este informe no modifica la base de datos."
    )

    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {
            "years": [],
            "bank_codes": [],
            "account_types": [],
        }

    health_year_options = [""] + [str(y) for y in sorted(filter_opts.get("years", []), reverse=True)]
    health_bank_options = [""] + sorted(filter_opts.get("bank_codes", []))
    health_type_options = [""] + sorted(filter_opts.get("account_types", []))

    hc1, hc2, hc3, hc4 = st.columns(4)
    with hc1:
        year_filter = st.selectbox(
            "Ano",
            options=health_year_options,
            key="health_year_filter",
        )
    with hc2:
        bank_filter = st.selectbox(
            "Banco",
            options=health_bank_options,
            format_func=lambda x: BANK_DISPLAY_NAMES.get(x, x.replace("_", " ").title()) if x else "Todos",
            key="health_bank_filter",
        )
    with hc3:
        type_filter = st.selectbox(
            "Tipo cuenta",
            options=health_type_options,
            format_func=lambda x: x.replace("_", " ").title() if x else "Todos",
            key="health_type_filter",
        )
    with hc4:
        health_limit = st.number_input(
            "Limite detalle",
            value=100,
            min_value=20,
            max_value=500,
            step=20,
            key="health_limit_filter",
        )

    payload = {
        "years": [int(year_filter)] if year_filter else [],
        "bank_codes": [bank_filter] if bank_filter else [],
        "account_types": [type_filter] if type_filter else [],
    }

    try:
        report = fetch_health_report(payload, limit=int(health_limit))
        summary = report.get("summary", {})

        mc1, mc2, mc3, mc4 = st.columns(4)
        with mc1:
            st.metric("Incumplimientos identidad", summary.get("identity_mismatch_count", 0))
        with mc2:
            st.metric("Faltantes mov/util", summary.get("missing_components_count", 0))
        with mc3:
            st.metric("Diferencias YTD mov", summary.get("ytd_movement_mismatch_count", 0))
        with mc4:
            st.metric("Diferencias YTD util", summary.get("ytd_profit_mismatch_count", 0))

        by_bank_type = report.get("by_bank_type", [])
        if by_bank_type:
            st.markdown("---")
            st.markdown("### Resumen por Banco y Tipo")
            df = pd.DataFrame(by_bank_type)
            if "bank_code" in df.columns:
                df["bank_code"] = df["bank_code"].apply(_display_health_bank_code)
            render_table(df)

        identity_issues = report.get("identity_issues", [])
        if identity_issues:
            st.markdown("---")
            hide_beginning_note = st.checkbox(
                "Ocultar casos donde el beginning value de la cartola no coincide con el prev_ending_value",
                value=False,
                key="health_hide_beginning_note",
            )
            if hide_beginning_note:
                identity_issues = [
                    row
                    for row in identity_issues
                    if row.get("note")
                    != "Beginning value de la cartola actual no coincide con prev_ending_value; prevalece el ending value auditado."
                ]
            st.markdown("### Incumplimientos de Identidad")
            if identity_issues:
                df = pd.DataFrame(identity_issues)
                if "bank_code" in df.columns:
                    df["bank_code"] = df["bank_code"].apply(_display_health_bank_code)
                render_table(df)
            else:
                st.info("Sin incumplimientos de identidad visibles con el filtro aplicado.")

        missing_issues = report.get("missing_component_issues", [])
        if missing_issues:
            st.markdown("---")
            st.markdown("### Filas con Datos Faltantes")
            df = pd.DataFrame(missing_issues)
            if "bank_code" in df.columns:
                df["bank_code"] = df["bank_code"].apply(_display_health_bank_code)
            render_table(df)

        ytd_issues = report.get("ytd_issues", [])
        if ytd_issues:
            st.markdown("---")
            st.markdown("### Diferencias YTD")
            df = pd.DataFrame(ytd_issues)
            if "bank_code" in df.columns:
                df["bank_code"] = df["bank_code"].apply(_display_health_bank_code)
            render_table(df)

        if not any([by_bank_type, identity_issues, missing_issues, ytd_issues]):
            st.success("Sin alertas para los filtros seleccionados.")
    except Exception as e:
        st.error(f"Error auditoria salud BD: {e}")


def render():
    st.title("Operacional")
    st.markdown("---")

    tab_health = st.tabs(["Salud BD"])[0]
    with tab_health:
        _render_health_tab()
