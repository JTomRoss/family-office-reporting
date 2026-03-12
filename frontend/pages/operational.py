"""
Página Operacional – Conciliación, logs, validación.

Estructura:
- Resultados de conciliación
- Logs de validación (audit trail)
- Estado de parsers
- Errores de clasificación
"""

import streamlit as st
import pandas as pd

from frontend import api_client
from frontend.components.data_health import fetch_health_report
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.table_utils import render_table


def render():
    st.title("⚙️ Operacional")
    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔄 Conciliación",
        "📝 Logs de Validación",
        "🔧 Parsers",
        "⚠️ Errores Clasificación",
        "🏥 Salud BD",
    ])

    # ═══════════════════════════════════════════════════════════════
    # TAB 1: Conciliación
    # ═══════════════════════════════════════════════════════════════
    with tab1:
        st.subheader("Conciliación: Datos Diarios vs Cartolas Mensuales")
        st.caption("La cartola manda como verdad de cierre.")

        try:
            recon = api_client.post("/data/reconciliation", json={})
            if recon.get("reconciliation_results"):
                df = pd.DataFrame(recon["reconciliation_results"])
                render_table(df)
            else:
                st.info("Sin resultados de conciliación. Cargue datos y ejecute conciliación.")

            st.metric("Sin resolver", recon.get("unresolved_count", 0))
        except Exception as e:
            st.error(f"Error: {e}")

        st.markdown("---")
        st.subheader("Asset Allocation Reports (PDF)")
        try:
            aa = api_client.post("/data/asset-allocation-report", json={})
            rows = aa.get("rows", [])
            if rows:
                render_table(pd.DataFrame(rows))
            else:
                st.info("Sin reportes de asset allocation cargados aún.")
        except Exception as e:
            st.error(f"Error asset allocation report: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TAB 2: Logs de validación
    # ═══════════════════════════════════════════════════════════════
    with tab2:
        st.subheader("Audit Trail – Logs de Validación")

        col1, col2, col3 = st.columns(3)
        with col1:
            sev_filter = st.selectbox(
                "Severidad",
                ["", "info", "warning", "error", "critical"],
                key="log_severity",
            )
        with col2:
            type_filter = st.selectbox(
                "Tipo",
                ["", "parse", "load", "reconcile", "calculate", "master_check", "idempotency"],
                key="log_type",
            )
        with col3:
            log_limit = st.number_input("Límite", value=100, min_value=10, max_value=1000)

        try:
            params = {"limit": log_limit}
            if sev_filter:
                params["severity"] = sev_filter
            if type_filter:
                params["validation_type"] = type_filter

            logs = api_client.get("/data/validation-logs", params=params)
            if logs:
                df = pd.DataFrame(logs)
                render_table(df)
            else:
                st.info("Sin logs de validación.")
        except Exception as e:
            st.error(f"Error: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TAB 3: Parsers
    # ═══════════════════════════════════════════════════════════════
    with tab3:
        st.subheader("Parsers Registrados")

        try:
            parsers = api_client.get("/parsers")
            if parsers:
                df = pd.DataFrame(parsers)
                render_table(df)
            else:
                st.warning("No hay parsers registrados.")
        except Exception as e:
            st.error(f"Error: {e}")

        st.markdown("---")
        st.subheader("QA Parser vs Carga")
        thr = st.number_input("Threshold diferencia %", value=0.01, min_value=0.0, step=0.01)
        try:
            qa = api_client.get("/data/parser-quality", params={"threshold_pct": thr, "limit": 200})
            st.metric("Registros críticos", qa.get("critical_count", 0))
            rows = qa.get("rows", [])
            if rows:
                render_table(pd.DataFrame(rows))
            else:
                st.info("Sin datos QA aún.")
        except Exception as e:
            st.error(f"Error QA parsers: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TAB 4: Errores de clasificación
    # ═══════════════════════════════════════════════════════════════
    with tab4:
        st.subheader("Errores de Clasificación en Maestro")

        try:
            errors = api_client.get("/accounts/classification-errors")
            if errors:
                df = pd.DataFrame(errors)
                render_table(df)
                st.warning(f"⚠️ {len(errors)} error(es) detectado(s)")
            else:
                st.success("✅ Sin errores de clasificación")
        except Exception as e:
            st.error(f"Error: {e}")

    # ═══════════════════════════════════════════════════════════════
    # TAB 5: Salud BD
    # ═══════════════════════════════════════════════════════════════
    with tab5:
        st.subheader("Auditoría Read-Only de Salud de Datos")
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
                "Año",
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
                "Límite detalle",
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
                render_table(pd.DataFrame(by_bank_type))

            identity_issues = report.get("identity_issues", [])
            if identity_issues:
                st.markdown("---")
                st.markdown("### Incumplimientos de Identidad")
                render_table(pd.DataFrame(identity_issues))

            missing_issues = report.get("missing_component_issues", [])
            if missing_issues:
                st.markdown("---")
                st.markdown("### Filas con Datos Faltantes")
                render_table(pd.DataFrame(missing_issues))

            ytd_issues = report.get("ytd_issues", [])
            if ytd_issues:
                st.markdown("---")
                st.markdown("### Diferencias YTD")
                render_table(pd.DataFrame(ytd_issues))

            if not any([by_bank_type, identity_issues, missing_issues, ytd_issues]):
                st.success("✅ Sin alertas para los filtros seleccionados.")
        except Exception as e:
            st.error(f"Error auditoría salud BD: {e}")


