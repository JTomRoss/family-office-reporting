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
from frontend.components.table_utils import render_table


def render():
    st.title("⚙️ Operacional")
    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "🔄 Conciliación",
        "📝 Logs de Validación",
        "🔧 Parsers",
        "⚠️ Errores Clasificación",
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


