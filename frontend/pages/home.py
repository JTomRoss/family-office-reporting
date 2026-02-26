"""
Página de inicio – Health check y resumen del sistema.
"""

import streamlit as st
from frontend.api_client import health_check


def render():
    st.title("🏠 FO Reporting")
    st.markdown("Sistema de reporting financiero interno.")
    st.markdown("---")

    # ── Health check ─────────────────────────────────────────────
    st.subheader("Estado del sistema")

    health = health_check()

    if health.get("status") == "ok":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Estado", "✅ Online")
        with col2:
            st.metric("Versión", health.get("version", "?"))
        with col3:
            st.metric("Base de datos", health.get("database", "?"))
        with col4:
            st.metric("Parsers cargados", health.get("parsers_loaded", 0))

        git_hash = health.get("git_hash")
        if git_hash:
            st.caption(f"Git HEAD: `{git_hash[:12]}`")
    else:
        st.error(
            f"⚠️ Backend no disponible: {health.get('message', 'Sin respuesta')}\n\n"
            "Asegúrate de que el backend esté corriendo en http://localhost:8000"
        )

    st.markdown("---")

    # ── Quick links ──────────────────────────────────────────────
    st.subheader("Acceso rápido")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("📁 **Carga** → Subir documentos")
        st.markdown("📋 **Resumen** → Vista consolidada")
    with col2:
        st.markdown("📑 **Mandatos** → Asset allocation")
        st.markdown("📈 **ETF** → Composición y rendimiento")
    with col3:
        st.markdown("👤 **Personal** → Vista por persona")
        st.markdown("⚙️ **Operacional** → Conciliación y logs")
