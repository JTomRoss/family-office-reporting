"""
FO Reporting – Streamlit UI (entrypoint).

REGLA: Esta capa SOLO presenta datos. CERO lógica de negocio.
Todas las consultas van al backend vía API REST.

Ejecutar:
    streamlit run frontend/app.py --server.port 8501
"""

import streamlit as st

st.set_page_config(
    page_title="FO Reporting",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar: Navegación ─────────────────────────────────────────
st.sidebar.title("📊 FO Reporting")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navegación",
    [
        "🏠 Inicio",
        "📁 Carga",
        "📋 Resumen",
        "📑 Mandatos",
        "📈 ETF",
        "👤 Personal",
        "⚙️ Operacional",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption("v0.1.0 | Uso interno")

# ── Routing de páginas ───────────────────────────────────────────
if page == "🏠 Inicio":
    from frontend.pages.home import render
    render()
elif page == "📁 Carga":
    from frontend.pages.upload import render
    render()
elif page == "📋 Resumen":
    from frontend.pages.summary import render
    render()
elif page == "📑 Mandatos":
    from frontend.pages.mandates import render
    render()
elif page == "📈 ETF":
    from frontend.pages.etf import render
    render()
elif page == "👤 Personal":
    from frontend.pages.personal import render
    render()
elif page == "⚙️ Operacional":
    from frontend.pages.operational import render
    render()
