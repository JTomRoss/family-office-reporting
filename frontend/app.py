"""
FO Reporting - Streamlit UI (entrypoint).

REGLA: Esta capa SOLO presenta datos. CERO logica de negocio.
Todas las consultas van al backend via API REST.

Ejecutar:
    streamlit run frontend/app.py --server.port 8501
"""

import os
import streamlit as st

IS_PREVIEW = os.getenv("FO_UI_MODE", "").strip().lower() == "preview"

st.set_page_config(
    page_title="FO Reporting",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

base_css = """
    <style>
      /* Forzar escala visual base para evitar desorden por zoom heredado */
      html, body {
        zoom: 100% !important;
        font-size: 16px !important;
      }
      [data-testid="stAppViewBlockContainer"] {
        max-width: 90%;
        padding-left: 0 !important;
        padding-right: 0 !important;
        margin-left: auto;
        margin-right: auto;
      }
      /* Regla global tablas: primera columna izquierda, resto derecha */

      /* st.dataframe */
      div[data-testid="stDataFrame"] [role="gridcell"],
      div[data-testid="stDataFrame"] [role="columnheader"] {
        text-align: right !important;
        justify-content: flex-end !important;
      }
      div[data-testid="stDataFrame"] [role="columnheader"][aria-colindex="1"],
      div[data-testid="stDataFrame"] [role="gridcell"][aria-colindex="1"] {
        text-align: left !important;
        justify-content: flex-start !important;
      }

      /* st.data_editor */
      div[data-testid="stDataEditor"] [role="gridcell"],
      div[data-testid="stDataEditor"] [role="columnheader"] {
        text-align: right !important;
        justify-content: flex-end !important;
      }
      div[data-testid="stDataEditor"] [role="columnheader"][aria-colindex="1"],
      div[data-testid="stDataEditor"] [role="gridcell"][aria-colindex="1"] {
        text-align: left !important;
        justify-content: flex-start !important;
      }

      /* st.table: primera columna izquierda, resto derecha */
      div[data-testid="stTable"] table {
        table-layout: auto !important;
        width: 100% !important;
      }
      div[data-testid="stTable"] {
        overflow-x: auto !important;
      }
      div[data-testid="stTable"] table th,
      div[data-testid="stTable"] table td {
        white-space: nowrap !important;
        font-variant-numeric: tabular-nums !important;
        text-align: right !important;
      }
      div[data-testid="stTable"] table thead tr th:first-child,
      div[data-testid="stTable"] table tbody tr th {
        display: none !important;
      }
      /* Primera columna visible (indice oculto): izquierda */
      div[data-testid="stTable"] table thead tr th:nth-child(2),
      div[data-testid="stTable"] table tbody tr td:nth-child(2),
      div[data-testid="stTable"] table td.col0,
      div[data-testid="stTable"] table th.col_heading.level0.col0 {
        text-align: left !important;
      }
    </style>
"""

if IS_PREVIEW:
    preview_css = """
      [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffd6a8 0%, #ffbc73 100%) !important;
        border-right: 4px solid #ff8f00 !important;
      }
      [data-testid="stSidebar"] * {
        color: #2f1d00 !important;
      }
      [data-testid="stSidebar"] hr {
        border-color: rgba(47, 29, 0, 0.25) !important;
      }
    """
    base_css = base_css.replace("</style>", f"{preview_css}\n</style>")

st.markdown(base_css, unsafe_allow_html=True)

if IS_PREVIEW:
    st.sidebar.markdown("### PREVIEW")
st.sidebar.title("📊 FO Reporting")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navegacion",
    [
        "🇨🇱 Detalle Bice",
        "👤 Detalle Internacional",
        "📑 Mandatos",
        "📈 ETF",
        "📋 Detalle Cartolas",
        "📁 Carga",
        "⚙️ Operacional",
    ],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption("v0.1.0 | Uso interno")

if page == "🇨🇱 Detalle Bice":
    from frontend.pages.bice import render
    render()
elif page == "👤 Detalle Internacional":
    from frontend.pages.personal import render
    render()
elif page == "📑 Mandatos":
    from frontend.pages.mandates import render
    render()
elif page == "📈 ETF":
    from frontend.pages.etf import render
    render()
elif page == "📋 Detalle Cartolas":
    from frontend.pages.summary import render
    render()
elif page == "📁 Carga":
    from frontend.pages.upload import render
    render()
elif page == "⚙️ Operacional":
    from frontend.pages.operational import render
    render()
