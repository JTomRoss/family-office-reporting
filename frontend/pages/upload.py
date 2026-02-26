"""
Página de Carga – Upload de documentos.

Funcionalidades:
- Drag & drop PDFs
- Clasificación con metadata
- Auto-relleno desde maestro de cuentas
- Carga masiva
- Botón procesar con barra 0-100%
- Tabla filtrable de documentos
- Borrado individual y total
- 4 cargas Excel: posiciones, movimientos, precios, maestro
- Carga PDFs reportes cualitativos
"""

import streamlit as st
import tempfile
from pathlib import Path

from frontend import api_client


def render():
    st.title("📁 Carga de Documentos")
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs([
        "📄 PDFs (Cartolas / Reportes)",
        "📊 Excel / CSV",
        "📋 Documentos cargados",
    ])

    # ═══════════════════════════════════════════════════════════════
    # TAB 1: Carga de PDFs
    # ═══════════════════════════════════════════════════════════════
    with tab1:
        st.subheader("Carga de PDFs")

        col1, col2 = st.columns(2)
        with col1:
            pdf_type = st.selectbox(
                "Tipo de documento",
                ["pdf_cartola", "pdf_report"],
                format_func=lambda x: {
                    "pdf_cartola": "📃 Cartola bancaria",
                    "pdf_report": "📘 Reporte cualitativo",
                }[x],
            )
        with col2:
            bank_code = st.selectbox(
                "Banco",
                ["", "jpmorgan", "ubs", "goldman_sachs"],
                format_func=lambda x: x if x else "Auto-detectar",
            )

        col3, col4 = st.columns(2)
        with col3:
            period_year = st.number_input("Año", min_value=2015, max_value=2030, value=2025)
        with col4:
            period_month = st.number_input("Mes", min_value=1, max_value=12, value=1)

        uploaded_files = st.file_uploader(
            "Arrastra PDFs aquí (uno o varios)",
            type=["pdf"],
            accept_multiple_files=True,
            key="pdf_upload",
        )

        if uploaded_files:
            st.info(f"📎 {len(uploaded_files)} archivo(s) seleccionado(s)")

            if st.button("🚀 Procesar PDFs", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()

                for idx, file in enumerate(uploaded_files):
                    pct = int((idx / len(uploaded_files)) * 100)
                    progress_bar.progress(pct)
                    status_text.text(f"Procesando: {file.name}...")

                    # Guardar temp y subir
                    with tempfile.NamedTemporaryFile(
                        delete=False, suffix=".pdf"
                    ) as tmp:
                        tmp.write(file.read())
                        tmp_path = tmp.name

                    try:
                        result = api_client.upload_file(
                            "/documents/upload",
                            filepath=tmp_path,
                            filename=file.name,
                            file_type=pdf_type,
                            extra_data={
                                "bank_code": bank_code or None,
                                "period_year": str(period_year),
                                "period_month": str(period_month),
                            },
                        )

                        if result.get("is_duplicate"):
                            st.warning(f"⚠️ {file.name}: Duplicado (ya existe)")
                        else:
                            st.success(f"✅ {file.name}: Cargado (ID: {result.get('id')})")

                    except Exception as e:
                        st.error(f"❌ {file.name}: {e}")
                    finally:
                        Path(tmp_path).unlink(missing_ok=True)

                progress_bar.progress(100)
                status_text.text("✅ Procesamiento completado")

    # ═══════════════════════════════════════════════════════════════
    # TAB 2: Carga Excel/CSV
    # ═══════════════════════════════════════════════════════════════
    with tab2:
        st.subheader("Carga de Excel / CSV")

        excel_type = st.selectbox(
            "Tipo de archivo",
            ["excel_positions", "excel_movements", "excel_prices", "excel_master"],
            format_func=lambda x: {
                "excel_positions": "📊 Posiciones diarias",
                "excel_movements": "💱 Movimientos diarios",
                "excel_prices": "💰 Precios (FX + activos)",
                "excel_master": "🏛️ Maestro de cuentas (SSOT)",
            }[x],
        )

        if excel_type == "excel_master":
            st.warning(
                "⚠️ El maestro de cuentas es el **Single Source of Truth**. "
                "Al cargar, se actualiza la metadata de TODAS las cuentas."
            )

        excel_file = st.file_uploader(
            "Seleccionar archivo",
            type=["xlsx", "xls", "csv"],
            key="excel_upload",
        )

        if excel_file:
            if st.button("🚀 Cargar Excel", type="primary"):
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=Path(excel_file.name).suffix,
                ) as tmp:
                    tmp.write(excel_file.read())
                    tmp_path = tmp.name

                try:
                    result = api_client.upload_file(
                        "/documents/upload",
                        filepath=tmp_path,
                        filename=excel_file.name,
                        file_type=excel_type,
                    )

                    if result.get("is_duplicate"):
                        st.warning(f"⚠️ Archivo duplicado: ya existe en el sistema")
                    else:
                        st.success(f"✅ Cargado exitosamente (ID: {result.get('id')})")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                finally:
                    Path(tmp_path).unlink(missing_ok=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 3: Documentos cargados
    # ═══════════════════════════════════════════════════════════════
    with tab3:
        st.subheader("Documentos en el sistema")

        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_type = st.selectbox(
                "Tipo",
                ["", "pdf_cartola", "pdf_report", "excel_positions",
                 "excel_movements", "excel_prices", "excel_master"],
                key="doc_filter_type",
            )
        with col2:
            filter_bank = st.selectbox(
                "Banco",
                ["", "jpmorgan", "ubs", "goldman_sachs"],
                key="doc_filter_bank",
            )
        with col3:
            filter_status = st.selectbox(
                "Estado",
                ["", "uploaded", "processing", "parsed", "validated", "error"],
                key="doc_filter_status",
            )

        try:
            params = {}
            if filter_type:
                params["file_type"] = filter_type
            if filter_bank:
                params["bank_code"] = filter_bank
            if filter_status:
                params["status"] = filter_status

            docs = api_client.get("/documents/", params=params)

            if docs:
                st.dataframe(docs, use_container_width=True)

                # Botones de acción
                col1, col2 = st.columns([1, 5])
                with col1:
                    if st.button("🗑️ Eliminar todo", type="secondary"):
                        try:
                            api_client.delete("/documents/")
                            st.success("✅ Todos los documentos eliminados")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            else:
                st.info("No hay documentos cargados con los filtros seleccionados.")

        except Exception as e:
            st.error(f"Error conectando al backend: {e}")
