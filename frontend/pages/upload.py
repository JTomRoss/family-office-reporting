"""
Página de Carga – Upload de documentos.

Funcionalidades:
- Drag & drop PDFs
- Clasificación con metadata completa
- Auto-relleno desde maestro de cuentas (por número de cuenta)
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

# ── Opciones estáticas para selectboxes ──────────────────────────
BANCOS = {
    "": "— Seleccionar —",
    "jpmorgan": "JP Morgan",
    "ubs": "UBS Suiza",
    "ubs_miami": "UBS Miami",
    "goldman_sachs": "Goldman Sachs",
    "bbh": "BBH (Brown Brothers Harriman)",
    "bice": "BICE",
}

TIPOS_CUENTA = {
    "": "— Seleccionar —",
    "custody": "Custodia",
    "current": "Cuenta Corriente",
    "savings": "Ahorro",
    "investment": "Inversión",
    "etf": "ETF",
}

TIPOS_ENTIDAD = {
    "": "— Seleccionar —",
    "sociedad": "Portafolio (Sociedad)",
    "persona": "Personal (Persona natural)",
}

MONEDAS = ["", "USD", "EUR", "CHF", "CLP", "GBP", "JPY", "BRL", "MXN"]


def _try_auto_fill(account_number: str) -> dict | None:
    """Intenta auto-completar metadata desde el maestro de cuentas."""
    if not account_number or not account_number.strip():
        return None
    try:
        return api_client.get(f"/accounts/{account_number.strip()}/auto-fill")
    except Exception:
        return None


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

        st.caption(
            "💡 **Tip:** Carga primero el maestro de cuentas (pestaña Excel) para "
            "que el auto-relleno funcione al ingresar el número de cuenta."
        )

        # ── Fila 1: Tipo documento y Banco ──────────────────────
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
            bank_options = list(BANCOS.keys())
            bank_code = st.selectbox(
                "Banco *",
                bank_options,
                format_func=lambda x: BANCOS[x],
                key="pdf_bank",
            )

        # ── Fila 2: Número de cuenta + multi-cuenta ────────────
        is_multi_account = st.checkbox(
            "📑 Varias cuentas en un documento (ej: JP Morgan Mandatos)",
            key="pdf_multi_account",
        )

        if is_multi_account:
            account_number = "Varios"
            sub_accounts = st.text_input(
                "Sub-cuentas (separadas por coma) *",
                placeholder="Ej: 2600, 3400, 9200",
                key="pdf_sub_accounts",
                help="El parser detectará automáticamente la información de cada sub-cuenta del PDF",
            )
            if sub_accounts:
                st.caption(
                    f"🔢 Sub-cuentas: {', '.join(s.strip() for s in sub_accounts.split(',') if s.strip())}"
                )
        else:
            sub_accounts = ""
            col_acct, col_btn = st.columns([3, 1])
            with col_acct:
                account_number = st.text_input(
                    "Número de cuenta *",
                    placeholder="Ej: 1234-5678-90",
                    key="pdf_account_number",
                )
            with col_btn:
                st.markdown("<br>", unsafe_allow_html=True)  # alinear con input
                auto_fill_clicked = st.button("🔍 Auto-llenar", key="btn_autofill")

        # ── Auto-fill logic ─────────────────────────────────────
        # Almacenar datos del auto-relleno en session_state
        if "autofill_data" not in st.session_state:
            st.session_state.autofill_data = {}

        if not is_multi_account and auto_fill_clicked and account_number:
            af = _try_auto_fill(account_number)
            if af:
                st.session_state.autofill_data = af
                st.success("✅ Datos auto-completados desde el maestro de cuentas")
            else:
                st.session_state.autofill_data = {}
                st.warning(
                    "⚠️ Cuenta no encontrada en el maestro. "
                    "Completa los campos manualmente."
                )

        af = st.session_state.get("autofill_data", {}) if not is_multi_account else {}

        # ── Fila 3: Sociedad / Código interno ──────────────────
        col3, col4 = st.columns(2)
        with col3:
            entity_name = st.text_input(
                "Sociedad / Nombre entidad *",
                value=af.get("entity_name", ""),
                placeholder="Ej: Inversiones ABC SpA",
                key="pdf_entity_name",
            )
        with col4:
            internal_code = st.text_input(
                "Código interno",
                placeholder="Código de referencia interno",
                key="pdf_internal_code",
            )

        # ── Fila 4: Tipo cuenta / Moneda ───────────────────────
        col5, col6 = st.columns(2)
        with col5:
            acct_type_options = list(TIPOS_CUENTA.keys())
            # Preseleccionar si auto-fill tiene dato
            af_acct_type = af.get("account_type", "")
            default_idx = (
                acct_type_options.index(af_acct_type)
                if af_acct_type in acct_type_options
                else 0
            )
            account_type = st.selectbox(
                "Tipo de cuenta *",
                acct_type_options,
                index=default_idx,
                format_func=lambda x: TIPOS_CUENTA[x],
                key="pdf_account_type",
            )
        with col6:
            af_currency = af.get("currency", "")
            cur_default = (
                MONEDAS.index(af_currency)
                if af_currency in MONEDAS
                else 0
            )
            currency = st.selectbox(
                "Moneda *",
                MONEDAS,
                index=cur_default,
                format_func=lambda x: x if x else "— Seleccionar —",
                key="pdf_currency",
            )

        # ── Fila 5: Portafolio/Personal / Nombre persona ──────
        col7, col8 = st.columns(2)
        with col7:
            entity_type_options = list(TIPOS_ENTIDAD.keys())
            af_entity_type = af.get("entity_type", "")
            et_default = (
                entity_type_options.index(af_entity_type)
                if af_entity_type in entity_type_options
                else 0
            )
            entity_type = st.selectbox(
                "Portafolio o Personal *",
                entity_type_options,
                index=et_default,
                format_func=lambda x: TIPOS_ENTIDAD[x],
                key="pdf_entity_type",
            )
        with col8:
            person_name = ""
            if entity_type == "persona":
                person_name = st.text_input(
                    "Nombre persona *",
                    placeholder="Nombre completo de la persona",
                    key="pdf_person_name",
                )
            else:
                st.text_input(
                    "Nombre persona",
                    value="",
                    disabled=True,
                    help="Solo aplica cuando es Personal",
                    key="pdf_person_name_disabled",
                )

        st.markdown("---")

        # ── File uploader ───────────────────────────────────────
        uploaded_files = st.file_uploader(
            "Arrastra PDFs aquí (uno o varios)",
            type=["pdf"],
            accept_multiple_files=True,
            key="pdf_upload",
        )

        if uploaded_files:
            st.info(f"📎 {len(uploaded_files)} archivo(s) seleccionado(s)")

            # Validar campos obligatorios
            missing = []
            if not bank_code:
                missing.append("Banco")
            if is_multi_account and not sub_accounts.strip():
                missing.append("Sub-cuentas")
            elif not is_multi_account and not account_number.strip():
                missing.append("Número de cuenta")
            if not entity_name.strip():
                missing.append("Sociedad / Nombre entidad")
            if not account_type:
                missing.append("Tipo de cuenta")
            if not currency:
                missing.append("Moneda")
            if not entity_type:
                missing.append("Portafolio o Personal")
            if entity_type == "persona" and not person_name.strip():
                missing.append("Nombre persona")

            if missing:
                st.warning(f"⚠️ Campos obligatorios faltantes: **{', '.join(missing)}**")

            if st.button("🚀 Procesar PDFs", type="primary", disabled=bool(missing)):
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
                        upload_data = {
                                "bank_code": bank_code,
                                "account_number": account_number.strip(),
                                "entity_name": entity_name.strip(),
                                "account_type": account_type,
                                "entity_type": entity_type,
                                "person_name": person_name.strip() if entity_type == "persona" else "",
                                "internal_code": internal_code.strip(),
                                "currency": currency,
                        }
                        if is_multi_account and sub_accounts.strip():
                            upload_data["sub_accounts"] = sub_accounts.strip()

                        result = api_client.upload_file(
                            "/documents/upload",
                            filepath=tmp_path,
                            filename=file.name,
                            file_type=pdf_type,
                            extra_data=upload_data,
                        )

                        if result.get("is_duplicate"):
                            dup_id = result.get("id")
                            dup_key = f"dup_action_{file.name}"
                            existing_meta = result.get("existing_metadata", {})

                            st.warning(
                                f"⚠️ **{file.name}** ya existe en el sistema "
                                f"(ID: {dup_id})."
                            )
                            if existing_meta:
                                with st.expander("📋 Clasificación actual del documento existente"):
                                    for k, v in existing_meta.items():
                                        if v:
                                            st.text(f"  {k}: {v}")

                            # Detectar si la clasificación nueva es diferente
                            old_bank = existing_meta.get("bank_code", "")
                            old_acct = existing_meta.get("account_number", "")
                            old_entity = existing_meta.get("entity_name", "")
                            new_classification_differs = (
                                (old_bank and old_bank != bank_code) or
                                (old_acct and old_acct != account_number.strip()) or
                                (old_entity and old_entity != entity_name.strip())
                            )

                            if new_classification_differs:
                                st.info(
                                    "🔄 La clasificación que ingresaste es **diferente** "
                                    "a la del documento existente."
                                )

                            col_a, col_b = st.columns(2)
                            with col_a:
                                if st.button(
                                    f"🔄 Reclasificar",
                                    key=f"reclass_{idx}",
                                    help="Actualiza la clasificación del documento existente con los nuevos datos",
                                ):
                                    try:
                                        reclass_result = api_client.post(
                                            f"/documents/{dup_id}/reclassify",
                                            json={
                                                "bank_code": bank_code,
                                                "account_number": account_number.strip(),
                                                "entity_name": entity_name.strip(),
                                                "account_type": account_type,
                                                "entity_type": entity_type,
                                                "currency": currency,
                                            },
                                        )
                                        st.success(f"✅ {file.name}: Reclasificado correctamente")
                                    except Exception as re_err:
                                        st.error(f"❌ Error reclasificando: {re_err}")
                            with col_b:
                                st.button(
                                    "⏭️ Omitir",
                                    key=f"skip_{idx}",
                                    help="No hacer nada, mantener el documento como está",
                                )
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
                    # Para excel_master, usar upload-and-process para que
                    # se parsee y se carguen las cuentas inmediatamente
                    if excel_type == "excel_master":
                        result = api_client.upload_file(
                            "/documents/upload-and-process",
                            filepath=tmp_path,
                            filename=excel_file.name,
                            file_type=excel_type,
                        )
                    else:
                        result = api_client.upload_file(
                            "/documents/upload",
                            filepath=tmp_path,
                            filename=excel_file.name,
                            file_type=excel_type,
                        )

                    if result.get("is_duplicate"):
                        if excel_type == "excel_master":
                            st.info("🔄 Maestro ya existía, se reprocesaron las cuentas.")
                        else:
                            st.warning(f"⚠️ Archivo duplicado: ya existe en el sistema")
                    else:
                        proc = result.get("process_result", {})
                        proc_status = proc.get("status", "")
                        if proc_status in ("success", "partial"):
                            ms = proc.get("master_stats", {})
                            if ms:
                                st.success(
                                    f"✅ Maestro cargado: "
                                    f"{ms.get('created', 0)} cuentas creadas, "
                                    f"{ms.get('updated', 0)} actualizadas"
                                )
                                if ms.get("errors"):
                                    st.warning(
                                        f"⚠️ {len(ms['errors'])} filas con problemas"
                                    )
                            else:
                                st.success(
                                    f"✅ Cargado y procesado "
                                    f"(ID: {result.get('id')}, "
                                    f"{proc.get('rows_parsed', 0)} filas)"
                                )
                        elif proc_status == "error":
                            st.error(
                                f"❌ Error procesando: "
                                f"{proc.get('errors', ['desconocido'])}"
                            )
                        elif proc:
                            st.warning(
                                f"⚠️ Cargado (ID: {result.get('id')}), "
                                f"estado: {proc_status}"
                            )
                        else:
                            st.success(f"✅ Cargado exitosamente (ID: {result.get('id')})")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                finally:
                    Path(tmp_path).unlink(missing_ok=True)

        # ── Tabla maestro de cuentas ────────────────────────────
        if excel_type == "excel_master":
            st.markdown("---")
            st.subheader("📋 Maestro de cuentas cargado")
            try:
                import pandas as pd
                accounts = api_client.get("/accounts/", params={"active_only": "false"})
                if accounts:
                    df = pd.DataFrame(accounts)
                    # Seleccionar y renombrar columnas para mostrar
                    display_cols = {
                        "account_number": "Nº Cuenta",
                        "bank_code": "Banco",
                        "account_type": "Tipo Cuenta",
                        "entity_name": "Sociedad",
                        "entity_type": "Tipo Entidad",
                        "currency": "Moneda",
                        "person_name": "Persona",
                        "internal_code": "Código Interno",
                        "is_active": "Activa",
                    }
                    available = [c for c in display_cols if c in df.columns]
                    df_display = df[available].rename(
                        columns={c: display_cols[c] for c in available}
                    )
                    st.dataframe(df_display, use_container_width=True, hide_index=True)
                    st.caption(f"Total: {len(df_display)} cuentas")
                else:
                    st.info("No hay cuentas cargadas aún. Sube el Excel maestro arriba.")
            except Exception as e:
                st.info("No hay cuentas cargadas aún. Sube el Excel maestro para verlas aquí.")

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
                list(BANCOS.keys()),
                format_func=lambda x: BANCOS[x],
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
