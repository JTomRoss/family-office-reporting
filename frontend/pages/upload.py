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


_UPPERCASE_TYPES = {"etf"}


def _fmt_account_type(raw: str) -> str:
    """ETF → 'ETF', brokerage → 'Brokerage', etc."""
    if not raw:
        return raw
    return raw.upper() if raw.lower() in _UPPERCASE_TYPES else raw.capitalize()


def _try_auto_fill_by_id(identification_number: str, bank_code: str = "", entity_name: str = "") -> dict | None:
    """Intenta auto-completar metadata usando dígito verificador + banco + sociedad."""
    if not identification_number or not identification_number.strip():
        return None
    try:
        params = {"identification_number": identification_number.strip()}
        if bank_code:
            params["bank_code"] = bank_code
        if entity_name:
            params["entity_name"] = entity_name.strip()
        return api_client.get("/accounts/auto-fill", params=params)
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
            "que el auto-relleno funcione al ingresar el dígito verificador."
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

        # ── Fila 2: Sociedad, Dígito verificador, Auto-llenar ──
        is_multi_account = st.checkbox(
            "📑 Varias cuentas en un documento (ej: JP Morgan Mandatos)",
            key="pdf_multi_account",
        )

        if is_multi_account:
            identification_number = "Varios"
            col_soc, col_sub = st.columns(2)
            with col_soc:
                entity_name = st.text_input(
                    "Sociedad *",
                    placeholder="Ej: Boatview",
                    key="pdf_entity_name",
                )
            with col_sub:
                sub_accounts = st.text_input(
                    "Sub-cuentas (dígitos verificadores, separados por coma) *",
                    placeholder="Ej: 2600, 3400, 9200",
                    key="pdf_sub_accounts",
                    help="El parser detectará automáticamente la información de cada sub-cuenta del PDF",
                )
            if sub_accounts:
                st.caption(
                    f"🔢 Sub-cuentas: {', '.join(s.strip() for s in sub_accounts.split(',') if s.strip())}"
                )
            auto_fill_clicked = False
        else:
            sub_accounts = ""
            col_soc, col_id, col_btn = st.columns([2, 1, 1])
            with col_soc:
                entity_name = st.text_input(
                    "Sociedad *",
                    placeholder="Ej: Armel Holdings",
                    key="pdf_entity_name",
                )
            with col_id:
                identification_number = st.text_input(
                    "Dígito verificador *",
                    placeholder="Ej: 5001",
                    key="pdf_identification_number",
                )
            with col_btn:
                st.markdown("<br>", unsafe_allow_html=True)  # alinear con input
                auto_fill_clicked = st.button("🔍 Auto-llenar", key="btn_autofill")

        # ── Auto-fill logic ─────────────────────────────────────
        if "autofill_data" not in st.session_state:
            st.session_state.autofill_data = {}
        if "autofill_version" not in st.session_state:
            st.session_state.autofill_version = 0

        if not is_multi_account and auto_fill_clicked and identification_number:
            af = _try_auto_fill_by_id(identification_number, bank_code, entity_name)
            if af:
                st.session_state.autofill_data = af
                st.session_state.autofill_version += 1
                st.success("✅ Datos auto-completados desde el maestro de cuentas")
            else:
                st.session_state.autofill_data = {}
                st.session_state.autofill_version += 1
                st.warning(
                    "⚠️ Cuenta no encontrada en el maestro. "
                    "Verifica sociedad, banco y dígito verificador."
                )

        af = st.session_state.get("autofill_data", {}) if not is_multi_account else {}
        _v = st.session_state.get("autofill_version", 0)

        # ── Campos auto-rellenados (expandible) ─────────────────
        if af:
            with st.expander("📋 Datos del maestro (auto-rellenados)", expanded=True):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.text_input("Nº Cuenta", value=af.get("account_number", ""), disabled=True, key=f"af_acct_{_v}")
                with c2:
                    st.text_input("Tipo de cuenta", value=_fmt_account_type(af.get("account_type", "")), disabled=True, key=f"af_type_{_v}")
                with c3:
                    st.text_input("Moneda", value=af.get("currency", ""), disabled=True, key=f"af_cur_{_v}")
                c4, c5, c6 = st.columns(3)
                with c4:
                    st.text_input("Portafolio/Personal", value=af.get("entity_type", ""), disabled=True, key=f"af_et_{_v}")
                with c5:
                    st.text_input("Persona", value=af.get("person_name", "") or "", disabled=True, key=f"af_person_{_v}")
                with c6:
                    st.text_input("Código interno", value=af.get("internal_code", "") or "", disabled=True, key=f"af_code_{_v}")

        # Resolver valores finales (del auto-fill o vacíos)
        account_number = af.get("account_number", "")
        account_type = af.get("account_type", "")
        currency = af.get("currency", "")
        entity_type = af.get("entity_type", "")
        person_name = af.get("person_name", "") or ""
        internal_code = af.get("internal_code", "") or ""

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

            # Validar campos obligatorios (solo banco, sociedad, dígito verificador)
            missing = []
            if not bank_code:
                missing.append("Banco")
            if not entity_name.strip():
                missing.append("Sociedad")
            if is_multi_account and not sub_accounts.strip():
                missing.append("Sub-cuentas")
            elif not is_multi_account and not identification_number.strip():
                missing.append("Dígito verificador")

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
                                "account_number": account_number.strip() if account_number else "",
                                "identification_number": identification_number.strip() if identification_number else "",
                                "entity_name": entity_name.strip(),
                                "account_type": account_type,
                                "entity_type": entity_type,
                                "person_name": person_name.strip() if person_name else "",
                                "internal_code": internal_code.strip() if internal_code else "",
                                "currency": currency,
                        }
                        if is_multi_account and sub_accounts.strip():
                            upload_data["sub_accounts"] = sub_accounts.strip()

                        result = api_client.upload_file(
                            "/documents/upload-and-process",
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
                            proc = result.get("process_result", {})
                            proc_status = proc.get("status", "")
                            loading = proc.get("loading_stats", {})
                            mc = loading.get("monthly_closings", 0)
                            ec = loading.get("etf_compositions", 0)
                            rows = proc.get("rows_parsed", 0)
                            if proc_status in ("success", "partial"):
                                detail = f"{rows} filas parseadas"
                                if mc:
                                    detail += f", {mc} cierres mensuales"
                                if ec:
                                    detail += f", {ec} composiciones ETF"
                                st.success(f"✅ {file.name}: {detail}")
                            elif proc_status == "error":
                                st.error(
                                    f"❌ {file.name}: "
                                    f"{proc.get('errors', ['error desconocido'])}"
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
                        "identification_number": "Nº Identificación",
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
                    if "Tipo Cuenta" in df_display.columns:
                        df_display["Tipo Cuenta"] = df_display["Tipo Cuenta"].apply(_fmt_account_type)
                    st.dataframe(df_display, use_container_width=True, hide_index=True)
                    st.caption(f"Total: {len(df_display)} cuentas")

                    # Botón eliminar cuentas
                    if st.button("🗑️ Eliminar todas las cuentas", type="secondary", key="del_accounts"):
                        try:
                            result = api_client.delete("/accounts/")
                            ct = result.get("count", 0)
                            st.success(f"✅ {ct} cuentas eliminadas del maestro")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
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
                import pandas as pd
                df_docs = pd.DataFrame(docs)

                # Renombrar columnas para display
                col_rename = {
                    "id": "ID",
                    "filename": "Archivo",
                    "file_type": "Tipo",
                    "bank_code": "Banco",
                    "status": "Estado",
                    "uploaded_at": "Subido",
                }
                show_cols = [c for c in col_rename if c in df_docs.columns]
                df_show = df_docs[show_cols].rename(columns=col_rename)

                # Agregar columna de selección para eliminación
                df_show.insert(len(df_show.columns), "Eliminar", False)

                edited_df = st.data_editor(
                    df_show,
                    use_container_width=True,
                    hide_index=True,
                    disabled=[c for c in df_show.columns if c != "Eliminar"],
                    column_config={
                        "Eliminar": st.column_config.CheckboxColumn(
                            "🗑️",
                            help="Selecciona los documentos a eliminar",
                            default=False,
                        ),
                    },
                    key="doc_table_editor",
                )

                # ── Eliminar seleccionados ──────────────────────
                selected_mask = edited_df["Eliminar"] == True
                selected_count = selected_mask.sum()

                col_del, col_reproc, col_info = st.columns([1, 1, 2])
                with col_del:
                    if st.button(
                        f"🗑️ Eliminar seleccionados ({selected_count})",
                        disabled=selected_count == 0,
                        key="btn_del_selected",
                    ):
                        selected_ids = df_docs.loc[selected_mask.values, "id"].tolist()
                        deleted = 0
                        for doc_id in selected_ids:
                            try:
                                api_client.delete(f"/documents/{doc_id}")
                                deleted += 1
                            except Exception:
                                pass
                        st.success(f"✅ {deleted} documento(s) eliminado(s)")
                        st.rerun()
                with col_reproc:
                    # Contar docs uploaded (no procesados)
                    unprocessed = df_docs[df_docs["status"] == "uploaded"]
                    if st.button(
                        f"🔄 Procesar pendientes ({len(unprocessed)})",
                        disabled=len(unprocessed) == 0,
                        key="btn_process_all",
                    ):
                        processed = 0
                        for doc_id in unprocessed["id"].tolist():
                            try:
                                api_client.post(f"/documents/{doc_id}/process")
                                processed += 1
                            except Exception:
                                pass
                        st.success(f"✅ {processed} documento(s) procesado(s)")
                        st.rerun()
                with col_info:
                    if selected_count > 0:
                        st.caption(f"📌 {selected_count} documento(s) seleccionado(s)")

                # ── Eliminación masiva ──────────────────────────
                st.markdown("---")
                if st.button("🗑️ Eliminar TODO (documentos + cuentas)", type="secondary"):
                    try:
                        result = api_client.delete("/documents/")
                        d_ct = result.get("documents_deleted", 0)
                        a_ct = result.get("accounts_deleted", 0)
                        st.success(
                            f"✅ Eliminados: {d_ct} documentos, {a_ct} cuentas"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
            else:
                st.info("No hay documentos cargados con los filtros seleccionados.")

        except Exception as e:
            st.error(f"Error conectando al backend: {e}")
