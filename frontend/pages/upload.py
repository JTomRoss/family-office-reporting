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
from html import escape
from io import BytesIO
from pathlib import Path

from frontend import api_client
from frontend.components.table_utils import render_table

# ── Opciones estáticas para selectboxes ──────────────────────────
BANCOS = {
    "": "— Seleccionar —",
    "jpmorgan": "JP Morgan",
    "ubs": "UBS Suiza",
    "ubs_miami": "UBS Miami",
    "goldman_sachs": "Goldman Sachs",
    "bbh": "BBH (Brown Brothers Harriman)",
    "bice": "BICE",
    "alternativos": "Alternativos",
}


_UPPERCASE_TYPES = {"etf"}
_BATCH_PREVIEW_EDITOR_KEY = "batch_preview_editor"
_BATCH_PREVIEW_PAYLOAD_FIELDS = (
    "status",
    "confidence",
    "recognition_reason",
    "account_id",
    "account_number",
    "identification_number",
    "bank_code",
    "entity_name",
    "account_type",
    "entity_type",
    "person_name",
    "internal_code",
    "currency",
)


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


def _load_master_accounts() -> list[dict]:
    """Carga maestro completo para poblar selectores en UI."""
    try:
        return api_client.get("/accounts/", params={"active_only": "false"})
    except Exception:
        return []


def _batch_account_label(account: dict, duplicated_bases: set[str]) -> str:
    base = str(account.get("identification_number") or account.get("account_number") or "").strip()
    if not base:
        return ""
    if base not in duplicated_bases:
        return base
    return " | ".join(
        part for part in [
            base,
            str(account.get("entity_name") or "").strip(),
            _fmt_account_type(str(account.get("account_type") or "").strip()),
            BANCOS.get(str(account.get("bank_code") or "").strip(), str(account.get("bank_code") or "").strip()),
        ] if part
    )


def _build_batch_preview_row(*, uploaded_file, row_key: str, payload_row: dict) -> dict:
    preview_row = {
        "row_key": row_key,
        "filename": payload_row.get("filename", uploaded_file.name),
        "selected": False,
    }
    for field in _BATCH_PREVIEW_PAYLOAD_FIELDS:
        preview_row[field] = payload_row.get(field, "")
    return preview_row


def _apply_preview_payload_to_rows(rows: list[dict], payload_rows: list[dict]) -> None:
    for row, payload_row in zip(rows, payload_rows):
        for field in _BATCH_PREVIEW_PAYLOAD_FIELDS:
            row[field] = payload_row.get(field, "")
        row["selected"] = False


def _stable_batch_file_key(uploaded_file) -> str:
    return f"{uploaded_file.name}:{getattr(uploaded_file, 'size', 0)}"


def _request_batch_context_reset() -> None:
    st.session_state["batch_ctx_reset_requested"] = True


def _bump_batch_preview_editor_nonce() -> int:
    current = int(st.session_state.get("batch_preview_editor_nonce", 0)) + 1
    st.session_state["batch_preview_editor_nonce"] = current
    return current


def _render_cartola_coverage_matrix(rows: list[dict], *, year: int) -> None:
    if not rows:
        st.info("No hay datos suficientes para construir la matriz de cobertura.")
        return

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        entity_name = str(row.get("entity_name") or "").strip()
        account_type = str(row.get("account_type") or "").strip()
        if not entity_name or not account_type:
            continue
        grouped.setdefault(entity_name, []).append(row)

    if not grouped:
        st.info("No hay datos suficientes para construir la matriz de cobertura.")
        return

    month_headers = [f"{year}-{month:02d}" for month in range(1, 13)]
    html_parts = [
        """
        <style>
          .coverage-wrap {
            overflow-x: auto;
            margin-top: 0.5rem;
          }
          table.coverage-matrix {
            border-collapse: collapse;
            width: 100%;
            min-width: 1180px;
            font-size: 0.92rem;
          }
          table.coverage-matrix th,
          table.coverage-matrix td {
            border: 1px solid #cfd7e3;
            padding: 6px 8px;
            text-align: center;
          }
          table.coverage-matrix th {
            background: #f3f6fb;
            font-weight: 600;
            white-space: nowrap;
          }
          table.coverage-matrix td.entity {
            text-align: left;
            font-weight: 600;
            min-width: 190px;
            background: #fbfcfe;
          }
          table.coverage-matrix td.account-type {
            text-align: left;
            min-width: 130px;
            background: #ffffff;
          }
          table.coverage-matrix td.month {
            min-width: 54px;
            height: 32px;
            padding: 0;
          }
          table.coverage-matrix td.month.loaded {
            background: #f2cfee;
          }
          table.coverage-matrix td.month.empty {
            background: #ffffff;
          }
        </style>
        <div class="coverage-wrap">
        <table class="coverage-matrix">
          <thead>
            <tr>
              <th>Sociedad</th>
              <th>Tipo de cuenta</th>
        """
    ]
    html_parts.extend(f"<th>{escape(header)}</th>" for header in month_headers)
    html_parts.append("</tr></thead><tbody>")

    for entity_name in sorted(grouped.keys()):
        entity_rows = sorted(
            grouped[entity_name],
            key=lambda item: _fmt_account_type(str(item.get("account_type") or "")),
        )
        rowspan = len(entity_rows)
        for idx, row in enumerate(entity_rows):
            loaded_months = {
                int(month)
                for month in row.get("loaded_months", [])
                if isinstance(month, int) or str(month).isdigit()
            }
            html_parts.append("<tr>")
            if idx == 0:
                html_parts.append(
                    f"<td class='entity' rowspan='{rowspan}'>{escape(entity_name)}</td>"
                )
            html_parts.append(
                f"<td class='account-type'>{escape(_fmt_account_type(str(row.get('account_type') or '')))}</td>"
            )
            for month in range(1, 13):
                css_class = "loaded" if month in loaded_months else "empty"
                html_parts.append(f"<td class='month {css_class}'></td>")
            html_parts.append("</tr>")

    html_parts.append("</tbody></table></div>")
    st.markdown("".join(html_parts), unsafe_allow_html=True)


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

        # Modo: Carga guiada (una por una) vs Carga por lote
        upload_mode = st.radio(
            "Modo de carga",
            ["Carga guiada (una por una)", "Carga por lote con reconocimiento"],
            key="pdf_upload_mode",
            horizontal=True,
        )
        is_batch_mode = upload_mode == "Carga por lote con reconocimiento"
        st.markdown("---")

        if is_batch_mode:
            if st.session_state.pop("batch_ctx_reset_requested", False):
                st.session_state["batch_ctx_bank"] = ""
                st.session_state["batch_ctx_entity"] = ""
                st.session_state["batch_ctx_type"] = ""
            st.caption(
                "Usa estos campos para confirmar manualmente las filas marcadas con ✕ cuando una cartola quede ambigua o sin match."
            )
            opts = api_client.get("/accounts/filter-options")
            batch_entities = sorted(opts.get("entity_names", []) or [])
            batch_types = sorted(opts.get("account_types", []) or [])
            bank_options = [k for k in BANCOS if k]
            col_b, col_e, col_t = st.columns(3)
            with col_b:
                batch_ctx_bank = st.selectbox(
                    "Banco (contexto)",
                    options=[""] + bank_options,
                    format_func=lambda x: BANCOS.get(x, x) if x else "— Ninguno —",
                    key="batch_ctx_bank",
                )
            with col_e:
                batch_ctx_entity = st.selectbox(
                    "Sociedad (contexto)",
                    options=[""] + batch_entities,
                    key="batch_ctx_entity",
                )
            with col_t:
                batch_ctx_type = st.selectbox(
                    "Tipo de cuenta (contexto)",
                    options=[""] + batch_types,
                    format_func=lambda x: _fmt_account_type(x) if x else "— Ninguno —",
                    key="batch_ctx_type",
                )
            pdf_type_batch = st.selectbox(
                "Tipo de documento",
                ["pdf_cartola", "pdf_report"],
                format_func=lambda x: "Cartola bancaria" if x == "pdf_cartola" else "Reporte mandato",
                key="batch_pdf_type",
            )
            st.markdown("---")
            batch_files = st.file_uploader(
                "Subir varias cartolas (PDFs)",
                type=["pdf"],
                accept_multiple_files=True,
                key="batch_pdf_upload",
            )
            if batch_files:
                import pandas as pd

                master_accounts = _load_master_accounts()
                file_entries = [(_stable_batch_file_key(file), file) for file in batch_files]
                batch_preview_signature = {
                    "file_keys": [row_key for row_key, _ in file_entries],
                    "file_type": pdf_type_batch,
                }
                if st.session_state.get("batch_preview_signature") != batch_preview_signature:
                    existing_rows_by_key = {
                        str(row.get("row_key") or ""): dict(row)
                        for row in st.session_state.get("batch_preview_rows", [])
                    }
                    new_file_entries = [
                        (row_key, uploaded_file)
                        for row_key, uploaded_file in file_entries
                        if row_key not in existing_rows_by_key
                    ]
                    new_rows_by_key: dict[str, dict] = {}
                    if new_file_entries:
                        preview_payload = api_client.post(
                            "/documents/preview-batch-recognition",
                            json={
                                "filenames": [uploaded_file.name for _, uploaded_file in new_file_entries],
                            },
                        )
                        payload_rows = preview_payload.get("rows", [])
                        for (row_key, uploaded_file), payload_row in zip(new_file_entries, payload_rows):
                            new_rows_by_key[row_key] = _build_batch_preview_row(
                                uploaded_file=uploaded_file,
                                row_key=row_key,
                                payload_row=payload_row,
                            )

                    preview_rows = []
                    for row_key, uploaded_file in file_entries:
                        existing_row = existing_rows_by_key.get(row_key)
                        row = existing_row if existing_row is not None else new_rows_by_key.get(row_key)
                        if row is None:
                            continue
                        row["selected"] = False
                        preview_rows.append(row)
                    st.session_state["batch_preview_rows"] = preview_rows
                    st.session_state["batch_preview_signature"] = batch_preview_signature
                    _bump_batch_preview_editor_nonce()
                    _request_batch_context_reset()
                    st.rerun()

                preview_rows = list(st.session_state.get("batch_preview_rows", []))
                file_lookup = {
                    row_key: uploaded_file
                    for row_key, uploaded_file in file_entries
                }
                preview_rows = [row for row in preview_rows if row.get("row_key") in file_lookup]
                st.session_state["batch_preview_rows"] = preview_rows

                base_account_labels = [
                    str(account.get("identification_number") or account.get("account_number") or "").strip()
                    for account in master_accounts
                    if str(account.get("identification_number") or account.get("account_number") or "").strip()
                ]
                duplicated_bases = {
                    label for label in base_account_labels
                    if base_account_labels.count(label) > 1
                }
                preview_df = pd.DataFrame(
                    [
                        {
                            "Archivo": row.get("filename", ""),
                            "Banco": BANCOS.get(row.get("bank_code", ""), row.get("bank_code", "")) or "— Seleccionar —",
                            "Sociedad": row.get("entity_name", "") or "— Seleccionar —",
                            "Tipo cuenta": _fmt_account_type(row.get("account_type", "")) or "— Seleccionar —",
                            "Cuenta": (
                                _batch_account_label(row, duplicated_bases)
                                if (row.get("identification_number") or row.get("account_number"))
                                else "— Seleccionar —"
                            ),
                            "Estado": row.get("status", ""),
                            "Confianza": row.get("confidence", ""),
                            "Detalle": row.get("recognition_reason", ""),
                            "✕": bool(row.get("selected")),
                        }
                        for row in preview_rows
                    ]
                )

                edited_preview_df = st.data_editor(
                    preview_df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    disabled=["Archivo", "Banco", "Sociedad", "Tipo cuenta", "Cuenta", "Estado", "Confianza", "Detalle"],
                    column_config={
                        "✕": st.column_config.CheckboxColumn(
                            "✕",
                            help="Quita esta fila del lote antes de procesar",
                            default=False,
                        ),
                    },
                    key=f"{_BATCH_PREVIEW_EDITOR_KEY}_{int(st.session_state.get('batch_preview_editor_nonce', 0))}",
                )

                for idx, edited_row in enumerate(edited_preview_df.to_dict("records")):
                    if idx >= len(preview_rows):
                        continue
                    preview_row = preview_rows[idx]
                    preview_row["selected"] = bool(edited_row.get("✕"))
                st.session_state["batch_preview_rows"] = preview_rows

                recognized_count = sum(1 for row in preview_rows if row.get("status") == "reconocido" and row.get("account_id"))
                ambiguous_count = sum(1 for row in preview_rows if row.get("status") == "ambiguo")
                no_match_count = sum(1 for row in preview_rows if row.get("status") == "sin_match")

                st.info(
                    f"Preview: {recognized_count} reconocidas, {ambiguous_count} ambiguas, "
                    f"{no_match_count} sin match. Marca ✕ para quitar filas o confirmar manualmente su contexto."
                )

                selected_rows = [row for row in preview_rows if row.get("selected")]
                selected_count = len(selected_rows)
                processable_rows = [
                    row for row in preview_rows
                    if row.get("status") == "reconocido" and row.get("account_id")
                ]
                unresolved_count = len(preview_rows) - len(processable_rows)

                action_col1, action_col2, action_col3, action_col4 = st.columns([1.2, 1.4, 1, 2])
                with action_col1:
                    if st.button(
                        f"Aplicar contexto ({selected_count})",
                        disabled=selected_count == 0 or not (batch_ctx_bank or batch_ctx_entity or batch_ctx_type),
                        key="batch_apply_context",
                    ):
                        preview_payload = api_client.post(
                            "/documents/apply-batch-context",
                            json={
                                "filenames": [row.get("filename", "") for row in selected_rows],
                                "bank_code": batch_ctx_bank,
                                "entity_name": batch_ctx_entity,
                                "account_type": batch_ctx_type,
                            },
                        )
                        _apply_preview_payload_to_rows(selected_rows, preview_payload.get("rows", []))
                        st.session_state["batch_preview_rows"] = preview_rows
                        _bump_batch_preview_editor_nonce()
                        _request_batch_context_reset()
                        st.rerun()
                with action_col2:
                    if st.button(
                        f"Quitar seleccionados ({selected_count})",
                        disabled=selected_count == 0,
                        key="batch_remove_selected",
                    ):
                        st.session_state["batch_preview_rows"] = [
                            row for row in preview_rows if not row.get("selected")
                        ]
                        _bump_batch_preview_editor_nonce()
                        st.rerun()
                with action_col3:
                    if st.button(
                        f"Procesar lote ({len(processable_rows)})",
                        type="primary",
                        disabled=len(processable_rows) == 0,
                        key="batch_process_all",
                    ):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        processed = 0
                        duplicates = 0
                        errors: list[str] = []
                        total = len(processable_rows)
                        for idx, row in enumerate(processable_rows, start=1):
                            uploaded_file = file_lookup.get(row.get("row_key", ""))
                            if uploaded_file is None:
                                errors.append(f"{row.get('filename', 'archivo')}: archivo no disponible en memoria")
                                continue
                            status_text.text(f"Procesando: {row.get('filename', '')}...")
                            progress_bar.progress(int(((idx - 1) / total) * 100))
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                                uploaded_file.seek(0)
                                tmp.write(uploaded_file.read())
                                tmp_path = tmp.name
                            try:
                                result = api_client.upload_file(
                                    "/documents/upload-and-process",
                                    filepath=tmp_path,
                                    filename=row.get("filename", ""),
                                    file_type=pdf_type_batch,
                                    extra_data={
                                        "account_id": row.get("account_id"),
                                        "bank_code": row.get("bank_code", ""),
                                        "account_number": row.get("account_number", ""),
                                        "identification_number": row.get("identification_number", ""),
                                        "entity_name": row.get("entity_name", ""),
                                        "account_type": row.get("account_type", ""),
                                        "entity_type": row.get("entity_type", ""),
                                        "person_name": row.get("person_name", "") or "",
                                        "internal_code": row.get("internal_code", "") or "",
                                        "currency": row.get("currency", ""),
                                    },
                                )
                                if result.get("is_duplicate"):
                                    duplicates += 1
                                else:
                                    processed += 1
                            except Exception as exc:
                                errors.append(f"{row.get('filename', '')}: {exc}")
                            finally:
                                Path(tmp_path).unlink(missing_ok=True)

                        progress_bar.progress(100)
                        status_text.text("Proceso terminado.")
                        if processed:
                            st.success(f"Procesadas: {processed} cartola(s).")
                        if duplicates:
                            st.warning(f"Duplicadas detectadas: {duplicates}.")
                        if unresolved_count:
                            st.info(f"Quedaron sin procesar {unresolved_count} fila(s) no reconocidas o ambiguas.")
                        for err in errors:
                            st.error(err)
                with action_col4:
                    st.caption(
                        f"Filas en lote: {len(preview_rows)}. Seleccionadas: {selected_count}. "
                        f"Procesables: {len(processable_rows)}."
                    )

                if unresolved_count:
                    st.warning(
                        "Para corregir filas ambiguas, marca ✕ en esas filas, completa Banco/Sociedad/Tipo de cuenta arriba "
                        "y usa 'Aplicar contexto'. Ese botón confirma manualmente la cuenta cuando el contexto identifica una sola."
                    )
            else:
                st.caption("Selecciona uno o más PDFs para ver la tabla de reconocimiento previo.")

        else:
            # ── Carga guiada (flujo actual) ─────────────────────
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
                        "pdf_report": "📘 Reporte mandato",
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
            master_accounts = _load_master_accounts()
            master_entities = sorted({
                (a.get("entity_name") or "").strip()
                for a in master_accounts
                if (a.get("entity_name") or "").strip()
            })

            if is_multi_account:
                identification_number = "Varios"
                col_soc, col_sub = st.columns(2)
                with col_soc:
                    use_new_entity_multi = st.checkbox(
                        "Crear nueva sociedad",
                        key="pdf_new_entity_multi",
                    )
                    if use_new_entity_multi:
                        entity_name = st.text_input(
                            "Sociedad nueva *",
                            placeholder="Ej: Boatview",
                            key="pdf_entity_name_new_multi",
                        )
                    else:
                        if master_entities:
                            entity_name = st.selectbox(
                                "Sociedad *",
                                options=[""] + master_entities,
                                key="pdf_entity_name_sel_multi",
                            )
                        else:
                            entity_name = st.text_input(
                                "Sociedad *",
                                placeholder="Ej: Boatview",
                                key="pdf_entity_name_no_master_multi",
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
                    use_new_entity = st.checkbox(
                        "Crear nueva sociedad",
                        key="pdf_new_entity",
                    )
                    if use_new_entity:
                        entity_name = st.text_input(
                            "Sociedad nueva *",
                            placeholder="Ej: Armel Holdings",
                            key="pdf_entity_name_new",
                        )
                    else:
                        if master_entities:
                            entity_name = st.selectbox(
                                "Sociedad *",
                                options=[""] + master_entities,
                                key="pdf_entity_name_sel",
                            )
                        else:
                            entity_name = st.text_input(
                                "Sociedad *",
                                placeholder="Ej: Armel Holdings",
                                key="pdf_entity_name_no_master",
                            )
                with col_id:
                    ids_for_combo = sorted({
                        (a.get("identification_number") or "").strip()
                        for a in master_accounts
                        if (a.get("identification_number") or "").strip()
                        and (not bank_code or (a.get("bank_code") or "") == bank_code)
                        and (not entity_name.strip() or (a.get("entity_name") or "").strip() == entity_name.strip())
                    })
                    use_new_id = st.checkbox("Nuevo dígito verificador", key="pdf_new_id")
                    if use_new_id:
                        identification_number = st.text_input(
                            "Dígito verificador nuevo *",
                            placeholder="Ej: 5001",
                            key="pdf_identification_number_new",
                        )
                    else:
                        if ids_for_combo:
                            identification_number = st.selectbox(
                                "Dígito verificador *",
                                options=[""] + ids_for_combo,
                                key="pdf_identification_number_sel",
                                help="Opciones filtradas por Banco + Sociedad desde maestro de cuentas",
                            )
                        else:
                            identification_number = st.text_input(
                                "Dígito verificador *",
                                placeholder="Ej: 5001",
                                key="pdf_identification_number_no_master",
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
            [
                "excel_positions",
                "excel_movements",
                "excel_prices",
                "excel_alternatives",
                "excel_master",
            ],
            format_func=lambda x: {
                "excel_positions": "📊 Posiciones diarias",
                "excel_movements": "💱 Movimientos diarios",
                "excel_prices": "💰 Precios (FX + activos)",
                "excel_alternatives": "🏗️ Alternativos (NAV + Movimientos)",
                "excel_master": "🏛️ Maestro de cuentas (SSOT)",
            }[x],
        )

        if excel_type == "excel_master":
            st.warning(
                "⚠️ El maestro de cuentas es el **Single Source of Truth**. "
                "Al cargar, se actualiza la metadata de TODAS las cuentas."
            )
        elif excel_type == "excel_alternatives":
            st.info(
                "Este motor carga el Excel de Alternativos como una cartola independiente. "
                "Persiste datos agregados por sociedad + clase de activo + estrategia + moneda "
                "en la capa normalizada y los expone como subcuentas del banco sintético `Alternativos`."
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
                    # Procesar inmediatamente para que los datos queden disponibles al cargar.
                    if excel_type in {
                        "excel_master",
                        "excel_positions",
                        "excel_movements",
                        "excel_prices",
                        "excel_alternatives",
                    }:
                        extra_data = {"bank_code": "alternativos"} if excel_type == "excel_alternatives" else None
                        result = api_client.upload_file(
                            "/documents/upload-and-process",
                            filepath=tmp_path,
                            filename=excel_file.name,
                            file_type=excel_type,
                            extra_data=extra_data,
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
                            ls = proc.get("loading_stats", {})
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
                            elif ls:
                                if excel_type == "excel_alternatives":
                                    st.success(
                                        "✅ Alternativos cargado: "
                                        f"{ls.get('normalized_rows', 0)} filas normalizadas, "
                                        f"{ls.get('accounts_created', 0)} subcuentas creadas, "
                                        f"{ls.get('accounts_updated', 0)} actualizadas"
                                    )
                                    if ls.get("accounts_deleted"):
                                        st.caption(
                                            f"Subcuentas eliminadas por quedar fuera del archivo: "
                                            f"{ls.get('accounts_deleted', 0)}"
                                        )
                                else:
                                    st.success(
                                        "✅ Datos operativos cargados: "
                                        f"{ls.get('daily_positions', 0)} posiciones, "
                                        f"{ls.get('daily_movements', 0)} movimientos, "
                                        f"{ls.get('daily_prices', 0)} precios"
                                    )
                                if ls.get("errors"):
                                    st.warning(
                                        f"⚠️ {len(ls['errors'])} fila(s) con problemas de carga"
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
                    export_cols = [
                        "account_number",
                        "identification_number",
                        "bank_code",
                        "bank_name",
                        "account_type",
                        "entity_name",
                        "entity_type",
                        "currency",
                        "country",
                        "mandate_type",
                        "person_name",
                        "internal_code",
                        "is_active",
                    ]
                    export_df = df[[c for c in export_cols if c in df.columns]].copy()

                    c_dl1, c_dl2 = st.columns(2)
                    with c_dl1:
                        csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
                        st.download_button(
                            "⬇️ Descargar maestro (CSV)",
                            data=csv_bytes,
                            file_name="maestro_cuentas_actual.csv",
                            mime="text/csv",
                            key="dl_master_csv",
                        )
                    with c_dl2:
                        try:
                            xlsx_buffer = BytesIO()
                            with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
                                export_df.to_excel(writer, index=False, sheet_name="master_accounts")
                            st.download_button(
                                "⬇️ Descargar maestro (XLSX)",
                                data=xlsx_buffer.getvalue(),
                                file_name="maestro_cuentas_actual.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="dl_master_xlsx",
                            )
                        except Exception:
                            st.caption("XLSX no disponible en este entorno (usa CSV).")

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
                    render_table(df_display)
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
        col1, col2 = st.columns(2)
        with col1:
            filter_type = st.selectbox(
                "Tipo",
                ["", "pdf_cartola", "pdf_report", "excel_positions",
                 "excel_movements", "excel_prices", "excel_alternatives", "excel_master"],
                key="doc_filter_type",
            )
        with col2:
            filter_bank = st.selectbox(
                "Banco",
                list(BANCOS.keys()),
                format_func=lambda x: BANCOS[x],
                key="doc_filter_bank",
            )

        # Buscador rápido (entre filtros y tabla)
        quick_search = st.text_input(
            "Buscar",
            value="",
            placeholder="Buscar por ID, archivo, tipo o banco...",
            key="doc_quick_search",
        ).strip().lower()

        try:
            params = {}
            if filter_type:
                params["file_type"] = filter_type
            if filter_bank:
                params["bank_code"] = filter_bank

            docs = api_client.get("/documents/", params=params)
            if quick_search and docs:
                filtered_docs = []
                for d in docs:
                    haystack = " ".join([
                        str(d.get("id", "")),
                        str(d.get("filename", "")),
                        str(d.get("file_type", "")),
                        str(d.get("bank_code", "")),
                    ]).lower()
                    if quick_search in haystack:
                        filtered_docs.append(d)
                docs = filtered_docs

            if docs:
                import pandas as pd
                df_docs = pd.DataFrame(docs)
                # Índice 0..n-1 para alinear selección con filas
                df_docs = df_docs.reset_index(drop=True)

                # Renombrar columnas para display
                col_rename = {
                    "id": "ID",
                    "filename": "Archivo",
                    "file_type": "Tipo",
                    "bank_code": "Banco",
                    "entity_name": "Sociedad",
                    "account_type": "Tipo de cuenta",
                    "status": "Estado",
                    "uploaded_at": "Subido",
                }
                show_cols = [c for c in col_rename if c in df_docs.columns]
                df_show = df_docs[show_cols].rename(columns=col_rename).reset_index(drop=True)
                if "ID" in df_show.columns:
                    df_show["ID"] = df_show["ID"].astype(str)
                if "Tipo de cuenta" in df_show.columns:
                    df_show["Tipo de cuenta"] = df_show["Tipo de cuenta"].map(
                        lambda value: "Multiple" if str(value or "").strip().lower() == "multiple" else (
                            _fmt_account_type(str(value or "")) if value else ""
                        )
                    )

                # Mensaje en verde tras eliminar (persiste después del rerun)
                if "doc_deleted_count" in st.session_state:
                    n = st.session_state.pop("doc_deleted_count", 0)
                    if n > 0:
                        st.success("✅ Selección eliminada.")

                # Agregar columna de selección para eliminación
                df_show.insert(len(df_show.columns), "Eliminar", False)

                edited_df = st.data_editor(
                    df_show,
                    use_container_width=True,
                    hide_index=True,
                    disabled=[c for c in df_show.columns if c != "Eliminar"],
                    column_config={
                        "ID": st.column_config.TextColumn("ID"),
                        "Eliminar": st.column_config.CheckboxColumn(
                            "🗑️",
                            help="Selecciona los documentos a eliminar",
                            default=False,
                        ),
                    },
                    key="doc_table_editor",
                )

                # ── Eliminar seleccionados ──────────────────────
                eliminar_col = edited_df["Eliminar"].fillna(False)
                # Aceptar True, 1, "true" por compatibilidad con el widget
                selected_mask = (
                    (eliminar_col == True)
                    | (eliminar_col.astype(str).str.lower() == "true")
                    | (eliminar_col == 1)
                )
                selected_count = int(selected_mask.sum())

                col_del, col_reproc, col_info = st.columns([1, 1, 2])
                with col_del:
                    if st.button(
                        f"🗑️ Eliminar seleccionados ({selected_count})",
                        disabled=selected_count == 0,
                        key="btn_del_selected",
                    ):
                        # IDs por índice desde df_docs: misma fila que ve el usuario
                        selected_indices = edited_df.index[selected_mask]
                        doc_ids = df_docs.loc[selected_indices, "id"].astype(int).tolist()
                        deleted = 0
                        errors = []
                        for doc_id in doc_ids:
                            try:
                                api_client.delete(f"/documents/{doc_id}")
                                deleted += 1
                            except Exception as e:
                                errors.append(f"ID {doc_id}: {e}")
                        if errors:
                            for err in errors:
                                st.error(err)
                        if deleted > 0:
                            st.session_state["doc_deleted_count"] = deleted
                            st.rerun()
                        elif doc_ids and not errors:
                            st.warning("No se eliminó ningún documento (revisar IDs).")
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

        st.markdown("---")
        st.subheader("Cobertura de Cartolas por Sociedad y Tipo de Cuenta")
        st.caption("Cada celda se pinta cuando existe cartola persistida para ese mes. Blanco = sin cartola.")

        try:
            coverage_seed = api_client.get("/documents/cartola-coverage")
            available_entities = coverage_seed.get("entities", [])
            year_options = sorted({2026, *coverage_seed.get("available_years", [])}, reverse=True)
            if not year_options:
                year_options = [2026]

            if "doc_coverage_entity" not in st.session_state:
                st.session_state["doc_coverage_entity"] = "__all__"
            valid_entities = {"__all__", *available_entities}
            if st.session_state["doc_coverage_entity"] not in valid_entities:
                st.session_state["doc_coverage_entity"] = "__all__"

            if "doc_coverage_year" not in st.session_state or st.session_state["doc_coverage_year"] not in year_options:
                st.session_state["doc_coverage_year"] = 2026 if 2026 in year_options else year_options[0]

            filter_soc_col, filter_year_col = st.columns(2)
            with filter_soc_col:
                selected_entity = st.selectbox(
                    "Sociedad",
                    options=["__all__"] + available_entities,
                    format_func=lambda value: "Todas las sociedades" if value == "__all__" else value,
                    key="doc_coverage_entity",
                )
            with filter_year_col:
                selected_year = st.selectbox(
                    "Año",
                    options=year_options,
                    key="doc_coverage_year",
                )

            coverage_params = {"year": int(selected_year)}
            if selected_entity != "__all__":
                coverage_params["entity_name"] = selected_entity
            coverage_payload = api_client.get("/documents/cartola-coverage", params=coverage_params)
            _render_cartola_coverage_matrix(
                coverage_payload.get("rows", []),
                year=int(coverage_payload.get("selected_year") or selected_year),
            )
        except Exception as exc:
            st.info(f"No se pudo construir la matriz de cobertura: {exc}")


