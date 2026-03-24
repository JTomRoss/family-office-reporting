"""
Pagina Operacional: solo salud de datos.
"""

import pandas as pd
import streamlit as st

from frontend import api_client
from frontend.components.data_health import fetch_health_report
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.table_utils import render_table


def _revision_fmt_num(value) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return fmt_number(value)


def _revision_fmt_pct(value) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return fmt_percent(value)


def _revision_results_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "fecha_cartola" in out.columns:
        fc = pd.to_datetime(out["fecha_cartola"], errors="coerce")
        out["fecha_cartola"] = fc.dt.strftime("%d-%m-%Y").fillna("")
    for col in ("monto_agente", "monto_bd", "diferencia"):
        if col in out.columns:
            out[col] = out[col].map(_revision_fmt_num)
    if "diferencia_pct" in out.columns:
        out["diferencia_pct"] = out["diferencia_pct"].map(_revision_fmt_pct)
    rename = {
        "fecha_cartola": "Fecha cartola",
        "documento_id": "ID documento",
        "archivo": "Archivo",
        "sociedad": "Sociedad",
        "banco": "Banco",
        "tipo_cuenta": "Tipo cuenta",
        "id_cuenta": "Id cuenta",
        "elemento_revisado": "Elemento revisado",
        "monto_agente": "Monto leído (agente)",
        "monto_bd": "Monto en BD",
        "diferencia": "Diferencia",
        "diferencia_pct": "Diferencia %",
        "nivel": "Nivel",
        "nota": "Nota",
        "parser": "Parser",
    }
    return out.rename(columns={k: v for k, v in rename.items() if k in out.columns})


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


def _render_revision_tab() -> None:
    st.subheader("Auditoria por LLM (PDF vs capa normalizada)")
    st.caption(
        "Solo lectura; no modifica la base de datos. Se extrae texto del PDF y el modelo "
        "interpreta montos usando las **reglas de los motores de lectura** del sistema (prompt interno). "
        "Requiere `OPENAI_API_KEY` (variable de entorno o línea en el archivo `.env` en la raíz del proyecto)."
    )

    try:
        rev_cfg = api_client.get_audit_revision_config()
        if not rev_cfg.get("openai_configured"):
            st.warning(
                "**OpenAI no configurado en el backend:** añade `OPENAI_API_KEY=sk-...` al archivo `.env` "
                "en la raíz del proyecto (junto a `pyproject.toml`) o como variable de entorno del sistema, "
                "y reinicia con `scripts/stop.ps1` y `scripts/start.ps1`."
            )
    except Exception:
        pass

    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {
            "years": [],
            "bank_codes": [],
            "account_types": [],
            "entity_names": [],
        }

    years = sorted(filter_opts.get("years", []), reverse=True)
    month_options = list(range(1, 13))

    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1:
        bank_sel = st.selectbox(
            "Banco",
            options=[""] + sorted(filter_opts.get("bank_codes", [])),
            format_func=lambda x: BANK_DISPLAY_NAMES.get(x, x.replace("_", " ").title()) if x else "Todos",
            key="rev_bank",
        )
    with r1c2:
        entities = filter_opts.get("entity_names", []) or []
        entity_sel = st.multiselect(
            "Sociedad",
            options=sorted(entities),
            default=[],
            key="rev_entities",
        )
    with r1c3:
        type_sel = st.selectbox(
            "Tipo cuenta",
            options=[""] + sorted(filter_opts.get("account_types", [])),
            format_func=lambda x: x.replace("_", " ").title() if x else "Todos",
            key="rev_type",
        )

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    with r2c1:
        focus = st.selectbox(
            "Foco",
            options=[
                "todos",
                "valor_cierre",
                "movimientos_netos",
                "caja",
                "instrumentos",
                "aportes",
                "retiros",
            ],
            format_func=lambda x: {
                "todos": "Todos (cierre, movimientos, caja, instrumentos)",
                "valor_cierre": "Valor de cierre",
                "movimientos_netos": "Movimientos netos",
                "caja": "Caja",
                "instrumentos": "Instrumentos (diccionario)",
                "aportes": "Aportes (sin BD comparable)",
                "retiros": "Retiros (sin BD comparable)",
            }.get(x, x),
            key="rev_focus",
            help=(
                "Todos: cuatro revisiones por cartola (cierre, movimientos, caja, instrumentos), "
                "cada una con una llamada al modelo. Aportes y retiros por separado (sin columna comparable en BD)."
            ),
        )
    with r2c2:
        sample_pct = st.selectbox(
            "Porcentaje de muestra",
            options=[10, 25, 50, 100],
            index=1,
            key="rev_sample_pct",
        )
    with r2c3:
        max_docs = st.number_input(
            "Limite max. documentos",
            min_value=1,
            max_value=500,
            value=50,
            step=1,
            key="rev_max_docs",
        )
    with r2c4:
        sample_mode = st.selectbox(
            "Modo seleccion",
            options=["recentes", "aleatorio"],
            format_func=lambda x: "Mas recientes primero" if x == "recentes" else "Aleatorio",
            key="rev_sample_mode",
        )

    r3c1, r3c2, r3c3, r3c4 = st.columns(4)
    with r3c1:
        y_start = st.selectbox("Ano desde", options=[None] + years, format_func=lambda x: str(x) if x else "Cualquiera", key="rev_y0")
    with r3c2:
        m_start = st.selectbox("Mes desde", options=[None] + month_options, format_func=lambda x: str(x) if x else "Cualquiera", key="rev_m0")
    with r3c3:
        y_end = st.selectbox("Ano hasta", options=[None] + years, format_func=lambda x: str(x) if x else "Cualquiera", key="rev_y1")
    with r3c4:
        m_end = st.selectbox("Mes hasta", options=[None] + month_options, format_func=lambda x: str(x) if x else "Cualquiera", key="rev_m1")

    run = st.button("Revisar", type="primary", key="rev_run")

    if run:
        payload = {
            "bank_codes": [bank_sel] if bank_sel else [],
            "entity_names": list(entity_sel),
            "account_types": [type_sel] if type_sel else [],
            "focus": focus,
            "year_start": y_start,
            "year_end": y_end,
            "month_start": m_start,
            "month_end": m_end,
            "sample_pct": int(sample_pct),
            "max_docs": int(max_docs),
            "sample_mode": sample_mode,
        }
        with st.spinner("Ejecutando auditoria..."):
            try:
                st.session_state["_rev_last"] = api_client.run_audit_revision(payload)
            except ValueError as e:
                st.error(f"No se pudo completar la revision: {e}")
                st.session_state["_rev_last"] = None
            except Exception as e:
                st.error(f"No se pudo completar la revision: {e}")
                st.session_state["_rev_last"] = None

    rep = st.session_state.get("_rev_last")
    if not rep:
        return

    st.markdown("---")
    st.markdown("### Resumen ejecutivo")
    res = rep.get("resumen", {})
    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric("Documentos revisados", res.get("documentos_revisados", 0))
    with s2:
        st.metric("% filas con diferencias", f"{res.get('pct_con_diferencias', 0):.2f}%")
    with s3:
        st.metric("% filas ambiguas", f"{res.get('pct_ambiguos', 0):.2f}%")
    with s4:
        st.metric("% filas no auditables", f"{res.get('pct_no_auditables', 0):.2f}%")

    st.caption(
        f"Candidatos en universo filtrado: {rep.get('total_candidatos', 0)} · "
        f"Muestra revisada: {rep.get('revisados', 0)} · "
        "Los porcentajes del resumen son sobre el **total de filas de hallazgo**, no sobre documentos."
    )

    top_b = res.get("top_bancos_incidencias") or []
    top_p = res.get("top_parsers_incidencias") or []
    if top_b:
        st.markdown("**Top bancos con incidencias**")
        st.dataframe(pd.DataFrame(top_b), hide_index=True, use_container_width=True)
    if top_p:
        st.markdown("**Top parsers con incidencias**")
        st.dataframe(pd.DataFrame(top_p), hide_index=True, use_container_width=True)

    rows = rep.get("hallazgos") or []
    st.markdown("### Resultados")
    if not rows:
        st.info("No se encontraron diferencias ni casos ambiguos en la muestra revisada.")
        return

    df = pd.DataFrame(rows)
    display_cols = [
        "fecha_cartola",
        "documento_id",
        "archivo",
        "sociedad",
        "banco",
        "tipo_cuenta",
        "id_cuenta",
        "elemento_revisado",
        "monto_agente",
        "monto_bd",
        "diferencia",
        "diferencia_pct",
        "nivel",
        "nota",
        "parser",
    ]
    cols = [c for c in display_cols if c in df.columns]
    df_view = _revision_results_display_df(df[cols])
    st.caption(
        "Usa **Fecha cartola**, **ID documento** o **Archivo** para localizar la cartola en Carga > Documentos."
    )
    st.dataframe(df_view, hide_index=True, use_container_width=True, height=420)


def render():
    st.title("Operacional")
    st.markdown("---")

    tab_health, tab_revision = st.tabs(["Salud BD", "Revisión"])
    with tab_health:
        _render_health_tab()
    with tab_revision:
        _render_revision_tab()
