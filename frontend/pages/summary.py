"""
Página Resumen – Vista consolidada con filtros multi-selección.

Estructura:
- Filtros multi-selección (banco, sociedad, tipo cuenta, año)
- 3 gráficos (Total Assets, Utilidad, Rentabilidad) consolidados
- Tabla Resumen consolidada (13 meses: dic anterior + 12 del año)
- Rango Personalizado (2 selectores YYYY-MM, KPIs verticales)
- Detalle Cartolas (tabla por cuenta)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.table_utils import render_table
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.filters import (
    render_filters,
    BANK_DISPLAY_NAMES,
)


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def _fmt_number(val):
    """Format a number for display."""
    return fmt_number(val, decimals=2)


def _fmt_pct(val):
    """Format a percentage for display."""
    return fmt_percent(val, decimals=2)


def _to_float(val):
    """Best-effort float conversion."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _fmt_bank(code):
    """Format bank code for display."""
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fecha_label(fecha_str):
    """'2025-03' → 'Mar 25'"""
    parts = fecha_str.split("-")
    return f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}"


# ── Columnas numéricas que se alinean a la derecha ───────────────
def _build_ym_options():
    """Generate YYYY-MM options from 2020-01 to 2027-12."""
    opts = []
    for y in range(2020, 2028):
        for m in range(1, 13):
            opts.append(f"{y}-{m:02d}")
    return opts


def render():
    st.title("📋 Resumen")
    st.markdown("---")

    # ── Obtener opciones de filtro ───────────────────────────────
    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {
            "bank_codes": [],
            "entity_names": [],
            "account_types": [],
        }

    filter_opts = {
        "bank_codes": filter_opts.get("bank_codes", []),
        "entity_names": filter_opts.get("entity_names", []),
        "account_types": filter_opts.get("account_types", []),
        "years": [str(y) for y in range(2020, 2027)],
    }

    # ── Renderizar filtros ───────────────────────────────────────
    def _fmt_account_type(t: str) -> str:
        return t.replace("_", " ").title()

    selections = render_filters(
        filter_opts, key_prefix="summary",
        format_labels={"account_types": _fmt_account_type},
    )

    st.markdown("---")

    # ── Obtener datos ────────────────────────────────────────────
    try:
        data = api_client.post("/data/summary", json={
            "years": [int(y) for y in selections.get("years", [])],
            "bank_codes": selections.get("bank_codes", []),
            "entity_names": selections.get("entity_names", []),
            "account_types": selections.get("account_types", []),
        })
    except Exception as e:
        data = {"rows": [], "consolidated_rows": [], "chart_data": []}
        st.error(f"Error: {e}")

    detail_rows = data.get("rows", [])
    consolidated_rows = data.get("consolidated_rows", [])
    chart_data = data.get("chart_data", [])

    # ── Gráficos (3 gráficos — datos consolidados) ──────────────
    st.subheader("📊 Evolución 12 meses")

    chart_months = []
    chart_ev = []
    chart_util = []
    chart_ret = []
    for cd in chart_data:
        chart_months.append(_fecha_label(cd["fecha"]))
        chart_ev.append(cd["ending_value"])
        chart_util.append(cd["utilidad"])
        chart_ret.append(cd["rent_pct"] if cd["rent_pct"] is not None else 0)

    if not chart_months:
        chart_months = MONTHS
        chart_ev = [0] * 12
        chart_util = [0] * 12
        chart_ret = [0] * 12

    chart_col1, chart_col2, chart_col3 = st.columns(3)

    with chart_col1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=chart_ev,
            name="Total Assets",
            marker_color="steelblue",
        ))
        fig.update_layout(
            title="Total Assets por Mes",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_months, y=chart_util,
            name="Utilidad",
            marker_color="mediumseagreen",
        ))
        fig.update_layout(
            title="Utilidad Mensual",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col3:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_months, y=chart_ret,
            mode="lines+markers",
            name="Rentabilidad",
            line=dict(color="coral"),
        ))
        fig.update_layout(
            title="Rentabilidad Mensual %",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Tablas: Resumen | Rango Personalizado ────────────────────
    col_table1, col_table2 = st.columns([6, 4])

    # ── Tabla Resumen (consolidada, 13 meses) ────────────────────
    with col_table1:
        st.subheader("📋 Tabla Resumen")
        if consolidated_rows:
            table_data = []
            for cr in consolidated_rows:
                is_prev = cr.get("is_prev_year", False)
                table_data.append({
                    "Fecha": _fecha_label(cr["fecha"]),
                    "Ending Value": _fmt_number(cr["ending_value"]),
                    "Caja": _fmt_number(cr.get("caja")),
                    "Movimientos": _fmt_number(cr["movimientos"]),
                    "Utilidad": _fmt_number(cr["utilidad"]),
                    "Rent. Mensual (%)": (
                        "" if is_prev
                        else _fmt_pct(cr["rent_mensual_pct"])
                    ),
                    "Rent. Mensual sin Caja (%)": (
                        "" if is_prev
                        else _fmt_pct(cr["rent_mensual_sin_caja_pct"])
                    ),
                })
            df_summary = pd.DataFrame(table_data)
            render_table(df_summary)
        else:
            st.info("Sin datos. Cargue documentos y aplique filtros.")

    # ── Rango Personalizado ──────────────────────────────────────
    with col_table2:
        st.subheader("📅 Rango Personalizado")

        ym_available = sorted({
            cr["fecha"] for cr in consolidated_rows
            if not cr.get("is_prev_year", False)
        })
        ym_options = ym_available if ym_available else _build_ym_options()
        rc1, rc2 = st.columns(2)
        with rc1:
            ym_start = st.selectbox(
                "Desde",
                options=ym_options,
                index=ym_options.index("2025-01") if "2025-01" in ym_options else 0,
                key="summary_range_start",
            )
        with rc2:
            ym_end = st.selectbox(
                "Hasta",
                options=ym_options,
                index=ym_options.index("2025-12") if "2025-12" in ym_options else len(ym_options) - 1,
                key="summary_range_end",
            )

        if ym_start > ym_end:
            st.warning("Rango inválido: 'Desde' debe ser menor o igual que 'Hasta'.")
            return

        # Filtrar consolidated_rows dentro del rango (excluir prev_year)
        range_rows = [
            cr for cr in consolidated_rows
            if ym_start <= cr["fecha"] <= ym_end
            and not cr.get("is_prev_year", False)
        ]

        # Mes anterior al inicio para Valor Inicial
        prev_month_row = None
        for cr in consolidated_rows:
            if cr["fecha"] < ym_start:
                prev_month_row = cr

        if range_rows:
            # Valor Inicial = ending_value del mes anterior al inicio
            valor_inicial = prev_month_row["ending_value"] if prev_month_row else None

            # Ending Value = ending_value del último mes del rango
            ending_value = range_rows[-1]["ending_value"]

            # Movimientos / Utilidad: suma de los valores mensuales mostrados
            # (misma granularidad que Tabla Resumen para evitar descuadres visibles).
            total_mov = sum(round(_to_float(r.get("movimientos")) or 0.0, 2) for r in range_rows)
            total_util = sum(round(_to_float(r.get("utilidad")) or 0.0, 2) for r in range_rows)

            # Rentabilidad compuesta sobre rentabilidades mensuales mostradas
            # (redondeadas a 2 decimales, consistente con la tabla visible).
            compound_ret = 1.0
            compound_ret_sc = 1.0
            has_ret = False
            has_ret_sc = False
            for r in range_rows:
                ret_val = _to_float(r.get("rent_mensual_pct"))
                ret_sc_val = _to_float(r.get("rent_mensual_sin_caja_pct"))
                if ret_val is not None:
                    compound_ret *= (1 + round(ret_val, 2) / 100)
                    has_ret = True
                if ret_sc_val is not None:
                    compound_ret_sc *= (1 + round(ret_sc_val, 2) / 100)
                    has_ret_sc = True

            rent_pct = (compound_ret - 1) * 100 if has_ret else None
            rent_sc_pct = (compound_ret_sc - 1) * 100 if has_ret_sc else None

            # Tabla vertical de KPIs (2 columnas)
            kpi_data = [
                {"Concepto": "Valor Inicial", "Valor": _fmt_number(valor_inicial)},
                {"Concepto": "Ending Value", "Valor": _fmt_number(ending_value)},
                {"Concepto": "Movimientos", "Valor": _fmt_number(total_mov)},
                {"Concepto": "Utilidad", "Valor": _fmt_number(total_util)},
                {"Concepto": "Rentabilidad (%)", "Valor": _fmt_pct(rent_pct)},
                {"Concepto": "Rentabilidad sin Caja (%)", "Valor": _fmt_pct(rent_sc_pct)},
            ]
            df_range = pd.DataFrame(kpi_data)
            render_table(df_range)
        else:
            st.info("Sin datos en el rango seleccionado.")

    st.markdown("---")

    # ── Detalle Cartolas ─────────────────────────────────────────
    st.subheader("📄 Detalle Cartolas")
    st.caption("Detalle individual de cada cartola (cuenta/período).")

    if detail_rows:
        table_data = []
        for r in detail_rows:
            table_data.append({
                "Fecha": r["fecha"],
                "Sociedad": r["sociedad"],
                "Banco": _fmt_bank(r["banco"]),
                "ID": r["id"],
                "Moneda": r["moneda"],
                "Ending Value": _fmt_number(r["ending_value"]),
                "Caja": _fmt_number(r.get("caja")),
                "Movimientos": _fmt_number(r["movimientos"]),
                "Utilidad": _fmt_number(r["utilidad"]),
                "Rent. Mensual (%)": _fmt_pct(r["rent_mensual_pct"]),
                "Rent. Mensual sin Caja (%)": _fmt_pct(r["rent_mensual_sin_caja_pct"]),
            })
        df_detail = pd.DataFrame(table_data)
        render_table(df_detail)
    else:
        st.info("Sin datos. Cargue cartolas para ver el detalle.")




