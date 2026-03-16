"""
Pagina Resumen.

Estructura:
- Filtros: Banco, Sociedad, Tipo de cuenta, Fecha (YYYY-MM)
- 3 graficos consolidados (Total Assets, Utilidad, Rentabilidad YTD)
- Tabla resumen consolidada
- Rango personalizado
- Detalle de cartolas
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from frontend import api_client
from frontend.components.data_health import render_health_warning
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.table_utils import render_table


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

ACCOUNT_TYPE_DISPLAY = {
    "etf": "ETF",
    "brokerage": "Brokerage",
    "mandato": "Mandato",
    "current": "Current",
    "checking": "Checking",
    "savings": "Savings",
    "custody": "Custody",
    "investment": "Investment",
    "bonds": "Bonds",
}

CONSOLIDATED_PRESETS = {
    "Mi Investments": ["Boatview", "Telmar", "White Alaska"],
    "Mi Inv + Ect. Int": [
        "Boatview",
        "Telmar",
        "White Alaska",
        "Ecoterra Internacional",
    ],
    "Mi Inv + Ect. Int+ Armel": [
        "Boatview",
        "Telmar",
        "White Alaska",
        "Ecoterra Internacional",
        "Armel Holdings",
    ],
}


def _fmt_num(val) -> str:
    return fmt_number(val, decimals=1)


def _fmt_pct(val) -> str:
    return fmt_percent(val, decimals=2)


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _fmt_bank(code: str) -> str:
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fmt_account_type(account_type: str) -> str:
    key = (account_type or "").strip().lower()
    if key in ACCOUNT_TYPE_DISPLAY:
        return ACCOUNT_TYPE_DISPLAY[key]
    if key == "etf":
        return "ETF"
    return (account_type or "").replace("_", " ").title()


def _fecha_label(fecha_str: str) -> str:
    parts = str(fecha_str).split("-")
    if len(parts) != 2:
        return str(fecha_str)
    return f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}"


def _build_fecha_options(years: list[int]) -> list[str]:
    if not years:
        return []
    values: list[str] = []
    for year in sorted(set(int(y) for y in years), reverse=True):
        for month in range(12, 0, -1):
            values.append(f"{year}-{month:02d}")
    return values


def _compute_ytd_from_monthly(monthly_returns: list[float | None]) -> list[float | None]:
    ytd_values: list[float | None] = []
    compound = 1.0
    has_data = False
    for ret in monthly_returns:
        if ret is None:
            ytd_values.append(round((compound - 1) * 100, 4) if has_data else None)
            continue
        compound *= (1 + (ret / 100))
        has_data = True
        ytd_values.append(round((compound - 1) * 100, 4))
    return ytd_values


def _summarize_visible_scope(detail_rows: list[dict], *, max_groups: int = 3) -> str | None:
    grouped: dict[tuple[str, str], set[str]] = {}
    for row in detail_rows:
        sociedad = str(row.get("sociedad") or "").strip()
        banco = str(row.get("banco") or "").strip()
        account_id = str(row.get("id") or "").strip()
        account_type = _fmt_account_type(str(row.get("account_type") or ""))
        if not sociedad or not banco or not account_id:
            continue
        label = f"{account_id} ({account_type})" if account_type else account_id
        grouped.setdefault((sociedad, banco), set()).add(label)

    overlapping = [
        f"{sociedad} + {_fmt_bank(banco)}: {', '.join(sorted(labels))}"
        for (sociedad, banco), labels in grouped.items()
        if len(labels) > 1
    ]
    if not overlapping:
        return None
    if len(overlapping) > max_groups:
        shown = overlapping[:max_groups]
        shown.append(f"+{len(overlapping) - max_groups} grupo(s) mas")
        return "; ".join(shown)
    return "; ".join(overlapping)


def _summary_seed_rows(
    *,
    selected_year: int | None,
    bank_codes: list[str] | None = None,
    entity_names: list[str] | None = None,
    account_types: list[str] | None = None,
) -> list[dict]:
    payload = {
        "years": [selected_year] if selected_year else [],
        "bank_codes": bank_codes or [],
        "entity_names": entity_names or [],
        "account_types": account_types or [],
    }
    try:
        return api_client.post("/data/summary", json=payload).get("rows", [])
    except Exception:
        return []


def _distinct_values(rows: list[dict], field: str) -> list[str]:
    return sorted(
        {
            str(row.get(field) or "").strip()
            for row in rows
            if str(row.get(field) or "").strip()
        }
    )


def _sanitize_multiselect_state(key: str, valid_options: list[str]) -> list[str]:
    selected = [value for value in st.session_state.get(key, []) if value in valid_options]
    st.session_state[key] = selected
    return selected


def render():
    st.title("Resumen")
    st.markdown("---")

    try:
        opts = api_client.get("/accounts/filter-options")
    except Exception:
        opts = {
            "bank_codes": [],
            "entity_names": [],
            "account_types": [],
            "years": [],
        }

    bank_options_all = sorted(opts.get("bank_codes", []))
    entity_options_all = sorted(opts.get("entity_names", []))
    account_type_options_all = sorted(opts.get("account_types", []))
    year_options = [int(y) for y in opts.get("years", []) if y is not None]
    fecha_options = sorted(opts.get("available_fechas", []), reverse=True)
    if not fecha_options:
        fecha_options = _build_fecha_options(year_options)

    raw_selected_consolidated = st.session_state.get("summary_consolidated", "")
    raw_selected_entities = list(st.session_state.get("summary_entity_names", []))
    raw_selected_banks = list(st.session_state.get("summary_bank_codes", []))
    raw_selected_types = list(st.session_state.get("summary_account_types", []))
    current_fecha = st.session_state.get("summary_fecha")
    if current_fecha not in fecha_options and fecha_options:
        current_fecha = fecha_options[0]
        st.session_state["summary_fecha"] = current_fecha
    selected_year_seed = int(current_fecha[:4]) if current_fecha else None
    preset_entities_seed = list(CONSOLIDATED_PRESETS.get(raw_selected_consolidated, []))
    effective_entities_seed = sorted(set(raw_selected_entities) | set(preset_entities_seed))

    bank_seed_rows = _summary_seed_rows(
        selected_year=selected_year_seed,
        entity_names=effective_entities_seed,
        account_types=raw_selected_types,
    )
    entity_seed_rows = _summary_seed_rows(
        selected_year=selected_year_seed,
        bank_codes=raw_selected_banks,
        account_types=raw_selected_types,
    )
    type_seed_rows = _summary_seed_rows(
        selected_year=selected_year_seed,
        bank_codes=raw_selected_banks,
        entity_names=effective_entities_seed,
    )

    bank_options = _distinct_values(bank_seed_rows, "banco") or bank_options_all
    entity_options = _distinct_values(entity_seed_rows, "sociedad") or entity_options_all
    account_type_options = _distinct_values(type_seed_rows, "account_type") or account_type_options_all

    selected_banks_default = _sanitize_multiselect_state("summary_bank_codes", bank_options)
    selected_entities_default = _sanitize_multiselect_state("summary_entity_names", entity_options)
    selected_types_default = _sanitize_multiselect_state("summary_account_types", account_type_options)

    st.markdown("### Filtros")
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            default=selected_banks_default,
            format_func=_fmt_bank,
            key="summary_bank_codes",
        )
    with c2:
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            default=selected_entities_default,
            key="summary_entity_names",
        )
    with c3:
        selected_consolidated = st.selectbox(
            "Consolidado",
            options=[""] + list(CONSOLIDATED_PRESETS.keys()),
            format_func=lambda x: x or "Sin consolidado",
            key="summary_consolidated",
        )
    with c4:
        selected_types = st.multiselect(
            "Tipo de cuenta",
            options=account_type_options,
            default=selected_types_default,
            format_func=_fmt_account_type,
            key="summary_account_types",
        )
    with c5:
        if fecha_options:
            if "summary_fecha" not in st.session_state or st.session_state["summary_fecha"] not in fecha_options:
                st.session_state["summary_fecha"] = fecha_options[0]
            selected_fecha = st.selectbox(
                "Fecha",
                options=fecha_options,
                key="summary_fecha",
            )
        else:
            selected_fecha = None
            st.selectbox("Fecha", options=["Sin datos"], disabled=True, key="summary_fecha_empty")

    preset_entities = list(CONSOLIDATED_PRESETS.get(selected_consolidated, []))
    selected_entities_effective = sorted(set(selected_entities) | set(preset_entities))
    if selected_consolidated and preset_entities:
        st.caption(f"Consolidado activo: {', '.join(preset_entities)}")

    selected_year = int(selected_fecha[:4]) if selected_fecha else None

    st.markdown("---")

    try:
        data = api_client.post(
            "/data/summary",
            json={
                "years": [selected_year] if selected_year else [],
                "bank_codes": selected_banks,
                "entity_names": selected_entities_effective,
                "account_types": selected_types,
            },
        )
    except Exception as exc:
        data = {"rows": [], "consolidated_rows": [], "chart_data": []}
        st.error(f"Error: {exc}")

    detail_rows = data.get("rows", [])
    consolidated_rows = data.get("consolidated_rows", [])
    chart_data = data.get("chart_data", [])
    visible_scope_summary = _summarize_visible_scope(detail_rows)

    render_health_warning(
        {
            "years": [selected_year] if selected_year else [],
            "bank_codes": selected_banks,
            "entity_names": selected_entities_effective,
            "account_types": selected_types,
        },
        label="Resumen",
    )

    if visible_scope_summary:
        st.info(
            "Las cifras consolidadas combinan multiples cuentas visibles del mismo banco/sociedad: "
            f"{visible_scope_summary}. Revisa 'Detalle Cartolas' para el valor por ID."
        )

    st.subheader("Evolucion 12 meses")

    if selected_year is None and chart_data:
        selected_year = max(int(str(r.get("fecha", ""))[:4]) for r in chart_data if r.get("fecha"))
    if selected_year is None:
        selected_year = 0

    month_keys = [f"{selected_year}-{m:02d}" for m in range(1, 13)] if selected_year else []
    chart_map = {str(row.get("fecha")): row for row in chart_data if row.get("fecha")}

    chart_ev: list[float] = []
    chart_util: list[float] = []
    chart_ret_monthly: list[float | None] = []
    for key in month_keys:
        row = chart_map.get(key, {})
        chart_ev.append(_to_float(row.get("ending_value")) or 0.0)
        chart_util.append(_to_float(row.get("utilidad")) or 0.0)
        chart_ret_monthly.append(_to_float(row.get("rent_pct")))
    chart_ret_ytd = _compute_ytd_from_monthly(chart_ret_monthly)

    if not month_keys:
        month_keys = MONTHS
        chart_ev = [0.0] * 12
        chart_util = [0.0] * 12
        chart_ret_ytd = [None] * 12

    col_ch1, col_ch2, col_ch3 = st.columns(3)

    with col_ch1:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=MONTHS,
                y=chart_ev,
                name="Total Assets",
                marker_color="#4F81BD",
            )
        )
        fig.update_layout(
            title="Total Assets por Mes",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_ch2:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=MONTHS,
                y=chart_util,
                name="Utilidad",
                marker_color="#22A06B",
            )
        )
        fig.update_layout(
            title="Utilidad Mensual",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_ch3:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=MONTHS,
                y=chart_ret_ytd,
                mode="lines+markers",
                name="Rentabilidad YTD",
                line=dict(color="#E67E22", width=2),
            )
        )
        fig.update_layout(
            title="Rentabilidad YTD (%)",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        fig.update_yaxes(tickformat=",.2f")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    col_table1, col_table2 = st.columns([6, 4])

    with col_table1:
        st.subheader("Tabla Resumen")
        if consolidated_rows:
            table_data = []
            for row in consolidated_rows:
                is_prev = row.get("is_prev_year", False)
                table_data.append(
                    {
                        "Fecha": _fecha_label(row["fecha"]),
                        "Ending Value": _fmt_num(row.get("ending_value")),
                        "Caja": _fmt_num(row.get("caja")),
                        "Movimientos": _fmt_num(row.get("movimientos")),
                        "Utilidad": _fmt_num(row.get("utilidad")),
                        "Rent. Mensual (%)": "" if is_prev else _fmt_pct(row.get("rent_mensual_pct")),
                        "Rent. Mensual sin Caja (%)": "" if is_prev else _fmt_pct(row.get("rent_mensual_sin_caja_pct")),
                    }
                )
            render_table(pd.DataFrame(table_data), fixed_equal_cols=True)
        else:
            st.info("Sin datos para los filtros seleccionados.")

    with col_table2:
        st.subheader("Rango Personalizado")

        ym_available = sorted(
            {
                row["fecha"]
                for row in consolidated_rows
                if not row.get("is_prev_year", False) and row.get("fecha")
            }
        )
        ym_options = sorted(ym_available if ym_available else fecha_options)
        if not ym_options:
            st.info("Sin datos para rango personalizado.")
        else:
            rc1, rc2 = st.columns(2)
            with rc1:
                ym_start = st.selectbox(
                    "Desde",
                    options=ym_options,
                    index=0,
                    key="summary_range_start",
                )
            with rc2:
                ym_end = st.selectbox(
                    "Hasta",
                    options=ym_options,
                    index=len(ym_options) - 1,
                    key="summary_range_end",
                )

            if ym_start > ym_end:
                st.warning("Rango invalido: 'Desde' debe ser menor o igual que 'Hasta'.")
            else:
                range_rows = [
                    row
                    for row in consolidated_rows
                    if ym_start <= row.get("fecha", "") <= ym_end and not row.get("is_prev_year", False)
                ]
                prev_month_row = None
                for row in consolidated_rows:
                    if row.get("fecha", "") < ym_start:
                        prev_month_row = row

                if range_rows:
                    valor_inicial = prev_month_row.get("ending_value") if prev_month_row else None
                    ending_value = range_rows[-1].get("ending_value")
                    total_mov = sum(round(_to_float(r.get("movimientos")) or 0.0, 2) for r in range_rows)
                    total_util = sum(round(_to_float(r.get("utilidad")) or 0.0, 2) for r in range_rows)

                    comp_ret = 1.0
                    comp_ret_nc = 1.0
                    has_ret = False
                    has_ret_nc = False
                    for r in range_rows:
                        ret = _to_float(r.get("rent_mensual_pct"))
                        ret_nc = _to_float(r.get("rent_mensual_sin_caja_pct"))
                        if ret is not None:
                            comp_ret *= (1 + round(ret, 2) / 100)
                            has_ret = True
                        if ret_nc is not None:
                            comp_ret_nc *= (1 + round(ret_nc, 2) / 100)
                            has_ret_nc = True

                    rent_pct = (comp_ret - 1) * 100 if has_ret else None
                    rent_nc_pct = (comp_ret_nc - 1) * 100 if has_ret_nc else None

                    range_df = pd.DataFrame(
                        [
                            {"Concepto": "Valor Inicial", "Valor": _fmt_num(valor_inicial)},
                            {"Concepto": "Ending Value", "Valor": _fmt_num(ending_value)},
                            {"Concepto": "Movimientos", "Valor": _fmt_num(total_mov)},
                            {"Concepto": "Utilidad", "Valor": _fmt_num(total_util)},
                            {"Concepto": "Rentabilidad (%)", "Valor": _fmt_pct(rent_pct)},
                            {"Concepto": "Rentabilidad sin Caja (%)", "Valor": _fmt_pct(rent_nc_pct)},
                        ]
                    )
                    render_table(range_df, fixed_equal_cols=True)
                else:
                    st.info("Sin datos en el rango seleccionado.")

    st.markdown("---")

    st.subheader("Detalle Cartolas")
    st.caption("Detalle individual por cuenta y periodo.")

    if detail_rows:
        table_rows = []
        for row in detail_rows:
            table_rows.append(
                {
                    "Fecha": row.get("fecha"),
                    "Sociedad": row.get("sociedad"),
                    "Banco": _fmt_bank(str(row.get("banco", ""))),
                    "ID": row.get("id"),
                    "Moneda": row.get("moneda"),
                    "Ending Value": _fmt_num(row.get("ending_value")),
                    "Caja": _fmt_num(row.get("caja")),
                    "Movimientos": _fmt_num(row.get("movimientos")),
                    "Utilidad": _fmt_num(row.get("utilidad")),
                    "Rent. Mensual (%)": _fmt_pct(row.get("rent_mensual_pct")),
                    "Rent. Mensual sin Caja (%)": _fmt_pct(row.get("rent_mensual_sin_caja_pct")),
                }
            )
        render_table(pd.DataFrame(table_rows), fixed_equal_cols=True)
    else:
        st.info("Sin datos para mostrar.")
