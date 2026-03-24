"""
Pagina ETF.

Estructura:
- Filtros: Banco, Sociedad, Con/Sin Caja, Fecha (YYYY-MM)
- Tabla 1: Instrumentos x Sociedades (montos) - solo Fecha
- Tabla 2: Instrumentos x Sociedades (pesos %) - solo Fecha, afectado por Sin Caja
- Graficos torta + Tabla rango personalizado
- Tabla sociedades x meses (montos) - todos los filtros
- Tabla sociedades x meses (rent %) con toggle Mensual/YTD
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from asset_taxonomy import asset_bucket_series
from frontend import api_client
from frontend.components.chart_utils import aligned_dual_return_axes
from frontend.components.data_health import render_health_warning
from frontend.components.filters import BANK_DISPLAY_NAMES, render_fecha_filter
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.table_utils import render_table


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

SOCIETY_COLUMNS = [
    "Boatview JPM",
    "Boatview GS",
    "Telmar",
    "Armel Holdings",
    "Ecoterra Internacional",
    "Raices LP",
    "Total",
]

INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "SPDR", "IHYA", "Money Market"]
BENCHMARK_BY_INSTRUMENT = {
    "IWDA": 36.0,
    "IEMA": 4.0,
    "VDCA": 27.0,
    "VDPA": 23.0,
    "SPDR": 0.0,
    "IHYA": 10.0,
    "Money Market": 0.0,
}
INSTRUMENT_DISPLAY_MAP = {
    "SPDR": "Bloom. 1-10years",
}
SOCIETY_DISPLAY_MAP = {
    "Armel Holdings": "Armel Hold.",
    "Ecoterra Internacional": "Ect. Internacional",
}
ETF_BANK_ORDER = ["jpmorgan", "goldman_sachs", "bbh", "ubs", "ubs_miami"]
ASSET_SERIES = asset_bucket_series()


def _fmt_bank(code):
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fmt_num(val):
    if val is None or val == 0:
        return ""
    return fmt_number(val, decimals=1)


def _fmt_pct(val):
    return fmt_percent(val, decimals=1)


def _fmt_pct_ret(val):
    return fmt_percent(val, decimals=2)


def _society_label(name: str) -> str:
    return SOCIETY_DISPLAY_MAP.get(name, name)


def _instrument_label(name: str) -> str:
    return INSTRUMENT_DISPLAY_MAP.get(name, name)


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _month_label(fecha_key: str) -> str:
    year = int(fecha_key[:4])
    month = int(fecha_key[5:7])
    return f"{MONTHS[month - 1]} {str(year)[-2:]}"


def _rolling_month_keys(end_key: str, size: int = 12) -> list[str]:
    year = int(end_key[:4])
    month = int(end_key[5:7])
    keys: list[str] = []
    for _ in range(size):
        keys.append(f"{year}-{month:02d}")
        month -= 1
        if month == 0:
            year -= 1
            month = 12
    return list(reversed(keys))


def _compute_accumulated_from_monthly(monthly_returns: list[float | None]) -> list[float | None]:
    accumulated_values: list[float | None] = []
    compound = 1.0
    has_data = False
    for ret in monthly_returns:
        if ret is None:
            accumulated_values.append(round((compound - 1) * 100, 4) if has_data else None)
            continue
        compound *= 1 + (ret / 100)
        has_data = True
        accumulated_values.append(round((compound - 1) * 100, 4))
    return accumulated_values


def render():
    st.title("ETF")
    st.markdown("---")

    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {"bank_codes": [], "entity_names": []}

    try:
        etf_dates = api_client.get("/data/etf-dates")
    except Exception:
        etf_dates = {"dates": []}

    available_dates = etf_dates.get("dates", [])

    st.markdown("### Filtros")

    fcol1, fcol2, fcol3, fcol4 = st.columns(4)

    with fcol1:
        bank_options = filter_opts.get("bank_codes", [])
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            format_func=_fmt_bank,
            key="etf_banco_filter",
        )

    with fcol2:
        entity_options = filter_opts.get("entity_names", [])
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            key="etf_sociedad_filter",
        )

    with fcol3:
        caja_option = st.selectbox(
            "Caja",
            options=["Con Caja", "Sin Caja"],
            key="etf_caja_filter",
        )
        sin_caja = caja_option == "Sin Caja"

    with fcol4:
        fecha = render_fecha_filter(available_dates, key_prefix="etf")

    fcol5, _, _, _ = st.columns(4)
    with fcol5:
        personal_option = st.selectbox(
            "Personal",
            options=["Sin Personal", "Con Personal"],
            key="etf_personal_filter",
        )
        sin_personal = personal_option == "Sin Personal"

    st.markdown("---")

    try:
        data = api_client.post(
            "/data/etf",
            json={
                "fecha": fecha,
                "bank_codes": selected_banks,
                "entity_names": selected_entities,
                "sin_caja": sin_caja,
                "sin_personal": sin_personal,
            },
        )
    except Exception as e:
        data = {}
        st.error(f"Error obteniendo datos: {e}")

    instruments_table = data.get("instruments_table", {})
    control_expected = data.get("control_expected", {})
    instruments_pct_table = data.get("instruments_pct_table", {})
    composition_by_society = data.get("composition_by_society", [])
    composition_by_instrument = data.get("composition_by_instrument", [])
    asset_pct_by_bank = data.get("asset_pct_by_bank", {})
    society_montos_table = data.get("society_montos_table", [])
    society_movements_table = data.get("society_movements_table", [])
    society_returns_monthly = data.get("society_returns_monthly", [])
    society_returns_ytd = data.get("society_returns_ytd", [])
    selected_year = data.get("selected_year")

    backend_cols = data.get("society_cols", [])
    active_society_cols = backend_cols + ["Total"] if backend_cols else SOCIETY_COLUMNS

    render_health_warning(
        {
            "years": [selected_year] if selected_year else [],
            "bank_codes": selected_banks,
            "entity_names": selected_entities,
            "account_types": ["etf"],
            "sin_personal": sin_personal,
        },
        label="ETF",
    )

    try:
        years_payload = [selected_year - 1, selected_year] if selected_year else []
        etf_summary = api_client.post(
            "/data/summary",
            json={
                "years": years_payload,
                "bank_codes": selected_banks,
                "entity_names": selected_entities,
                "account_types": ["etf"],
                "sin_personal": sin_personal,
            },
        )
    except Exception:
        etf_summary = {"consolidated_rows": []}
    etf_consolidated = etf_summary.get("consolidated_rows", [])

    st.subheader("Rentabilidad ultimos 12 meses (%)")
    chart_map = {str(row.get("fecha")): row for row in etf_consolidated if row.get("fecha")}
    if chart_map:
        end_key = str(fecha) if fecha in chart_map else max(chart_map.keys())
        month_keys = _rolling_month_keys(end_key, 12)
        x_labels = [_month_label(key) for key in month_keys]
        rent_key = "rent_mensual_sin_caja_pct" if sin_caja else "rent_mensual_pct"
        monthly_returns = [_to_float(chart_map.get(key, {}).get(rent_key)) for key in month_keys]
        accumulated_returns = _compute_accumulated_from_monthly(monthly_returns)
        monthly_vals = [v for v in monthly_returns if v is not None]
        if monthly_vals:
            axis_ranges = aligned_dual_return_axes(
                monthly_returns,
                accumulated_returns,
                secondary_min_padding=1.0,
            )

            fig_ret = go.Figure()
            fig_ret.add_trace(
                go.Bar(
                    x=x_labels,
                    y=monthly_returns,
                    name="Rentabilidad Mensual",
                    marker_color="#AFC8E2",
                    opacity=0.95,
                    yaxis="y",
                )
            )
            fig_ret.add_trace(
                go.Scatter(
                    x=x_labels,
                    y=accumulated_returns,
                    mode="lines+markers",
                    name="Rentabilidad acumulada",
                    line=dict(color="#E67E22", width=2),
                    yaxis="y2",
                )
            )
            fig_ret.update_layout(
                height=340,
                margin=dict(l=20, r=20, t=20, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                yaxis=dict(
                    title="% Mensual",
                    tickformat=",.2f",
                    range=axis_ranges["primary_range"],
                    showgrid=True,
                    gridcolor="#D6DCE5",
                    zeroline=True,
                    zerolinecolor="#9EA7B3",
                ),
                yaxis2=dict(
                    title="% acumulada",
                    tickformat=",.2f",
                    range=axis_ranges["secondary_range"],
                    dtick=2,
                    overlaying="y",
                    side="right",
                    showgrid=False,
                    zeroline=False,
                ),
            )
            fig_ret.update_xaxes(showgrid=False)
            st.plotly_chart(fig_ret, use_container_width=True)
        else:
            st.info("Sin datos para graficar rentabilidad ETF.")
    else:
        st.info("Sin datos para graficar rentabilidad ETF.")

    st.markdown("---")

    st.subheader("Instrumentos por sociedad (USD)")
    st.caption("Afectado por Fecha, Con/Sin Caja y Sin Personal.")

    if instruments_table:
        instr_rows = []
        totals_raw = {col: 0.0 for col in active_society_cols}

        for instr in INSTRUMENT_ORDER:
            vals = instruments_table.get(instr, {})
            row = {"Instrumento": _instrument_label(instr)}
            for col in active_society_cols:
                v = float(vals.get(col, 0) or 0)
                row[col] = _fmt_num(v)
                totals_raw[col] += v
            instr_rows.append(row)

        total_row = {"Instrumento": "Total"}
        for col in active_society_cols:
            total_row[col] = _fmt_num(totals_raw[col])
        instr_rows.append(total_row)

        control_row = {"Instrumento": ""}
        has_any_difference = False
        for col in active_society_cols:
            expected = float(control_expected.get(col, 0) or 0)
            diff = abs(totals_raw[col] - expected)
            if diff <= 1:
                control_row[col] = ""
            else:
                has_any_difference = True
                control_row[col] = "Diferencia"
        if has_any_difference:
            instr_rows.append(control_row)

        df_instr = pd.DataFrame(instr_rows, columns=["Instrumento"] + active_society_cols)
        df_instr = df_instr.rename(columns={c: _society_label(c) for c in active_society_cols})
        render_table(
            df_instr,
            bold_row_labels={"Total"},
            bold_cols=["Total"],
            label_col="Instrumento",
            fixed_equal_cols=True,
        )
    else:
        st.info("Sin datos. Seleccione una fecha con datos ETF.")

    st.markdown("---")

    st.subheader("Instrumentos x Sociedades (%)")
    st.caption("Peso porcentual de cada instrumento. Total = suma real (no forzada a 100%).")

    if instruments_pct_table:
        pct_rows = []
        totals_pct = {"Instrumento": "Total", "Benchmark": ""}
        for col in active_society_cols:
            totals_pct[col] = 0.0
        benchmark_total = 0.0

        for instr in INSTRUMENT_ORDER:
            if instr in instruments_pct_table:
                if sin_caja and instr == "Money Market":
                    continue
                vals = instruments_pct_table[instr]
                row = {"Instrumento": _instrument_label(instr)}
                for col in active_society_cols:
                    v = vals.get(col, 0)
                    row[col] = _fmt_pct(v)
                    totals_pct[col] += float(v or 0)
                bench = BENCHMARK_BY_INSTRUMENT.get(instr)
                row["Benchmark"] = _fmt_pct(bench) if bench is not None else ""
                benchmark_total += float(bench or 0.0)
                pct_rows.append(row)

        for instr, vals in instruments_pct_table.items():
            if instr not in INSTRUMENT_ORDER:
                if sin_caja and instr == "Money Market":
                    continue
                row = {"Instrumento": _instrument_label(instr)}
                for col in active_society_cols:
                    v = vals.get(col, 0)
                    row[col] = _fmt_pct(v)
                    totals_pct[col] += float(v or 0)
                row["Benchmark"] = ""
                pct_rows.append(row)

        for col in active_society_cols:
            totals_pct[col] = _fmt_pct(totals_pct[col])
        totals_pct["Benchmark"] = _fmt_pct(benchmark_total)
        pct_rows.append(totals_pct)

        df_pct = pd.DataFrame(pct_rows, columns=["Instrumento"] + active_society_cols + ["Benchmark"])
        df_pct = df_pct.rename(columns={c: _society_label(c) for c in active_society_cols})
        render_table(
            df_pct,
            bold_row_labels={"Total"},
            bold_cols=["Total"],
            label_col="Instrumento",
            fixed_equal_cols=True,
        )
    else:
        st.info("Sin datos de pesos %.")

    st.markdown("---")

    st.subheader("Distribucion")
    st.caption("Afectado por filtro Con/Sin Caja.")

    gcol1, gcol2, gcol3 = st.columns(3)

    with gcol1:
        st.markdown("**Por Sociedades**")
        if composition_by_society:
            labels = [_society_label(c["label"]) for c in composition_by_society]
            values = [float(c["value"]) for c in composition_by_society]
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
            fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos.")

    with gcol2:
        st.markdown("**% por Tipo de Activo en cada Banco**")
        ordered_banks = [bank for bank in ETF_BANK_ORDER if bank in asset_pct_by_bank]
        ordered_banks.extend(
            bank for bank in sorted(asset_pct_by_bank.keys()) if bank not in ordered_banks
        )
        if ordered_banks:
            fig = go.Figure()
            for asset_key, label, color in ASSET_SERIES:
                y_values = []
                for bank in ordered_banks:
                    payload = asset_pct_by_bank.get(bank, {}) if isinstance(asset_pct_by_bank.get(bank), dict) else {}
                    y_values.append(float(payload.get(asset_key, 0.0) or 0.0))
                fig.add_trace(
                    go.Bar(
                        x=[_fmt_bank(bank) for bank in ordered_banks],
                        y=y_values,
                        name=label,
                        marker_color=color or None,
                    )
                )
            fig.update_layout(
                barmode="stack",
                height=350,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis_title="% del banco",
            )
            fig.update_yaxes(range=[0, 100], tickformat=",.0f")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos.")

    with gcol3:
        st.markdown("**Rango Personalizado**")
        ym_available = sorted({cr["fecha"] for cr in etf_consolidated if not cr.get("is_prev_year", False)})
        if ym_available:
            rc1, rc2 = st.columns(2)
            with rc1:
                ym_start = st.selectbox(
                    "Desde",
                    options=ym_available,
                    index=0,
                    key="etf_range_start",
                )
            with rc2:
                ym_end = st.selectbox(
                    "Hasta",
                    options=ym_available,
                    index=len(ym_available) - 1,
                    key="etf_range_end",
                )

            if ym_start > ym_end:
                st.warning("Rango invalido: 'Desde' debe ser menor o igual que 'Hasta'.")
            else:
                range_rows = [
                    cr
                    for cr in etf_consolidated
                    if ym_start <= cr["fecha"] <= ym_end and not cr.get("is_prev_year", False)
                ]
                prev_month_row = None
                for cr in etf_consolidated:
                    if cr["fecha"] < ym_start:
                        prev_month_row = cr
                if range_rows:
                    valor_inicial = prev_month_row["ending_value"] if prev_month_row else None
                    ending_value = range_rows[-1]["ending_value"]
                    total_mov = sum(round(_to_float(r.get("movimientos")) or 0.0, 2) for r in range_rows)
                    total_util = sum(round(_to_float(r.get("utilidad")) or 0.0, 2) for r in range_rows)

                    compound_ret = 1.0
                    compound_ret_sc = 1.0
                    has_ret = False
                    has_ret_sc = False
                    for r in range_rows:
                        ret_val = _to_float(r.get("rent_mensual_pct"))
                        ret_sc_val = _to_float(r.get("rent_mensual_sin_caja_pct"))
                        if ret_val is not None:
                            compound_ret *= 1 + round(ret_val, 2) / 100
                            has_ret = True
                        if ret_sc_val is not None:
                            compound_ret_sc *= 1 + round(ret_sc_val, 2) / 100
                            has_ret_sc = True
                    rent_pct = (compound_ret - 1) * 100 if has_ret else None
                    rent_sc_pct = (compound_ret_sc - 1) * 100 if has_ret_sc else None

                    df_rng = pd.DataFrame(
                        [
                            {"Concepto": "Valor Inicial", "Valor": _fmt_num(valor_inicial)},
                            {"Concepto": "Ending Value", "Valor": _fmt_num(ending_value)},
                            {"Concepto": "Movimientos", "Valor": _fmt_num(total_mov)},
                            {"Concepto": "Utilidad", "Valor": _fmt_num(total_util)},
                            {"Concepto": "Rentabilidad (%)", "Valor": _fmt_pct_ret(rent_pct)},
                            {"Concepto": "Rentabilidad sin Caja (%)", "Valor": _fmt_pct_ret(rent_sc_pct)},
                        ]
                    )
                    render_table(df_rng)
                else:
                    st.info("Sin datos en rango.")
        else:
            st.info("Sin datos ETF para rango personalizado.")

    st.markdown("---")

    year_label = str(selected_year) if selected_year else ""
    st.subheader(f"Evolucion del Portafolio ETF por sociedad {year_label}")
    st.caption("Sociedades x Meses. Afectado por filtros Fecha, Banco, Sociedad, Con/Sin Caja y Sin Personal.")

    if society_montos_table:
        df_montos = pd.DataFrame(society_montos_table)

        col_rename = {"sociedad": "Sociedad"}
        month_cols = []
        for m in range(1, 13):
            mk = f"{m:02d}"
            if mk in df_montos.columns:
                suffix = f" {str(selected_year)[-2:]}" if selected_year else ""
                new_name = f"{MONTHS[m - 1]}{suffix}"
                col_rename[mk] = new_name
                month_cols.append(new_name)
        df_montos = df_montos.rename(columns=col_rename)
        if "Sociedad" in df_montos.columns:
            df_montos["Sociedad"] = df_montos["Sociedad"].apply(_society_label)

        for col in month_cols:
            if col in df_montos.columns:
                df_montos[col] = df_montos[col].apply(lambda x: _fmt_num(x))

        render_table(
            df_montos,
            bold_row_labels={"Total"},
            label_col="Sociedad",
        )
    else:
        st.info("Sin datos de montos por sociedad.")

    st.markdown("---")

    st.subheader(f"Movimientos portafolio ETF por sociedad {year_label}")
    st.caption("Sociedades x Meses. Afectado por filtros Fecha, Banco, Sociedad, Con/Sin Caja y Sin Personal.")

    if society_movements_table:
        df_movs = pd.DataFrame(society_movements_table)

        col_rename = {"sociedad": "Sociedad"}
        mov_cols = []
        for m in range(1, 13):
            mk = f"{m:02d}"
            if mk in df_movs.columns:
                suffix = f" {str(selected_year)[-2:]}" if selected_year else ""
                new_name = f"{MONTHS[m - 1]}{suffix}"
                col_rename[mk] = new_name
                mov_cols.append(new_name)
        df_movs = df_movs.rename(columns=col_rename)
        if "Sociedad" in df_movs.columns:
            df_movs["Sociedad"] = df_movs["Sociedad"].apply(_society_label)

        for col in mov_cols:
            if col in df_movs.columns:
                df_movs[col] = df_movs[col].apply(lambda x: _fmt_num(x))

        render_table(
            df_movs,
            bold_row_labels={"Total"},
            label_col="Sociedad",
        )
    else:
        st.info("Sin datos de movimientos por sociedad.")

    st.markdown("---")

    st.subheader(f"Rentabilidad del portafolio ETF por Sociedad {year_label}")

    ret_mode = st.radio(
        "Tipo de rentabilidad",
        options=["Mensual", "YTD"],
        horizontal=True,
        key="etf_return_mode",
    )

    returns_data = society_returns_monthly if ret_mode == "Mensual" else society_returns_ytd

    if returns_data:
        df_ret = pd.DataFrame(returns_data)

        col_rename = {"sociedad": "Sociedad"}
        ret_cols = []
        for m in range(1, 13):
            mk = f"{m:02d}"
            if mk in df_ret.columns:
                suffix = f" {str(selected_year)[-2:]}" if selected_year else ""
                new_name = f"{MONTHS[m - 1]}{suffix}"
                col_rename[mk] = new_name
                ret_cols.append(new_name)
        df_ret = df_ret.rename(columns=col_rename)
        if "Sociedad" in df_ret.columns:
            df_ret["Sociedad"] = df_ret["Sociedad"].apply(_society_label)

        for col in ret_cols:
            if col in df_ret.columns:
                df_ret[col] = df_ret[col].apply(lambda x: _fmt_pct_ret(x))

        render_table(
            df_ret,
            bold_row_labels={"Total"},
            label_col="Sociedad",
        )
    else:
        st.info("Sin datos de rentabilidad.")
