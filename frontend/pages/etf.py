"""
Página ETF.

Estructura:
- Filtros: Fecha (YYYY-MM), Banco, Sociedad, Con/Sin Caja
- Tabla 1: Instrumentos × Sociedades (montos) — solo Fecha
- Tabla 2: Instrumentos × Sociedades (pesos %) — solo Fecha, afectado por Sin Caja
- Gráficos torta + Tabla rango personalizado (tercios)
- Tabla sociedades × meses (montos) — todos los filtros
- Tabla sociedades × meses (rent %) con toggle Mensual/YTD
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from frontend import api_client
from frontend.components.table_utils import render_table
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.filters import (
    render_fecha_filter,
    BANK_DISPLAY_NAMES,
)


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

SOCIETY_COLUMNS = [
    "Boatview JPM", "Boatview GS", "Telmar",
    "Armel Holdings", "Ecoterra Internacional", "Total",
]

INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "IHYA", "Money Market"]
SOCIETY_DISPLAY_MAP = {
    "Armel Holdings": "Armel Hold.",
    "Ecoterra Internacional": "Ect. Internacional",
}


def _fmt_bank(code):
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fmt_num(val):
    if val is None or val == 0:
        return ""
    return fmt_number(val, decimals=2)


def _fmt_pct(val):
    return fmt_percent(val, decimals=2)


def _society_label(name: str) -> str:
    return SOCIETY_DISPLAY_MAP.get(name, name)


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def render():
    st.title("📈 ETF")
    st.markdown("---")

    # ── Obtener opciones de filtro ───────────────────────────────
    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {"bank_codes": [], "entity_names": []}

    try:
        etf_dates = api_client.get("/data/etf-dates")
    except Exception:
        etf_dates = {"dates": []}

    available_dates = etf_dates.get("dates", [])

    # ── Renderizar filtros ───────────────────────────────────────
    st.markdown("### 🔍 Filtros")

    fcol1, fcol2, fcol3, fcol4 = st.columns(4)

    with fcol1:
        fecha = render_fecha_filter(available_dates, key_prefix="etf")

    with fcol2:
        bank_options = filter_opts.get("bank_codes", [])
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            format_func=_fmt_bank,
            key="etf_banco_filter",
        )

    with fcol3:
        entity_options = filter_opts.get("entity_names", [])
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            key="etf_sociedad_filter",
        )

    with fcol4:
        caja_option = st.selectbox(
            "💰 Caja",
            options=["Con Caja", "Sin Caja"],
            key="etf_caja_filter",
        )
        sin_caja = caja_option == "Sin Caja"

    st.markdown("---")

    # ── Obtener datos del backend ────────────────────────────────
    try:
        data = api_client.post("/data/etf", json={
            "fecha": fecha,
            "bank_codes": selected_banks,
            "entity_names": selected_entities,
            "sin_caja": sin_caja,
        })
    except Exception as e:
        data = {}
        st.error(f"Error obteniendo datos: {e}")

    instruments_table = data.get("instruments_table", {})
    control_expected = data.get("control_expected", {})
    instruments_pct_table = data.get("instruments_pct_table", {})
    composition_by_society = data.get("composition_by_society", [])
    composition_by_instrument = data.get("composition_by_instrument", [])
    society_montos_table = data.get("society_montos_table", [])
    society_movements_table = data.get("society_movements_table", [])
    society_returns_monthly = data.get("society_returns_monthly", [])
    society_returns_ytd = data.get("society_returns_ytd", [])
    selected_year = data.get("selected_year")
    try:
        etf_summary = api_client.post("/data/summary", json={
            "years": [selected_year] if selected_year else [],
            "bank_codes": selected_banks,
            "entity_names": selected_entities,
            "account_types": ["etf"],
        })
    except Exception:
        etf_summary = {"consolidated_rows": []}
    etf_consolidated = etf_summary.get("consolidated_rows", [])

    # ── Tabla 1: Instrumentos × Sociedades (MONTOS) ─────────────
    st.subheader("Instrumentos × Sociedades (Montos)")
    st.caption("Solo afectado por el filtro Fecha. Muestra la foto de instrumentos en la cartola.")

    if instruments_table:
        instr_rows = []
        totals_raw = {col: 0.0 for col in SOCIETY_COLUMNS}

        for instr in INSTRUMENT_ORDER:
            vals = instruments_table.get(instr, {})
            row = {"Instrumento": instr}
            for col in SOCIETY_COLUMNS:
                v = float(vals.get(col, 0) or 0)
                row[col] = _fmt_num(v)
                totals_raw[col] += v
            instr_rows.append(row)

        total_row = {"Instrumento": "Total"}
        for col in SOCIETY_COLUMNS:
            total_row[col] = _fmt_num(totals_raw[col])
        instr_rows.append(total_row)

        # Control: Total tabla vs ending value sin accruals por sociedad y total.
        control_row = {"Instrumento": "control"}
        for col in SOCIETY_COLUMNS:
            expected = float(control_expected.get(col, 0) or 0)
            diff = abs(totals_raw[col] - expected)
            if col == "Total":
                signed_diff = totals_raw[col] - expected
                control_row[col] = (
                    "OK"
                    if diff <= 1
                    else f"DIFERENCIA ({fmt_number(signed_diff, decimals=2)})"
                )
            else:
                control_row[col] = "OK" if diff <= 1 else "DIFERENCIA"
        instr_rows.append(control_row)

        df_instr = pd.DataFrame(instr_rows, columns=["Instrumento"] + SOCIETY_COLUMNS)
        df_instr = df_instr.rename(columns={c: _society_label(c) for c in SOCIETY_COLUMNS})
        render_table(
            df_instr,
            bold_row_labels={"Total"},
            bold_cols=["Total"],
            small_row_labels={"control"},
            label_col="Instrumento",
        )
    else:
        st.info("Sin datos. Seleccione una fecha con datos ETF.")

    st.markdown("---")

    # ── Tabla 2: Instrumentos × Sociedades (PESOS %) ────────────
    st.subheader("Instrumentos × Sociedades (%)")
    st.caption("Peso porcentual de cada instrumento. Total = suma real (no forzada a 100%).")

    if instruments_pct_table:
        pct_rows = []
        totals_pct = {"Instrumento": "Total"}
        for col in SOCIETY_COLUMNS:
            totals_pct[col] = 0.0

        for instr in INSTRUMENT_ORDER:
            if instr in instruments_pct_table:
                # Skip cash if sin_caja
                if sin_caja and instr == "Money Market":
                    continue
                vals = instruments_pct_table[instr]
                row = {"Instrumento": instr}
                for col in SOCIETY_COLUMNS:
                    v = vals.get(col, 0)
                    row[col] = _fmt_pct(v)
                    totals_pct[col] += float(v or 0)
                pct_rows.append(row)

        for instr, vals in instruments_pct_table.items():
            if instr not in INSTRUMENT_ORDER:
                if sin_caja and instr == "Money Market":
                    continue
                row = {"Instrumento": instr}
                for col in SOCIETY_COLUMNS:
                    v = vals.get(col, 0)
                    row[col] = _fmt_pct(v)
                    totals_pct[col] += float(v or 0)
                pct_rows.append(row)

        for col in SOCIETY_COLUMNS:
            totals_pct[col] = _fmt_pct(totals_pct[col])
        pct_rows.append(totals_pct)

        df_pct = pd.DataFrame(pct_rows, columns=["Instrumento"] + SOCIETY_COLUMNS)
        df_pct = df_pct.rename(columns={c: _society_label(c) for c in SOCIETY_COLUMNS})
        render_table(
            df_pct,
            bold_row_labels={"Total"},
            bold_cols=["Total"],
            label_col="Instrumento",
        )
    else:
        st.info("Sin datos de pesos %.")

    st.markdown("---")

    # ── Gráficos + Rango Personalizado (tercios) ────────────────
    st.subheader("Distribución")
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
        st.markdown("**Por Instrumentos**")
        if composition_by_instrument:
            labels = [c["label"][:25] for c in composition_by_instrument]
            values = [float(c["value"]) for c in composition_by_instrument]
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
            fig.update_layout(height=350, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos.")

    with gcol3:
        st.markdown("**📅 Rango Personalizado**")
        ym_available = sorted({
            cr["fecha"] for cr in etf_consolidated
            if not cr.get("is_prev_year", False)
        })
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
                st.warning("Rango inválido: 'Desde' debe ser menor o igual que 'Hasta'.")
            else:
                range_rows = [
                    cr for cr in etf_consolidated
                    if ym_start <= cr["fecha"] <= ym_end
                    and not cr.get("is_prev_year", False)
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
                            compound_ret *= (1 + round(ret_val, 2) / 100)
                            has_ret = True
                        if ret_sc_val is not None:
                            compound_ret_sc *= (1 + round(ret_sc_val, 2) / 100)
                            has_ret_sc = True
                    rent_pct = (compound_ret - 1) * 100 if has_ret else None
                    rent_sc_pct = (compound_ret_sc - 1) * 100 if has_ret_sc else None

                    df_rng = pd.DataFrame(
                        [
                            {"Concepto": "Valor Inicial", "Valor": _fmt_num(valor_inicial)},
                            {"Concepto": "Ending Value", "Valor": _fmt_num(ending_value)},
                            {"Concepto": "Movimientos", "Valor": _fmt_num(total_mov)},
                            {"Concepto": "Utilidad", "Valor": _fmt_num(total_util)},
                            {"Concepto": "Rentabilidad (%)", "Valor": _fmt_pct(rent_pct)},
                            {"Concepto": "Rentabilidad sin Caja (%)", "Valor": _fmt_pct(rent_sc_pct)},
                        ]
                    )
                    render_table(df_rng)
                else:
                    st.info("Sin datos en rango.")
        else:
            st.info("Sin datos ETF para rango personalizado.")

    st.markdown("---")

    # ── ETF Montos por Sociedad × Meses ──────────────────────────
    year_label = str(selected_year) if selected_year else ""
    st.subheader(f"ETF Montos por Sociedad {year_label}")
    st.caption("Sociedades × Meses. Afectado por filtros Fecha, Banco y Sociedad.")

    if society_montos_table:
        df_montos = pd.DataFrame(society_montos_table)

        # Renombrar columnas de meses
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

        # Format numbers
        for col in month_cols:
            if col in df_montos.columns:
                df_montos[col] = df_montos[col].apply(
                    lambda x: _fmt_num(x)
                )

        render_table(
            df_montos,
            bold_row_labels={"Total"},
            label_col="Sociedad",
        )
    else:
        st.info("Sin datos de montos por sociedad.")

    st.markdown("---")

    # ── ETF Movimientos por Sociedad × Meses ─────────────────────
    st.subheader(f"ETF Movimientos por Sociedad {year_label}")
    st.caption("Sociedades × Meses. Afectado por filtros Fecha, Banco y Sociedad.")

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

    # ── Rentabilidad por Sociedad × Meses ────────────────────────
    st.subheader(f"Rentabilidad por Sociedad {year_label}")

    ret_mode = st.radio(
        "Tipo de rentabilidad",
        options=["Mensual", "YTD"],
        horizontal=True,
        key="etf_return_mode",
    )

    returns_data = society_returns_monthly if ret_mode == "Mensual" else society_returns_ytd
    total_monthly = {
        cr["fecha"]: _to_float(cr.get("rent_mensual_pct"))
        for cr in etf_consolidated
        if not cr.get("is_prev_year", False)
    }
    total_row = {"sociedad": "Total"}
    if selected_year:
        cumulative = 0.0
        for m in range(1, 13):
            mk = f"{selected_year}-{m:02d}"
            month_ret = total_monthly.get(mk)
            key = f"{m:02d}"
            if ret_mode == "Mensual":
                total_row[key] = month_ret
            else:
                if month_ret is not None:
                    cumulative = (1 + cumulative / 100) * (1 + month_ret / 100) * 100 - 100
                    total_row[key] = round(cumulative, 4)
                else:
                    total_row[key] = round(cumulative, 4) if cumulative != 0 else None

    if returns_data:
        df_ret = pd.DataFrame(returns_data + [total_row])

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
                df_ret[col] = df_ret[col].apply(lambda x: _fmt_pct(x))

        render_table(
            df_ret,
            bold_row_labels={"Total"},
            label_col="Sociedad",
        )
    else:
        st.info("Sin datos de rentabilidad.")


