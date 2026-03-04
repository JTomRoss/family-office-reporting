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
from frontend.components.filters import (
    render_fecha_filter,
    render_date_range_filter,
    BANK_DISPLAY_NAMES,
)


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

SOCIETY_COLUMNS = [
    "Boatview JPM", "Boatview GS", "Telmar",
    "Armel Holdings", "Ect Internacional", "Total",
]

INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "IHYA", "Money Market"]


def _fmt_bank(code):
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fmt_num(val):
    if val is None or val == 0:
        return ""
    try:
        return f"{float(val):,.2f}"
    except (ValueError, TypeError):
        return ""


def _fmt_pct(val):
    if val is None:
        return ""
    try:
        return f"{float(val):.2f}%"
    except (ValueError, TypeError):
        return ""


def _style_right_align(df, num_cols=None):
    """Right-align specified or all non-first columns."""
    if num_cols is None:
        num_cols = list(df.columns[1:])
    props = "text-align: right"
    subset = [c for c in num_cols if c in df.columns]
    if subset:
        return df.style.set_properties(subset=subset, **{"text-align": "right"}).format(
            {c: "{}" for c in df.columns}
        )
    return df.style.format({c: "{}" for c in df.columns})


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
    instruments_pct_table = data.get("instruments_pct_table", {})
    composition_by_society = data.get("composition_by_society", [])
    composition_by_instrument = data.get("composition_by_instrument", [])
    society_montos_table = data.get("society_montos_table", [])
    society_returns_monthly = data.get("society_returns_monthly", [])
    society_returns_ytd = data.get("society_returns_ytd", [])
    selected_year = data.get("selected_year")

    # ── Tabla 1: Instrumentos × Sociedades (MONTOS) ─────────────
    st.subheader("Instrumentos × Sociedades (Montos)")
    st.caption("Solo afectado por el filtro Fecha. Muestra la foto de instrumentos en la cartola.")

    if instruments_table:
        instr_rows = []
        totals_row = {"Instrumento": "Total"}
        for col in SOCIETY_COLUMNS:
            totals_row[col] = 0.0

        for instr in INSTRUMENT_ORDER:
            if instr in instruments_table:
                vals = instruments_table[instr]
                row = {"Instrumento": instr}
                for col in SOCIETY_COLUMNS:
                    v = vals.get(col, 0)
                    row[col] = _fmt_num(v)
                    if col != "Total":
                        totals_row[col] += float(v or 0)
                instr_rows.append(row)

        # Add instruments not in fixed order
        for instr, vals in instruments_table.items():
            if instr not in INSTRUMENT_ORDER:
                row = {"Instrumento": instr}
                for col in SOCIETY_COLUMNS:
                    v = vals.get(col, 0)
                    row[col] = _fmt_num(v)
                    if col != "Total":
                        totals_row[col] += float(v or 0)
                instr_rows.append(row)

        # Total row
        totals_row["Total"] = _fmt_num(sum(
            float(totals_row[c]) for c in SOCIETY_COLUMNS[:-1]
        ))
        for col in SOCIETY_COLUMNS[:-1]:
            totals_row[col] = _fmt_num(totals_row[col])
        instr_rows.append(totals_row)

        df_instr = pd.DataFrame(instr_rows, columns=["Instrumento"] + SOCIETY_COLUMNS)
        st.table(_style_right_align(df_instr, SOCIETY_COLUMNS))
    else:
        st.info("Sin datos. Seleccione una fecha con datos ETF.")

    st.markdown("---")

    # ── Tabla 2: Instrumentos × Sociedades (PESOS %) ────────────
    st.subheader("Instrumentos × Sociedades (Pesos %)")
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
        st.table(_style_right_align(df_pct, SOCIETY_COLUMNS))
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
            labels = [c["label"] for c in composition_by_society]
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
        y_start, m_start, y_end, m_end = render_date_range_filter(
            key_prefix="etf_range"
        )
        # Use summary data to populate this table
        try:
            range_data = api_client.post("/data/summary", json={
                "years": list(range(y_start, y_end + 1)),
            })
            range_chart = range_data.get("chart_data", [])
        except Exception:
            range_chart = []

        range_start = f"{y_start}-{m_start:02d}"
        range_end = f"{y_end}-{m_end:02d}"
        range_filtered = [
            cd for cd in range_chart
            if range_start <= cd["fecha"] <= range_end
        ]
        if range_filtered:
            rt = []
            for cd in range_filtered:
                parts = cd["fecha"].split("-")
                rt.append({
                    "Mes": f"{MONTHS[int(parts[1]) - 1]} {parts[0][-2:]}",
                    "Ending Value": _fmt_num(cd["ending_value"]),
                    "Utilidad": _fmt_num(cd["utilidad"]),
                    "Rent. (%)": _fmt_pct(cd["rent_pct"]),
                })
            df_rng = pd.DataFrame(rt)
            st.table(_style_right_align(df_rng, ["Ending Value", "Utilidad", "Rent. (%)"]))
        else:
            st.info("Sin datos en rango.")

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

        # Format numbers
        for col in month_cols:
            if col in df_montos.columns:
                df_montos[col] = df_montos[col].apply(
                    lambda x: _fmt_num(x)
                )

        st.table(_style_right_align(df_montos, month_cols))
    else:
        st.info("Sin datos de montos por sociedad.")

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

        for col in ret_cols:
            if col in df_ret.columns:
                df_ret[col] = df_ret[col].apply(lambda x: _fmt_pct(x))

        st.table(_style_right_align(df_ret, ret_cols))
    else:
        st.info("Sin datos de rentabilidad.")
