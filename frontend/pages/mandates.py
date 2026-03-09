"""
Pagina Mandatos.

Estructura:
- 2 graficos superiores
- Tabla bancos x meses
- Tabla rentabilidad mensual / YTD
"""

from datetime import datetime
import math

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from frontend import api_client
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.number_format import fmt_number, fmt_percent
from frontend.components.table_utils import render_table


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

FIXED_BANKS = [
    ("bbh", "BBH"),
    ("goldman_sachs", "Goldman Sachs"),
    ("jpmorgan", "JPMorgan"),
    ("ubs", "UBS Suiza"),
    ("ubs_miami", "UBS Miami"),
]

ASSET_SERIES = [
    ("Cash, Deposits & Money Market", "Caja", "#0B66C3"),
    ("Fixed Income", "Renta Fija", "#88C6F2"),
    ("Equities", "Renta Variable", "#FF3131"),
]


def _pick_asset_value(payload: dict, asset_key: str) -> float:
    if not isinstance(payload, dict):
        return 0.0

    if asset_key in payload:
        try:
            return float(payload[asset_key] or 0)
        except (TypeError, ValueError):
            return 0.0

    key_map = {
        "Cash, Deposits & Money Market": ("cash", "deposit", "money market", "liquidity"),
        "Fixed Income": ("fixed income", "bond"),
        "Equities": ("equity", "equities"),
    }
    tokens = key_map.get(asset_key, ())
    total = 0.0
    found = False
    for raw_key, raw_val in payload.items():
        key_l = str(raw_key).lower()
        if not any(tok in key_l for tok in tokens):
            continue
        try:
            total += float(raw_val or 0)
            found = True
        except (TypeError, ValueError):
            continue
    return total if found else 0.0


def _year_from_fecha(fecha: str | None) -> int:
    if fecha and len(fecha) >= 4 and fecha[:4].isdigit():
        return int(fecha[:4])
    return datetime.now().year


def _calc_table_years(fecha: str | None) -> list[int]:
    if fecha and len(fecha) >= 4 and fecha[:4].isdigit():
        y = int(fecha[:4])
        return [y - 1, y]
    return []


def _fmt_bank(code: str) -> str:
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _to_float(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _fmt_num_cell(value) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return fmt_number(numeric, decimals=1)


def _fmt_pct_cell(value) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return fmt_percent(numeric, decimals=2)


def _calc_ytd_like_summary(returns_row: dict, effective_fecha: str | None) -> float | None:
    if not effective_fecha:
        return None
    try:
        year = int(effective_fecha[:4])
        month = int(effective_fecha[5:7])
    except (TypeError, ValueError):
        return None

    compound = 1.0
    has_ret = False
    for m in range(1, month + 1):
        key = f"{year}-{m:02d}_monthly"
        raw = returns_row.get(key)
        try:
            ret = float(raw)
        except (TypeError, ValueError):
            continue
        compound *= (1 + (ret / 100))
        has_ret = True
    return ((compound - 1) * 100) if has_ret else None


def _build_bank_kpi_table(
    banks_rows: list[dict],
    returns_table: list[dict],
    etf_totals_by_month: dict,
    etf_total_returns: dict,
    mov_ytd_by_bank: dict[str, float],
    etf_mov_ytd: float | None,
    effective_fecha: str | None,
) -> pd.DataFrame:
    if not effective_fecha:
        return pd.DataFrame(columns=["Banco", "Monto", "Rent. Mes", "Rent. YTD", "Movimientos", "Mov. YTD"])

    month_rows = {}
    month_net_totals = {}
    month_mov_totals = {}
    bank_mov_by_key: dict[str, dict[str, float]] = {}
    for row in banks_rows:
        key = f"{int(row.get('year', 0)):04d}-{int(row.get('month', 0)):02d}"
        month_net_totals[key] = month_net_totals.get(key, 0.0) + float(row.get("net_value") or 0.0)
        month_mov_totals[key] = month_mov_totals.get(key, 0.0) + float(row.get("movements") or 0.0)
        bank = str(row.get("bank_code") or "")
        bank_mov_by_key.setdefault(bank, {})
        bank_mov_by_key[bank][key] = bank_mov_by_key[bank].get(key, 0.0) + float(row.get("movements") or 0.0)
        if key != effective_fecha:
            continue
        agg = month_rows.setdefault(bank, {"net_value": 0.0, "movements": 0.0})
        agg["net_value"] += float(row.get("net_value") or 0.0)
        agg["movements"] += float(row.get("movements") or 0.0)

    returns_by_bank = {str(r.get("bank_code") or ""): r for r in returns_table}
    monthly_key = f"{effective_fecha}_monthly"

    table_rows = []
    total_monto = 0.0
    total_mov = 0.0
    total_mov_ytd = 0.0
    sel_year = int(effective_fecha[:4])
    sel_month = int(effective_fecha[5:7])

    for bank_code, bank_label in FIXED_BANKS:
        month_vals = month_rows.get(bank_code, {})
        ret_row = returns_by_bank.get(bank_code, {})
        monto = float(month_vals.get("net_value", 0.0) or 0.0)
        movs = float(month_vals.get("movements", 0.0) or 0.0)
        mov_ytd = float(mov_ytd_by_bank.get(bank_code, 0.0) or 0.0)
        ret_month = ret_row.get(monthly_key)
        ret_ytd = _calc_ytd_like_summary(ret_row, effective_fecha)

        total_monto += monto
        total_mov += movs
        total_mov_ytd += mov_ytd

        table_rows.append({
            "Banco": bank_label,
            "Monto": round(monto, 2),
            "Rent. Mes": round(float(ret_month), 4) if ret_month is not None else None,
            "Rent. YTD": round(float(ret_ytd), 4) if ret_ytd is not None else None,
            "Movimientos": round(movs, 2),
            "Mov. YTD": round(mov_ytd, 2),
        })

    # Fila adicional: total cuenta ETF.
    etf_month = etf_totals_by_month.get(effective_fecha, {}) if isinstance(etf_totals_by_month, dict) else {}
    etf_monto = float(etf_month.get("net_value", 0.0) or 0.0)
    etf_mov = float(etf_month.get("movements", 0.0) or 0.0)
    etf_mov_ytd = float(etf_mov_ytd or 0.0)
    etf_ret_month = etf_total_returns.get(monthly_key) if isinstance(etf_total_returns, dict) else None
    etf_ret_ytd = _calc_ytd_like_summary(etf_total_returns if isinstance(etf_total_returns, dict) else {}, effective_fecha)
    table_rows.append({
        "Banco": "Mandato ETF",
        "Monto": round(etf_monto, 2),
        "Rent. Mes": round(float(etf_ret_month), 4) if etf_ret_month is not None else None,
        "Rent. YTD": round(float(etf_ret_ytd), 4) if etf_ret_ytd is not None else None,
        "Movimientos": round(etf_mov, 2),
        "Mov. YTD": round(etf_mov_ytd, 2),
    })
    total_monto += etf_monto
    total_mov += etf_mov
    total_mov_ytd += etf_mov_ytd
    month_net_totals[effective_fecha] = month_net_totals.get(effective_fecha, 0.0) + etf_monto
    month_mov_totals[effective_fecha] = month_mov_totals.get(effective_fecha, 0.0) + etf_mov
    for mk, vals in (etf_totals_by_month.items() if isinstance(etf_totals_by_month, dict) else []):
        if mk == effective_fecha:
            continue
        month_net_totals[mk] = month_net_totals.get(mk, 0.0) + float(vals.get("net_value", 0.0) or 0.0)
        month_mov_totals[mk] = month_mov_totals.get(mk, 0.0) + float(vals.get("movements", 0.0) or 0.0)

    def _prev_month_key(fecha: str) -> str:
        year = int(fecha[:4])
        month = int(fecha[5:7])
        if month == 1:
            return f"{year - 1}-12"
        return f"{year}-{month - 1:02d}"

    total_ret_month = None
    prev_key = _prev_month_key(effective_fecha)
    prev_total = month_net_totals.get(prev_key)
    curr_total = month_net_totals.get(effective_fecha)
    mov_curr = month_mov_totals.get(effective_fecha)
    if prev_total not in (None, 0) and curr_total is not None and mov_curr is not None:
        total_ret_month = (((curr_total - mov_curr) / prev_total) - 1) * 100

    total_ret_ytd = None
    try:
        year = int(effective_fecha[:4])
        month = int(effective_fecha[5:7])
    except (TypeError, ValueError):
        year = None
        month = None

    if year and month:
        compound = 1.0
        has_ret = False
        for m in range(1, month + 1):
            mk = f"{year}-{m:02d}"
            pk = _prev_month_key(mk)
            prev_val = month_net_totals.get(pk)
            cur_val = month_net_totals.get(mk)
            mov_val = month_mov_totals.get(mk)
            if prev_val in (None, 0) or cur_val is None or mov_val is None:
                continue
            r = (((cur_val - mov_val) / prev_val) - 1) * 100
            compound *= (1 + (r / 100))
            has_ret = True
        if has_ret:
            total_ret_ytd = (compound - 1) * 100

    table_rows.append({
        "Banco": "Total",
        "Monto": round(total_monto, 2),
        "Rent. Mes": round(float(total_ret_month), 4) if total_ret_month is not None else None,
        "Rent. YTD": round(float(total_ret_ytd), 4) if total_ret_ytd is not None else None,
        "Movimientos": round(total_mov, 2),
        "Mov. YTD": round(total_mov_ytd, 2),
    })

    return pd.DataFrame(table_rows)


def _build_bank_month_tables(
    banks_rows: list[dict],
    returns_table: list[dict],
    etf_totals_by_month: dict,
    etf_total_returns: dict,
    selected_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    month_keys = [f"{m:02d}" for m in range(1, 13)]
    fixed_codes = [code for code, _ in FIXED_BANKS]

    montos_by_bank = {code: {mk: 0.0 for mk in month_keys} for code in fixed_codes}
    movs_by_bank = {code: {mk: 0.0 for mk in month_keys} for code in fixed_codes}
    totals_by_key_net: dict[str, float] = {}
    totals_by_key_mov: dict[str, float] = {}

    for row in banks_rows:
        year = int(row.get("year", 0))
        month = int(row.get("month", 0))
        if month < 1 or month > 12:
            continue
        mk = f"{month:02d}"
        key = f"{year:04d}-{mk}"
        bank = str(row.get("bank_code") or "")
        net = float(row.get("net_value") or 0.0)
        mov = float(row.get("movements") or 0.0)

        if bank in montos_by_bank and year == selected_year:
            montos_by_bank[bank][mk] += net
            movs_by_bank[bank][mk] += mov

        if bank in montos_by_bank and year in {selected_year - 1, selected_year}:
            totals_by_key_net[key] = totals_by_key_net.get(key, 0.0) + net
            totals_by_key_mov[key] = totals_by_key_mov.get(key, 0.0) + mov

    monto_rows: list[dict] = []
    mov_rows: list[dict] = []
    for code, label in FIXED_BANKS:
        monto_row = {"Banco": label}
        mov_row = {"Banco": label}
        for mk in month_keys:
            monto_row[mk] = montos_by_bank[code][mk]
            mov_row[mk] = movs_by_bank[code][mk]
        monto_rows.append(monto_row)
        mov_rows.append(mov_row)

    total_monto_row = {"Banco": "Total"}
    total_mov_row = {"Banco": "Total"}
    for mk in month_keys:
        total_monto_row[mk] = sum(float(r.get(mk) or 0.0) for r in monto_rows)
        total_mov_row[mk] = sum(float(r.get(mk) or 0.0) for r in mov_rows)
    monto_rows.append(total_monto_row)
    mov_rows.append(total_mov_row)

    returns_by_bank = {str(r.get("bank_code") or ""): r for r in returns_table}
    ret_m_rows: list[dict] = []
    ret_y_rows: list[dict] = []
    for code, label in FIXED_BANKS:
        rr = returns_by_bank.get(code, {})
        row_m = {"Banco": label}
        row_y = {"Banco": label}
        for mk in month_keys:
            k = f"{selected_year}-{mk}"
            row_m[mk] = rr.get(f"{k}_monthly")
            row_y[mk] = rr.get(f"{k}_ytd")
        ret_m_rows.append(row_m)
        ret_y_rows.append(row_y)

    def _prev_month_key(fecha: str) -> str:
        y = int(fecha[:4])
        m = int(fecha[5:7])
        if m == 1:
            return f"{y - 1}-12"
        return f"{y}-{m - 1:02d}"

    total_m = {"Banco": "Total"}
    total_y = {"Banco": "Total"}
    cumulative = 1.0
    has_cum = False
    for mk in month_keys:
        key = f"{selected_year}-{mk}"
        prev_key = _prev_month_key(key)
        prev_val = totals_by_key_net.get(prev_key)
        cur_val = totals_by_key_net.get(key)
        mov_val = totals_by_key_mov.get(key)
        rm = None
        if prev_val not in (None, 0) and cur_val is not None and mov_val is not None:
            rm = (((cur_val - mov_val) / prev_val) - 1) * 100
        total_m[mk] = rm
        if rm is not None:
            cumulative *= (1 + rm / 100)
            has_cum = True
            total_y[mk] = (cumulative - 1) * 100
        else:
            total_y[mk] = (cumulative - 1) * 100 if has_cum else None
    ret_m_rows.append(total_m)
    ret_y_rows.append(total_y)

    return (
        pd.DataFrame(monto_rows),
        pd.DataFrame(mov_rows),
        pd.DataFrame(ret_m_rows),
        pd.DataFrame(ret_y_rows),
    )


def _rename_month_columns(df: pd.DataFrame, selected_year: int) -> tuple[pd.DataFrame, list[str]]:
    col_rename = {"Banco": "Banco"}
    month_cols = []
    suffix = f" {str(selected_year)[-2:]}"
    for m in range(1, 13):
        mk = f"{m:02d}"
        if mk in df.columns:
            name = f"{MONTHS[m - 1]}{suffix}"
            col_rename[mk] = name
            month_cols.append(name)
    return df.rename(columns=col_rename), month_cols


def _summary_mov_ytd_by_bank(summary_rows: list[dict], selected_year: int, selected_month: int) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in summary_rows:
        fecha = str(row.get("fecha") or "")
        if len(fecha) < 7:
            continue
        year = int(fecha[:4])
        month = int(fecha[5:7])
        if year != selected_year or month < 1 or month > selected_month:
            continue
        bank = str(row.get("banco") or "")
        out[bank] = out.get(bank, 0.0) + float(row.get("movimientos") or 0.0)
    return out


def render():
    st.title("Mandatos")
    st.markdown("---")

    try:
        filter_opts = api_client.get("/accounts/filter-options")
    except Exception:
        filter_opts = {"bank_codes": [], "entity_names": []}

    bank_options = filter_opts.get("bank_codes", [])

    st.markdown("### Filtros")
    col_bank, col_entity, col_fecha = st.columns(3)

    with col_bank:
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            format_func=_fmt_bank,
            key="mandates_bank_codes",
        )
        bank_seed = selected_banks

    seed_payload = {
        "bank_codes": bank_seed,
    }
    seed_data = api_client.post("/data/mandates", json=seed_payload)
    available_fechas = sorted(seed_data.get("available_fechas", []), reverse=True)
    mandates_societies = sorted({
        str(r.get("entity_name"))
        for r in seed_data.get("banks_by_month", [])
        if r.get("entity_name")
    })

    with col_entity:
        selected_entities = st.multiselect(
            "Sociedad",
            options=mandates_societies,
            key="mandates_entity_names",
        )

    with col_fecha:
        if available_fechas:
            if "mandates_fecha" not in st.session_state or st.session_state["mandates_fecha"] not in available_fechas:
                st.session_state["mandates_fecha"] = available_fechas[0]
            selected_fecha = st.selectbox(
                "Fecha",
                options=available_fechas,
                key="mandates_fecha",
            )
        else:
            selected_fecha = None
            st.selectbox("Fecha", options=["Sin datos"], disabled=True, key="mandates_fecha_empty")

    st.markdown("---")

    payload = {
        "bank_codes": selected_banks,
        "entity_names": selected_entities,
        "fecha": selected_fecha,
    }
    data = api_client.post("/data/mandates", json=payload)
    table_years = _calc_table_years(selected_fecha)
    table_data = api_client.post("/data/mandates", json={
        "entity_names": selected_entities,
        "years": table_years,
    })
    series_data = api_client.post("/data/mandates", json={
        "bank_codes": selected_banks,
        "entity_names": selected_entities,
        "years": table_years,
    })

    effective_fecha = data.get("selected_fecha") or selected_fecha
    chart_year = _year_from_fecha(effective_fecha)
    chart_month = int(effective_fecha[5:7]) if effective_fecha and len(effective_fecha) >= 7 else 12

    # Mov YTD alineado a Resumen para Mandato y ETF.
    summary_mand = api_client.post("/data/summary", json={
        "years": [chart_year],
        "entity_names": selected_entities,
        "account_types": ["mandato"],
    })
    mov_ytd_by_bank = _summary_mov_ytd_by_bank(summary_mand.get("rows", []), chart_year, chart_month)
    summary_etf = api_client.post("/data/summary", json={
        "years": [chart_year],
        "entity_names": selected_entities,
        "account_types": ["etf"],
    })
    etf_mov_ytd = sum(
        float(r.get("movimientos") or 0.0)
        for r in summary_etf.get("rows", [])
        if str(r.get("fecha", "")).startswith(f"{chart_year}-")
        and int(str(r.get("fecha"))[5:7]) <= chart_month
    )

    st.subheader("Mandato por Banco")
    aa_rows = data.get("asset_allocation", [])
    aa_bank = data.get("aa_by_bank", {})
    month_keys = [f"{chart_year}-{m:02d}" for m in range(1, 13)]
    month_data: dict[str, dict] = {str(r.get("fecha")): r for r in aa_rows if r.get("fecha")}
    bank_data = aa_bank if isinstance(aa_bank, dict) else {}

    kpi_df = _build_bank_kpi_table(
        banks_rows=table_data.get("banks_by_month", []),
        returns_table=table_data.get("returns_table", []),
        etf_totals_by_month=table_data.get("etf_totals_by_month", {}),
        etf_total_returns=table_data.get("etf_total_returns", {}),
        mov_ytd_by_bank=mov_ytd_by_bank,
        etf_mov_ytd=etf_mov_ytd,
        effective_fecha=effective_fecha,
    )
    if not kpi_df.empty:
        sort_col_left, sort_col_right = st.columns([2, 1])
        with sort_col_left:
            sort_col = st.selectbox(
                "Ordenar por",
                options=["Banco", "Monto", "Rent. Mes", "Rent. YTD", "Movimientos", "Mov. YTD"],
                key="mandates_kpi_sort_col",
            )
        with sort_col_right:
            sort_dir = st.selectbox(
                "Sentido",
                options=["Ascendente", "Descendente"],
                key="mandates_kpi_sort_dir",
            )

        ascending = sort_dir == "Ascendente"
        sorted_kpi = kpi_df.copy()
        if sort_col == "Banco":
            sorted_kpi = sorted_kpi.sort_values(
                by=sort_col,
                ascending=ascending,
                kind="mergesort",
                key=lambda s: s.astype(str).str.lower(),
            )
        else:
            sorted_kpi = sorted_kpi.sort_values(
                by=sort_col,
                ascending=ascending,
                na_position="last",
                kind="mergesort",
            )

        kpi_display = sorted_kpi.copy()
        kpi_display["Monto"] = kpi_display["Monto"].apply(_fmt_num_cell)
        kpi_display["Rent. Mes"] = kpi_display["Rent. Mes"].apply(_fmt_pct_cell)
        kpi_display["Rent. YTD"] = kpi_display["Rent. YTD"].apply(_fmt_pct_cell)
        kpi_display["Movimientos"] = kpi_display["Movimientos"].apply(_fmt_num_cell)
        kpi_display["Mov. YTD"] = kpi_display["Mov. YTD"].apply(_fmt_num_cell)
        render_table(
            kpi_display,
            bold_row_labels={"Total"},
            label_col="Banco",
            fixed_equal_cols=True,
        )
    else:
        st.info("Sin datos para Mandato por Banco.")

    # Evolucion mensual YTD (5 bancos mandato + total ETF), con 12 meses fijos.
    st.subheader(f"Evolucion Mensual de Rentabilidad YTD ({chart_year})")
    ytd_month_keys = [f"{chart_year}-{m:02d}" for m in range(1, 13)]
    returns_by_bank = {
        str(r.get("bank_code") or ""): r for r in table_data.get("returns_table", [])
    }
    etf_total_returns = table_data.get("etf_total_returns", {})
    fig_ytd = go.Figure()
    line_colors = {
        "bbh": "#1f77b4",
        "goldman_sachs": "#ff7f0e",
        "jpmorgan": "#2ca02c",
        "ubs": "#d62728",
        "ubs_miami": "#17becf",
        "etf_total": "#8C564B",
    }
    for bank_code, bank_label in FIXED_BANKS:
        row = returns_by_bank.get(bank_code, {})
        y_vals = [row.get(f"{mk}_ytd") for mk in ytd_month_keys]
        fig_ytd.add_trace(
            go.Scatter(
                x=MONTHS,
                y=y_vals,
                mode="lines+markers",
                name=bank_label,
                line=dict(color=line_colors.get(bank_code)),
            )
        )
    y_etf = [etf_total_returns.get(f"{mk}_ytd") for mk in ytd_month_keys] if isinstance(etf_total_returns, dict) else [None] * 12
    fig_ytd.add_trace(
        go.Scatter(
            x=MONTHS,
            y=y_etf,
            mode="lines+markers",
            name="Mandato ETF",
            line=dict(color=line_colors["etf_total"], width=3),
        )
    )
    fig_ytd.update_layout(
        height=384,
        yaxis_title="% YTD",
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=24),
        ),
    )
    fig_ytd.update_yaxes(tickformat=",.2f")
    st.plotly_chart(fig_ytd, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Desglose por Tipo de Activo (MM USD) ({chart_year})")
        fig = go.Figure()
        for asset_key, label, color in ASSET_SERIES:
            values_mm = []
            for mk in month_keys:
                row = month_data.get(mk, {})
                val = _pick_asset_value(row, asset_key)
                values_mm.append(val / 1_000_000)
            fig.add_trace(
                go.Bar(
                    x=MONTHS,
                    y=values_mm,
                    name=label,
                    marker_color=color,
                )
            )
        fig.update_layout(
            barmode="stack",
            height=432,
            yaxis_title="USD MM",
            margin=dict(l=20, r=20, t=30, b=20),
        )
        fig.update_yaxes(tickformat=",.1f")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        month_caption = effective_fecha or "Mes seleccionado"
        st.subheader(f"% por Tipo de Activo en cada Banco ({month_caption})")
        bank_x = [label for _, label in FIXED_BANKS]
        fig = go.Figure()
        for asset_key, label, color in ASSET_SERIES:
            y_vals = []
            for bank_code, _ in FIXED_BANKS:
                row = bank_data.get(bank_code, {}) if isinstance(bank_data.get(bank_code), dict) else {}
                y_vals.append(_pick_asset_value(row, asset_key))
            fig.add_trace(
                go.Bar(
                    x=bank_x,
                    y=y_vals,
                    name=label,
                    marker_color=color,
                )
            )
        fig.update_layout(
            barmode="stack",
            height=432,
            yaxis_title="% del banco",
            margin=dict(l=20, r=20, t=30, b=20),
        )
        fig.update_yaxes(range=[0, 100], tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Tablas finales estilo ETF, adaptadas a bancos.
    montos_df, movs_df, ret_m_df, ret_y_df = _build_bank_month_tables(
        banks_rows=series_data.get("banks_by_month", []),
        returns_table=series_data.get("returns_table", []),
        etf_totals_by_month=series_data.get("etf_totals_by_month", {}),
        etf_total_returns=series_data.get("etf_total_returns", {}),
        selected_year=chart_year,
    )

    st.subheader(f"Mandato por Banco {chart_year}")
    st.caption("Bancos x Meses. Afectado por filtros Fecha, Banco y Sociedad.")
    if not montos_df.empty:
        montos_df, monto_cols = _rename_month_columns(montos_df, chart_year)
        for col in monto_cols:
            montos_df[col] = montos_df[col].apply(lambda x: fmt_number(x, decimals=1) if x not in (None, 0) else "")
        render_table(montos_df, bold_row_labels={"Total"}, label_col="Banco")
    else:
        st.info("Sin datos de montos por banco.")

    st.markdown("---")

    st.subheader(f"Movimientos por Banco {chart_year}")
    st.caption("Bancos x Meses. Afectado por filtros Fecha, Banco y Sociedad.")
    if not movs_df.empty:
        month_keys = [f"{m:02d}" for m in range(1, 13) if f"{m:02d}" in movs_df.columns]
        movs_df["Total"] = movs_df[month_keys].sum(axis=1) if month_keys else 0.0
        movs_df, mov_cols = _rename_month_columns(movs_df, chart_year)
        mov_cols = mov_cols + (["Total"] if "Total" in movs_df.columns else [])
        for col in mov_cols:
            movs_df[col] = movs_df[col].apply(lambda x: fmt_number(x, decimals=1) if x not in (None, 0) else "")
        render_table(movs_df, bold_row_labels={"Total"}, bold_cols=["Total"], label_col="Banco")
    else:
        st.info("Sin datos de movimientos por banco.")

    st.markdown("---")

    st.subheader(f"Rentabilidad por Banco {chart_year}")
    ret_mode = st.radio(
        "Tipo de rentabilidad",
        options=["Mensual", "YTD"],
        horizontal=True,
        key="mandates_return_mode",
    )
    ret_df = ret_m_df if ret_mode == "Mensual" else ret_y_df
    if not ret_df.empty:
        ret_df, ret_cols = _rename_month_columns(ret_df, chart_year)
        for col in ret_cols:
            ret_df[col] = ret_df[col].apply(lambda x: fmt_percent(x, decimals=2))
        render_table(ret_df, bold_row_labels={"Total"}, label_col="Banco")
    else:
        st.info("Sin datos de rentabilidad por banco.")
