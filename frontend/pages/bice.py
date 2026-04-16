"""
Pagina Detalle Bice — Inversiones nacionales (BICE, Banchile).

Estructura:
- Filtros: Banco, Sociedad, Fecha, Personas
- KPIs: saldo CLP y USD (separados, sin conversión)
- Rentabilidad mensual en CLP y en USD (tabs)
- Detalle últimos 12 meses en CLP y en USD (tabs)
- Detalle por Activo, por Banco, por Sociedad, por Cuenta
  → cada sección muestra tab CLP y tab USD

Diferencias con Detalle Internacional:
- Sin filtro Consolidado ni Tipo de cuenta
- Columnas de movimientos separadas: Aportes / Retiros (no "Movimientos")
- Todas las tablas duplicadas por moneda
- Solo bancos nacionales: BICE, Banchile
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from frontend import api_client
from frontend.components.chart_utils import aligned_dual_return_axes
from frontend.components.filters import use_apply_filters
from frontend.components.number_format import fmt_currency, fmt_number, fmt_percent
from frontend.components.table_utils import render_table


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

_BANK_DISPLAY = {"bice": "BICE", "bice_inversiones": "Bice Inversiones", "banchile": "Banchile"}
_ASSET_COLORS = {
    "Caja": "#D5DEE9",
    "Renta Fija": "#2D6FB7",
    "Equities": "#B53639",
}
_FALLBACK_COLORS = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
    "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _fmt_or_blank(value, *, decimals: int = 0) -> str:
    if value is None:
        return ""
    return fmt_number(value, decimals=decimals)


def _fmt_pct_or_blank(value, *, decimals: int = 2) -> str:
    if value is None:
        return ""
    return fmt_percent(value, decimals=decimals)


def _fmt_bank(code: str) -> str:
    return _BANK_DISPLAY.get(code, code.replace("_", " ").title())


def _fecha_label(fecha_str: str) -> str:
    parts = str(fecha_str).split("-")
    if len(parts) != 2:
        return str(fecha_str)
    month = int(parts[1])
    return f"{MONTHS[month - 1]} {parts[0][-2:]}"


def _build_fecha_options(years: list[int]) -> list[str]:
    if not years:
        return []
    values: list[str] = []
    for year in sorted(set(int(y) for y in years), reverse=True):
        for month in range(12, 0, -1):
            values.append(f"{year}-{month:02d}")
    return values


def _sanitize_multiselect_state(key: str, valid_options: list[str]) -> list[str]:
    selected = [v for v in st.session_state.get(key, []) if v in valid_options]
    st.session_state[key] = selected
    return selected


def _compute_accumulated(monthly_returns: list[float | None]) -> list[float | None]:
    accumulated: list[float | None] = []
    compound = 1.0
    has_data = False
    for ret in monthly_returns:
        if ret is None:
            accumulated.append(round((compound - 1) * 100, 4) if has_data else None)
            continue
        compound *= 1 + (ret / 100)
        has_data = True
        accumulated.append(round((compound - 1) * 100, 4))
    return accumulated


# ── Secciones de detalle ──────────────────────────────────────────────────────

def _render_currency_detail_table(
    *,
    rows: list[dict],
    label_col: str,
    currency_label: str,
    decimals: int = 0,
) -> None:
    """
    Tabla de detalle por activo/banco/sociedad/cuenta para una moneda.
    Columnas: label | Saldo | % | Aportes | Retiros | Utilidad
    """
    if not rows:
        st.info(f"Sin datos ({currency_label}).")
        return

    total_ending = sum((_to_float(r.get("ending")) or 0.0) for r in rows)

    table_data = []
    for r in rows:
        ending = _to_float(r.get("ending"))
        pct = round((ending / total_ending) * 100, 1) if total_ending and ending else None
        table_data.append({
            label_col: str(r.get("name") or r.get("entity_name") or r.get("account_number") or ""),
            "Saldo": _fmt_or_blank(ending, decimals=decimals),
            "%": _fmt_pct_or_blank(pct, decimals=1),
            "Aportes": _fmt_or_blank(_to_float(r.get("aportes")), decimals=decimals),
            "Retiros": _fmt_or_blank(_to_float(r.get("retiros")), decimals=decimals),
            "Utilidad": _fmt_or_blank(_to_float(r.get("profit")), decimals=decimals),
        })

    # Fila total
    total_aportes = sum((_to_float(r.get("aportes")) or 0.0) for r in rows)
    total_retiros = sum((_to_float(r.get("retiros")) or 0.0) for r in rows)
    profits = [_to_float(r.get("profit")) for r in rows]
    total_profit = sum(p for p in profits if p is not None) if any(p is not None for p in profits) else None
    table_data.append({
        label_col: "Total",
        "Saldo": _fmt_or_blank(total_ending, decimals=decimals),
        "%": _fmt_pct_or_blank(100.0 if total_ending else None, decimals=1),
        "Aportes": _fmt_or_blank(total_aportes, decimals=decimals),
        "Retiros": _fmt_or_blank(total_retiros, decimals=decimals),
        "Utilidad": _fmt_or_blank(total_profit, decimals=decimals),
    })

    df = pd.DataFrame(table_data, columns=[label_col, "Saldo", "%", "Aportes", "Retiros", "Utilidad"])
    render_table(df, label_col=label_col, bold_row_labels={"Total"})


def _render_asset_detail_table(
    *,
    rows: list[dict],
    currency_label: str,
    decimals: int = 0,
) -> None:
    """
    Tabla detalle por activo (Caja / Renta Fija / Equities) para una moneda.
    """
    if not rows:
        st.info(f"Sin datos ({currency_label}).")
        return

    total_ending = sum((_to_float(r.get("value")) or 0.0) for r in rows)
    table_data = []
    for r in rows:
        val = _to_float(r.get("value"))
        pct = round((val / total_ending) * 100, 1) if total_ending and val else None
        table_data.append({
            "Activo": str(r.get("name") or ""),
            "Saldo": _fmt_or_blank(val, decimals=decimals),
            "%": _fmt_pct_or_blank(pct, decimals=1),
        })

    table_data.append({
        "Activo": "Total",
        "Saldo": _fmt_or_blank(total_ending, decimals=decimals),
        "%": _fmt_pct_or_blank(100.0 if total_ending else None, decimals=1),
    })

    df = pd.DataFrame(table_data, columns=["Activo", "Saldo", "%"])
    render_table(df, label_col="Activo", bold_row_labels={"Total"})


def _render_donut(rows: list[dict], value_key: str = "value") -> None:
    if not rows:
        st.info("Sin datos para la composición.")
        return
    labels = [str(r.get("name") or "") for r in rows]
    values = [_to_float(r.get(value_key)) or 0.0 for r in rows]
    colors = [_ASSET_COLORS.get(lbl, _FALLBACK_COLORS[i % len(_FALLBACK_COLORS)]) for i, lbl in enumerate(labels)]
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.42,
        marker=dict(colors=colors),
    )])
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)


def _render_return_chart(
    *,
    months: list[str],
    returns: list[float | None],
    currency_label: str,
    color: str = "#AFC8E2",
) -> None:
    accumulated = _compute_accumulated(returns)
    if not any(r is not None for r in returns):
        st.info(f"Sin datos de rentabilidad ({currency_label}).")
        return
    axis_ranges = aligned_dual_return_axes(returns, accumulated)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=months, y=returns, name="Rent. mensual", marker_color=color, opacity=0.9),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=months, y=accumulated, mode="lines+markers", name="Acumulada",
                   line=dict(color="#E67E22", width=2)),
        secondary_y=True,
    )
    fig.update_layout(
        height=340,
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_yaxes(title_text="% mensual", tickformat=",.2f", showgrid=False,
                     zeroline=True, zerolinecolor="#9EA7B3",
                     range=axis_ranges["primary_range"], secondary_y=False)
    fig.update_yaxes(title_text="% acumulada", tickformat=",.2f", showgrid=True,
                     gridcolor="#D6DCE5", zeroline=False,
                     range=axis_ranges["secondary_range"], secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


def _render_12m_table(
    *,
    months: list[str],
    monthly_rows: list[dict],
    currency: str,
    decimals: int = 0,
) -> None:
    """
    Tabla resumen de últimos 12 meses para una moneda (CLP o USD).
    Columnas: Sociedad | mes1 | ... | mes12
    Rows: una por cuenta, más total.
    Cada celda: saldo (se puede expandir).
    """
    if not monthly_rows:
        st.info(f"Sin datos ({currency}).")
        return

    series_key = "clp" if currency == "CLP" else "usd"

    # Construir tabla con totales por mes
    totals = [0.0] * len(months)
    rows_data = []
    for row in monthly_rows:
        series = row.get(series_key) or []
        row_values = []
        for idx, entry in enumerate(series):
            val = (_to_float((entry or {}).get("ending")) if entry else None)
            row_values.append(_fmt_or_blank(val, decimals=decimals) if val is not None else "—")
            if val is not None:
                totals[idx] += val
        rows_data.append([row.get("label", "")] + row_values)

    total_row = ["Total"] + [
        _fmt_or_blank(t, decimals=decimals) if t else "—" for t in totals
    ]
    rows_data.append(total_row)

    df = pd.DataFrame(rows_data, columns=["Sociedad"] + months)
    render_table(df, label_col="Sociedad", bold_row_labels={"Total"})


def _render_account_detail_table(
    *,
    rows: list[dict],
    currency_label: str,
    decimals: int = 0,
) -> None:
    """
    Tabla detalle por cuenta con saldo por activo + movimientos.
    """
    if not rows:
        st.info(f"Sin datos ({currency_label}).")
        return

    total_ending = sum((_to_float(r.get("ending")) or 0.0) for r in rows)
    table_data = []
    for r in rows:
        ending = _to_float(r.get("ending"))
        pct = round((ending / total_ending) * 100, 1) if total_ending and ending else None
        table_data.append({
            "Sociedad": str(r.get("entity_name") or ""),
            "Cuenta": str(r.get("account_number") or ""),
            "Banco": _fmt_bank(str(r.get("bank_code") or "")),
            "Saldo": _fmt_or_blank(ending, decimals=decimals),
            "%": _fmt_pct_or_blank(pct, decimals=1),
            "Caja": _fmt_or_blank(_to_float(r.get("caja")), decimals=decimals),
            "Renta Fija": _fmt_or_blank(_to_float(r.get("renta_fija")), decimals=decimals),
            "Equities": _fmt_or_blank(_to_float(r.get("equities")), decimals=decimals),
            "Aportes": _fmt_or_blank(_to_float(r.get("aportes")), decimals=decimals),
            "Retiros": _fmt_or_blank(_to_float(r.get("retiros")), decimals=decimals),
            "Utilidad": _fmt_or_blank(_to_float(r.get("profit")), decimals=decimals),
        })

    total_aportes = sum((_to_float(r.get("aportes")) or 0.0) for r in rows)
    total_retiros = sum((_to_float(r.get("retiros")) or 0.0) for r in rows)
    profits = [_to_float(r.get("profit")) for r in rows]
    total_profit = sum(p for p in profits if p is not None) if any(p is not None for p in profits) else None
    table_data.append({
        "Sociedad": "Total",
        "Cuenta": "",
        "Banco": "",
        "Saldo": _fmt_or_blank(total_ending, decimals=decimals),
        "%": _fmt_pct_or_blank(100.0 if total_ending else None, decimals=1),
        "Caja": "",
        "Renta Fija": "",
        "Equities": "",
        "Aportes": _fmt_or_blank(total_aportes, decimals=decimals),
        "Retiros": _fmt_or_blank(total_retiros, decimals=decimals),
        "Utilidad": _fmt_or_blank(total_profit, decimals=decimals),
    })

    df = pd.DataFrame(
        table_data,
        columns=["Sociedad", "Cuenta", "Banco", "Saldo", "%", "Caja", "Renta Fija", "Equities",
                 "Aportes", "Retiros", "Utilidad"],
    )
    render_table(df, label_col="Sociedad", bold_row_labels={"Total"})


# ── Render principal ──────────────────────────────────────────────────────────

def render() -> None:
    st.title("Detalle Bice")
    st.markdown("---")

    # ── Filtros ───────────────────────────────────────────────────────────────
    # Obtener opciones de la BD mediante una llamada inicial sin filtros
    try:
        init_data = api_client.post("/data/bice", json={
            "years": [], "months": [], "bank_codes": [],
            "entity_names": [], "person_names": [],
        })
        filter_opts = init_data.get("filter_options", {})
    except Exception:
        filter_opts = {}

    available_years = [int(y) for y in filter_opts.get("years", [])]
    available_bancos = filter_opts.get("bancos", [])
    available_sociedades = filter_opts.get("sociedades", [])
    available_personas = filter_opts.get("personas", [])

    fecha_options = _build_fecha_options(available_years)

    if "bice_fecha" not in st.session_state or st.session_state.get("bice_fecha") not in fecha_options:
        st.session_state["bice_fecha"] = fecha_options[0] if fecha_options else None

    _sanitize_multiselect_state("bice_banco", available_bancos)
    _sanitize_multiselect_state("bice_sociedad", available_sociedades)
    _sanitize_multiselect_state("bice_persona", available_personas)

    st.markdown("### Filtros")
    f1, f2, f3, f4, f5 = st.columns(5)

    with f1:
        selected_bancos = st.multiselect(
            "Banco",
            options=available_bancos,
            format_func=_fmt_bank,
            key="bice_banco",
        )
    with f2:
        selected_sociedades = st.multiselect(
            "Sociedad",
            options=available_sociedades,
            key="bice_sociedad",
        )
    with f3:
        selected_consolidado = st.selectbox(
            "Consolidado",
            options=["", "Todas las sociedades"],
            format_func=lambda x: x or "Sin consolidado",
            key="bice_consolidado",
        )
    with f4:
        if fecha_options:
            selected_fecha = st.selectbox("Fecha", options=fecha_options, key="bice_fecha")
        else:
            selected_fecha = None
            st.selectbox("Fecha", options=["Sin datos"], disabled=True, key="bice_fecha_empty")
    with f5:
        selected_personas = st.multiselect(
            "Personas",
            options=available_personas,
            key="bice_persona",
        )

    applied_filters, _ = use_apply_filters(
        state_key="bice_filters_applied",
        current_filters={
            "bank_codes": list(selected_bancos),
            "entity_names": list(selected_sociedades),
            "consolidado": selected_consolidado,
            "fecha": selected_fecha,
            "person_names": list(selected_personas),
        },
    )

    applied_bancos = list(applied_filters.get("bank_codes", []))
    applied_sociedades = list(applied_filters.get("entity_names", []))
    applied_consolidado = applied_filters.get("consolidado", "")
    applied_fecha = applied_filters.get("fecha")
    applied_personas = list(applied_filters.get("person_names", []))

    selected_year = int(applied_fecha[:4]) if applied_fecha else None
    selected_month = int(applied_fecha[5:7]) if applied_fecha else None
    only_sociedades = applied_consolidado == "Todas las sociedades"

    if only_sociedades:
        st.caption("Consolidado activo: Solo sociedades (excluye cuentas personales)")

    has_filter = bool(applied_bancos or applied_sociedades or applied_personas or applied_fecha or only_sociedades)

    # ── Cargar datos ──────────────────────────────────────────────────────────
    data: dict = {}
    if has_filter or fecha_options:
        payload = {
            "years": [selected_year] if selected_year else [],
            "months": [selected_month] if selected_month else [],
            "bank_codes": applied_bancos,
            "entity_names": applied_sociedades,
            "person_names": applied_personas,
            "only_sociedades": only_sociedades,
        }
        try:
            data = api_client.post("/data/bice", json=payload)
        except Exception as exc:
            st.error(f"Error obteniendo datos: {exc}")
            data = {}

    if not data:
        st.info("Selecciona filtros para visualizar información.")
        return

    kpis = data.get("kpis", {})
    kpi_clp = kpis.get("clp", {})
    kpi_usd = kpis.get("usd", {})
    months_labels = data.get("returns_panel", {}).get("months", [])
    returns_rows = data.get("returns_panel", {}).get("rows", [])

    # ── KPIs ──────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Saldo")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("Total $ (CLP)", fmt_currency(kpi_clp.get("ending", 0), decimals=0))
    with k2:
        usd_ending = kpi_usd.get("ending", 0)
        st.metric("Total US$", f"US${fmt_number(usd_ending, decimals=2)}")
    with k3:
        clp_profit = kpi_clp.get("profit")
        st.metric(
            "Utilidad mes $ (CLP)",
            fmt_currency(clp_profit, decimals=0) if clp_profit is not None else "—",
        )
    with k4:
        usd_profit = kpi_usd.get("profit")
        st.metric(
            "Utilidad mes US$",
            f"US${fmt_number(usd_profit, decimals=2)}" if usd_profit is not None else "—",
        )

    # ── Rentabilidad ──────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Rentabilidad últimos 12 meses (%)")

    # Agregar retornos por moneda a nivel de todos los rows (retorno consolidado)
    # Usamos el primer row si solo hay una cuenta, o calculamos promedio ponderado
    def _aggregate_returns(currency_key: str) -> list[float | None]:
        """Retorno consolidado: ponderado por saldo cuando hay múltiples cuentas."""
        if not returns_rows or not months_labels:
            return []
        n = len(months_labels)
        result: list[float | None] = []
        for idx in range(n):
            weighted_sum = 0.0
            total_weight = 0.0
            has_any = False
            for row in returns_rows:
                ret_list = row.get(f"returns_{currency_key}", [])
                ret = ret_list[idx] if idx < len(ret_list) else None
                if ret is not None:
                    has_any = True
                    weighted_sum += ret
                    total_weight += 1.0
            if has_any and total_weight > 0:
                result.append(round(weighted_sum / total_weight, 4))
            else:
                result.append(None)
        return result

    tab_rent_clp, tab_rent_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_rent_clp:
        agg_clp = _aggregate_returns("clp")
        left_c, right_c = st.columns([2, 1])
        with left_c:
            _render_return_chart(
                months=months_labels,
                returns=agg_clp,
                currency_label="CLP",
                color="#AFC8E2",
            )
        with right_c:
            # Tabla últimos 12 meses CLP (compacta: fecha | saldo | aportes | retiros | utilidad)
            _render_12m_compact(data, months_labels, "CLP", decimals=0)
    with tab_rent_usd:
        agg_usd = _aggregate_returns("usd")
        left_u, right_u = st.columns([2, 1])
        with left_u:
            _render_return_chart(
                months=months_labels,
                returns=agg_usd,
                currency_label="USD",
                color="#8EC9A8",
            )
        with right_u:
            _render_12m_compact(data, months_labels, "USD", decimals=2)

    # ── Detalle últimos 12 meses (tabla expandida) ────────────────────────────
    st.markdown("---")
    st.subheader("Detalle últimos 12 meses")
    tab_12m_clp, tab_12m_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_12m_clp:
        _render_12m_table(
            months=months_labels,
            monthly_rows=data.get("monthly_detail", {}).get("rows", []),
            currency="CLP",
            decimals=0,
        )
    with tab_12m_usd:
        _render_12m_table(
            months=months_labels,
            monthly_rows=data.get("monthly_detail", {}).get("rows", []),
            currency="USD",
            decimals=2,
        )

    # ── Detalle por Activo ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Detalle por Activo")
    by_asset = data.get("by_asset", {})
    tab_asset_clp, tab_asset_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_asset_clp:
        col_t, col_d = st.columns([1, 1])
        with col_t:
            _render_asset_detail_table(rows=by_asset.get("clp", []), currency_label="CLP", decimals=0)
        with col_d:
            _render_donut(by_asset.get("clp", []))
    with tab_asset_usd:
        col_t, col_d = st.columns([1, 1])
        with col_t:
            _render_asset_detail_table(rows=by_asset.get("usd", []), currency_label="USD", decimals=2)
        with col_d:
            _render_donut(by_asset.get("usd", []))

    # ── Detalle por Banco ─────────────────────────────────────────────────────
    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown("#### Detalle por Banco")
    by_bank = data.get("by_bank", {})
    tab_bank_clp, tab_bank_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_bank_clp:
        col_t, col_d = st.columns([1, 1])
        with col_t:
            _render_currency_detail_table(
                rows=[{**r, "name": r.get("name"), "aportes": None, "retiros": None, "profit": None}
                      for r in by_bank.get("clp", [])],
                label_col="Banco",
                currency_label="CLP",
                decimals=0,
            )
        with col_d:
            _render_donut(by_bank.get("clp", []))
    with tab_bank_usd:
        col_t, col_d = st.columns([1, 1])
        with col_t:
            _render_currency_detail_table(
                rows=[{**r, "name": r.get("name"), "aportes": None, "retiros": None, "profit": None}
                      for r in by_bank.get("usd", [])],
                label_col="Banco",
                currency_label="USD",
                decimals=2,
            )
        with col_d:
            _render_donut(by_bank.get("usd", []))

    # ── Detalle por Sociedad ──────────────────────────────────────────────────
    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown("#### Detalle por Sociedad")
    by_soc = data.get("by_sociedad", {})
    tab_soc_clp, tab_soc_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_soc_clp:
        col_t, col_d = st.columns([2, 1])
        with col_t:
            _render_currency_detail_table(
                rows=by_soc.get("clp", []),
                label_col="Sociedad",
                currency_label="CLP",
                decimals=0,
            )
        with col_d:
            _render_donut(by_soc.get("clp", []), value_key="ending")
    with tab_soc_usd:
        col_t, col_d = st.columns([2, 1])
        with col_t:
            _render_currency_detail_table(
                rows=by_soc.get("usd", []),
                label_col="Sociedad",
                currency_label="USD",
                decimals=2,
            )
        with col_d:
            _render_donut(by_soc.get("usd", []), value_key="ending")

    # ── Detalle por Cuenta ────────────────────────────────────────────────────
    st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)
    st.markdown("#### Detalle por Cuenta")
    by_acct = data.get("by_account", {})
    tab_acct_clp, tab_acct_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_acct_clp:
        _render_account_detail_table(rows=by_acct.get("clp", []), currency_label="CLP", decimals=0)
    with tab_acct_usd:
        _render_account_detail_table(rows=by_acct.get("usd", []), currency_label="USD", decimals=2)


def _render_12m_compact(data: dict, months_labels: list[str], currency: str, decimals: int) -> None:
    """
    Tabla compacta de rentabilidad: una fila por mes (totales consolidados).
    Columnas: Fecha | Saldo | Aportes | Retiros | Utilidad
    """
    series_key = "clp" if currency == "CLP" else "usd"
    monthly_rows = data.get("monthly_detail", {}).get("rows", [])
    if not monthly_rows or not months_labels:
        st.info("Sin datos.")
        return

    rows_out = []
    for idx, label in enumerate(months_labels):
        ending_total = 0.0
        aportes_total = 0.0
        retiros_total = 0.0
        profit_total = None
        has_profit = False
        for row in monthly_rows:
            series = row.get(series_key) or []
            entry = series[idx] if idx < len(series) else None
            if entry:
                ending_total += _to_float(entry.get("ending")) or 0.0
                aportes_total += _to_float(entry.get("aportes")) or 0.0
                retiros_total += _to_float(entry.get("retiros")) or 0.0
                p = _to_float(entry.get("profit"))
                if p is not None:
                    profit_total = (profit_total or 0.0) + p
                    has_profit = True
        rows_out.append({
            "Fecha": label,
            "Saldo": _fmt_or_blank(ending_total if ending_total else None, decimals=decimals),
            "Aportes": _fmt_or_blank(aportes_total if aportes_total else None, decimals=decimals),
            "Retiros": _fmt_or_blank(retiros_total if retiros_total else None, decimals=decimals),
            "Utilidad": _fmt_or_blank(profit_total if has_profit else None, decimals=decimals),
        })

    # Mostrar solo filas con datos
    rows_with_data = [r for r in rows_out if r["Saldo"] not in ("", "—")]
    if not rows_with_data:
        st.info("Sin datos.")
        return

    df = pd.DataFrame(rows_with_data, columns=["Fecha", "Saldo", "Aportes", "Retiros", "Utilidad"])
    render_table(df, label_col="Fecha")
