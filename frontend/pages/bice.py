"""
Pagina Detalle Bice — Inversiones nacionales (BICE, Banchile).

Estructura post-refactor (5 cambios):
  1. Saldo Consolidado: 4 KPIs + breakdown por sociedad/banco
  2. Gráficos: 2 columnas (Evolución saldo + Rentabilidad), tabs CLP/USD
  3. Detalle Transacciones: tabla editable con categorías + persistencia
  4. Sección inferior: 12m table | Detalle por Activo (estructura fija)
  5. Tabs CLP/USD en todas las secciones

Reglas:
- Sin lógica de negocio en esta capa.
- TODA comunicación con el backend vía api_client.
- No tocar ninguna otra pestaña.
"""

from __future__ import annotations

import hashlib
import html as _html

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

_BANK_DISPLAY = {
    "bice": "BICE",
    "bice_inversiones": "Bice Inversiones",
    "bice_asesorias": "Bice Asesorias",
    "banchile": "Banchile",
}
_ASSET_COLORS = {
    "Caja": "#D5DEE9",
    "Renta Fija": "#2D6FB7",
    "Equities": "#B53639",
}
_FALLBACK_COLORS = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2",
    "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD",
]

# Estructura fija del árbol de activos
_ASSET_TREE = [
    ("CAJA",          "Caja",          ["Money Market"]),
    ("RENTA FIJA",    "Renta Fija",    ["Depósitos a Plazo", "Bonos", "Fondos Mutuos RF"]),
    ("RENTA VARIABLE","Renta Variable",["Acciones", "Fondos de Inversión"]),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _fmt_or_blank(value, *, decimals: int = 0) -> str:
    if value is None:
        return "—"
    return fmt_number(value, decimals=decimals)


def _fmt_pct_or_blank(value, *, decimals: int = 2) -> str:
    if value is None:
        return "—"
    return fmt_percent(value, decimals=decimals)


def _fmt_bank(code: str) -> str:
    return _BANK_DISPLAY.get(code, code.replace("_", " ").title())


def _fecha_label(fecha_str: str) -> str:
    parts = str(fecha_str).split("-")
    if len(parts) != 2:
        return str(fecha_str)
    month = int(parts[1])
    return f"{MONTHS[month - 1]} {parts[0][-2:]}"


def _build_fecha_options(fechas: list[str]) -> list[str]:
    return sorted(set(fechas), reverse=True)


def _sanitize_multiselect_state(key: str, valid_options: list[str]) -> list[str]:
    selected = [v for v in st.session_state.get(key, []) if v in valid_options]
    st.session_state[key] = selected
    return selected


def _compute_accumulated(monthly_returns: list) -> list:
    accumulated = []
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


def _tx_key(tx: dict) -> str:
    """Clave estable para identificar una transacción auto-detectada."""
    op = tx.get("operacion") or tx.get("tipo_operacion") or ""
    monto_raw = tx.get("monto_raw") or str(tx.get("monto") or "")
    raw = f"{tx.get('account_id','')}_{tx.get('fecha','')}_{op}_{tx.get('instrumento','')}_{monto_raw}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _compute_effective_movements(
    transactions: list[dict],
    overrides: dict[str, str],
    manual_rows: list[dict] | None = None,
) -> tuple[float, float, float, float]:
    """
    Calcula aportes y retiros efectivos aplicando los overrides de categoría.
    Incluye filas manuales no eliminadas. Retorna (aportes_clp, retiros_clp, aportes_usd, retiros_usd).
    """
    aportes_clp = retiros_clp = aportes_usd = retiros_usd = 0.0
    for tx in transactions:
        key = _tx_key(tx)
        cat = overrides.get(key, tx.get("categoria_auto", "Cambio de instrumento"))
        monto = _to_float(tx.get("monto")) or 0.0
        moneda = (tx.get("moneda") or "CLP").upper()
        if cat == "Aporte" and monto > 0:
            if moneda == "USD":
                aportes_usd += monto
            else:
                aportes_clp += monto
        elif cat == "Retiro" and monto < 0:
            if moneda == "USD":
                retiros_usd += abs(monto)
            else:
                retiros_clp += abs(monto)
    for mr in (manual_rows or []):
        if mr.get("__deleted"):
            continue
        cat = mr.get("categoria", "Cambio de instrumento")
        monto = _to_float(mr.get("monto")) or 0.0
        moneda = (mr.get("moneda") or "CLP").upper()
        if cat == "Aporte" and monto > 0:
            if moneda == "USD":
                aportes_usd += monto
            else:
                aportes_clp += monto
        elif cat == "Retiro" and monto < 0:
            if moneda == "USD":
                retiros_usd += abs(monto)
            else:
                retiros_clp += abs(monto)
    return aportes_clp, retiros_clp, aportes_usd, retiros_usd


def _fmt_mov(value: float, decimals: int = 0, is_usd: bool = False) -> str:
    """Formatea movimiento neto con signo y color via HTML."""
    if value == 0:
        return "—"
    prefix = "US$" if is_usd else "$"
    formatted = fmt_number(abs(value), decimals=decimals)
    sign = "+" if value > 0 else "−"
    color = "#16A34A" if value > 0 else "#DC2626"
    return f'<span style="color:{color}">{sign}{prefix}{formatted}</span>'


# ── CAMBIO 1: Saldo Consolidado ───────────────────────────────────────────────

def _kpi_html_row(
    label1: str, val1: str,
    label2: str, val2: str,
    label3: str, val3: str,
    label4: str, val4: str,
    row_name: str = "",
    indent: bool = False,
) -> str:
    """Genera una fila HTML de 4 KPIs (con nombre opcional a la izquierda)."""
    indent_css = "padding-left:1.5rem; font-size:0.8rem; color:#374151;" if indent else ""
    name_cell = (
        f'<div style="width:160px;align-self:center;{indent_css}">{row_name}</div>'
        if row_name else ""
    )
    def _cell(lbl: str, v: str) -> str:
        return (
            f'<div style="flex:1;min-width:0">'
            f'  <div style="font-size:0.72rem;color:#6B7280;white-space:nowrap">{lbl}</div>'
            f'  <div style="font-size:1.35rem;font-weight:600;white-space:nowrap">{v}</div>'
            f'</div>'
        )
    return (
        f'<div style="display:flex;gap:0.5rem;align-items:flex-start;'
        f'margin:0.4rem 0;padding:0.25rem 0">'
        f'{name_cell}'
        f'{_cell(label1, val1)}'
        f'{_cell(label2, val2)}'
        f'{_cell(label3, val3)}'
        f'{_cell(label4, val4)}'
        f'</div>'
    )


def _render_saldo_consolidado(
    kpi_clp: dict,
    kpi_usd: dict,
    by_sociedad_merged: list[dict],
    by_bank_merged: list[dict],
    *,
    show_sociedad_breakdown: bool,
    show_bank_breakdown: bool,
) -> None:
    st.markdown("---")
    st.subheader("Saldo Consolidado")

    # Fila consolidada
    aportes_clp = _to_float(kpi_clp.get("aportes")) or 0.0
    retiros_clp = _to_float(kpi_clp.get("retiros")) or 0.0
    aportes_usd = _to_float(kpi_usd.get("aportes")) or 0.0
    retiros_usd = _to_float(kpi_usd.get("retiros")) or 0.0
    mov_clp = aportes_clp - retiros_clp
    mov_usd = aportes_usd - retiros_usd

    ending_clp = _to_float(kpi_clp.get("ending")) or 0.0
    ending_usd = _to_float(kpi_usd.get("ending")) or 0.0

    html = _kpi_html_row(
        "Saldo $ (CLP)", f"${fmt_number(ending_clp, decimals=0)}",
        "Saldo US$", f"US${fmt_number(ending_usd, decimals=2)}",
        "Movimientos mes $ (CLP)", _fmt_mov(mov_clp, decimals=0),
        "Movimientos mes US$", _fmt_mov(mov_usd, decimals=2, is_usd=True),
    )
    st.markdown(html, unsafe_allow_html=True)

    # Breakdown por sociedad
    if show_sociedad_breakdown and len(by_sociedad_merged) > 1:
        st.markdown("---")
        for row in by_sociedad_merged:
            e_clp = _to_float(row.get("ending_clp")) or 0.0
            e_usd = _to_float(row.get("ending_usd")) or 0.0
            a_clp = _to_float(row.get("aportes_clp")) or 0.0
            r_clp = _to_float(row.get("retiros_clp")) or 0.0
            a_usd = _to_float(row.get("aportes_usd")) or 0.0
            r_usd = _to_float(row.get("retiros_usd")) or 0.0
            m_clp = a_clp - r_clp
            m_usd = a_usd - r_usd
            html_row = _kpi_html_row(
                "Saldo $", f"${fmt_number(e_clp, decimals=0)}",
                "Saldo US$", f"US${fmt_number(e_usd, decimals=2)}",
                "Mov. mes $", _fmt_mov(m_clp, decimals=0),
                "Mov. mes US$", _fmt_mov(m_usd, decimals=2, is_usd=True),
                row_name=str(row.get("name", "")),
                indent=True,
            )
            st.markdown(html_row, unsafe_allow_html=True)

    # Breakdown por banco
    if show_bank_breakdown and len(by_bank_merged) > 1:
        st.markdown("---")
        for row in by_bank_merged:
            e_clp = _to_float(row.get("ending_clp")) or 0.0
            e_usd = _to_float(row.get("ending_usd")) or 0.0
            a_clp = _to_float(row.get("aportes_clp")) or 0.0
            r_clp = _to_float(row.get("retiros_clp")) or 0.0
            a_usd = _to_float(row.get("aportes_usd")) or 0.0
            r_usd = _to_float(row.get("retiros_usd")) or 0.0
            m_clp = a_clp - r_clp
            m_usd = a_usd - r_usd
            html_row = _kpi_html_row(
                "Saldo $", f"${fmt_number(e_clp, decimals=0)}",
                "Saldo US$", f"US${fmt_number(e_usd, decimals=2)}",
                "Mov. mes $", _fmt_mov(m_clp, decimals=0),
                "Mov. mes US$", _fmt_mov(m_usd, decimals=2, is_usd=True),
                row_name=str(row.get("name", "")),
                indent=True,
            )
            st.markdown(html_row, unsafe_allow_html=True)


# ── CAMBIO 2: Gráficos ────────────────────────────────────────────────────────

def _render_return_chart(
    *,
    months: list[str],
    returns: list,
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
        height=320,
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


def _render_saldo_chart(
    *,
    months: list[str],
    values: list,
    currency_label: str,
    color: str = "#AFC8E2",
) -> None:
    if not any(v is not None for v in values):
        st.info(f"Sin datos de saldo ({currency_label}).")
        return
    y_vals = [v if v is not None else 0 for v in values]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=months, y=y_vals,
        marker_color=color, opacity=0.9,
        name="Saldo",
    ))
    fig.update_layout(
        height=320,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
    )
    fig.update_yaxes(tickformat=",", showgrid=True, gridcolor="#D6DCE5",
                     zeroline=True, zerolinecolor="#9EA7B3")
    st.plotly_chart(fig, use_container_width=True)


def _aggregate_returns(returns_rows: list[dict], months_labels: list[str], currency_key: str) -> list:
    if not returns_rows or not months_labels:
        return []
    n = len(months_labels)
    result = []
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


def _aggregate_saldo(monthly_detail_rows: list[dict], months_labels: list[str], series_key: str) -> list:
    n = len(months_labels)
    totals = [0.0] * n
    has_data = [False] * n
    for row in monthly_detail_rows:
        series = row.get(series_key) or []
        for idx, entry in enumerate(series):
            if entry:
                val = _to_float(entry.get("ending"))
                if val and val > 0:
                    totals[idx] += val
                    has_data[idx] = True
    return [totals[i] if has_data[i] else None for i in range(n)]


def _render_charts_section(
    months_labels: list[str],
    returns_rows: list[dict],
    monthly_detail_rows: list[dict],
) -> None:
    st.markdown("---")
    st.subheader("Evolución y Rentabilidad últimos 12 meses")

    tab_clp, tab_usd = st.tabs(["$ (CLP)", "US$"])

    with tab_clp:
        col_saldo, col_rent = st.columns(2)
        with col_saldo:
            st.markdown("**Evolución de saldo (CLP)**")
            _render_saldo_chart(
                months=months_labels,
                values=_aggregate_saldo(monthly_detail_rows, months_labels, "clp"),
                currency_label="CLP",
                color="#AFC8E2",
            )
        with col_rent:
            st.markdown("**Rentabilidad (CLP)**")
            _render_return_chart(
                months=months_labels,
                returns=_aggregate_returns(returns_rows, months_labels, "clp"),
                currency_label="CLP",
                color="#AFC8E2",
            )

    with tab_usd:
        col_saldo, col_rent = st.columns(2)
        with col_saldo:
            st.markdown("**Evolución de saldo (US$)**")
            _render_saldo_chart(
                months=months_labels,
                values=_aggregate_saldo(monthly_detail_rows, months_labels, "usd"),
                currency_label="USD",
                color="#8EC9A8",
            )
        with col_rent:
            st.markdown("**Rentabilidad (US$)**")
            _render_return_chart(
                months=months_labels,
                returns=_aggregate_returns(returns_rows, months_labels, "usd"),
                currency_label="USD",
                color="#8EC9A8",
            )


# ── CAMBIO 3: Detalle Transacciones ──────────────────────────────────────────

def _render_transactions_section(
    transactions: list[dict],
    tx_state_key: str,
    applied_fecha: str,
    applied_bancos: list[str],
    transaction_overrides_api: dict[str, str],
    manual_rows_api: list[dict],
    available_bancos: list[str] | None = None,
) -> None:
    """
    Sección Detalle Transacciones con tabs CLP / US$.
    - Filas auto-detectadas: solo editable la Categoría; sin botón eliminar.
    - Filas manuales: todos los campos editables + checkbox 🗑️ para eliminar.
    - DAP y eventos inferidos: marcados con ⚠️ en la columna Descripción.
    - Cambios recalculan KPIs en tiempo real; se persisten con "Guardar".
    """
    st.markdown("---")
    st.markdown("#### Detalle Transacciones")

    manual_key = f"{tx_state_key}_manual"

    # ── Inicializar estados de session_state ──────────────────────────────────
    if tx_state_key not in st.session_state:
        st.session_state[tx_state_key] = {
            _tx_key(t): transaction_overrides_api.get(
                _tx_key(t), t.get("categoria_auto", "Cambio de instrumento")
            )
            for t in transactions
        }
    if manual_key not in st.session_state:
        st.session_state[manual_key] = [dict(mr) for mr in manual_rows_api]

    current_overrides: dict[str, str] = st.session_state[tx_state_key]
    current_manual: list[dict] = st.session_state[manual_key]

    tab_clp, tab_usd = st.tabs(["$ (CLP)", "US$"])
    for tab_currency, moneda_filter in [(tab_clp, "CLP"), (tab_usd, "USD")]:
        with tab_currency:
            _render_tx_tab(
                transactions=transactions,
                current_overrides=current_overrides,
                current_manual=current_manual,
                moneda_filter=moneda_filter,
                tx_state_key=tx_state_key,
                manual_key=manual_key,
                applied_fecha=applied_fecha,
            )


def _render_tx_tab(
    transactions: list[dict],
    current_overrides: dict[str, str],
    current_manual: list[dict],
    moneda_filter: str,
    tx_state_key: str,
    manual_key: str,
    applied_fecha: str,
) -> None:
    """Contenido de un tab CLP o USD dentro de Detalle Transacciones."""
    decimals = 0 if moneda_filter == "CLP" else 2
    cat_options = ["Aporte", "Retiro", "Cambio de instrumento"]

    # ── Filas auto-detectadas para esta moneda ───────────────────────────────
    auto_rows = [t for t in transactions if (t.get("moneda") or "CLP").upper() == moneda_filter]

    if auto_rows:
        # Sincronizar selectboxes → overrides antes de renderizar
        for t in auto_rows:
            k = _tx_key(t)
            sbox_key = f"cat_{tx_state_key}_{k}_{moneda_filter}"
            if sbox_key in st.session_state:
                st.session_state[tx_state_key][k] = st.session_state[sbox_key]

        # Construir datos de filas con categoría actualizada
        rows_data = []
        for t in auto_rows:
            k = _tx_key(t)
            cat = st.session_state[tx_state_key].get(k, t.get("categoria_auto", "Cambio de instrumento"))
            monto = _to_float(t.get("monto")) or 0.0
            desc = str(t.get("instrumento", ""))
            if t.get("es_warning"):
                desc = f"⚠️ {desc}"
            rows_data.append({
                "_key": k,
                "_account_id": t.get("account_id"),
                "Fecha": str(t.get("fecha", "")),
                "Descripción": desc,
                "monto_raw": monto,
                "Monto_fmt": fmt_number(monto, decimals=decimals),
                "Categoría": cat,
            })

        # HTML table — mismo estilo que otras tablas de la pestaña
        TH = ("background-color:#7A838F;color:white;font-weight:700;"
              "padding:7px 12px;white-space:nowrap;font-size:14px;text-align:{a}")
        TD = ("padding:6px 12px;background-color:white;font-size:14px;"
              "white-space:nowrap;border-bottom:1px solid #eee;text-align:{a}")
        tbody = "".join(
            f"<tr>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(r['Fecha'])}</td>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(r['Descripción'])}</td>"
            f"<td style='{TD.format(a='right')}'>{r['Monto_fmt']}</td>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(r['Categoría'])}</td>"
            f"</tr>"
            for r in rows_data
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;font-family:inherit'>"
            f"<thead><tr>"
            f"<th style='{TH.format(a='left')}'>Fecha</th>"
            f"<th style='{TH.format(a='left')}'>Descripción</th>"
            f"<th style='{TH.format(a='right')}'>Monto</th>"
            f"<th style='{TH.format(a='left')}'>Categoría</th>"
            f"</tr></thead><tbody>{tbody}</tbody></table>",
            unsafe_allow_html=True,
        )

        # Selectboxes para editar Categoría (una fila por movimiento)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        for r in rows_data:
            c1, c2 = st.columns([4, 1])
            c1.caption(f"{r['Fecha']} — {r['Descripción']}  ({r['Monto_fmt']})")
            idx = cat_options.index(r["Categoría"]) if r["Categoría"] in cat_options else 2
            c2.selectbox(
                "cat",
                options=cat_options,
                index=idx,
                key=f"cat_{tx_state_key}_{r['_key']}_{moneda_filter}",
                label_visibility="collapsed",
            )

        # Validaciones de signo
        for r in rows_data:
            m, cat = r["monto_raw"], r["Categoría"]
            if cat == "Aporte" and m < 0:
                st.warning(f"{r['Fecha']} {r['Descripción']}: «Aporte» con monto negativo.")
            elif cat == "Retiro" and m > 0:
                st.warning(f"{r['Fecha']} {r['Descripción']}: «Retiro» con monto positivo.")
    else:
        st.info(f"Sin movimientos detectados en {moneda_filter} para el período.")

    # ── Filas manuales para esta moneda ──────────────────────────────────────
    manual_moneda = [mr for mr in current_manual
                     if (mr.get("moneda") or "CLP").upper() == moneda_filter
                     and not mr.get("__deleted")]

    if manual_moneda:
        st.markdown("**Movimientos manuales**")
        # Render como HTML table con misma cabecera
        TH = ("background-color:#7A838F;color:white;font-weight:700;"
              "padding:7px 12px;white-space:nowrap;font-size:14px;text-align:{a}")
        TD = ("padding:6px 12px;background-color:white;font-size:14px;"
              "white-space:nowrap;border-bottom:1px solid #eee;text-align:{a}")
        tbody_m = "".join(
            f"<tr>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(str(mr.get('fecha', '')))}</td>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(str(mr.get('instrumento', '')))}</td>"
            f"<td style='{TD.format(a='right')}'>{fmt_number(_to_float(mr.get('monto')) or 0.0, decimals=decimals)}</td>"
            f"<td style='{TD.format(a='left')}'>{_html.escape(str(mr.get('categoria', '')))}</td>"
            f"<td style='{TD.format(a='center')}'><span data-mid='{mr.get('__manual_id','')}'>🗑️</span></td>"
            f"</tr>"
            for mr in manual_moneda
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;font-family:inherit'>"
            f"<thead><tr>"
            f"<th style='{TH.format(a='left')}'>Fecha</th>"
            f"<th style='{TH.format(a='left')}'>Descripción</th>"
            f"<th style='{TH.format(a='right')}'>Monto</th>"
            f"<th style='{TH.format(a='left')}'>Categoría</th>"
            f"<th style='{TH.format(a='center')}'></th>"
            f"</tr></thead><tbody>{tbody_m}</tbody></table>",
            unsafe_allow_html=True,
        )
        # Edición de filas manuales con selectboxes + campos
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        for mr in manual_moneda:
            mid = mr.get("__manual_id", "")
            ec1, ec2, ec3, ec4, ec5 = st.columns([1.2, 2, 1.2, 1.2, 0.4])
            new_fecha = ec1.text_input("Fecha", value=str(mr.get("fecha", "")),
                                       key=f"mfecha_{mid}", label_visibility="collapsed")
            new_desc = ec2.text_input("Desc", value=str(mr.get("instrumento", "")),
                                      key=f"mdesc_{mid}", label_visibility="collapsed")
            new_monto = ec3.number_input("Monto", value=float(mr.get("monto") or 0.0),
                                         key=f"mmonto_{mid}", label_visibility="collapsed")
            cat_idx = cat_options.index(mr.get("categoria", "Cambio de instrumento")) if mr.get("categoria") in cat_options else 2
            new_cat = ec4.selectbox("Cat", options=cat_options, index=cat_idx,
                                    key=f"mcat_{mid}", label_visibility="collapsed")
            if ec5.button("🗑️", key=f"mdel_{mid}"):
                for m2 in st.session_state[manual_key]:
                    if m2.get("__manual_id") == mid:
                        m2["__deleted"] = True
                        break
                st.rerun()
            # Sincronizar cambios al estado
            for m2 in st.session_state[manual_key]:
                if m2.get("__manual_id") == mid:
                    m2["fecha"] = new_fecha
                    m2["instrumento"] = new_desc
                    m2["monto"] = float(new_monto)
                    m2["categoria"] = new_cat
                    break

    # ── Botón agregar movimiento ──────────────────────────────────────────────
    if st.button(f"＋ Agregar movimiento ({moneda_filter})",
                 key=f"add_tx_{tx_state_key}_{moneda_filter}"):
        import uuid as _uuid
        st.session_state[manual_key].append({
            "__manual_id": _uuid.uuid4().hex[:10],
            "fecha": "",
            "instrumento": "",
            "monto": 0.0,
            "moneda": moneda_filter,
            "categoria": "Cambio de instrumento",
            "__deleted": False,
        })
        st.rerun()

    # ── Guardar ───────────────────────────────────────────────────────────────
    year_m = int(applied_fecha[:4]) if applied_fecha else 0
    month_m = int(applied_fecha[5:7]) if applied_fecha and len(applied_fecha) >= 7 else 0

    if st.button("💾 Guardar", key=f"save_{tx_state_key}_{moneda_filter}"):
        # Agrupar overrides por account_id
        by_acct: dict[int, dict[str, list]] = {}
        for t in transactions:
            acct_id = t.get("account_id")
            if acct_id is None:
                continue
            key = _tx_key(t)
            if acct_id not in by_acct:
                by_acct[acct_id] = {"overrides": {}, "manual_rows": []}
            by_acct[acct_id]["overrides"][key] = current_overrides.get(key, "Cambio de instrumento")
        # Añadir filas manuales no eliminadas al primer account_id disponible
        non_deleted = [mr for mr in st.session_state[manual_key] if not mr.get("__deleted")]
        if non_deleted and by_acct:
            first_acct = next(iter(by_acct))
            by_acct[first_acct]["manual_rows"] = non_deleted
        elif non_deleted and transactions:
            acct_id = transactions[0].get("account_id")
            if acct_id:
                by_acct[acct_id] = {"overrides": {}, "manual_rows": non_deleted}

        all_ok = True
        for acct_id, payload in by_acct.items():
            try:
                resp = api_client.post("/data/bice/overrides", json={
                    "account_id": acct_id, "year": year_m, "month": month_m,
                    "overrides": payload["overrides"],
                    "manual_rows": payload.get("manual_rows", []),
                })
                if not resp.get("ok"):
                    all_ok = False
            except Exception:
                all_ok = False
        if all_ok:
            st.success("Guardado.")
        else:
            st.error("Error al guardar.")


# ── CAMBIO 4: Tabla 12m compacta ─────────────────────────────────────────────

def _render_12m_compact(data: dict, months_labels: list[str], currency: str, decimals: int) -> None:
    """Tabla compacta de últimos 12 meses (Fecha | Saldo | Aportes | Retiros | Utilidad)."""
    series_key = "clp" if currency == "CLP" else "usd"
    monthly_rows = data.get("monthly_detail", {}).get("rows", [])
    if not monthly_rows or not months_labels:
        st.info("Sin datos.")
        return

    rows_out = []
    for idx, label in enumerate(months_labels):
        ending_total = aportes_total = retiros_total = 0.0
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

    rows_with_data = [r for r in rows_out if r["Saldo"] not in ("", "—")]
    if not rows_with_data:
        st.info("Sin datos.")
        return

    df = pd.DataFrame(rows_with_data, columns=["Fecha", "Saldo", "Aportes", "Retiros", "Utilidad"])
    render_table(df, label_col="Fecha")


# ── CAMBIO 4: Tabla de activo con estructura fija ─────────────────────────────

def _render_fixed_asset_table(currency: str, by_asset_detail: dict, total_fallback: float = 0.0) -> None:
    """
    Tabla Detalle por Activo con estructura fija predefinida.
    Muestra instrumentos individuales cuando están disponibles en el parsed_data_json.
    Todas las filas tienen el mismo alto (padding uniforme).
    """
    series_key = "clp" if currency == "CLP" else "usd"
    detail = by_asset_detail.get(series_key, {})
    decimals = 0 if currency == "CLP" else 2

    # Total global para %
    grand_total = sum(
        (_to_float(detail.get(parent_key, {}).get("total")) or 0.0)
        for _, parent_key, _ in _ASSET_TREE
    ) or total_fallback or 0.0

    rows = []
    for display_label, parent_key, subcats in _ASSET_TREE:
        parent_data = detail.get(parent_key, {})
        parent_total = _to_float(parent_data.get("total")) or 0.0
        pct = (parent_total / grand_total * 100) if grand_total and parent_total else None
        rows.append({
            "Categoría": display_label,
            "Saldo": _fmt_or_blank(parent_total if parent_total else None, decimals=decimals),
            "%": _fmt_pct_or_blank(pct, decimals=1),
            "_bold": True,
        })
        subcats_data = parent_data.get("subcategories", {})
        insts_data = parent_data.get("instruments", {})
        for sub in subcats:
            sub_val = _to_float(subcats_data.get(sub))
            sub_pct = (sub_val / grand_total * 100) if (grand_total and sub_val) else None
            rows.append({
                "Categoría": f"    {sub}",
                "Saldo": _fmt_or_blank(sub_val, decimals=decimals),
                "%": _fmt_pct_or_blank(sub_pct, decimals=1),
                "_bold": False,
            })
            for inst in insts_data.get(sub, []):
                inst_amount = _to_float(inst.get("amount"))
                inst_pct = (inst_amount / grand_total * 100) if (grand_total and inst_amount) else None
                rows.append({
                    "Categoría": f"        {inst.get('name', '')}",
                    "Saldo": _fmt_or_blank(inst_amount, decimals=decimals),
                    "%": _fmt_pct_or_blank(inst_pct, decimals=1),
                    "_bold": False,
                })

    total_pct = 100.0 if grand_total else None
    rows.append({
        "Categoría": "Total",
        "Saldo": _fmt_or_blank(grand_total if grand_total else None, decimals=decimals),
        "%": _fmt_pct_or_blank(total_pct, decimals=1),
        "_bold": True,
    })

    # ── Render directo en HTML para control total de estilos ──────────────
    # st.table usa CSS de Streamlit que sobreescribe fondos incluso con inline styles.
    # Con st.markdown + HTML crudo los estilos inline son soberanos.

    _TD_BASE = (
        "background-color:white;padding:3px 8px;line-height:1.3;"
        "white-space:nowrap;font-size:14px;border:none;"
    )
    _TD_LEFT  = _TD_BASE + "text-align:left;"
    _TD_RIGHT = _TD_BASE + "text-align:right;font-variant-numeric:tabular-nums;"
    _TD_BOLD_LEFT  = _TD_LEFT  + "font-weight:700;"
    _TD_BOLD_RIGHT = _TD_RIGHT + "font-weight:700;"
    _TD_TOTAL_LEFT  = "background-color:#7A838F;color:#FFFFFF;font-weight:700;padding:3px 8px;line-height:1.3;white-space:nowrap;font-size:14px;border:none;text-align:left;"
    _TD_TOTAL_RIGHT = "background-color:#7A838F;color:#FFFFFF;font-weight:700;padding:3px 8px;line-height:1.3;white-space:nowrap;font-size:14px;border:none;text-align:right;font-variant-numeric:tabular-nums;"
    _TH_STYLE = (
        "background-color:#7A838F;color:#FFFFFF;font-weight:700;"
        "padding:4px 8px;line-height:1.3;white-space:nowrap;font-size:14px;border:none;"
    )

    def _tr(cat: str, saldo: str, pct: str, bold: bool) -> str:
        is_total = cat.strip().lower() == "total"
        if is_total:
            sl, sr = _TD_TOTAL_LEFT, _TD_TOTAL_RIGHT
        elif bold:
            sl, sr = _TD_BOLD_LEFT, _TD_BOLD_RIGHT
        else:
            sl, sr = _TD_LEFT, _TD_RIGHT
        c = _html.escape(cat)
        return (
            f'<tr>'
            f'<td style="{sl}">{c}</td>'
            f'<td style="{sr}">{_html.escape(saldo)}</td>'
            f'<td style="{sr}">{_html.escape(pct)}</td>'
            f'</tr>'
        )

    tbody = "".join(
        _tr(r["Categoría"], r["Saldo"], r["%"], r["_bold"])
        for r in rows
    )
    table_html = (
        f'<table style="width:100%;border-collapse:collapse;">'
        f'<thead><tr>'
        f'<th style="{_TH_STYLE}text-align:left;">Categoría</th>'
        f'<th style="{_TH_STYLE}text-align:right;">Saldo</th>'
        f'<th style="{_TH_STYLE}text-align:right;">%</th>'
        f'</tr></thead>'
        f'<tbody>{tbody}</tbody>'
        f'</table>'
    )
    st.markdown(table_html, unsafe_allow_html=True)


# ── Tabla detalle por cuenta (sin cambios) ────────────────────────────────────

def _render_account_detail_table(*, rows: list[dict], currency_label: str, decimals: int = 0) -> None:
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
        "Sociedad": "Total", "Cuenta": "", "Banco": "",
        "Saldo": _fmt_or_blank(total_ending, decimals=decimals),
        "%": _fmt_pct_or_blank(100.0 if total_ending else None, decimals=1),
        "Caja": "", "Renta Fija": "", "Equities": "",
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
    try:
        init_data = api_client.post("/data/bice", json={
            "years": [], "months": [], "bank_codes": [],
            "entity_names": [], "person_names": [],
        })
        filter_opts = init_data.get("filter_options", {})
    except Exception:
        filter_opts = {}

    available_bancos = filter_opts.get("bancos", [])
    available_sociedades = filter_opts.get("sociedades", [])
    available_personas = filter_opts.get("personas", [])
    fecha_options = _build_fecha_options(filter_opts.get("fechas", []))
    default_fecha = fecha_options[0] if fecha_options else None

    if "bice_fecha" not in st.session_state or st.session_state.get("bice_fecha") not in fecha_options:
        st.session_state["bice_fecha"] = default_fecha

    _sanitize_multiselect_state("bice_banco", available_bancos)
    _sanitize_multiselect_state("bice_sociedad", available_sociedades)
    _sanitize_multiselect_state("bice_persona", available_personas)

    st.markdown("### Filtros")
    f1, f2, f3, f4, f5 = st.columns(5)

    with f1:
        selected_bancos = st.multiselect(
            "Banco", options=available_bancos, format_func=_fmt_bank, key="bice_banco",
        )
    with f2:
        selected_sociedades = st.multiselect(
            "Sociedad", options=available_sociedades, key="bice_sociedad",
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
            "Personas", options=available_personas, key="bice_persona",
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
    kpi_clp = dict(kpis.get("clp", {}))
    kpi_usd = dict(kpis.get("usd", {}))
    months_labels = data.get("returns_panel", {}).get("months", [])
    returns_rows = data.get("returns_panel", {}).get("rows", [])
    monthly_detail_rows = data.get("monthly_detail", {}).get("rows", [])
    by_sociedad_merged = data.get("by_sociedad_merged", [])
    by_bank_merged = data.get("by_bank_merged", [])
    by_asset_detail = data.get("by_asset_detail", {})
    transactions = data.get("transactions", [])
    transaction_overrides_api = data.get("transaction_overrides", {})
    manual_rows_api = data.get("manual_rows", [])

    # ── CAMBIO 3: Recalcular movimientos efectivos desde transaction overrides ─
    # Clave de estado asociada al mes y a los filtros activos
    tx_state_key = f"bice_tx_{applied_fecha}_{'_'.join(sorted(applied_bancos))}_{'_'.join(sorted(applied_sociedades))}"

    manual_key = f"{tx_state_key}_manual"

    # Inicializar overrides en session_state si no existe aún (primer load del mes)
    if tx_state_key not in st.session_state and transactions:
        st.session_state[tx_state_key] = {
            _tx_key(t): transaction_overrides_api.get(
                _tx_key(t), t.get("categoria_auto", "Cambio de instrumento")
            )
            for t in transactions
        }
    if manual_key not in st.session_state:
        st.session_state[manual_key] = [dict(mr) for mr in manual_rows_api]

    current_overrides: dict[str, str] = st.session_state.get(tx_state_key, {})
    current_manual: list[dict] = st.session_state.get(manual_key, [])

    # Si hay transacciones o manuales, sobreescribir aportes/retiros en KPIs
    if transactions or current_manual:
        eff_a_clp, eff_r_clp, eff_a_usd, eff_r_usd = _compute_effective_movements(
            transactions, current_overrides, current_manual
        )
        kpi_clp["aportes"] = eff_a_clp
        kpi_clp["retiros"] = eff_r_clp
        kpi_usd["aportes"] = eff_a_usd
        kpi_usd["retiros"] = eff_r_usd

    # ── CAMBIO 1: Saldo Consolidado ───────────────────────────────────────────
    _render_saldo_consolidado(
        kpi_clp=kpi_clp,
        kpi_usd=kpi_usd,
        by_sociedad_merged=by_sociedad_merged,
        by_bank_merged=by_bank_merged,
        show_sociedad_breakdown=len(by_sociedad_merged) > 1,
        show_bank_breakdown=len(by_bank_merged) > 1,
    )

    # ── CAMBIO 2: Gráficos ────────────────────────────────────────────────────
    _render_charts_section(months_labels, returns_rows, monthly_detail_rows)

    # ── CAMBIO 3: Detalle Transacciones ───────────────────────────────────────
    _render_transactions_section(
        transactions=transactions,
        tx_state_key=tx_state_key,
        applied_fecha=applied_fecha,
        applied_bancos=applied_bancos,
        transaction_overrides_api=transaction_overrides_api,
        manual_rows_api=manual_rows_api,
        available_bancos=available_bancos,
    )

    # ── CAMBIO 4: Sección inferior — 12m + Detalle por Activo ─────────────────
    st.markdown("---")
    st.markdown("#### Historial y Detalle por Activo")
    tab_bottom_clp, tab_bottom_usd = st.tabs(["$ (CLP)", "US$"])

    with tab_bottom_clp:
        col_hist, col_asset = st.columns(2)
        with col_hist:
            st.markdown("**Últimos 12 meses**")
            _render_12m_compact(data, months_labels, "CLP", decimals=0)
        with col_asset:
            st.markdown("**Detalle por Activo**")
            _render_fixed_asset_table(
                "CLP", by_asset_detail,
                total_fallback=_to_float(kpi_clp.get("ending")) or 0.0,
            )

    with tab_bottom_usd:
        col_hist, col_asset = st.columns(2)
        with col_hist:
            st.markdown("**Últimos 12 meses**")
            _render_12m_compact(data, months_labels, "USD", decimals=2)
        with col_asset:
            st.markdown("**Detalle por Activo**")
            _render_fixed_asset_table(
                "USD", by_asset_detail,
                total_fallback=_to_float(kpi_usd.get("ending")) or 0.0,
            )

    # ── Detalle por Cuenta ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Detalle por Cuenta")
    by_acct = data.get("by_account", {})
    tab_acct_clp, tab_acct_usd = st.tabs(["$ (CLP)", "US$"])
    with tab_acct_clp:
        _render_account_detail_table(rows=by_acct.get("clp", []), currency_label="CLP", decimals=0)
    with tab_acct_usd:
        _render_account_detail_table(rows=by_acct.get("usd", []), currency_label="USD", decimals=2)
