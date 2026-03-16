"""
Pagina Detalle.

Estructura:
- Filtros: Sociedad, Nombre, Fecha
- Saldo consolidado (sin caja en metrica)
- Boton "Detalle por Banco" con tabla desplegable
- Graficos: Por Banco, Por Tipo de Cuenta, Evolucion mensual YTD
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from frontend import api_client
from frontend.components.data_health import render_health_warning
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.number_format import fmt_currency, fmt_number
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


def _fmt_bank(code: str) -> str:
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


def _fmt_account_type(account_type: str) -> str:
    key = (account_type or "").strip().lower()
    if key in ACCOUNT_TYPE_DISPLAY:
        return ACCOUNT_TYPE_DISPLAY[key]
    if key == "etf":
        return "ETF"
    return (account_type or "").replace("_", " ").title()


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_fecha_options(years: list[int]) -> list[str]:
    if not years:
        return []
    values: list[str] = []
    for year in sorted(set(int(y) for y in years), reverse=True):
        for month in range(12, 0, -1):
            values.append(f"{year}-{month:02d}")
    return values


def _build_ytd_series(chart_data: list[dict], selected_year: int) -> list[float | None]:
    by_month: dict[int, float | None] = {}
    for row in chart_data:
        fecha = str(row.get("fecha") or "")
        if len(fecha) < 7:
            continue
        year = int(fecha[:4])
        month = int(fecha[5:7])
        if year != selected_year:
            continue
        by_month[month] = _to_float(row.get("rent_pct"))

    out: list[float | None] = []
    compound = 1.0
    has_data = False
    for month in range(1, 13):
        ret = by_month.get(month)
        if ret is None:
            out.append(round((compound - 1) * 100, 4) if has_data else None)
            continue
        compound *= (1 + (ret / 100))
        has_data = True
        out.append(round((compound - 1) * 100, 4))
    return out


def _aggregate_detail_rows(
    rows: list[dict],
    *,
    key_field: str,
) -> dict[str, dict[str, float]]:
    aggregated: dict[str, dict[str, float]] = {}
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        net = _to_float(row.get("net_value")) or 0.0
        mov = _to_float(row.get("movimientos")) or 0.0
        cash = _to_float(row.get("caja")) or 0.0
        currency = str(row.get("moneda") or "").upper()
        if key not in aggregated:
            aggregated[key] = {
                "monto_usd": 0.0,
                "movimientos_mes": 0.0,
                "caja_disponible": 0.0,
            }
        if currency == "USD":
            aggregated[key]["monto_usd"] += net
        aggregated[key]["movimientos_mes"] += mov
        aggregated[key]["caja_disponible"] += cash
    return aggregated


def _fmt_or_blank(value) -> str:
    if value is None:
        return ""
    return fmt_number(value, decimals=1)


def _summarize_account_labels(labels: list[str], *, max_items: int = 3) -> str:
    if not labels:
        return ""
    if len(labels) <= max_items:
        return ", ".join(labels)
    return ", ".join(labels[:max_items]) + f", +{len(labels) - max_items} mas"


def _build_bank_account_labels(rows: list[dict]) -> dict[str, str]:
    labels_by_bank: dict[str, set[str]] = {}
    for row in rows:
        bank_code = str(row.get("banco") or "").strip()
        if not bank_code:
            continue
        account_id = str(row.get("id") or row.get("account_number") or "").strip()
        account_type = _fmt_account_type(str(row.get("tipo_cuenta") or ""))
        if account_id and account_type:
            label = f"{account_id} ({account_type})"
        elif account_id:
            label = account_id
        elif account_type:
            label = account_type
        else:
            continue
        labels_by_bank.setdefault(bank_code, set()).add(label)
    return {
        bank_code: _summarize_account_labels(sorted(labels))
        for bank_code, labels in labels_by_bank.items()
    }


def render():
    st.title("Detalle")
    st.markdown("---")

    try:
        opts = api_client.get("/accounts/filter-options")
    except Exception:
        opts = {
            "entity_names": [],
            "person_names": [],
            "years": [],
            "available_fechas": [],
        }

    bank_options_all = sorted(opts.get("bank_codes", []))
    entity_options = sorted(opts.get("entity_names", []))
    person_options = sorted(opts.get("person_names", []))
    account_type_options_all = sorted(opts.get("account_types", []))
    year_options = [int(y) for y in opts.get("years", []) if y is not None]
    fecha_options = sorted(opts.get("available_fechas", []), reverse=True)
    if not fecha_options:
        fecha_options = _build_fecha_options(year_options)

    st.markdown("### Filtros")
    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options_all,
            format_func=_fmt_bank,
            key="detalle_banco",
        )
    with f2:
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            key="detalle_sociedad",
        )
    with f3:
        selected_people = st.multiselect(
            "Nombre",
            options=person_options,
            key="detalle_nombre",
        )
    with f4:
        selected_consolidated = st.selectbox(
            "Consolidado",
            options=[""] + list(CONSOLIDATED_PRESETS.keys()),
            format_func=lambda x: x or "Sin consolidado",
            key="detalle_consolidated",
        )
    with f5:
        if fecha_options:
            if "detalle_fecha" not in st.session_state or st.session_state["detalle_fecha"] not in fecha_options:
                st.session_state["detalle_fecha"] = fecha_options[0]
            selected_fecha = st.selectbox(
                "Fecha",
                options=fecha_options,
                key="detalle_fecha",
            )
        else:
            selected_fecha = None
            st.selectbox("Fecha", options=["Sin datos"], disabled=True, key="detalle_fecha_empty")

    preset_entities = list(CONSOLIDATED_PRESETS.get(selected_consolidated, []))
    selected_entities_effective = sorted(set(selected_entities) | set(preset_entities))
    if selected_consolidated and preset_entities:
        st.caption(f"Consolidado activo: {', '.join(preset_entities)}")

    selected_year = int(selected_fecha[:4]) if selected_fecha else None
    selected_month = int(selected_fecha[5:7]) if selected_fecha else None
    effective_people = [] if preset_entities else selected_people
    has_scope_filter = bool(selected_banks or selected_entities_effective or effective_people)

    if has_scope_filter:
        payload = {
            "bank_codes": selected_banks,
            "entity_names": selected_entities_effective,
            "person_names": effective_people,
            "years": [selected_year] if selected_year else [],
            "months": [selected_month] if selected_month else [],
        }
        try:
            data = api_client.post("/data/personal", json=payload)
        except Exception as exc:
            st.error(f"Error obteniendo datos de detalle: {exc}")
            data = {
                "consolidated_usd": 0.0,
                "consolidated_clp": 0.0,
                "pie_charts": {"by_bank": [], "by_type": []},
                "by_bank_detail": [],
                "entities_table": [],
            }
    else:
        data = {
            "consolidated_usd": 0.0,
            "consolidated_clp": 0.0,
            "pie_charts": {"by_bank": [], "by_type": []},
            "by_bank_detail": [],
            "entities_table": [],
        }

    ytd_payload = {
        "bank_codes": selected_banks,
        "entity_names": selected_entities_effective,
        "person_names": effective_people,
        "years": [selected_year] if selected_year else [],
    }
    try:
        ytd_data = api_client.post("/data/summary", json=ytd_payload) if has_scope_filter else {"chart_data": []}
    except Exception:
        ytd_data = {"chart_data": []}
    ytd_values = _build_ytd_series(ytd_data.get("chart_data", []), selected_year) if selected_year else [None] * 12

    if has_scope_filter:
        render_health_warning(
            {
                "years": [selected_year] if selected_year else [],
                "months": [selected_month] if selected_month else [],
                "bank_codes": selected_banks,
                "entity_names": selected_entities_effective,
                "person_names": effective_people,
            },
            label="Detalle",
        )

    st.markdown("---")
    st.subheader("Saldo Consolidado")
    if not has_scope_filter:
        st.caption("Activa al menos un filtro de Banco, Sociedad, Nombre o Consolidado para poblar esta vista.")

    if "detalle_show_bank_detail" not in st.session_state:
        st.session_state["detalle_show_bank_detail"] = False
    if "detalle_show_type_detail" not in st.session_state:
        st.session_state["detalle_show_type_detail"] = False
    if "detalle_show_society_detail" not in st.session_state:
        st.session_state["detalle_show_society_detail"] = False

    m1, m2, m3, m4, m5 = st.columns([1, 1, 1.1, 1.4, 1.2])
    with m1:
        st.metric("Total USD", fmt_currency(data.get("consolidated_usd", 0), decimals=2))
    with m2:
        st.metric("Total CLP", fmt_currency(data.get("consolidated_clp", 0), decimals=0))
    with m3:
        if st.button("Detalle por Banco", key="detalle_toggle_bank_detail"):
            st.session_state["detalle_show_bank_detail"] = not st.session_state["detalle_show_bank_detail"]
    with m4:
        if st.button("Detalle por Tipo de Cuenta", key="detalle_toggle_type_detail"):
            st.session_state["detalle_show_type_detail"] = not st.session_state["detalle_show_type_detail"]
    with m5:
        if st.button("Detalle por Sociedad", key="detalle_toggle_society_detail"):
            st.session_state["detalle_show_society_detail"] = not st.session_state["detalle_show_society_detail"]

    if st.session_state["detalle_show_bank_detail"]:
        st.markdown("#### Detalle por Banco")
        st.caption("Los montos por banco agregan todas las cuentas visibles del filtro actual.")
        bank_rows = data.get("by_bank_detail", [])
        bank_account_labels = _build_bank_account_labels(data.get("entities_table", []))
        bank_map = {
            str(row.get("bank_code", "")): row
            for row in bank_rows
            if str(row.get("bank_code", "")).strip()
        }
        table_rows: list[dict] = []
        total_monto = 0.0
        total_mov = 0.0
        total_caja = 0.0
        for bank_code in bank_options_all:
            row = bank_map.get(bank_code)
            monto = _to_float(row.get("monto_usd")) if row else None
            mov = _to_float(row.get("movimientos_mes")) if row else None
            caja = _to_float(row.get("caja_disponible")) if row else None
            total_monto += monto or 0.0
            total_mov += mov or 0.0
            total_caja += caja or 0.0
            table_rows.append(
                {
                    "Banco": _fmt_bank(str(bank_code)),
                    "Monto USD": _fmt_or_blank(monto),
                    "Movimientos del mes": _fmt_or_blank(mov),
                    "Caja disponible": _fmt_or_blank(caja),
                    "Cuentas visibles": bank_account_labels.get(bank_code, ""),
                }
            )
        table_rows.append(
            {
                "Banco": "Total",
                "Monto USD": _fmt_or_blank(total_monto),
                "Movimientos del mes": _fmt_or_blank(total_mov),
                "Caja disponible": _fmt_or_blank(total_caja),
                "Cuentas visibles": "",
            }
        )
        render_table(
            pd.DataFrame(
                table_rows,
                columns=[
                    "Banco",
                    "Monto USD",
                    "Movimientos del mes",
                    "Caja disponible",
                    "Cuentas visibles",
                ],
            ),
            label_col="Banco",
            bold_row_labels={"Total"},
        )

    if st.session_state["detalle_show_type_detail"]:
        st.markdown("#### Detalle por Tipo de Cuenta")
        type_agg = _aggregate_detail_rows(
            data.get("entities_table", []),
            key_field="tipo_cuenta",
        )
        type_rows = []
        total_monto = 0.0
        total_mov = 0.0
        total_caja = 0.0
        for account_type in account_type_options_all:
            vals = type_agg.get(account_type)
            monto = _to_float(vals.get("monto_usd")) if vals else None
            mov = _to_float(vals.get("movimientos_mes")) if vals else None
            caja = _to_float(vals.get("caja_disponible")) if vals else None
            total_monto += monto or 0.0
            total_mov += mov or 0.0
            total_caja += caja or 0.0
            type_rows.append(
                {
                    "Tipo de Cuenta": _fmt_account_type(account_type),
                    "Monto USD": _fmt_or_blank(monto),
                    "Movimientos del mes": _fmt_or_blank(mov),
                    "Caja disponible": _fmt_or_blank(caja),
                }
            )
        type_rows.append(
            {
                "Tipo de Cuenta": "Total",
                "Monto USD": _fmt_or_blank(total_monto),
                "Movimientos del mes": _fmt_or_blank(total_mov),
                "Caja disponible": _fmt_or_blank(total_caja),
            }
        )
        render_table(
            pd.DataFrame(
                type_rows,
                columns=["Tipo de Cuenta", "Monto USD", "Movimientos del mes", "Caja disponible"],
            ),
            label_col="Tipo de Cuenta",
            bold_row_labels={"Total"},
        )

    if st.session_state["detalle_show_society_detail"]:
        st.markdown("#### Detalle por Sociedad")
        entities_rows = data.get("entities_table", []) if has_scope_filter else []
        society_agg = _aggregate_detail_rows(entities_rows, key_field="sociedad")
        visible_societies = sorted(
            {
                str(row.get("sociedad") or "").strip()
                for row in entities_rows
                if str(row.get("sociedad") or "").strip()
            }
        )
        society_scope: list[str] = []
        for name in selected_entities_effective + visible_societies:
            clean = str(name or "").strip()
            if clean and clean not in society_scope:
                society_scope.append(clean)

        society_rows = []
        total_monto = 0.0
        total_mov = 0.0
        total_caja = 0.0
        for society in society_scope:
            vals = society_agg.get(society)
            monto = _to_float(vals.get("monto_usd")) if vals else None
            mov = _to_float(vals.get("movimientos_mes")) if vals else None
            caja = _to_float(vals.get("caja_disponible")) if vals else None
            total_monto += monto or 0.0
            total_mov += mov or 0.0
            total_caja += caja or 0.0
            society_rows.append(
                {
                    "Sociedad": society,
                    "Monto USD": _fmt_or_blank(monto),
                    "Movimientos del mes": _fmt_or_blank(mov),
                    "Caja disponible": _fmt_or_blank(caja),
                }
            )
        if society_rows:
            society_rows.append(
                {
                    "Sociedad": "Total",
                    "Monto USD": _fmt_or_blank(total_monto),
                    "Movimientos del mes": _fmt_or_blank(total_mov),
                    "Caja disponible": _fmt_or_blank(total_caja),
                }
            )

        render_table(
            pd.DataFrame(
                society_rows,
                columns=["Sociedad", "Monto USD", "Movimientos del mes", "Caja disponible"],
            ),
            label_col="Sociedad",
            bold_row_labels={"Total"},
        )

        if not has_scope_filter:
            st.caption("Activa al menos un filtro para poblar esta tabla.")
        elif not society_rows:
            st.caption("Sin sociedades visibles para el filtro actual.")
        else:
            st.caption("Tabla por sociedad aplicada sobre el mismo scope visible en Detalle.")

    st.markdown("---")

    g1, g2, g3 = st.columns(3)

    with g1:
        st.subheader("Por Banco")
        by_bank = data.get("pie_charts", {}).get("by_bank", [])
        labels = [_fmt_bank(str(r.get("label", ""))) for r in by_bank]
        values = [_to_float(r.get("value")) or 0.0 for r in by_bank]
        if values and sum(values) > 0:
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos.")

    with g2:
        st.subheader("Por Tipo de Cuenta")
        by_type = data.get("pie_charts", {}).get("by_type", [])
        labels = [_fmt_account_type(str(r.get("label", ""))) for r in by_type]
        values = [_to_float(r.get("value")) or 0.0 for r in by_type]
        if values and sum(values) > 0:
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4)])
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos.")

    with g3:
        label_year = selected_year if selected_year else "Ano"
        st.subheader(f"Evolucion Rentabilidad YTD ({label_year})")
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=MONTHS,
                y=ytd_values,
                mode="lines+markers",
                name="YTD",
                line=dict(color="#E67E22", width=2),
            )
        )
        fig.update_layout(
            height=320,
            yaxis_title="% YTD",
            margin=dict(l=20, r=20, t=20, b=20),
        )
        fig.update_yaxes(tickformat=",.2f")
        st.plotly_chart(fig, use_container_width=True)
