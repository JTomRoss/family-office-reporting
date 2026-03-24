"""
Pagina Detalle.

Estructura:
- Filtros dependientes: Banco, Sociedad, Consolidado, Nombre, Fecha
- Saldo consolidado
- Panel superior: rentabilidad mensual/YTD + tabla de movimientos (12 meses)
- Vistas detalladas por banco, cuenta y sociedad
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from asset_taxonomy import asset_bucket_colors, default_chart_color_sequence
from frontend import api_client
from frontend.components.chart_utils import aligned_dual_return_axes
from frontend.components.data_health import render_health_warning
from frontend.components.filters import BANK_DISPLAY_NAMES
from frontend.components.number_format import fmt_currency, fmt_number
from frontend.components.table_utils import render_table


MONTHS = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

ACCOUNT_TYPE_DISPLAY = {
    "etf": "ETF",
    "brokerage": "Brokerage",
    "mandato": "Mandato",
    "pe": "Private Equity (PE)",
    "re": "Real Estate (RE)",
    "current": "Current",
    "checking": "Checking",
    "savings": "Savings",
    "custody": "Custody",
    "investment": "Investment",
    "bonds": "Bonds",
}

CONSOLIDATED_PRESETS = {
    "Mi Investments": [
        "Boatview",
        "Telmar",
        "White Alaska",
        "Ecoterra RE",
        "Ecoterra RE II",
        "Ecoterra RE III",
    ],
    "Mi Inv + Ect. Int": [
        "Boatview",
        "Telmar",
        "White Alaska",
        "Ecoterra RE",
        "Ecoterra RE II",
        "Ecoterra RE III",
        "Ecoterra Internacional",
    ],
    "Mi Inv + Ect. Int+ Armel": [
        "Boatview",
        "Telmar",
        "White Alaska",
        "Ecoterra RE",
        "Ecoterra RE II",
        "Ecoterra RE III",
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


def _compute_accumulated_from_monthly(monthly_returns: list[float | None]) -> list[float | None]:
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


def _build_fecha_options(years: list[int]) -> list[str]:
    if not years:
        return []
    values: list[str] = []
    for year in sorted(set(int(y) for y in years), reverse=True):
        for month in range(12, 0, -1):
            values.append(f"{year}-{month:02d}")
    return values


def _fecha_label(fecha_str: str) -> str:
    parts = str(fecha_str).split("-")
    if len(parts) != 2:
        return str(fecha_str)
    month = int(parts[1])
    return f"{MONTHS[month - 1]} {parts[0][-2:]}"


def _distinct_values(rows: list[dict], field: str) -> list[str]:
    return sorted(
        {
            str(row.get(field) or "").strip()
            for row in rows
            if str(row.get(field) or "").strip()
        }
    )


def _distinct_account_type_values(rows: list[dict]) -> list[str]:
    values: set[str] = set()
    for row in rows:
        bank_code = str(row.get("banco") or "").strip().lower()
        asset_class = str(row.get("asset_class") or "").strip().upper()
        if bank_code == "alternativos" and asset_class in {"PE", "RE"}:
            values.add(asset_class.lower())
            continue
        account_type = str(row.get("account_type") or row.get("tipo_cuenta") or "").strip().lower()
        if account_type:
            values.add(account_type)
    return sorted(values)


def _sanitize_multiselect_state(key: str, valid_options: list[str]) -> list[str]:
    selected = [value for value in st.session_state.get(key, []) if value in valid_options]
    st.session_state[key] = selected
    return selected


def _seed_detail_rows(
    *,
    selected_year: int | None,
    selected_month: int | None,
    bank_codes: list[str] | None = None,
    entity_names: list[str] | None = None,
    account_types: list[str] | None = None,
    person_names: list[str] | None = None,
) -> list[dict]:
    payload = {
        "years": [selected_year] if selected_year else [],
        "months": [selected_month] if selected_month else [],
        "bank_codes": bank_codes or [],
        "entity_names": entity_names or [],
        "account_types": account_types or [],
        "person_names": person_names or [],
    }
    try:
        return api_client.post("/data/personal", json=payload).get("entities_table", [])
    except Exception:
        return []


def _fmt_or_blank(value, *, decimals: int = 1) -> str:
    if value is None:
        return ""
    return fmt_number(value, decimals=decimals)


def _default_detail_payload() -> dict:
    return {
        "selected_fecha": None,
        "consolidated_usd": 0.0,
        "consolidated_clp": 0.0,
        "cash": 0.0,
        "pie_charts": {"by_bank": [], "by_type": []},
        "by_bank_detail": [],
        "entities_table": [],
        "summary_table": [],
        "range_table": [],
        "returns_panel": {"months": [], "rows": []},
        "detail_views": {
            "bank": {"table_rows": [], "composition": [], "history_months": [], "history_series": [], "total_monto_usd": 0.0, "show_activity_columns": True},
            "account": {"table_rows": [], "composition": [], "history_months": [], "history_series": [], "total_monto_usd": 0.0, "show_activity_columns": True},
            "account_grouped": {"table_rows": [], "composition": [], "history_months": [], "history_series": [], "total_monto_usd": 0.0, "show_activity_columns": True},
            "society": {"table_rows": [], "composition": [], "history_months": [], "history_series": [], "total_monto_usd": 0.0, "show_activity_columns": True},
            "asset": {"table_rows": [], "composition": [], "history_months": [], "history_series": [], "total_monto_usd": 0.0, "show_activity_columns": False},
        },
    }


def _display_detail_label(view_key: str, label: str) -> str:
    if view_key == "bank":
        return _fmt_bank(label)
    return label


def _render_movements_table(rows: list[dict], *, height: int = 360) -> None:
    if not rows:
        st.info("Sin datos de movimientos.")
        return

    ordered_rows = sorted(rows, key=lambda row: str(row.get("fecha") or ""), reverse=True)
    df = pd.DataFrame(
        [
            {
                "Fecha": _fecha_label(str(row.get("fecha") or "")),
                "Monto USD": _fmt_or_blank(row.get("ending_value")),
                "Movimientos": _fmt_or_blank(row.get("movimientos")),
            }
            for row in ordered_rows
        ]
    )
    st.dataframe(df, hide_index=True, use_container_width=True, height=height)


def _render_detail_table(
    *,
    view_key: str,
    title_col: str,
    rows: list[dict],
    total_monto_usd: float,
    show_activity_columns: bool,
) -> None:
    processed_rows = rows
    if view_key == "asset" and rows:
        aggregated: dict[str, dict[str, float]] = {}
        order: list[str] = []
        for row in rows:
            display_label = str(row.get("table_label") or row.get("label") or "")
            if display_label not in aggregated:
                aggregated[display_label] = {
                    "monto_usd": 0.0,
                    "movimientos_mes": 0.0,
                    "caja_disponible": 0.0,
                }
                order.append(display_label)
            aggregated[display_label]["monto_usd"] += _to_float(row.get("monto_usd")) or 0.0
            aggregated[display_label]["movimientos_mes"] += _to_float(row.get("movimientos_mes")) or 0.0
            aggregated[display_label]["caja_disponible"] += _to_float(row.get("caja_disponible")) or 0.0

        processed_rows = []
        for label in order:
            values = aggregated[label]
            pct_total = round((values["monto_usd"] / total_monto_usd) * 100, 2) if total_monto_usd > 0 else None
            processed_rows.append(
                {
                    "table_label": label,
                    "label": label,
                    "monto_usd": values["monto_usd"],
                    "movimientos_mes": values["movimientos_mes"],
                    "caja_disponible": values["caja_disponible"],
                    "pct_total": pct_total,
                }
            )

    table_rows: list[dict] = []
    total_mov = 0.0
    total_caja = 0.0
    for row in processed_rows:
        total_mov += _to_float(row.get("movimientos_mes")) or 0.0
        total_caja += _to_float(row.get("caja_disponible")) or 0.0
        raw_label = str(row.get("table_label") or row.get("label") or "")
        table_rows.append(
            {
                title_col: _display_detail_label(view_key, raw_label),
                "Monto USD": _fmt_or_blank(row.get("monto_usd")),
                "%": _fmt_or_blank(row.get("pct_total"), decimals=2),
            }
        )
        if show_activity_columns:
            table_rows[-1]["Mov mes"] = _fmt_or_blank(row.get("movimientos_mes"))
            table_rows[-1]["Caja disponible"] = _fmt_or_blank(row.get("caja_disponible"))

    if table_rows:
        total_row = {
            title_col: "Total",
            "Monto USD": _fmt_or_blank(total_monto_usd),
            "%": _fmt_or_blank(100.0 if total_monto_usd > 0 else None, decimals=2),
        }
        if show_activity_columns:
            total_row["Mov mes"] = _fmt_or_blank(total_mov)
            total_row["Caja disponible"] = _fmt_or_blank(total_caja)
        table_rows.append(total_row)

    columns = [title_col, "Monto USD"]
    if show_activity_columns:
        columns.extend(["Mov mes", "Caja disponible"])
    columns.append("%")

    render_table(
        pd.DataFrame(
            table_rows,
            columns=columns,
        ),
        label_col=title_col,
        bold_row_labels={"Total"},
    )


def _detail_chart_spacer_px(view_key: str, row_count: int) -> int:
    if view_key == "bank":
        return 0
    if view_key == "account":
        return min(max((row_count - 4) * 18, 48), 144)
    return 0


def _detail_chart_color(
    *,
    view_key: str,
    label: str,
    color_map: dict[str, str],
    fallback_color_map: dict[str, str],
) -> str | None:
    taxonomy_colors = asset_bucket_colors()
    normalized = str(label or "").strip()
    normalized_lower = normalized.lower()

    if view_key == "bank" and normalized_lower == "alternativos":
        return taxonomy_colors.get("Alternativos") or "#2E7D5A"
    if view_key == "account":
        if normalized.endswith("-ALT-PE"):
            return taxonomy_colors.get("PE") or taxonomy_colors.get("Alternativos") or "#2E7D5A"
        if normalized.endswith("-ALT-RE"):
            return taxonomy_colors.get("RE") or taxonomy_colors.get("Real Estate") or "#6AA56A"
    if view_key == "asset":
        if normalized == "PE":
            return taxonomy_colors.get("PE") or taxonomy_colors.get("Alternativos") or "#2E7D5A"
        if normalized == "RE":
            return taxonomy_colors.get("RE") or taxonomy_colors.get("Real Estate") or "#6AA56A"

    return color_map.get(normalized) or fallback_color_map.get(normalized)


def _render_detail_section(
    *,
    section_title: str,
    view_key: str,
    label_title: str,
    payload: dict,
    show_heading: bool = True,
) -> None:
    if show_heading:
        st.markdown(f"#### {section_title}")
    table_rows = payload.get("table_rows", [])
    composition = payload.get("composition", [])
    history_months = payload.get("history_months", [])
    history_series = payload.get("history_series", [])
    total_monto_usd = _to_float(payload.get("total_monto_usd")) or 0.0
    show_activity_columns = bool(payload.get("show_activity_columns", True))
    chart_spacer_px = _detail_chart_spacer_px(view_key, len(table_rows))
    color_map = asset_bucket_colors() if view_key == "asset" else {}
    fallback_palette = default_chart_color_sequence()
    fallback_color_map = {
        str(row.get("label") or ""): fallback_palette[idx % len(fallback_palette)]
        for idx, row in enumerate(composition)
    } if fallback_palette else {}

    left, middle, right = st.columns([3, 3, 4])

    with left:
        if table_rows:
            _render_detail_table(
                view_key=view_key,
                title_col=label_title,
                rows=table_rows,
                total_monto_usd=total_monto_usd,
                show_activity_columns=show_activity_columns,
            )
            if view_key == "asset":
                st.caption("\\*No se incluye accruals")
        else:
            st.info("Sin datos para la tabla.")

    with middle:
        if chart_spacer_px:
            st.markdown(f"<div style='height:{chart_spacer_px}px'></div>", unsafe_allow_html=True)
        if composition:
            labels = [_display_detail_label(view_key, str(row.get("label") or "")) for row in composition]
            values = [_to_float(row.get("value")) or 0.0 for row in composition]
            marker_colors = [
                _detail_chart_color(
                    view_key=view_key,
                    label=str(row.get("label") or ""),
                    color_map=color_map,
                    fallback_color_map=fallback_color_map,
                )
                for row in composition
            ]
            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=labels,
                        values=values,
                        hole=0.42,
                        marker=dict(colors=marker_colors) if any(marker_colors) else None,
                    )
                ]
            )
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos para la composición.")

    with right:
        if chart_spacer_px:
            st.markdown(f"<div style='height:{chart_spacer_px}px'></div>", unsafe_allow_html=True)
        if history_months and history_series:
            fig = go.Figure()
            x_labels = [_fecha_label(month) for month in history_months]
            history_color_map = {
                str(series.get("label") or ""): fallback_palette[idx % len(fallback_palette)]
                for idx, series in enumerate(history_series)
            } if fallback_palette else {}
            for series in history_series:
                pct_values = [_to_float(val) or 0.0 for val in series.get("pct_values", [])]
                amount_values = [_to_float(val) or 0.0 for val in series.get("amount_values", [])]
                text_values = [f"{val:.1f}%" if val >= 7 else "" for val in pct_values]
                label = str(series.get("label") or "")
                fig.add_trace(
                    go.Bar(
                        x=x_labels,
                        y=pct_values,
                        name=_display_detail_label(view_key, label),
                        text=text_values,
                        textposition="inside",
                        customdata=amount_values,
                        marker_color=_detail_chart_color(
                            view_key=view_key,
                            label=label,
                            color_map=color_map,
                            fallback_color_map=history_color_map,
                        ),
                        hovertemplate="%{fullData.name}<br>%{x}<br>%{y:.2f}%<br>USD %{customdata:,.2f}<extra></extra>",
                    )
                )
            fig.update_layout(
                barmode="stack",
                height=360,
                margin=dict(l=20, r=20, t=20, b=20),
                yaxis_title="% del total",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            )
            fig.update_yaxes(range=[0, 100], tickformat=",.0f")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos para la evolución mensual.")


def render():
    st.title("Detalle")
    st.markdown("---")

    try:
        opts = api_client.get("/accounts/filter-options")
    except Exception:
        opts = {
            "bank_codes": [],
            "entity_names": [],
            "person_names": [],
            "account_types": [],
            "years": [],
            "available_fechas": [],
        }

    bank_options_all = sorted(opts.get("bank_codes", []))
    entity_options_all = sorted(opts.get("entity_names", []))
    person_options_all = sorted(opts.get("person_names", []))
    account_type_options_all = sorted(opts.get("account_types", []))
    year_options = [int(y) for y in opts.get("years", []) if y is not None]
    fecha_options = sorted(opts.get("available_fechas", []), reverse=True)
    if not fecha_options:
        fecha_options = _build_fecha_options(year_options)

    raw_selected_consolidated = st.session_state.get("detalle_consolidated", "")
    raw_selected_entities = list(st.session_state.get("detalle_sociedad", []))
    raw_selected_banks = list(st.session_state.get("detalle_banco", []))
    raw_selected_people = list(st.session_state.get("detalle_nombre", []))
    raw_selected_types = list(st.session_state.get("detalle_account_types", []))
    current_fecha = st.session_state.get("detalle_fecha")
    if current_fecha not in fecha_options and fecha_options:
        current_fecha = fecha_options[0]
        st.session_state["detalle_fecha"] = current_fecha

    selected_year_seed = int(current_fecha[:4]) if current_fecha else None
    selected_month_seed = int(current_fecha[5:7]) if current_fecha else None
    preset_entities_seed = list(CONSOLIDATED_PRESETS.get(raw_selected_consolidated, []))
    effective_entities_seed = sorted(set(raw_selected_entities) | set(preset_entities_seed))
    effective_people_seed = [] if preset_entities_seed else raw_selected_people

    bank_seed_rows = _seed_detail_rows(
        selected_year=selected_year_seed,
        selected_month=selected_month_seed,
        entity_names=effective_entities_seed,
        account_types=raw_selected_types,
        person_names=effective_people_seed,
    )
    entity_seed_rows = _seed_detail_rows(
        selected_year=selected_year_seed,
        selected_month=selected_month_seed,
        bank_codes=raw_selected_banks,
        account_types=raw_selected_types,
        person_names=effective_people_seed,
    )
    type_seed_rows = _seed_detail_rows(
        selected_year=selected_year_seed,
        selected_month=selected_month_seed,
        bank_codes=raw_selected_banks,
        entity_names=effective_entities_seed,
        person_names=effective_people_seed,
    )
    person_seed_rows = _seed_detail_rows(
        selected_year=selected_year_seed,
        selected_month=selected_month_seed,
        bank_codes=raw_selected_banks,
        entity_names=effective_entities_seed,
        account_types=raw_selected_types,
    )

    bank_options = _distinct_values(bank_seed_rows, "banco") or bank_options_all
    entity_options = _distinct_values(entity_seed_rows, "sociedad") or entity_options_all
    person_options = _distinct_values(person_seed_rows, "nombre") or person_options_all
    account_type_options = _distinct_account_type_values(type_seed_rows) or account_type_options_all

    _sanitize_multiselect_state("detalle_banco", bank_options)
    _sanitize_multiselect_state("detalle_sociedad", entity_options)
    _sanitize_multiselect_state("detalle_account_types", account_type_options)
    _sanitize_multiselect_state("detalle_nombre", person_options)

    st.markdown("### Filtros")
    top_f1, top_f2, top_f3, top_f4 = st.columns(4)
    bottom_f1, bottom_f2 = st.columns(2)
    with top_f1:
        selected_banks = st.multiselect(
            "Banco",
            options=bank_options,
            format_func=_fmt_bank,
            key="detalle_banco",
        )
    with top_f2:
        selected_entities = st.multiselect(
            "Sociedad",
            options=entity_options,
            key="detalle_sociedad",
        )
    with top_f3:
        selected_types = st.multiselect(
            "Tipo de cuenta",
            options=account_type_options,
            format_func=_fmt_account_type,
            key="detalle_account_types",
        )
    with top_f4:
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
    with bottom_f1:
        selected_consolidated = st.selectbox(
            "Consolidado",
            options=[""] + list(CONSOLIDATED_PRESETS.keys()),
            format_func=lambda x: x or "Sin consolidado",
            key="detalle_consolidated",
        )
    with bottom_f2:
        selected_people = st.multiselect(
            "Nombre",
            options=person_options,
            key="detalle_nombre",
        )

    preset_entities = list(CONSOLIDATED_PRESETS.get(selected_consolidated, []))
    selected_entities_effective = sorted(set(selected_entities) | set(preset_entities))
    effective_people = [] if preset_entities else selected_people
    if selected_consolidated and preset_entities:
        st.caption(f"Consolidado activo: {', '.join(preset_entities)}")

    selected_year = int(selected_fecha[:4]) if selected_fecha else None
    selected_month = int(selected_fecha[5:7]) if selected_fecha else None
    has_scope_filter = bool(selected_banks or selected_entities_effective or selected_types or effective_people)

    data = _default_detail_payload()
    if has_scope_filter:
        payload = {
            "bank_codes": selected_banks,
            "entity_names": selected_entities_effective,
            "account_types": selected_types,
            "person_names": effective_people,
            "years": [selected_year] if selected_year else [],
            "months": [selected_month] if selected_month else [],
        }
        try:
            data = api_client.post("/data/personal", json=payload)
        except Exception as exc:
            st.error(f"Error obteniendo datos de detalle: {exc}")

        render_health_warning(
            {
                "years": [selected_year] if selected_year else [],
                "months": [selected_month] if selected_month else [],
                "bank_codes": selected_banks,
                "entity_names": selected_entities_effective,
                "account_types": selected_types,
                "person_names": effective_people,
            },
            label="Detalle",
        )

    st.markdown("---")
    st.subheader("Saldo Consolidado")
    if not has_scope_filter:
        st.caption("Activa al menos un filtro de Banco, Sociedad, Tipo de cuenta, Nombre o Consolidado para poblar esta vista.")

    m1, m2 = st.columns([1, 1])
    with m1:
        st.metric("Total USD", fmt_currency(data.get("consolidated_usd", 0), decimals=2))
    with m2:
        st.metric("Total CLP", fmt_currency(data.get("consolidated_clp", 0), decimals=0))

    st.markdown("---")

    top_left, top_right = st.columns([2, 1])
    returns_rows = data.get("returns_panel", {}).get("rows", [])
    with top_left:
        st.subheader("Rentabilidad ultimos 12 meses (%)")
        if returns_rows:
            x_labels = [_fecha_label(str(row.get("fecha") or "")) for row in returns_rows]
            monthly_values = [_to_float(row.get("rent_mensual_pct")) for row in returns_rows]
            accumulated_values = _compute_accumulated_from_monthly(monthly_values)
            axis_ranges = aligned_dual_return_axes(monthly_values, accumulated_values)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Bar(
                    x=x_labels,
                    y=monthly_values,
                    name="Rentabilidad mensual",
                    marker_color="#AFC8E2",
                    opacity=0.95,
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=x_labels,
                    y=accumulated_values,
                    mode="lines+markers",
                    name="Rentabilidad acumulada",
                    line=dict(color="#E67E22", width=2),
                ),
                secondary_y=True,
            )
            fig.update_layout(
                height=360,
                margin=dict(l=20, r=20, t=20, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            )
            fig.update_yaxes(
                title_text="% Mensual",
                tickformat=",.2f",
                showgrid=False,
                zeroline=True,
                zerolinecolor="#9EA7B3",
                range=axis_ranges["primary_range"],
                secondary_y=False,
            )
            fig.update_yaxes(
                title_text="% acumulada",
                tickformat=",.2f",
                showgrid=True,
                gridcolor="#D6DCE5",
                zeroline=False,
                range=axis_ranges["secondary_range"],
                secondary_y=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos para la rentabilidad.")
    with top_right:
        st.subheader("Detalle ultimos 12 meses")
        _render_movements_table(returns_rows, height=360)

    detail_views = data.get("detail_views", {})
    _render_detail_section(
        section_title="Detalle por Banco",
        view_key="bank",
        label_title="Banco",
        payload=detail_views.get("bank", {}),
    )

    st.markdown("<div style='height:2rem;'></div>", unsafe_allow_html=True)
    st.markdown("#### Detalle por Cuenta")
    st.session_state.setdefault("detalle_account_grouped", True)
    grouped_accounts = st.toggle(
        "Agrupar cuentas por sociedad-banco-tipo",
        key="detalle_account_grouped",
    )
    _render_detail_section(
        section_title="Detalle por Cuenta",
        view_key="account",
        label_title="Cuenta",
        payload=detail_views.get("account_grouped" if grouped_accounts else "account", {}),
        show_heading=False,
    )

    st.markdown("<div style='height:2rem;'></div>", unsafe_allow_html=True)
    _render_detail_section(
        section_title="Detalle por Sociedad",
        view_key="society",
        label_title="Sociedad",
        payload=detail_views.get("society", {}),
    )

    st.markdown("<div style='height:2rem;'></div>", unsafe_allow_html=True)
    _render_detail_section(
        section_title="Detalle por Activo",
        view_key="asset",
        label_title="Activo",
        payload=detail_views.get("asset", {}),
    )
