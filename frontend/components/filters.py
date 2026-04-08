"""
Componentes de filtro reutilizables para Streamlit.

Reglas de filtros:
1) Multi-selección visible (checkboxes/multiselect con lista expandible)
2) Selecciones activas visibles (chips arriba)
3) Nunca ocultar opciones por cascada
4) Botón "Limpiar filtros" y "Seleccionar todo"
5) Indicador visible del estado actual del filtro
"""

from copy import deepcopy
from typing import Any, Optional

import streamlit as st


# ── Display names for bank codes ─────────────────────────────────
BANK_DISPLAY_NAMES: dict[str, str] = {
    "jpmorgan": "JPMorgan",
    "ubs": "UBS Suiza",
    "ubs_miami": "UBS Miami",
    "goldman_sachs": "Goldman Sachs",
    "bbh": "BBH",
    "bice": "BICE",
    "alternativos": "Alternativos",
}


def _default_format(value: str) -> str:
    """Formatea un valor crudo para mostrar en la UI."""
    return value


def _format_bank_code(code: str) -> str:
    """goldman_sachs → 'Goldman Sachs', etc."""
    return BANK_DISPLAY_NAMES.get(code, code.replace("_", " ").title())


# Map de filter_name → función de formato
_FORMAT_FUNCS: dict[str, callable] = {
    "bank_codes": _format_bank_code,
}


def render_filters(
    filter_options: dict[str, list[str]],
    key_prefix: str = "filter",
    format_labels: dict[str, callable] | None = None,
) -> dict[str, list[str]]:
    """
    Renderiza el bloque de filtros estándar.

    Reglas:
    - Multi-selección visible
    - Sin cascada destructiva
    - Indicador de estado visible
    - Botones limpiar/seleccionar todo

    Returns:
        Dict con selecciones actuales por filtro.
    """

    st.markdown("### 🔍 Filtros")

    # ── Botones de control ───────────────────────────────────────
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        clear_all = st.button("🧹 Limpiar filtros", key=f"{key_prefix}_clear")
    with col2:
        select_all = st.button("✅ Seleccionar todo", key=f"{key_prefix}_select_all")

    # ── Filtros multi-selección ──────────────────────────────────
    selections = {}
    _fmts = {**_FORMAT_FUNCS, **(format_labels or {})}

    filter_cols = st.columns(len(filter_options))
    for idx, (filter_name, options) in enumerate(filter_options.items()):
        with filter_cols[idx]:
            label = filter_name.replace("_", " ").title()
            fmt_fn = _fmts.get(filter_name, _default_format)

            if clear_all:
                default = []
            elif select_all:
                default = options
            else:
                default = st.session_state.get(f"{key_prefix}_{filter_name}", [])

            selected = st.multiselect(
                label,
                options=options,
                default=default if all(d in options for d in default) else [],
                format_func=fmt_fn,
                key=f"{key_prefix}_{filter_name}",
            )
            selections[filter_name] = selected

    # ── Indicador de estado activo ───────────────────────────────
    active_parts = []
    for name, selected in selections.items():
        if selected:
            label = name.replace("_", " ").title()
            fmt_fn = _fmts.get(name, _default_format)
            values = ", ".join(fmt_fn(str(v)) for v in selected[:5])
            if len(selected) > 5:
                values += f" (+{len(selected) - 5} más)"
            active_parts.append(f"**{label}:** {values}")

    if active_parts:
        st.info("🏷️ Mostrando: " + " | ".join(active_parts))
    else:
        st.warning("⚠️ Sin filtros activos. Seleccione al menos un criterio.")

    return selections


def render_date_range_filter(
    key_prefix: str = "date",
    years: Optional[list[int]] = None,
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """
    Filtro de rango de fechas personalizado.

    Returns:
        (year_start, month_start, year_end, month_end)
    """
    if years is None:
        years = list(range(2020, 2027))

    st.markdown("#### 📅 Rango personalizado")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        y_start = st.selectbox("Año inicio", years, key=f"{key_prefix}_y_start")
    with col2:
        m_start = st.selectbox("Mes inicio", list(range(1, 13)), key=f"{key_prefix}_m_start")
    with col3:
        y_end = st.selectbox("Año fin", years, index=len(years) - 1, key=f"{key_prefix}_y_end")
    with col4:
        m_end = st.selectbox("Mes fin", list(range(1, 13)), index=11, key=f"{key_prefix}_m_end")

    return y_start, m_start, y_end, m_end


def render_fecha_filter(
    available_dates: list[str],
    key_prefix: str = "fecha",
) -> Optional[str]:
    """
    Renderiza un selectbox de fecha YYYY-MM para la pestaña ETF.

    Args:
        available_dates: Lista de strings "YYYY-MM" disponibles.
        key_prefix: Prefijo de key para widget Streamlit.

    Returns:
        String "YYYY-MM" seleccionado, o None si no hay datos.
    """
    if not available_dates:
        st.warning("No hay fechas disponibles")
        return None

    sorted_dates = sorted(available_dates, reverse=True)
    widget_key = f"{key_prefix}_fecha"

    # Default = ultima cartola cargada para la pestaña actual.
    if widget_key not in st.session_state or st.session_state[widget_key] not in sorted_dates:
        st.session_state[widget_key] = sorted_dates[0]

    selected = st.selectbox(
        "📅 Fecha",
        options=sorted_dates,
        key=widget_key,
    )
    return selected


def _normalize_filter_value(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(sorted(_normalize_filter_value(item) for item in value))
    if isinstance(value, dict):
        return tuple(sorted((str(key), _normalize_filter_value(item)) for key, item in value.items()))
    return value


def use_apply_filters(
    *,
    state_key: str,
    current_filters: dict[str, Any],
    button_label: str = "Aplicar",
) -> tuple[dict[str, Any], bool]:
    """
    Mantiene un estado de filtros "aplicados" separado del borrador visible.

    Los widgets pueden cambiar libremente sin disparar consultas pesadas.
    Solo al presionar el botón se promueven al estado aplicado.
    """
    stored = st.session_state.get(state_key)
    if not isinstance(stored, dict):
        stored = deepcopy(current_filters)
        st.session_state[state_key] = stored

    applied_filters = {
        key: deepcopy(stored.get(key, value))
        for key, value in current_filters.items()
    }
    pending_changes = any(
        _normalize_filter_value(current_filters.get(key))
        != _normalize_filter_value(applied_filters.get(key))
        for key in current_filters
    )

    col_apply, col_status = st.columns([1, 3])
    with col_apply:
        apply_pressed = st.button(button_label, key=f"{state_key}_apply")
    with col_status:
        if pending_changes:
            st.caption("Hay cambios de filtros sin aplicar.")
        else:
            st.caption("Mostrando datos con los filtros aplicados.")

    if apply_pressed:
        applied_filters = deepcopy(current_filters)
        st.session_state[state_key] = applied_filters
        return applied_filters, False

    return applied_filters, pending_changes
