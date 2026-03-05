"""Helpers de formato para tablas Streamlit."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def style_table(
    df: pd.DataFrame,
    *,
    right_align_cols: list[str] | None = None,
):
    """
    Retorna un Styler homogéneo para toda la app:
    - Sin índice visible
    - Celdas alineadas a la derecha
    - Tabla de ancho fijo/equidistribuido
    """
    cols = list(df.columns)
    align_subset = cols if right_align_cols is None else [c for c in right_align_cols if c in cols]

    styler = df.style.hide(axis="index")
    if align_subset:
        styler = styler.set_properties(subset=align_subset, **{"text-align": "right"})

    return styler.set_table_styles(
        [
            {"selector": "table", "props": [("table-layout", "fixed"), ("width", "100%")]},
            {"selector": "th, td", "props": [("text-align", "right")]},
            {"selector": "th.row_heading", "props": [("display", "none")]},
            {"selector": "th.blank", "props": [("display", "none")]},
        ],
        overwrite=False,
    ).format({c: "{}" for c in cols})


def render_table(
    df: pd.DataFrame,
    *,
    right_align_cols: list[str] | None = None,
    use_container_width: bool = True,
) -> None:
    """Renderiza tabla sin índice (columna izquierda) en toda la app."""
    st.table(style_table(df, right_align_cols=right_align_cols))
