"""Helpers de formato para tablas Streamlit."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from frontend.components.number_format import fmt_number


def style_table(
    df: pd.DataFrame,
    *,
    right_align_cols: list[str] | None = None,
    bold_row_labels: set[str] | None = None,
    bold_cols: list[str] | None = None,
    small_row_labels: set[str] | None = None,
    label_col: str | None = None,
    fixed_equal_cols: bool = False,
    pinned_row_labels: set[str] | None = None,
):
    """
    Retorna un Styler homogéneo para toda la app:
    - Sin índice visible
    - Celdas alineadas a la derecha
    - Tabla de ancho fijo/equidistribuido
    """
    cols = list(df.columns)
    label_col = label_col or (cols[0] if cols else None)
    display_df = df.copy()
    if pinned_row_labels and label_col and label_col in display_df.columns:
        pinned_mask = display_df[label_col].astype(str).isin(pinned_row_labels)
        if pinned_mask.any():
            display_df = pd.concat(
                [display_df.loc[~pinned_mask], display_df.loc[pinned_mask]],
                ignore_index=True,
            )
    for idx, col in enumerate(cols):
        if idx == 0:
            continue
        if pd.api.types.is_numeric_dtype(display_df[col]):
            display_df[col] = display_df[col].apply(
                lambda v: "" if pd.isna(v) else fmt_number(v, decimals=1)
            )
    if right_align_cols is None:
        align_subset = cols[1:] if len(cols) > 1 else []
    else:
        align_subset = [c for c in right_align_cols if c in cols]

    styler = display_df.style.hide(axis="index")
    if align_subset:
        styler = styler.set_properties(subset=align_subset, **{"text-align": "right"})
    if label_col and label_col in cols:
        styler = styler.set_properties(subset=[label_col], **{"text-align": "left"})
    if bold_cols:
        bcols = [c for c in bold_cols if c in cols]
        if bcols:
            styler = styler.set_properties(subset=bcols, **{"font-weight": "700"})
    if bold_row_labels and label_col and label_col in df.columns:
        idx = display_df.index[display_df[label_col].astype(str).isin(bold_row_labels)]
        if len(idx) > 0:
            styler = styler.set_properties(subset=pd.IndexSlice[idx, cols], **{"font-weight": "700"})
    if small_row_labels and label_col and label_col in df.columns:
        idx = display_df.index[display_df[label_col].astype(str).isin(small_row_labels)]
        if len(idx) > 0:
            styler = styler.set_properties(subset=pd.IndexSlice[idx, cols], **{"font-size": "50%"})

    table_layout = "fixed" if fixed_equal_cols else "auto"
    styles = [
        {"selector": "table", "props": [("table-layout", table_layout), ("width", "100%")]},
        {
            "selector": "th, td",
            "props": [
                ("text-align", "right"),
                ("white-space", "nowrap"),
                ("font-variant-numeric", "tabular-nums"),
            ],
        },
        {"selector": "th.row_heading", "props": [("display", "none")]},
        {"selector": "th.blank", "props": [("display", "none")]},
    ]
    if cols:
        styles.extend(
            [
                {"selector": "th.col_heading.level0.col0", "props": [("text-align", "left")]},
                {"selector": "td.col0", "props": [("text-align", "left")]},
            ]
        )
    if fixed_equal_cols and cols:
        col_width = f"{100 / len(cols):.3f}%"
        styles.append({"selector": "th, td", "props": [("width", col_width)]})

    return styler.set_table_styles(styles, overwrite=False).format({c: "{}" for c in cols})


def render_table(
    df: pd.DataFrame,
    *,
    right_align_cols: list[str] | None = None,
    bold_row_labels: set[str] | None = None,
    bold_cols: list[str] | None = None,
    small_row_labels: set[str] | None = None,
    label_col: str | None = None,
    fixed_equal_cols: bool = False,
    use_container_width: bool = True,
    pinned_row_labels: set[str] | None = None,
) -> None:
    """Renderiza tabla sin índice (columna izquierda) en toda la app."""
    st.table(
        style_table(
            df,
            right_align_cols=right_align_cols,
            bold_row_labels=bold_row_labels,
            bold_cols=bold_cols,
            small_row_labels=small_row_labels,
            label_col=label_col,
            fixed_equal_cols=fixed_equal_cols,
            pinned_row_labels=pinned_row_labels,
        )
    )
