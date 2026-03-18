"""Helpers compartidos para escalado visual de graficos."""

from __future__ import annotations

import math


def _finite_values(values: list[float | None]) -> list[float]:
    cleaned: list[float] = []
    for value in values:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isnan(numeric) or math.isinf(numeric):
            continue
        cleaned.append(numeric)
    return cleaned


def aligned_dual_return_axes(
    monthly_values: list[float | None],
    ytd_values: list[float | None],
    *,
    monthly_padding_ratio: float = 0.1,
    monthly_min_padding: float = 0.5,
    secondary_padding_ratio: float = 0.1,
    secondary_min_padding: float = 0.5,
) -> dict[str, list[float]]:
    """Retorna rangos para dos ejes que comparten el mismo cero visual."""
    monthly_clean = _finite_values(monthly_values)
    ytd_clean = _finite_values(ytd_values)

    monthly_min_raw = min(monthly_clean + [0.0])
    monthly_max_raw = max(monthly_clean + [0.0])
    monthly_span = monthly_max_raw - monthly_min_raw
    monthly_reference = max(monthly_span, abs(monthly_min_raw), abs(monthly_max_raw), 1.0)
    monthly_padding = max(monthly_reference * monthly_padding_ratio, monthly_min_padding)

    primary_min = monthly_min_raw - monthly_padding if monthly_min_raw < 0 else 0.0
    primary_max = monthly_max_raw + monthly_padding if monthly_max_raw > 0 else monthly_padding
    if primary_max <= primary_min:
        primary_max = primary_min + 1.0

    primary_negative = abs(primary_min)
    primary_positive = primary_max

    ytd_min_raw = min(ytd_clean + [0.0])
    ytd_max_raw = max(ytd_clean + [0.0])
    ytd_span = ytd_max_raw - ytd_min_raw
    ytd_reference = max(ytd_span, abs(ytd_min_raw), abs(ytd_max_raw), 1.0)
    ytd_padding = max(ytd_reference * secondary_padding_ratio, secondary_min_padding)
    required_secondary_min = ytd_min_raw - ytd_padding if ytd_min_raw < 0 else 0.0
    required_secondary_max = ytd_max_raw + ytd_padding if ytd_max_raw > 0 else ytd_padding

    scale_candidates = [1.0]
    if primary_positive > 0:
        scale_candidates.append(required_secondary_max / primary_positive)
    if primary_negative > 0:
        scale_candidates.append(abs(required_secondary_min) / primary_negative)
    scale = max(scale_candidates)

    secondary_min = -primary_negative * scale if primary_negative > 0 else 0.0
    secondary_max = primary_positive * scale if primary_positive > 0 else scale
    if secondary_max <= secondary_min:
        secondary_max = secondary_min + 1.0

    return {
        "primary_range": [round(primary_min, 4), round(primary_max, 4)],
        "secondary_range": [round(secondary_min, 4), round(secondary_max, 4)],
    }
