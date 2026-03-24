"""Muestreo: porcentaje, límite máximo y modo reciente/aleatorio."""

from __future__ import annotations

import random
from typing import TypeVar

T = TypeVar("T")


def sample_rows(
    rows: list[T],
    *,
    sample_pct: int,
    max_docs: int,
    sample_mode: str,
) -> list[T]:
    """
    Aplica min(porcentaje del universo, max_docs) sobre la lista ya ordenada
    (recientes primero = orden descendente de período).
    """
    if not rows:
        return []
    pct = max(1, min(100, int(sample_pct)))
    n_from_pct = max(1, (len(rows) * pct + 99) // 100)
    n = min(len(rows), n_from_pct, max(1, int(max_docs)))

    work = list(rows)
    if sample_mode == "aleatorio":
        random.shuffle(work)
    return work[:n]
