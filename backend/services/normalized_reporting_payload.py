from __future__ import annotations

import json
import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from mandate_taxonomy import (
    MANDATE_CATEGORY_CASH,
    MANDATE_CATEGORY_EQUITIES,
    MANDATE_CATEGORY_FIXED,
    MANDATE_CATEGORY_GLOBAL_EQUITIES,
    MANDATE_CATEGORY_HY_FIXED,
    MANDATE_CATEGORY_IG_FIXED,
    MANDATE_CATEGORY_NON_US_EQUITIES,
    MANDATE_CATEGORY_OTHER_INVESTMENTS,
    MANDATE_CATEGORY_PRIVATE_EQUITY,
    MANDATE_CATEGORY_REAL_ESTATE,
    MANDATE_CATEGORY_US_EQUITIES,
    classify_mandate_asset_label,
)

logger = logging.getLogger(__name__)

CANONICAL_ASSET_ORDER = (
    "Cash, Deposits & Money Market",
    "Investment Grade Fixed Income",
    "High Yield Fixed Income",
    "US Equities",
    "Non US Equities",
    "Private Equity",
    "Real Estate",
    "Other Investments",
)

DERIVED_ASSET_ORDER = (
    "Fixed Income",
    "Equities",
    "Non US Equities",
    "Global Equities",
    "Alternativos",
)

CANONICAL_BREAKDOWN_KEY = "__canonical_breakdown"
DERIVED_BREAKDOWN_KEY = "__derived_breakdown"
INSTRUMENT_BREAKDOWN_KEY = "__instrument_breakdown"
FI_METRICS_KEY = "__fi_metrics"
LEGACY_MANDATE_METRICS_KEY = "__mandate_metrics"
RAW_LIST_KEY = "__raw_entries"

_GLOBAL_EQUITY_US_WEIGHT = Decimal("0.6666666667")
_GLOBAL_EQUITY_NON_US_WEIGHT = Decimal("0.3333333333")


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def decode_asset_allocation_json(raw_json: str | None) -> dict | list | None:
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return None


def cash_from_asset_allocation_json(asset_alloc_json: str | None) -> Decimal | None:
    """Extrae caja desde asset_allocation_json persistido.

    Retorna Decimal si encuentra un monto de caja, None si no hay dato.
    Canónica: usada por loader y router para garantizar consistencia.
    """
    if not asset_alloc_json:
        return None
    try:
        alloc = json.loads(asset_alloc_json)
    except (TypeError, ValueError):
        return None

    def _is_cash_umbrella(label_norm: str) -> bool:
        return (
            "cash" in label_norm
            and "deposit" in label_norm
            and ("moneymarket" in label_norm or "shortterm" in label_norm)
        )

    def _is_mixed_cash_bucket(label_norm: str) -> bool:
        # Ej: "Cash & Fixed Income" no es caja pura.
        return "cash" in label_norm and any(
            tok in label_norm for tok in ("fixedincome", "bond", "equity", "stock")
        )

    def _value_from_payload(payload: Any) -> Decimal | None:
        if isinstance(payload, dict):
            raw = (
                payload.get("value")
                or payload.get("total")
                or payload.get("ending")
                or payload.get("ending_value")
                or payload.get("market_value")
                or payload.get("amount")
            )
        else:
            raw = payload
        return to_decimal(raw)

    total = Decimal("0")
    found = False
    if isinstance(alloc, dict):
        umbrella_values: list[Decimal] = []
        for key, payload in alloc.items():
            key_norm = normalize_label(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not _is_cash_umbrella(key_norm):
                continue
            val = _value_from_payload(payload)
            if val is not None:
                umbrella_values.append(val)
        if umbrella_values:
            return max(umbrella_values)

        for key, payload in alloc.items():
            key_norm = normalize_label(key)
            if _is_mixed_cash_bucket(key_norm):
                continue
            if not any(
                tok in key_norm for tok in ("cash", "deposit", "moneymarket", "liquidity")
            ):
                continue
            val = _value_from_payload(payload)
            if val is None:
                continue
            total += val
            found = True
    elif isinstance(alloc, list):
        for row in alloc:
            if not isinstance(row, dict):
                continue
            name_norm = normalize_label(
                row.get("asset_class") or row.get("name") or row.get("label") or ""
            )
            if _is_mixed_cash_bucket(name_norm):
                continue
            if not any(
                tok in name_norm for tok in ("cash", "deposit", "moneymarket", "liquidity")
            ):
                continue
            val = _value_from_payload(row)
            if val is None:
                continue
            total += val
            found = True
    return total if found else None


def _extract_amount_and_unit(payload: Any) -> tuple[Decimal | None, str | None]:
    if isinstance(payload, dict):
        raw = (
            payload.get("value")
            or payload.get("total")
            or payload.get("ending")
            or payload.get("ending_value")
            or payload.get("market_value")
            or payload.get("amount")
        )
        unit_raw = payload.get("unit")
        unit = str(unit_raw).strip() if unit_raw is not None else None
    else:
        raw = payload
        unit = None
    return to_decimal(raw), unit


def _convert_amount_by_unit(*, value: Decimal | None, unit: str | None, ending_value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if str(unit or "").strip() != "%":
        return value
    if ending_value is None or ending_value <= 0:
        return None
    return (ending_value * value) / Decimal("100")


def _empty_canonical() -> dict[str, Decimal]:
    return {label: Decimal("0") for label in CANONICAL_ASSET_ORDER}


def _is_cash_account_type(account_type: str | None) -> bool:
    return str(account_type or "").strip().lower() in {"current", "checking", "savings"}


def _category_from_generic_label(*, label: str | None, bank_code: str | None = None) -> str | None:
    key = normalize_label(label)
    if not key:
        return None

    if key in {"pe", "alt", "alternativos"} or "privateequity" in key:
        return MANDATE_CATEGORY_PRIVATE_EQUITY
    if key in {"re"} or "realestate" in key:
        return MANDATE_CATEGORY_REAL_ESTATE
    if any(token in key for token in ("otherinvestment", "assetallocationinvestment", "miscellaneous", "hedgefund")):
        return MANDATE_CATEGORY_OTHER_INVESTMENTS

    if key == "caja" or any(tok in key for tok in ("cash", "deposit", "moneymarket", "liquidity")):
        if any(tok in key for tok in ("fixedincome", "bond", "equity", "stock")):
            return None
        return MANDATE_CATEGORY_CASH

    if key == "rvem" or (("emerging" in key or "em" in key) and "equit" in key):
        return MANDATE_CATEGORY_NON_US_EQUITIES
    if "nonus" in key and "equit" in key:
        return MANDATE_CATEGORY_NON_US_EQUITIES
    if ("usequit" in key or "usequity" in key) and "nonus" not in key:
        return MANDATE_CATEGORY_US_EQUITIES
    if key in {"rvdm", "globalequity"}:
        return MANDATE_CATEGORY_GLOBAL_EQUITIES
    if key in {"equity", "equities"} or "equit" in key:
        return MANDATE_CATEGORY_EQUITIES

    if key == "hy" or "highyield" in key or "noninvestmentgrade" in key:
        return MANDATE_CATEGORY_HY_FIXED
    if str(bank_code or "").strip().lower() == "ubs_miami" and "emerging" in key and (
        "fixed" in key or "income" in key or "bond" in key
    ):
        return MANDATE_CATEGORY_HY_FIXED
    if key in {"rfigshort", "rfiglong", "nonusrf", "rf"}:
        return MANDATE_CATEGORY_IG_FIXED
    if "investmentgrade" in key:
        return MANDATE_CATEGORY_IG_FIXED
    if "fixedincome" in key or "bond" in key:
        return MANDATE_CATEGORY_FIXED

    return None


def _accumulate_by_category(
    *,
    totals: dict[str, Decimal],
    category: str | None,
    amount: Decimal | None,
) -> None:
    if not category or amount is None:
        return
    if amount <= 0:
        return
    totals[category] = totals.get(category, Decimal("0")) + amount


def _canonical_from_category_totals(category_totals: dict[str, Decimal]) -> dict[str, Decimal]:
    canonical = _empty_canonical()

    canonical["Cash, Deposits & Money Market"] = max(
        category_totals.get(MANDATE_CATEGORY_CASH, Decimal("0")),
        Decimal("0"),
    )

    ig_total = max(category_totals.get(MANDATE_CATEGORY_IG_FIXED, Decimal("0")), Decimal("0"))
    hy_total = max(category_totals.get(MANDATE_CATEGORY_HY_FIXED, Decimal("0")), Decimal("0"))
    fi_total = max(category_totals.get(MANDATE_CATEGORY_FIXED, Decimal("0")), Decimal("0"))

    if ig_total > 0 or hy_total > 0:
        residual = fi_total - (ig_total + hy_total)
        if residual > 0:
            ig_total += residual
    elif fi_total > 0:
        ig_total += fi_total

    canonical["Investment Grade Fixed Income"] = ig_total
    canonical["High Yield Fixed Income"] = hy_total

    us_total = max(category_totals.get(MANDATE_CATEGORY_US_EQUITIES, Decimal("0")), Decimal("0"))
    non_us_total = max(category_totals.get(MANDATE_CATEGORY_NON_US_EQUITIES, Decimal("0")), Decimal("0"))
    global_total = max(category_totals.get(MANDATE_CATEGORY_GLOBAL_EQUITIES, Decimal("0")), Decimal("0"))
    equities_total = max(category_totals.get(MANDATE_CATEGORY_EQUITIES, Decimal("0")), Decimal("0"))

    if global_total > 0:
        us_total += global_total * _GLOBAL_EQUITY_US_WEIGHT
        non_us_total += global_total * _GLOBAL_EQUITY_NON_US_WEIGHT

    if equities_total > 0:
        if us_total > 0 or non_us_total > 0:
            residual = equities_total - (us_total + non_us_total)
            if residual > 0:
                non_us_total += residual
        else:
            us_total += equities_total * _GLOBAL_EQUITY_US_WEIGHT
            non_us_total += equities_total * _GLOBAL_EQUITY_NON_US_WEIGHT

    canonical["US Equities"] = us_total
    canonical["Non US Equities"] = non_us_total

    canonical["Private Equity"] = max(
        category_totals.get(MANDATE_CATEGORY_PRIVATE_EQUITY, Decimal("0")),
        Decimal("0"),
    )
    canonical["Real Estate"] = max(
        category_totals.get(MANDATE_CATEGORY_REAL_ESTATE, Decimal("0")),
        Decimal("0"),
    )
    canonical["Other Investments"] = max(
        category_totals.get(MANDATE_CATEGORY_OTHER_INVESTMENTS, Decimal("0")),
        Decimal("0"),
    )

    return canonical


def canonical_breakdown_from_payload(
    *,
    payload: dict | list | None,
    ending_value: Decimal | None,
    bank_code: str | None,
    account_type: str | None,
    fallback_asset_class: str | None = None,
) -> dict[str, Decimal]:
    if isinstance(payload, dict):
        stored = extract_canonical_breakdown(payload)
        if stored:
            return stored

    category_totals: dict[str, Decimal] = {}

    if isinstance(payload, dict):
        iterable: Iterable[tuple[Any, Any]] = [
            (label, row)
            for label, row in payload.items()
            if not str(label).startswith("__")
        ]
    elif isinstance(payload, list):
        iterable = [
            (
                row.get("asset_class") or row.get("name") or row.get("label") or "",
                row,
            )
            for row in payload
            if isinstance(row, dict)
        ]
    else:
        iterable = []

    is_mandate = str(account_type or "").strip().lower() == "mandato"

    for raw_label, raw_payload in iterable:
        amount_raw, unit = _extract_amount_and_unit(raw_payload)
        amount = _convert_amount_by_unit(value=amount_raw, unit=unit, ending_value=ending_value)
        if amount is None or amount <= 0:
            if amount is not None and amount < Decimal("-1"):
                logger.warning(
                    "Negative amount discarded from canonical breakdown: label=%r amount=%s",
                    raw_label,
                    amount,
                )
            continue

        if is_mandate:
            category = classify_mandate_asset_label(label=str(raw_label), bank_code=bank_code)
        else:
            category = _category_from_generic_label(label=str(raw_label), bank_code=bank_code)
        _accumulate_by_category(totals=category_totals, category=category, amount=amount)

    canonical = _canonical_from_category_totals(category_totals)

    if all(value <= 0 for value in canonical.values()):
        fallback_amount = ending_value if ending_value is not None and ending_value > 0 else Decimal("0")
        fallback_key = str(fallback_asset_class or "").strip().upper()
        if fallback_amount > 0 and fallback_key in {"PE", "RE", "OI"}:
            if fallback_key == "PE":
                canonical["Private Equity"] = fallback_amount
            elif fallback_key == "RE":
                canonical["Real Estate"] = fallback_amount
            else:
                canonical["Other Investments"] = fallback_amount
        elif fallback_amount > 0 and _is_cash_account_type(account_type):
            canonical["Cash, Deposits & Money Market"] = fallback_amount

    return canonical


def _amount_pct_payload(*, amount: Decimal, ending_value: Decimal | None) -> dict[str, str]:
    pct = Decimal("0")
    if ending_value is not None and ending_value > 0:
        pct = (amount / ending_value) * Decimal("100")
    return {
        "amount": str(amount),
        "pct": str(pct),
    }


def canonical_breakdown_payload(
    *,
    canonical_amounts: dict[str, Decimal],
    ending_value: Decimal | None,
) -> dict[str, dict[str, str]]:
    return {
        label: _amount_pct_payload(
            amount=max(canonical_amounts.get(label, Decimal("0")), Decimal("0")),
            ending_value=ending_value,
        )
        for label in CANONICAL_ASSET_ORDER
    }


def derived_breakdown_amounts(canonical_amounts: dict[str, Decimal]) -> dict[str, Decimal]:
    ig = max(canonical_amounts.get("Investment Grade Fixed Income", Decimal("0")), Decimal("0"))
    hy = max(canonical_amounts.get("High Yield Fixed Income", Decimal("0")), Decimal("0"))
    us = max(canonical_amounts.get("US Equities", Decimal("0")), Decimal("0"))
    non_us = max(canonical_amounts.get("Non US Equities", Decimal("0")), Decimal("0"))
    pe = max(canonical_amounts.get("Private Equity", Decimal("0")), Decimal("0"))
    re_ = max(canonical_amounts.get("Real Estate", Decimal("0")), Decimal("0"))
    oi = max(canonical_amounts.get("Other Investments", Decimal("0")), Decimal("0"))

    return {
        "Fixed Income": ig + hy,
        "Equities": us + non_us,
        "Non US Equities": non_us,
        "Global Equities": (us * _GLOBAL_EQUITY_US_WEIGHT) + (non_us * _GLOBAL_EQUITY_NON_US_WEIGHT),
        "Alternativos": pe + re_ + oi,
    }


def derived_breakdown_payload(
    *,
    canonical_amounts: dict[str, Decimal],
    ending_value: Decimal | None,
) -> dict[str, dict[str, str]]:
    amounts = derived_breakdown_amounts(canonical_amounts)
    return {
        label: _amount_pct_payload(amount=max(amounts.get(label, Decimal("0")), Decimal("0")), ending_value=ending_value)
        for label in DERIVED_ASSET_ORDER
    }


def instrument_breakdown_payload(
    *,
    instrument_amounts: dict[str, Decimal],
    ending_value: Decimal | None,
) -> dict[str, dict[str, str]]:
    payload: dict[str, dict[str, str]] = {}
    for name, amount in instrument_amounts.items():
        amount_clean = max(to_decimal(amount) or Decimal("0"), Decimal("0"))
        payload[str(name)] = _amount_pct_payload(amount=amount_clean, ending_value=ending_value)
    return payload


def extract_canonical_breakdown(payload: dict | list | None) -> dict[str, Decimal]:
    if not isinstance(payload, dict):
        return {}
    raw = payload.get(CANONICAL_BREAKDOWN_KEY)
    if not isinstance(raw, dict):
        return {}

    parsed = _empty_canonical()
    for label in CANONICAL_ASSET_ORDER:
        row = raw.get(label)
        amount, _ = _extract_amount_and_unit(row if isinstance(row, dict) else {"amount": row})
        if amount is None:
            if isinstance(row, dict):
                amount = to_decimal(row.get("amount"))
        parsed[label] = max(amount or Decimal("0"), Decimal("0"))
    return parsed


def extract_derived_breakdown(payload: dict | list | None) -> dict[str, Decimal]:
    if not isinstance(payload, dict):
        return {}
    raw = payload.get(DERIVED_BREAKDOWN_KEY)
    if not isinstance(raw, dict):
        return {}

    parsed: dict[str, Decimal] = {}
    for label in DERIVED_ASSET_ORDER:
        row = raw.get(label)
        amount = None
        if isinstance(row, dict):
            amount = to_decimal(row.get("amount"))
            if amount is None:
                amount, _ = _extract_amount_and_unit(row)
        else:
            amount = to_decimal(row)
        if amount is not None:
            parsed[label] = max(amount, Decimal("0"))
    return parsed


def extract_instrument_breakdown(payload: dict | list | None) -> dict[str, Decimal]:
    if not isinstance(payload, dict):
        return {}
    raw = payload.get(INSTRUMENT_BREAKDOWN_KEY)
    if not isinstance(raw, dict):
        return {}

    parsed: dict[str, Decimal] = {}
    for name, row in raw.items():
        if isinstance(row, dict):
            amount = to_decimal(row.get("amount"))
            if amount is None:
                amount, _ = _extract_amount_and_unit(row)
        else:
            amount = to_decimal(row)
        if amount is None:
            continue
        parsed[str(name)] = max(amount, Decimal("0"))
    return parsed


def extract_fi_metrics(payload: dict | list | None) -> dict[str, dict[str, str]]:
    if not isinstance(payload, dict):
        return {}

    metrics = payload.get(FI_METRICS_KEY)
    if not isinstance(metrics, dict):
        metrics = payload.get(LEGACY_MANDATE_METRICS_KEY)
        if not isinstance(metrics, dict):
            return {}

    out: dict[str, dict[str, str]] = {}
    duration = metrics.get("fixed_income_duration")
    if isinstance(duration, dict):
        duration_val = to_decimal(duration.get("value"))
        if duration_val is not None:
            out["fixed_income_duration"] = {
                "value": str(duration_val),
                "unit": str(duration.get("unit") or "years"),
            }
    else:
        duration_val = to_decimal(duration)
        if duration_val is not None:
            out["fixed_income_duration"] = {"value": str(duration_val), "unit": "years"}

    yld = metrics.get("fixed_income_yield")
    if isinstance(yld, dict):
        yld_val = to_decimal(yld.get("value"))
        if yld_val is not None:
            out["fixed_income_yield"] = {
                "value": str(yld_val),
                "unit": str(yld.get("unit") or "%"),
            }
    else:
        yld_val = to_decimal(yld)
        if yld_val is not None:
            out["fixed_income_yield"] = {"value": str(yld_val), "unit": "%"}

    return out


def compose_asset_allocation_payload(
    *,
    raw_payload: dict | list | None,
    canonical_amounts: dict[str, Decimal],
    ending_value: Decimal | None,
    instrument_amounts: dict[str, Decimal] | None = None,
    fi_metrics: dict[str, dict[str, str]] | None = None,
) -> dict:
    base: dict[str, Any]
    if isinstance(raw_payload, dict):
        base = dict(raw_payload)
    elif isinstance(raw_payload, list):
        base = {RAW_LIST_KEY: raw_payload}
    else:
        base = {}

    base[CANONICAL_BREAKDOWN_KEY] = canonical_breakdown_payload(
        canonical_amounts=canonical_amounts,
        ending_value=ending_value,
    )
    base[DERIVED_BREAKDOWN_KEY] = derived_breakdown_payload(
        canonical_amounts=canonical_amounts,
        ending_value=ending_value,
    )

    if instrument_amounts:
        base[INSTRUMENT_BREAKDOWN_KEY] = instrument_breakdown_payload(
            instrument_amounts=instrument_amounts,
            ending_value=ending_value,
        )
    elif INSTRUMENT_BREAKDOWN_KEY in base:
        base.pop(INSTRUMENT_BREAKDOWN_KEY, None)

    if fi_metrics:
        base[FI_METRICS_KEY] = fi_metrics
    elif FI_METRICS_KEY not in base:
        legacy = extract_fi_metrics(base)
        if legacy:
            base[FI_METRICS_KEY] = legacy

    return base


def mandate_breakdown_from_canonical(canonical_amounts: dict[str, Decimal], *, include_cash: bool) -> dict[str, float]:
    cash = float(max(canonical_amounts.get("Cash, Deposits & Money Market", Decimal("0")), Decimal("0")))
    ig = float(max(canonical_amounts.get("Investment Grade Fixed Income", Decimal("0")), Decimal("0")))
    hy = float(max(canonical_amounts.get("High Yield Fixed Income", Decimal("0")), Decimal("0")))
    us = max(canonical_amounts.get("US Equities", Decimal("0")), Decimal("0"))
    non_us = max(canonical_amounts.get("Non US Equities", Decimal("0")), Decimal("0"))
    equities = float(us + non_us)

    return {
        "Cash, Deposits & Money Market": cash if include_cash else 0.0,
        "Investment Grade Fixed Income": ig,
        "High Yield Fixed Income": hy,
        "Equities": equities,
    }


def personal_breakdown_from_canonical(canonical_amounts: dict[str, Decimal]) -> dict[str, float]:
    return {
        "Cash": float(max(canonical_amounts.get("Cash, Deposits & Money Market", Decimal("0")), Decimal("0"))),
        "IG Fixed income": float(max(canonical_amounts.get("Investment Grade Fixed Income", Decimal("0")), Decimal("0"))),
        "HY Fixed income": float(max(canonical_amounts.get("High Yield Fixed Income", Decimal("0")), Decimal("0"))),
        "US equities": float(max(canonical_amounts.get("US Equities", Decimal("0")), Decimal("0"))),
        "Non-US equities": float(max(canonical_amounts.get("Non US Equities", Decimal("0")), Decimal("0"))),
        "PE": float(max(canonical_amounts.get("Private Equity", Decimal("0")), Decimal("0"))),
        "RE": float(max(canonical_amounts.get("Real Estate", Decimal("0")), Decimal("0"))),
        "Other investments": float(max(canonical_amounts.get("Other Investments", Decimal("0")), Decimal("0"))),
    }
