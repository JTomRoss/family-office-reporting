from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

MANDATE_CATEGORY_CASH = "cash"
MANDATE_CATEGORY_IG_FIXED = "ig_fixed_income"
MANDATE_CATEGORY_HY_FIXED = "hy_fixed_income"
MANDATE_CATEGORY_FIXED = "fixed_income"
MANDATE_CATEGORY_US_EQUITIES = "us_equities"
MANDATE_CATEGORY_NON_US_EQUITIES = "non_us_equities"
MANDATE_CATEGORY_GLOBAL_EQUITIES = "global_equities"
MANDATE_CATEGORY_EQUITIES = "equities"
MANDATE_CATEGORY_PRIVATE_EQUITY = "private_equity"
MANDATE_CATEGORY_REAL_ESTATE = "real_estate"
MANDATE_CATEGORY_OTHER_INVESTMENTS = "other_investments"

_DICTIONARY_PATH = Path(__file__).resolve().parent / "mandate_report_dictionary.json"


def _normalize_text(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _tokenize(value: str | None) -> str:
    raw = str(value or "").lower().replace("\xa0", " ")
    return re.sub(r"\s+", " ", raw).strip()


@lru_cache
def _load_dictionary() -> dict:
    return json.loads(_DICTIONARY_PATH.read_text(encoding="utf-8-sig"))


def _rule_matches(rule: dict, label_raw: str, label_norm: str) -> bool:
    contains_any = [str(token).lower() for token in rule.get("contains_any", []) if str(token).strip()]
    contains_all = [str(token).lower() for token in rule.get("contains_all", []) if str(token).strip()]
    exclude_any = [str(token).lower() for token in rule.get("exclude_any", []) if str(token).strip()]

    raw_l = label_raw.lower()
    if contains_any and not any(token in raw_l for token in contains_any):
        # Fallback compact check for tokens without spaces/punctuation.
        if not any(_normalize_text(token) in label_norm for token in contains_any):
            return False
    if contains_all and not all(
        (token in raw_l) or (_normalize_text(token) in label_norm)
        for token in contains_all
    ):
        return False
    if exclude_any and any(
        (token in raw_l) or (_normalize_text(token) in label_norm)
        for token in exclude_any
    ):
        return False
    return True


def classify_mandate_asset_label(*, label: str | None, bank_code: str | None = None) -> str | None:
    """Return a mandate category token for a raw mandate allocation label."""
    label_raw = _tokenize(label)
    label_norm = _normalize_text(label)
    if not label_norm:
        return None

    spec = _load_dictionary()

    ignore_tokens = [
        _normalize_text(token)
        for token in spec.get("ignore_contains", [])
        if str(token).strip()
    ]
    if any(token and token in label_norm for token in ignore_tokens):
        return None

    exact_map = {
        _normalize_text(key): str(value)
        for key, value in (spec.get("exact_map", {}) or {}).items()
        if _normalize_text(key)
    }
    if label_norm in exact_map:
        return exact_map[label_norm]

    if any(token in label_norm for token in ("otherinvestment", "assetallocationinvestment", "miscellaneous", "hedgefund")):
        return MANDATE_CATEGORY_OTHER_INVESTMENTS

    bank_key = str(bank_code or "").strip().lower()
    bank_overrides = (spec.get("bank_overrides", {}) or {}).get(bank_key, {})
    for rule in bank_overrides.get("contains_rules", []):
        if not isinstance(rule, dict):
            continue
        if _rule_matches(rule, label_raw, label_norm):
            category = str(rule.get("category") or "").strip()
            if category:
                return category

    for rule in spec.get("contains_rules", []):
        if not isinstance(rule, dict):
            continue
        if _rule_matches(rule, label_raw, label_norm):
            category = str(rule.get("category") or "").strip()
            if category:
                return category

    return None
