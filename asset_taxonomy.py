from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


_DICTIONARY_PATH = Path(__file__).resolve().parent / "asset_bucket_dictionary.json"


@lru_cache
def load_asset_taxonomy() -> dict:
    return json.loads(_DICTIONARY_PATH.read_text(encoding="utf-8"))


def asset_bucket_order() -> list[str]:
    return list(load_asset_taxonomy().get("order", []))


def asset_bucket_colors() -> dict[str, str]:
    buckets = load_asset_taxonomy().get("buckets", {})
    return {
        str(bucket): str(meta.get("color") or "")
        for bucket, meta in buckets.items()
    }


def asset_bucket_color(bucket: str) -> str:
    return asset_bucket_colors().get(str(bucket), "")


def asset_bucket_series() -> list[tuple[str, str, str]]:
    return [
        (bucket, bucket, asset_bucket_color(bucket))
        for bucket in asset_bucket_order()
    ]


def coarse_asset_bucket_series() -> list[tuple[str, str, str]]:
    return [
        ("Cash, Deposits & Money Market", "Caja", asset_bucket_color("Caja")),
        ("Fixed Income", "Renta Fija", asset_bucket_color("RF IG Short")),
        ("Equities", "Renta Variable", asset_bucket_color("RV DM")),
    ]


def default_chart_color_sequence() -> list[str]:
    preferred_buckets = [
        "RV DM",
        "RF IG Short",
        "RV EM",
        "RF IG Long",
        "HY",
        "Caja",
        "Alternativos",
        "Real Estate",
    ]
    return [
        color
        for color in (asset_bucket_color(bucket) for bucket in preferred_buckets)
        if color
    ]


def classify_etf_asset_bucket(name: str, *, normalized_name: str | None = None) -> str:
    spec = load_asset_taxonomy()
    aliases = {
        str(key).strip().upper(): str(value).strip()
        for key, value in spec.get("instrument_aliases", {}).items()
        if str(key).strip() and str(value).strip()
    }
    compact_aliases = {
        re.sub(r"[^A-Z0-9]", "", key): value
        for key, value in aliases.items()
    }

    candidates = [str(normalized_name or "").strip(), str(name or "").strip()]
    for candidate in candidates:
        if not candidate:
            continue
        upper = candidate.upper()
        compact = re.sub(r"[^A-Z0-9]", "", upper)
        if upper in aliases:
            return aliases[upper]
        if compact in compact_aliases:
            return compact_aliases[compact]

    haystack = " ".join(value for value in candidates if value).lower()
    for rule in spec.get("keyword_rules", []):
        bucket = str(rule.get("bucket") or "").strip()
        tokens = [str(token).lower() for token in rule.get("tokens", []) if str(token).strip()]
        if bucket and any(token in haystack for token in tokens):
            return bucket

    return str(spec.get("default_bucket") or "RV DM")
