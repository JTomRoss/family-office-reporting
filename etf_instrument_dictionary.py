from __future__ import annotations

import re

INSTRUMENT_NAME_MAP: dict[str, str] = {
    "IWDA": "IWDA",
    "ISHARES CORE MSCI WORLD": "IWDA",
    "P ISHARES CORE MSCI WORLD": "IWDA",
    "IEMA": "IEMA",
    "ISHARES MSCI EM-ACC": "IEMA",
    "ISHARES MSCI EM ACC": "IEMA",
    "P ISHARES MSCI EM-ACC": "IEMA",
    "IHYA": "IHYA",
    "ISHARES USD HY CORP USD ACC": "IHYA",
    "ISHARES USD HIGH YIELD CORP BOND": "IHYA",
    "P ISHARES USD HY CORP USD ACC": "IHYA",
    "VDCA": "VDCA",
    "VAND USDCP1-3 USDA": "VDCA",
    "VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF": "VDCA",
    "VDPA": "VDPA",
    "VANG USDCPBD USDA": "VDPA",
    "VANG USDCPBD USDA ACC": "VDPA",
    "VANGUARD USD CORPORATE BOND UCITS ETF": "VDPA",
    "VUCP": "VDPA",
    "USD CORPORATE BOND UCITS ETF": "VDPA",
    "USD CORPORATE BOND UCITS ETF (VUCP)": "VDPA",
    "VANGUARD FUNDS PLC-VANGUARD US CMN CLASS ETF": "VDCA",
    "VANGUARD FUNDS PLC - VANGUARD CMN CLASS ETF STAMP": "VDCA",
    "SPDR": "SPDR",
    "SPDR BLOOMBERG 1-10 YEAR U.S.": "SPDR",
    "SPDR BLOOMBERG 1-10 YEAR U.S": "SPDR",
    "SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG": "SPDR",
    "JPM LI-LIQ LVNAV FD - USD - W -": "Money Market",
    "P JPM LI-LIQ LVNAV FD - USD - W -": "Money Market",
    "PROCEEDS FROM PENDING SALES": "Money Market",
    "MSCI WORLD INDEX FUND (ISHARES)": "IWDA",
    "MSCI EMERGING MARKETS INDEX FUND (ISHARES)": "IEMA",
    "ISHARES III PLC-ISHARES MSCI EMERGING MARKETS ETF": "IEMA",
    "MARKIT IBOXX USD LIQUID HY CAPPED INDEX FUND (ISHARES)": "IHYA",
    "ISHARES II PLC-ISHARES $ HIGH YIELD CORP BOND UCITS ETF": "IHYA",
}

INSTRUMENT_ORDER = ["IWDA", "IEMA", "VDCA", "VDPA", "SPDR", "IHYA", "Money Market"]
CASH_INSTRUMENTS = {"Money Market"}


def normalize_etf_instrument(name: str) -> str:
    if not name:
        return "Other"
    upper = name.strip().upper()
    upper_compact = re.sub(r"[^A-Z0-9]", "", upper)
    if upper in INSTRUMENT_NAME_MAP:
        return INSTRUMENT_NAME_MAP[upper]
    if "VDPA" in upper_compact:
        return "VDPA"
    if "USDCPBD" in upper_compact and "USDA" in upper_compact:
        return "VDPA"
    if "SPDR" in upper_compact and "BLOOMBERG" in upper_compact:
        return "SPDR"
    low = name.lower()
    if any(
        kw in low
        for kw in (
            "sweep",
            "liquidity",
            "money market",
            "cash",
            "dep\u00f3sito",
            "deposito",
            "deposit",
            "deposits",
            "li-liq",
        )
    ):
        return "Money Market"
    return INSTRUMENT_NAME_MAP.get(upper, name)
