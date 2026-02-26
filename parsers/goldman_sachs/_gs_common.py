"""
Goldman Sachs – Shared extraction helpers for ETF and Custody parsers.

CRITICAL NOTE
=============
pdfplumber **cannot** extract text from Goldman Sachs PDFs (they appear as
empty pages — 0 text chars, 0 images via pdfplumber).
PyMuPDF (``fitz``) extracts text perfectly.  All Goldman Sachs parsers
MUST use ``fitz`` for text extraction.

FORMAT OVERVIEW
===============
Both ETF and Mandato/Custody statements follow the Goldman Sachs standard layout:

  Page 1 – Cover: period, portfolio number, team contacts
  Page 2 – General Info: portfolio information, sub-portfolios list
  Page 3 – Overview: total portfolio value, asset allocation, portfolio activity
  Page 4+ – US Tax Summary
  Page N  – Asset Strategy Analysis (per-instrument market value + %)
  Page N+ – Statement Detail per sub-portfolio:
              General Info → Overview → Tax Summary → Holdings → Cash Activity
              → Bank Statement → Realized Gains → Income/Expenses → Deposits

Numbers and labels appear on **separate lines** (not on the same line).
"""

from __future__ import annotations

import re
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Number parser
# ---------------------------------------------------------------------------

def parse_usd(raw: str | None) -> Decimal | None:
    """Parse a USD amount like '45,553,310.46' or '(37,100,000.00)'."""
    if not raw:
        return None
    cleaned = raw.strip().replace(" ", "")
    neg = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        neg = True
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        neg = True
        cleaned = cleaned[1:]
    cleaned = cleaned.replace(",", "")
    # Remove trailing % if present
    cleaned = cleaned.rstrip("%").strip()
    if not cleaned:
        return None
    try:
        val = Decimal(cleaned)
        return -val if neg else val
    except (InvalidOperation, ValueError):
        return None


def parse_pct(raw: str | None) -> str | None:
    """Parse a percentage like '58.44 %' or '0.19'."""
    if not raw:
        return None
    cleaned = raw.strip().replace(" ", "").rstrip("%")
    try:
        float(cleaned)
        return cleaned + "%"
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Text extraction via PyMuPDF
# ---------------------------------------------------------------------------

def extract_all_text_fitz(filepath) -> str:
    """Extract full text from a PDF using PyMuPDF (fitz)."""
    import fitz
    doc = fitz.open(str(filepath))
    parts = []
    for page in doc:
        parts.append(page.get_text())
    doc.close()
    return "\n".join(parts)


def extract_page_texts_fitz(filepath) -> list[str]:
    """Return a list of per-page text strings using PyMuPDF."""
    import fitz
    doc = fitz.open(str(filepath))
    texts = [page.get_text() for page in doc]
    doc.close()
    return texts


# ---------------------------------------------------------------------------
# Period extraction
# ---------------------------------------------------------------------------

def extract_period(text: str) -> dict[str, str] | None:
    """Extract 'Period Covering December 01, 2025 to December 31, 2025'."""
    m = re.search(
        r"Period\s+Covering\s+(\w+\s+\d{1,2},?\s+\d{4})\s+to\s+(\w+\s+\d{1,2},?\s+\d{4})",
        text,
    )
    if m:
        return {"start": m.group(1).strip(), "end": m.group(2).strip()}
    # Fallback: "Period Ended December 31, 2025"
    m = re.search(r"Period\s+Ended\s+(\w+\s+\d{1,2},?\s+\d{4})", text)
    if m:
        return {"end": m.group(1).strip()}
    return None


# ---------------------------------------------------------------------------
# Portfolio number extraction
# ---------------------------------------------------------------------------

def extract_portfolio_number(text: str) -> str | None:
    """Extract 'Portfolio No: XXX-XX452-2' pattern."""
    m = re.search(r"Portfolio\s+No:\s*([\w-]+)", text)
    return m.group(1).strip() if m else None


# ---------------------------------------------------------------------------
# Total portfolio & asset allocation (Overview page)
# ---------------------------------------------------------------------------

def extract_overview(text: str) -> dict[str, Any]:
    """Extract total portfolio value, asset allocation, and portfolio activity.

    The overview page has a pattern like:
      TOTAL PORTFOLIO
      45,553,310.46
      PORTFOLIO ASSET ALLOCATION (INCLUDES ACCRUALS)
      ...category name...
      value
      percentage
      ...

    Returns dict with keys: total_portfolio, asset_allocation, portfolio_activity, investment_results
    """
    result: dict[str, Any] = {}
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # ── Total portfolio value ──
    for i, line in enumerate(lines):
        if line == "TOTAL PORTFOLIO" and i + 1 < len(lines):
            val = parse_usd(lines[i + 1])
            if val and val > 1000:  # Filter out "100.00" percentages
                result["total_portfolio"] = val
                break

    # ── Asset allocation ──
    allocation: dict[str, dict[str, Any]] = {}
    in_alloc = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if "PORTFOLIO ASSET ALLOCATION" in line:
            in_alloc = True
            i += 1
            # Skip headers
            while i < len(lines) and lines[i] in ("Market Value", "Percentage"):
                i += 1
            continue

        if in_alloc:
            # Stop at PORTFOLIO ACTIVITY or end of data
            if "PORTFOLIO ACTIVITY" in line:
                in_alloc = False
                break

            # Category lines are ALL CAPS, followed by value + percentage
            # We look for patterns: CATEGORY_NAME, then number, then number
            if (line.isupper() or line.startswith("TOTAL")) and i + 2 < len(lines):
                # Could be a category
                cat_name = line
                val_str = lines[i + 1]
                pct_str = lines[i + 2] if i + 2 < len(lines) else ""

                val = parse_usd(val_str)
                pct = parse_pct(pct_str)

                if val is not None and cat_name not in ("Market Value", "Percentage"):
                    allocation[cat_name] = {
                        "market_value": val,
                        "percentage": pct,
                    }
                    i += 3
                    continue
        i += 1

    if allocation:
        result["asset_allocation"] = allocation

    # ── Portfolio activity ──
    activity: dict[str, Decimal | None] = {}
    in_activity = False
    for i, line in enumerate(lines):
        if "PORTFOLIO ACTIVITY" in line:
            in_activity = True
            continue
        if in_activity:
            if "INVESTMENT RESULTS" in line or "CURRENT MONTH" in line:
                break
            # Pattern: "MARKET VALUE AS OF DECEMBER 01, 2025" then value on next line
            if "MARKET VALUE AS OF" in line and i + 1 < len(lines):
                val = parse_usd(lines[i + 1])
                if "DECEMBER 01" in line or "beginning" in line.lower():
                    activity["opening_value"] = val
                elif "DECEMBER 31" in line or "ending" in line.lower():
                    activity["closing_value"] = val
                else:
                    # Generic - store with the date
                    m = re.search(r"AS OF\s+(\w+\s+\d{1,2},?\s+\d{4})", line)
                    key = m.group(1) if m else line
                    activity[f"value_{key}"] = val
            elif line.startswith("INTEREST RECEIVED") or line == "INTEREST RECEIVED":
                if i + 1 < len(lines):
                    activity["interest_received"] = parse_usd(lines[i + 1])
            elif line.startswith("DIVIDENDS RECEIVED") or line == "DIVIDENDS RECEIVED":
                if i + 1 < len(lines):
                    activity["dividends_received"] = parse_usd(lines[i + 1])
            elif "CHANGE IN MARKET VALUE" in line:
                if i + 1 < len(lines):
                    activity["change_in_value"] = parse_usd(lines[i + 1])

    if activity:
        result["portfolio_activity"] = activity

    # ── Investment results ──
    for i, line in enumerate(lines):
        if "INVESTMENT RESULTS" in line:
            # Look for the row: beginning_mv | net_deposits | results | ending_mv
            # Format varies — try to find "CURRENT MONTH" marker
            for j in range(i + 1, min(i + 15, len(lines))):
                if "CURRENT MONTH" in lines[j]:
                    break
            # Collect numbers between INVESTMENT RESULTS and CURRENT MONTH
            nums = []
            for j in range(i + 1, min(i + 15, len(lines))):
                if lines[j] in ("CURRENT MONTH", "CURRENT YEAR"):
                    break
                val = parse_usd(lines[j])
                if val is not None:
                    nums.append(val)
            if len(nums) >= 4:
                result["investment_results"] = {
                    "beginning_market_value": nums[0],
                    "net_deposits_withdrawals": nums[1],
                    "investment_results": nums[2],
                    "ending_market_value": nums[3],
                }
            break

    return result


# ---------------------------------------------------------------------------
# Tax summary
# ---------------------------------------------------------------------------

def extract_tax_summary(text: str) -> dict[str, Any]:
    """Extract US Tax Summary data (reportable income, realized gains, unrealized)."""
    result: dict[str, Any] = {}
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Look for key items with Month | QTD | YTD pattern
    for i, line in enumerate(lines):
        if "TOTAL REPORTABLE INCOME" in line:
            # The values are typically just before this line in the pattern
            pass
        if "TOTAL REPORTABLE INTEREST" in line:
            # Look backwards for 3 numbers (month, qtd, ytd)
            nums = []
            for j in range(max(0, i - 4), i):
                v = parse_usd(lines[j])
                if v is not None:
                    nums.append(v)
            if len(nums) >= 3:
                result["reportable_interest"] = {
                    "current_month": nums[-3],
                    "quarter_to_date": nums[-2],
                    "year_to_date": nums[-1],
                }

        if "TOTAL REALIZED CAPITAL GAINS" in line:
            nums = []
            for j in range(max(0, i - 4), i):
                v = parse_usd(lines[j])
                if v is not None:
                    nums.append(v)
            if len(nums) >= 3:
                result["realized_capital_gains"] = {
                    "current_month": nums[-3],
                    "quarter_to_date": nums[-2],
                    "year_to_date": nums[-1],
                }

        if "CURRENT UNREALIZED GAIN (LOSS)" in line:
            # Value is just before this line
            for j in range(max(0, i - 2), i):
                v = parse_usd(lines[j])
                if v is not None:
                    result["unrealized_gain_loss"] = v
                    break

    return result


# ---------------------------------------------------------------------------
# Asset Strategy Analysis
# ---------------------------------------------------------------------------

def extract_asset_strategy(text: str) -> list[dict[str, Any]]:
    """Extract asset strategy analysis (per-instrument breakdown).

    Pattern:
      value
      percentage %
      INSTRUMENT NAME

    The instrument name follows the value/percentage pair.
    """
    instruments: list[dict[str, Any]] = []
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    i = 0
    current_category = None
    current_sub = None

    while i < len(lines):
        line = lines[i]

        # Skip page headers/footers
        if any(kw in line for kw in [
            "Period Ended", "Asset Strategy", "Investment Summary",
            "Page ", "Portfolio No:", "Market Value", "as of Dec",
            "Percentage", "of Portfolio"
        ]):
            i += 1
            continue

        # Top-level categories (ALL CAPS, no numbers)
        if line.isupper() and not any(c.isdigit() for c in line) and "TOTAL" not in line:
            # Could be category or sub-category  
            if line in (
                "CASH, DEPOSITS & MONEY MARKET FUNDS", "FIXED INCOME",
                "PUBLIC EQUITY", "CASH"
            ):
                current_category = line
                i += 1
                continue
            elif line in (
                "DEPOSITS & MONEY MARKET FUNDS", "DEPOSITS",
                "INVESTMENT GRADE FIXED INCOME", "OTHER FIXED INCOME",
                "US EQUITY", "GLOBAL EQUITY", "NON-US EQUITY",
                "MONEY MARKET FUNDS",
            ):
                current_sub = line
                i += 1
                continue

        # Look for value + percentage + name pattern
        val = parse_usd(line)
        if val is not None and val > 0 and i + 2 < len(lines):
            pct_line = lines[i + 1]
            name_line = lines[i + 2] if i + 2 < len(lines) else ""

            if "%" in pct_line:
                # This is a value/percentage, but who is it for?
                # In GS format: value, pct%, NAME (or TOTAL line)
                pct = parse_pct(pct_line)
                # Check if name_line is a TOTAL line
                if "TOTAL" in name_line:
                    # This is a subtotal
                    i += 3
                    continue
                name = name_line.strip()
                if name and not name.startswith("Period") and not name.startswith("Page"):
                    instruments.append({
                        "name": name,
                        "category": current_category,
                        "sub_category": current_sub,
                        "market_value": val,
                        "percentage": pct,
                    })
                    i += 3
                    continue

        i += 1

    return instruments


# ---------------------------------------------------------------------------
# Holdings extraction
# ---------------------------------------------------------------------------

def extract_holdings(page_texts: list[str]) -> list[dict[str, Any]]:
    """Extract holdings from Statement Detail / Holdings pages.

    Holdings appear per asset class with this structure (lines are separate):
      ASSET_CLASS_NAME
      column headers...
      INSTRUMENT_NAME
      quantity | market_price | market_value | accrued | unit_cost | cost_basis | unrealized_gl | yield | income
      DESCRIPTION LINE(s)

    The key challenge is matching instrument names to their numeric data since
    they're on different lines.
    """
    holdings: list[dict[str, Any]] = []

    # Find pages that contain "Holdings" in footer
    for page_text in page_texts:
        lines = [l.strip() for l in page_text.splitlines() if l.strip()]

        # Check if this is a Holdings page
        is_holdings = any("Holdings" in l for l in lines[-10:]) if len(lines) > 10 else False
        if not is_holdings:
            continue

        current_class = None
        current_sub = None
        i = 0

        while i < len(lines):
            line = lines[i]

            # Skip page headers/footers
            if any(kw in line for kw in [
                "Period Ended", "Statement Detail", "Page ",
                "Portfolio No:", "Quantity", "Market Price",
                "Market Value", "Accrued Income", "Unit Cost",
                "Adjusted Cost", "Original Cost", "Unrealized",
                "Gain (Loss)", "Yield to Maturity", "Current Yield",
                "Estimated", "Annual Income", "Dividend", "Yield",
                "Current Face", "in Percentage", "Cost Basis",
                "Securities and investments",
                "This is a bank deposit",
                "at the end of this",
            ]):
                i += 1
                continue

            # Asset class headers
            if line in (
                "CASH, DEPOSITS & MONEY MARKET FUNDS",
                "FIXED INCOME", "PUBLIC EQUITY",
            ):
                current_class = line
                i += 1
                continue

            # Sub-class headers
            if line in (
                "DEPOSITS & MONEY MARKET FUNDS", "DEPOSITS",
                "INVESTMENT GRADE FIXED INCOME", "OTHER FIXED INCOME",
                "US EQUITY", "GLOBAL EQUITY", "NON-US EQUITY",
            ):
                current_sub = line
                i += 1
                continue

            # Check for "(Continued)" variants
            if "(Continued)" in line:
                i += 1
                continue

            # TOTAL lines — capture subtotals
            if line.startswith("TOTAL"):
                i += 1
                continue

            # Instrument detection: look for a named instrument followed by numbers
            # Named instruments are mixed case or specific patterns
            if not _is_number_line(line) and len(line) > 3 and not line.startswith("A\n"):
                # Could be an instrument name — look ahead for numbers
                numbers = []
                desc_lines = []
                j = i + 1
                while j < len(lines) and j < i + 20:
                    if _is_number_line(lines[j]):
                        v = parse_usd(lines[j])
                        if v is not None:
                            numbers.append(v)
                    elif lines[j].startswith("TOTAL") or lines[j] in (
                        "CASH, DEPOSITS & MONEY MARKET FUNDS",
                        "FIXED INCOME", "PUBLIC EQUITY",
                        "INVESTMENT GRADE FIXED INCOME",
                        "OTHER FIXED INCOME",
                        "US EQUITY", "GLOBAL EQUITY", "NON-US EQUITY",
                    ):
                        break
                    elif any(kw in lines[j] for kw in [
                        "Period Ended", "Holdings", "Page ",
                    ]):
                        break
                    elif not _is_number_line(lines[j]):
                        # Could be description continuation
                        desc_lines.append(lines[j])
                        # Check if next line switches to a new instrument
                        if j + 1 < len(lines) and _is_number_line(lines[j + 1]):
                            pass  # description before more numbers — continue
                        elif numbers:
                            break  # We have numbers and hit non-number — stop
                    j += 1

                if len(numbers) >= 3:
                    holding = {
                        "name": line,
                        "asset_class": current_class,
                        "sub_class": current_sub,
                    }
                    if desc_lines:
                        holding["description"] = " ".join(desc_lines)

                    # Map numbers based on asset class
                    if current_class == "CASH, DEPOSITS & MONEY MARKET FUNDS":
                        # quantity, price, market_value, accrued?, cost, cost_basis, unrealized, yield, income
                        _assign_cash_numbers(holding, numbers)
                    elif current_class == "FIXED INCOME":
                        _assign_fi_numbers(holding, numbers)
                    elif current_class == "PUBLIC EQUITY":
                        _assign_equity_numbers(holding, numbers)
                    else:
                        _assign_generic_numbers(holding, numbers)

                    holdings.append(holding)
                    i = j
                    continue

            i += 1

    return holdings


def _is_number_line(line: str) -> bool:
    """Check if a line is a numeric value."""
    cleaned = line.strip().replace(",", "").replace(" ", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        cleaned = cleaned[1:]
    cleaned = cleaned.rstrip("%")
    if not cleaned:
        return False
    try:
        float(cleaned)
        return True
    except ValueError:
        return False


def _assign_cash_numbers(holding: dict, nums: list[Decimal]) -> None:
    """Assign numbers for cash/deposit holdings."""
    if len(nums) >= 6:
        holding["quantity"] = nums[0]
        holding["market_price"] = nums[1]
        holding["market_value"] = nums[2]
        holding["unit_cost"] = nums[3]
        holding["cost_basis"] = nums[4]
        holding["unrealized_gl"] = nums[5]
    elif len(nums) >= 3:
        holding["market_value"] = nums[0]
        holding["cost_basis"] = nums[1]
        holding["unrealized_gl"] = nums[2]


def _assign_fi_numbers(holding: dict, nums: list[Decimal]) -> None:
    """Assign numbers for fixed income holdings."""
    if len(nums) >= 6:
        holding["quantity"] = nums[0]
        holding["market_price"] = nums[1]
        holding["market_value"] = nums[2]
        holding["unit_cost"] = nums[3]
        holding["cost_basis"] = nums[4]
        holding["unrealized_gl"] = nums[5]
    elif len(nums) >= 3:
        holding["market_value"] = nums[0]
        holding["cost_basis"] = nums[1]
        holding["unrealized_gl"] = nums[2]


def _assign_equity_numbers(holding: dict, nums: list[Decimal]) -> None:
    """Assign numbers for public equity holdings."""
    if len(nums) >= 6:
        holding["quantity"] = nums[0]
        holding["market_price"] = nums[1]
        holding["market_value"] = nums[2]
        holding["unit_cost"] = nums[3]
        holding["cost_basis"] = nums[4]
        holding["unrealized_gl"] = nums[5]
    elif len(nums) >= 3:
        holding["market_value"] = nums[0]
        holding["cost_basis"] = nums[1]
        holding["unrealized_gl"] = nums[2]


def _assign_generic_numbers(holding: dict, nums: list[Decimal]) -> None:
    """Fallback number assignment."""
    if nums:
        holding["market_value"] = nums[0]
    if len(nums) >= 2:
        holding["cost_basis"] = nums[1]
    if len(nums) >= 3:
        holding["unrealized_gl"] = nums[2]


# ---------------------------------------------------------------------------
# Sub-portfolio extraction (for multi-portfolio statements like Mandato)
# ---------------------------------------------------------------------------

def extract_sub_portfolios(page2_text: str) -> list[dict[str, str]]:
    """Extract sub-portfolio list from General Information page.

    Pattern:
      PORTFOLIO NAME    PORTFOLIO NUMBER    PAGE
      BOATVIEW LTD ADVISORY    XXX-XX063-1    9
    """
    portfolios: list[dict[str, str]] = []
    lines = [l.strip() for l in page2_text.splitlines() if l.strip()]

    # Find lines with portfolio number pattern
    num_pattern = re.compile(r"^(XXX-\w+-\d+)$")
    name_buffer = None

    for i, line in enumerate(lines):
        # Portfolio numbers appear on their own line
        m = num_pattern.match(line)
        if m:
            port_num = m.group(1)
            # Name is typically the line before
            if i > 0 and not num_pattern.match(lines[i - 1]):
                name = lines[i - 1]
                if name not in ("PORTFOLIO NAME", "PORTFOLIO NUMBER"):
                    # Page number should be next line
                    page_num = None
                    if i + 1 < len(lines):
                        try:
                            page_num = int(lines[i + 1])
                        except ValueError:
                            pass
                    portfolios.append({
                        "name": name,
                        "number": port_num,
                        "page": str(page_num) if page_num else "",
                    })

    return portfolios
