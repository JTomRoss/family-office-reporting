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
from functools import lru_cache
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
# OCR fallback for garbled font encoding
# ---------------------------------------------------------------------------
# Some GS PDFs use custom font encoding that PyMuPDF cannot decode,
# producing garbled text.  When detected, we render pages as images
# and run EasyOCR (optional dependency).

_GARBLED_KEYWORDS = ("goldman", "portfolio", "period", "statement", "market value", "overview")
_MAX_OCR_PAGES = 10
_OCR_DPI = 200

_ocr_reader = None
_ocr_available: bool | None = None


def _is_garbled_text(text: str, min_chars: int = 100) -> bool:
    """Return True if text has enough characters but none of the expected GS keywords."""
    if len(text.strip()) < min_chars:
        return False
    text_lower = text.lower()
    return not any(kw in text_lower for kw in _GARBLED_KEYWORDS)


def _get_ocr_reader():
    """Lazy-initialize and cache the EasyOCR reader singleton."""
    global _ocr_reader, _ocr_available
    if _ocr_available is False:
        return None
    if _ocr_reader is not None:
        return _ocr_reader
    try:
        import easyocr
        logger.info("Initializing EasyOCR reader for OCR fallback...")
        _ocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
        _ocr_available = True
        return _ocr_reader
    except ImportError:
        logger.warning("easyocr not installed — OCR fallback disabled")
        _ocr_available = False
        return None
    except Exception as exc:
        logger.warning("Failed to initialize OCR: %s", exc)
        _ocr_available = False
        return None


def _ocr_page_text(page, dpi: int = _OCR_DPI) -> str:
    """Extract text from a PyMuPDF page via EasyOCR (render → OCR)."""
    reader = _get_ocr_reader()
    if reader is None:
        return ""
    try:
        import numpy as np
        from PIL import Image
        import io as _io

        pix = page.get_pixmap(dpi=dpi)
        img = Image.open(_io.BytesIO(pix.tobytes("png")))
        results = reader.readtext(np.array(img), detail=0, paragraph=False)
        return "\n".join(results)
    except Exception as exc:
        logger.warning("OCR page extraction failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Text extraction via PyMuPDF (with automatic OCR fallback)
# ---------------------------------------------------------------------------

def extract_all_text_fitz(filepath) -> str:
    """Extract full text from a PDF using PyMuPDF, with OCR fallback."""
    return "\n".join(extract_page_texts_fitz(filepath))


def extract_page_texts_fitz(filepath) -> list[str]:
    """Return per-page text using PyMuPDF, with OCR fallback for garbled fonts."""
    import fitz
    doc = fitz.open(str(filepath))
    texts = [page.get_text() for page in doc]

    sample = "\n".join(texts[:min(3, len(texts))])
    if _is_garbled_text(sample):
        n_ocr = min(_MAX_OCR_PAGES, len(doc))
        logger.info("Garbled font encoding in %s — OCR fallback on first %d/%d pages",
                     filepath, n_ocr, len(doc))
        for i in range(n_ocr):
            ocr_text = _ocr_page_text(doc[i])
            if ocr_text:
                texts[i] = ocr_text

    doc.close()
    return texts


@lru_cache(maxsize=32)
def extract_detection_text(filepath_str: str) -> tuple[str, int]:
    """Extract text from first 3 pages for parser detection, with OCR fallback.

    Returns (combined_text, total_page_count).  Cached per filepath.
    """
    import fitz
    doc = fitz.open(filepath_str)
    n_pages = len(doc)
    if n_pages == 0:
        doc.close()
        return ("", 0)

    texts = []
    for i in range(min(3, n_pages)):
        texts.append(doc[i].get_text())

    combined = "\n".join(texts)
    if _is_garbled_text(combined):
        logger.info("Garbled text in detection for %s — applying OCR on first 3 pages",
                     filepath_str)
        for i in range(len(texts)):
            ocr_text = _ocr_page_text(doc[i])
            if ocr_text:
                texts[i] = ocr_text
        combined = "\n".join(texts)

    doc.close()
    return (combined, n_pages)


# ---------------------------------------------------------------------------
# Period extraction
# ---------------------------------------------------------------------------

def extract_period(text: str) -> dict[str, str] | None:
    """Extract 'Period Covering December 01, 2025 to December 31, 2025'."""
    m = re.search(
        r"Period\s+Covering\s+(\w+\s+\d{1,2},?\s*\d{4})\s+to\s+(\w+\s+\d{1,2},?\s*\d{4})",
        text,
    )
    if m:
        return {"start": m.group(1).strip(), "end": m.group(2).strip()}
    # Fallback: "Period Ended December 31, 2025" (OCR may omit space after comma)
    m = re.search(r"Period\s+Ended\s+(\w+\s+\d{1,2},?\s*\d{4})", text)
    if m:
        return {"end": m.group(1).strip()}
    return None


# ---------------------------------------------------------------------------
# Portfolio number extraction
# ---------------------------------------------------------------------------

def extract_portfolio_number(text: str) -> str | None:
    """Extract 'Portfolio No: XXX-XX452-2' → '452-2' (strip mask)."""
    m = re.search(r"Portfolio\s+No:\s*([\w-]+)", text)
    if not m:
        return None
    raw = m.group(1).strip()
    # GS PDFs mask account numbers with XXX-XX prefix; strip it
    stripped = re.sub(r"^[Xx]{2,}-[Xx]{2,}", "", raw)
    return stripped if stripped else raw


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
            else:
                # Fallback for OCR: numbers appear AFTER "CURRENT MONTH"
                for j in range(i + 1, min(i + 25, len(lines))):
                    if "CURRENT MONTH" in lines[j]:
                        after_nums = []
                        for k in range(j + 1, min(j + 8, len(lines))):
                            v = parse_usd(lines[k])
                            if v is not None:
                                after_nums.append(v)
                            elif lines[k] in ("CURRENT YEAR",):
                                break
                        if len(after_nums) >= 4:
                            result["investment_results"] = {
                                "beginning_market_value": after_nums[0],
                                "net_deposits_withdrawals": after_nums[1],
                                "investment_results": after_nums[2],
                                "ending_market_value": after_nums[3],
                            }
                        break
            break

    # ── Fallback: scan for activity patterns globally (OCR interleaved columns) ──
    if "portfolio_activity" not in result:
        activity_fb: dict[str, Decimal | None] = {}
        for i, line in enumerate(lines):
            if "MARKET VALUE AS OF" in line and i + 1 < len(lines):
                val = parse_usd(lines[i + 1])
                if val is not None and val > 10_000:
                    m_date = re.search(r"AS OF\s+(\w+)\s+(\d{1,2})", line)
                    if m_date:
                        day = int(m_date.group(2))
                        if day <= 2 and "opening_value" not in activity_fb:
                            activity_fb["opening_value"] = val
                        elif day >= 27 and "closing_value" not in activity_fb:
                            activity_fb["closing_value"] = val
            elif "CHANGE IN MARKET VALUE" in line and i + 1 < len(lines):
                val = parse_usd(lines[i + 1])
                if val is not None:
                    activity_fb["change_in_value"] = val
        if activity_fb:
            result["portfolio_activity"] = activity_fb

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
