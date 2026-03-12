"""
Parser: UBS Suiza – Statement of Assets (Cartola PDF).
v2.0.0 – Real extraction from concatenated-text UBS Switzerland PDFs.

FORMAT NOTES
============
- UBS Suiza PDFs have **concatenated text** (no spaces between words).
  e.g. "UBSSwitzerlandAG", "BoatviewLimitedRoadTown,Tortola"
- Numbers ARE separated from surrounding text in most cases.
- Portfolio number pattern: "Portfolio206-XXXXXX-NN"
- Page 3 ("Total assets") contains BOTH Portfolio 01 and Portfolio 02 summaries
  plus currency allocation – this is the richest data page.
- Later pages are specific to the current portfolio only.

TESTED AGAINST
==============
- 202512 Boatview UBS Suiza (Portfolio 1) - Mandato.pdf  (15 pages, Portfolio 01)
"""

from __future__ import annotations

import re
import logging
import calendar
from datetime import date
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus

logger = logging.getLogger(__name__)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_usd(raw: str | None) -> Decimal | None:
    """Parse a USD amount from UBS Suiza text.

    Numbers may appear as '92054308', '52974', '420163', '37633194'.
    Some may have decimals like '2974.10'.
    """
    if raw is None:
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, (int, float)):
        return Decimal(str(raw))
    cleaned = raw.strip().replace(",", "").replace(" ", "")
    # Remove parentheses (negative)
    neg = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        neg = True
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        neg = True
        cleaned = cleaned[1:]
    try:
        val = Decimal(cleaned)
        return -val if neg else val
    except (InvalidOperation, ValueError):
        return None


def _extract_all_text(pdf: pdfplumber.PDF) -> str:
    """Concatenate text from all pages."""
    parts = []
    for page in pdf.pages:
        txt = page.extract_text() or ""
        parts.append(txt)
    return "\n".join(parts)


def _extract_page_text(pdf: pdfplumber.PDF, page_idx: int) -> str:
    """Extract text from a specific page (0-indexed)."""
    if page_idx < len(pdf.pages):
        return pdf.pages[page_idx].extract_text() or ""
    return ""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class UBSSwitzerlandCustodyParser(BaseParser):
    BANK_CODE = "ubs"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.3.0"
    DESCRIPTION = "Parser para Statement of Assets UBS Suiza (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # ── detection ──────────────────────────────────────────────────────
    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                # Check first 3 pages
                text = ""
                for i in range(min(3, len(pdf.pages))):
                    text += (pdf.pages[i].extract_text() or "") + "\n"
                text_lower = text.lower()

                score = 0.0
                # Must have UBS Switzerland markers
                if "ubsswitzerlandag" in text_lower.replace(" ", ""):
                    score += 0.35
                elif "ubs" in text_lower and "switzerland" in text_lower:
                    score += 0.30
                # Statement of assets
                if "statementofassets" in text_lower.replace(" ", ""):
                    score += 0.25
                elif "statement of assets" in text_lower:
                    score += 0.25
                # Portfolio number pattern
                if re.search(r"portfolio\s*206-\d{6}-\d{2}", text_lower.replace(" ", "")):
                    score += 0.20
                # File name bonus
                fname = filepath.stem.lower()
                if "suiza" in fname or "switzerland" in fname:
                    score += 0.15
                if "portfolio" in fname:
                    score += 0.05

                return min(score, 1.0)
        except Exception:
            return 0.0

    # ── parse ─────────────────────────────────────────────────────────
    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        warnings: list[str] = []
        rows: list[ParsedRow] = []
        balances: dict[str, Any] = {}
        qualitative: dict[str, Any] = {}
        statement_date: date | None = None
        period_start: date | None = None
        period_end: date | None = None
        opening_balance: Decimal | None = None
        closing_balance: Decimal | None = None

        try:
            with pdfplumber.open(filepath) as pdf:
                page_texts = [page.extract_text() or "" for page in pdf.pages]
                all_text = "\n".join(page_texts)

                # 1) Portfolio number & period
                acct = self._extract_portfolio_number(all_text)
                period = self._extract_period(all_text)
                if acct:
                    balances["account_number"] = acct
                if period:
                    balances["period"] = period
                    statement_date = self._period_to_date(period)
                    period_end = statement_date
                    if statement_date:
                        period_start = date(statement_date.year, statement_date.month, 1)
                # Fallback robusto: algunas cartolas UBS vienen sin bloque de periodo parseable.
                if statement_date is None:
                    statement_date = self._statement_date_from_filename(filepath.name)
                    if statement_date:
                        period_start = date(statement_date.year, statement_date.month, 1)
                        period_end = statement_date
                balances["currency"] = "USD"

                # 2) Total assets overview (page index varies across UBS formats)
                total_assets_idx = self._find_page_by_title_prefix(
                    page_texts,
                    title_prefix="total assets",
                    start=0,
                    end=min(len(page_texts), 25),
                )
                if total_assets_idx is None:
                    total_assets_idx = self._find_first_page_by_any_marker(
                    page_texts,
                    markers=["total assets", "totalnetassets"],
                    start=0,
                    end=min(len(page_texts), 25),
                    )
                if total_assets_idx is None:
                    total_assets_idx = 2 if len(page_texts) > 2 else 0
                p3_text = page_texts[total_assets_idx]

                total_overview = self._extract_total_assets(p3_text)
                if total_overview:
                    balances["total_net_assets"] = total_overview.get("total_net_assets")
                    balances["portfolios"] = total_overview.get("portfolios", {})
                    balances["currency_allocation"] = total_overview.get("currency_allocation", {})

                # Structural rule: pick the portfolio block that matches account suffix (...-01 / ...-02).
                selected_suffix = self._portfolio_suffix_from_account(acct)
                selected_portfolio = self._extract_selected_portfolio_block(p3_text, selected_suffix)
                if selected_portfolio:
                    balances["selected_portfolio"] = selected_portfolio
                    selected_alloc = self._selected_portfolio_to_asset_allocation(selected_portfolio)
                    if selected_alloc:
                        qualitative["asset_allocation"] = selected_alloc

                # 3) Portfolio overview
                p4_idx = self._find_page_by_title_prefix(
                    page_texts,
                    title_prefix="portfolio overview",
                    start=total_assets_idx,
                    end=min(len(page_texts), total_assets_idx + 8),
                )
                if p4_idx is None:
                    p4_idx = self._find_first_page_by_any_marker(
                    page_texts,
                    markers=["portfolio overview"],
                    start=total_assets_idx,
                    end=min(len(page_texts), total_assets_idx + 8),
                    )
                p4_text = page_texts[p4_idx] if p4_idx is not None else ""
                overview = self._extract_portfolio_overview(p4_text)
                if overview:
                    balances["portfolio_overview"] = overview

                # 4) Asset allocation pages
                alloc_start = p4_idx if p4_idx is not None else total_assets_idx
                alloc_idxs = self._find_pages_by_any_marker(
                    page_texts,
                    markers=["asset allocation"],
                    start=alloc_start,
                    end=min(len(page_texts), alloc_start + 10),
                    max_count=4,
                )
                alloc_text = "\n".join(page_texts[i] for i in alloc_idxs)
                allocation = self._extract_asset_allocation(alloc_text)
                if allocation:
                    balances["asset_allocation"] = allocation

                # 5) Performance pages (quarter-end statements include monthly table)
                perf_idxs = self._find_pages_by_any_marker(
                    page_texts,
                    markers=[
                        "monthly performance before tax",
                        "monthlyperformancebeforetax",
                        "performance summary",
                        "performancesummary",
                    ],
                    start=0,
                    end=min(len(page_texts), 30),
                    max_count=6,
                )
                perf_text = "\n".join(page_texts[i] for i in perf_idxs)
                performance = self._extract_performance(perf_text)
                if performance:
                    qualitative["performance"] = performance

                # 5b) Build historical monthly activity from Performance table (UBS-only)
                history_activity = self._build_historical_activity_from_performance(
                    account_number=acct,
                    performance=performance or {},
                    statement_date=statement_date,
                )
                if history_activity:
                    qualitative["account_monthly_activity_history"] = history_activity

                # 6) Ending values (with/without accrual)
                pos_idx = self._find_page_by_title_prefix(
                    page_texts,
                    title_prefix="positions overview",
                    start=total_assets_idx,
                    end=min(len(page_texts), 40),
                )
                if pos_idx is None:
                    pos_idx = self._find_first_page_by_any_marker(
                    page_texts,
                    markers=["positions overview", "detailed positions"],
                    start=total_assets_idx,
                    end=min(len(page_texts), 40),
                    )
                if pos_idx is not None:
                    positions_text = "\n".join(
                        page_texts[i] for i in range(pos_idx, min(pos_idx + 3, len(page_texts)))
                    )
                else:
                    positions_text = ""
                ending_wo, ending_with, accrual = self._extract_ending_values(positions_text)
                if ending_wo is None or ending_with is None:
                    ending_wo, ending_with, accrual = self._extract_ending_values(p3_text)

                current_val, prev_val = self._extract_current_previous_from_performance(
                    performance or {},
                    statement_date=statement_date,
                )
                # If Performance monthly table is present, its final value is the most
                # reliable month-end for the selected portfolio.
                if current_val is not None:
                    ending_with = current_val
                elif ending_with is None:
                    ending_with = current_val

                if ending_wo is None:
                    ending_wo = ending_with
                elif (
                    ending_with is not None
                    and abs(ending_wo - ending_with) > max(Decimal("1000"), abs(ending_with) * Decimal("0.10"))
                ):
                    # Guardrail for multi-portfolio pages: avoid picking the wrong portfolio total.
                    ending_wo = ending_with

                if accrual is None and ending_with is not None and ending_wo is not None:
                    accrual = ending_with - ending_wo

                opening_balance = prev_val
                closing_balance = ending_with

                if acct and ending_with is not None:
                    current_hist = None
                    if statement_date and history_activity:
                        for row in history_activity:
                            if (
                                row.get("period_year") == statement_date.year
                                and row.get("period_month") == statement_date.month
                            ):
                                current_hist = row
                                break

                    qualitative["accounts"] = [{
                        "account_number": acct,
                        "beginning_value": str(prev_val) if prev_val is not None else None,
                        "ending_value": str(ending_with),
                    }]

                    # Requested rule: if this statement has no movement source, utility = ending_current - ending_previous.
                    utilidad_fallback = (
                        ending_with - prev_val if prev_val is not None else None
                    )
                    net_contributions = (
                        _parse_usd(current_hist.get("net_contributions"))
                        if current_hist else None
                    )
                    utilidad = (
                        _parse_usd(current_hist.get("utilidad"))
                        if current_hist and current_hist.get("utilidad") is not None
                        else utilidad_fallback
                    )
                    qualitative["account_monthly_activity"] = [{
                        "account_number": acct,
                        "ending_value_with_accrual": str(ending_with),
                        "ending_value_without_accrual": str(ending_wo) if ending_wo is not None else None,
                        "accrual_ending": str(accrual) if accrual is not None else None,
                        "net_contributions": (
                            str(net_contributions) if net_contributions is not None else None
                        ),
                        "utilidad": str(utilidad) if utilidad is not None else None,
                        "utilidad_rule": (
                            "performance_table_value"
                            if current_hist and current_hist.get("utilidad") is not None
                            else "ending_current_minus_ending_previous"
                        ),
                    }]

                # 7) Holdings / Detailed positions (pages 10-11)
                holdings = self._extract_holdings(pdf, warnings)
                rows.extend(holdings)

        except Exception as exc:
            logger.exception("UBS Suiza parse error: %s", exc)
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                bank_code=self.BANK_CODE,
                warnings=[f"Parse error: {exc}"],
            )

        status = ParserStatus.SUCCESS if balances.get("account_number") else ParserStatus.PARTIAL
        if not balances.get("account_number"):
            warnings.append("Could not extract portfolio number")

        return ParseResult(
            status=status,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            rows=rows,
            balances=balances,
            qualitative_data=qualitative,
            account_number=balances.get("account_number"),
            statement_date=statement_date,
            period_start=period_start,
            period_end=period_end,
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            currency="USD",
            warnings=warnings,
        )

    @staticmethod
    def _normalized(text: str) -> str:
        return (text or "").lower().replace(" ", "")

    @classmethod
    def _find_first_page_by_any_marker(
        cls,
        page_texts: list[str],
        markers: list[str],
        start: int = 0,
        end: int | None = None,
    ) -> int | None:
        indices = cls._find_pages_by_any_marker(
            page_texts=page_texts,
            markers=markers,
            start=start,
            end=end,
            max_count=1,
        )
        return indices[0] if indices else None

    @classmethod
    def _find_page_by_title_prefix(
        cls,
        page_texts: list[str],
        title_prefix: str,
        start: int = 0,
        end: int | None = None,
    ) -> int | None:
        if end is None:
            end = len(page_texts)
        prefix = title_prefix.lower().replace(" ", "")
        for idx in range(max(0, start), min(end, len(page_texts))):
            lines = [ln.strip() for ln in (page_texts[idx] or "").splitlines() if ln.strip()]
            if not lines:
                continue
            first = lines[0].lower().replace(" ", "")
            if first.startswith(prefix):
                return idx
        return None

    @classmethod
    def _find_pages_by_any_marker(
        cls,
        page_texts: list[str],
        markers: list[str],
        start: int = 0,
        end: int | None = None,
        max_count: int | None = None,
    ) -> list[int]:
        if end is None:
            end = len(page_texts)
        normalized_markers = [m.lower().replace(" ", "") for m in markers]
        found: list[int] = []
        for idx in range(max(0, start), min(end, len(page_texts))):
            norm = cls._normalized(page_texts[idx])
            if any(marker in norm for marker in normalized_markers):
                found.append(idx)
                if max_count is not None and len(found) >= max_count:
                    break
        return found

    @staticmethod
    def _period_to_date(period: dict[str, str]) -> date | None:
        month = _MONTHS.get(period.get("month", "").lower())
        if not month:
            return None
        try:
            return date(int(period["year"]), month, int(period["day"]))
        except (ValueError, KeyError, TypeError):
            return None

    @staticmethod
    def _statement_date_from_filename(filename: str | None) -> date | None:
        """Extrae cierre mensual desde prefijo YYYYMM del nombre de archivo."""
        if not filename:
            return None
        m = re.search(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)", filename)
        if not m:
            return None
        year = int(m.group(1))
        month = int(m.group(2))
        last_day = calendar.monthrange(year, month)[1]
        return date(year, month, last_day)

    @staticmethod
    def _portfolio_suffix_from_account(account_number: str | None) -> str | None:
        if not account_number:
            return None
        m = re.search(r"-(\d{2})$", account_number)
        return m.group(1) if m else None

    def _extract_selected_portfolio_block(self, page3_text: str, suffix: str | None) -> dict[str, Any] | None:
        if not suffix:
            return None
        start_token = f"Portfolio{suffix}"
        portfolio_variants = [start_token, f"Portfolio {suffix}"]
        block = None
        for label in portfolio_variants:
            block = self._extract_portfolio_block(page3_text, label)
            if block:
                break
        if not block:
            return None

        result: dict[str, Any] = {"portfolio": start_token}
        if block.get("net_assets") is not None:
            result["net_assets"] = block.get("net_assets")
        if block.get("performance_twr_ytd"):
            result["twr_ytd"] = block.get("performance_twr_ytd")
        if block.get("asset_classes"):
            result["asset_classes"] = block.get("asset_classes")
        return result

    @staticmethod
    def _selected_portfolio_to_asset_allocation(
        selected_portfolio: dict[str, Any] | None,
    ) -> dict[str, dict[str, str]] | None:
        """
        Convierte bloque de portfolio seleccionado a asset_allocation canónico UBS.

        Usa la columna "Total" por clase de activo (incluye accruals cuando aplica).
        """
        if not isinstance(selected_portfolio, dict):
            return None
        classes = selected_portfolio.get("asset_classes")
        net_assets = _parse_usd(selected_portfolio.get("net_assets"))
        if not isinstance(classes, dict):
            if net_assets == Decimal("0"):
                return {
                    "Liquidity": {"total": "0"},
                    "Bonds": {"total": "0"},
                    "Equities": {"total": "0"},
                }
            return None

        mapping = {
            "liquidity": "Liquidity",
            "bonds": "Bonds",
            "equities": "Equities",
        }
        alloc: dict[str, dict[str, str]] = {}
        for raw_key, label in mapping.items():
            val = _parse_usd(classes.get(raw_key))
            if val is None:
                continue
            alloc[label] = {"total": str(val)}

        return alloc or None

    @staticmethod
    def _extract_ending_values(text: str) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        number = r"[\d,\s]+(?:\.\d+)?"
        # Netassets <without_accrual> <accrual> <with_accrual> 100.00
        m = re.search(
            rf"Netassets\s+({number})\s+({number})\s+({number})\s+100\.00",
            text,
            re.IGNORECASE,
        )
        if m:
            without_accrual = _parse_usd(m.group(1))
            accrual = _parse_usd(m.group(2))
            with_accrual = _parse_usd(m.group(3))
            return without_accrual, with_accrual, accrual

        # Netassets <without_accrual> <with_accrual> 100.00
        m = re.search(
            rf"Netassets\s+({number})\s+({number})\s+100\.00",
            text,
            re.IGNORECASE,
        )
        if m:
            without_accrual = _parse_usd(m.group(1))
            with_accrual = _parse_usd(m.group(2))
            if without_accrual is not None and with_accrual is not None:
                return without_accrual, with_accrual, with_accrual - without_accrual
            return without_accrual, with_accrual, None

        # Netassets <with_accrual> 100.00 (single total column layout)
        m = re.search(
            rf"Netassets\s+({number})\s+100\.00",
            text,
            re.IGNORECASE,
        )
        if m:
            with_accrual = _parse_usd(m.group(1))
            return with_accrual, with_accrual, (Decimal("0") if with_accrual is not None else None)

        # Fallback from detailed totals
        mv = re.search(r"Totalmarketvalue\s+([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
        ai = re.search(r"Totalaccruedinterest\s+([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
        if mv:
            without_accrual = _parse_usd(mv.group(1))
            accrual = _parse_usd(ai.group(1)) if ai else None
            with_accrual = (
                without_accrual + accrual
                if without_accrual is not None and accrual is not None
                else without_accrual
            )
            return without_accrual, with_accrual, accrual

        return None, None, None

    @staticmethod
    def _extract_current_previous_from_performance(
        performance: dict[str, Any],
        statement_date: date | None = None,
    ) -> tuple[Decimal | None, Decimal | None]:
        monthly = performance.get("monthly", []) if performance else []
        if not monthly:
            return None, None

        parsed_rows: list[tuple[date, Decimal | None]] = []
        for row in monthly:
            period_iso = row.get("period_iso")
            if period_iso:
                try:
                    yr, mo, dy = (int(x) for x in str(period_iso).split("-"))
                    dt = date(yr, mo, dy)
                except (TypeError, ValueError):
                    continue
            else:
                continue
            parsed_rows.append((dt, _parse_usd(row.get("final_value"))))

        if not parsed_rows:
            return None, None

        parsed_rows.sort(key=lambda x: x[0])

        if statement_date is not None:
            current_idx = None
            for idx, (dt, _) in enumerate(parsed_rows):
                if dt.year == statement_date.year and dt.month == statement_date.month:
                    current_idx = idx
                    break
            if current_idx is None:
                current_idx = len(parsed_rows) - 1
        else:
            current_idx = len(parsed_rows) - 1

        current = parsed_rows[current_idx][1]
        previous = parsed_rows[current_idx - 1][1] if current_idx > 0 else None
        return current, previous

    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        # Cross-check: if total_net_assets available, verify portfolio sums
        bal = result.balances or {}
        total = bal.get("total_net_assets")
        portfolios = bal.get("portfolios", {})
        if total and portfolios:
            computed_sum = Decimal("0")
            for pkey, pdata in portfolios.items():
                net = pdata.get("net_assets")
                if net is not None:
                    computed_sum += Decimal(str(net))
            if computed_sum > 0:
                diff = abs(Decimal(str(total)) - computed_sum)
                if diff > Decimal("1"):
                    errors.append(
                        f"UBS portfolio sum {computed_sum} != total {total} (diff={diff})"
                    )
        return errors

    # ── internal extraction methods ───────────────────────────────────

    def _extract_portfolio_number(self, text: str) -> str | None:
        """Extract portfolio number like 206-560552-01."""
        # Text may be "Portfolio206-560552-01" or "Portfolio 206-560552-01"
        m = re.search(r"Portfolio\s*(\d{3}-\d{6}-\d{2})", text)
        return m.group(1) if m else None

    def _extract_period(self, text: str) -> dict[str, str] | None:
        """Extract statement date from 'Statementofassetsasof31December2025'."""
        # Concatenated form
        m = re.search(
            r"[Ss]tatement\s*of\s*assets\s*as\s*of\s*(\d{1,2})\s*([A-Z][a-z]+)\s*(\d{4})",
            text
        )
        if not m:
            # Try fully concatenated
            m = re.search(
                r"Statementofassetsasof(\d{1,2})([A-Z][a-z]+)(\d{4})",
                text
            )
        if m:
            return {
                "as_of_date": f"{m.group(1)} {m.group(2)} {m.group(3)}",
                "day": m.group(1),
                "month": m.group(2),
                "year": m.group(3),
            }
        return None

    def _extract_total_assets(self, page3_text: str) -> dict[str, Any] | None:
        """Extract total net assets and both portfolio summaries from page 3.

        Page 3 has:
        - Totalnetassetsasof31.12.2025 USD92054308
        - Portfolio01: asset class breakdown
        - Portfolio02-UBSManagePremium...: asset class breakdown
        - Currency allocation table
        """
        result: dict[str, Any] = {}

        # Total net assets
        m = re.search(
            r"Totalnetassets(?:asof[\d.]+)?USD([\d]+)",
            page3_text.replace(" ", "")
        )
        if not m:
            # Try with spaces
            m = re.search(r"Total\s*net\s*assets.*?USD\s*([\d,\s]+(?:\.\d+)?)", page3_text)
        if m:
            result["total_net_assets"] = _parse_usd(m.group(1))

        # Portfolio sections
        portfolios: dict[str, dict[str, Any]] = {}

        # Portfolio 01
        p01_block = self._extract_portfolio_block(page3_text, "Portfolio 01")
        if not p01_block:
            p01_block = self._extract_portfolio_block(page3_text, "Portfolio01")
        if p01_block:
            portfolios["Portfolio01"] = p01_block

        # Portfolio 02
        p02_block = self._extract_portfolio_block(page3_text, "Portfolio 02")
        if not p02_block:
            p02_block = self._extract_portfolio_block(page3_text, "Portfolio02")
        if p02_block:
            portfolios["Portfolio02"] = p02_block

        if portfolios:
            result["portfolios"] = portfolios

        # Currency allocation
        currencies = self._extract_currency_allocation(page3_text)
        if currencies:
            result["currency_allocation"] = currencies

        return result if result else None

    def _extract_portfolio_block(self, text: str, portfolio_label: str) -> dict[str, Any] | None:
        """Extract asset class breakdown for a portfolio from the Total assets page."""
        result: dict[str, Any] = {}
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return None

        target_norm = portfolio_label.lower().replace(" ", "")
        start_idx: int | None = None
        for idx, line in enumerate(lines):
            norm = line.lower().replace(" ", "")
            if target_norm in norm:
                start_idx = idx
                break
        if start_idx is None:
            return None

        end_idx = len(lines)
        for idx in range(start_idx + 1, len(lines)):
            norm = lines[idx].lower().replace(" ", "")
            if norm.startswith("portfolio0"):
                end_idx = idx
                break
            if "totalnetassetsbyexposurecurrency" in norm or "total net assets by exposure currency" in lines[idx].lower():
                end_idx = idx
                break

        block_lines = lines[start_idx:end_idx]
        if not block_lines:
            return None

        # Description can appear on the header line:
        # "Portfolio 02 - UBS Manage Premium [Ultra] - Customized Balanced"
        header = block_lines[0]
        desc_m = re.search(r"Portfolio\s*0[12]\s*-\s*(.+)$", header, re.IGNORECASE)
        if desc_m:
            result["description"] = desc_m.group(1).strip()

        asset_classes: dict[str, Decimal] = {}
        row_map = {
            "liquidity": "liquidity",
            "bonds": "bonds",
            "equities": "equities",
        }
        for line in block_lines:
            low = line.lower()
            matched_key = next((key for marker, key in row_map.items() if low.startswith(marker)), None)
            if not matched_key:
                continue
            amount = self._extract_total_amount_from_asset_row(line, matched_key)
            if amount is not None:
                asset_classes[matched_key] = amount

        if asset_classes:
            result["asset_classes"] = asset_classes

        for line in block_lines:
            low = line.lower()
            if low.startswith("net assets"):
                net_amount = self._extract_total_amount_from_asset_row(line, "net_assets")
                if net_amount is not None:
                    result["net_assets"] = net_amount
                break

        for line in block_lines:
            m = re.search(r"Cumulative performance before tax \(TWR\) in USD\s*([-\d.]+%)", line, re.IGNORECASE)
            if m:
                result["performance_twr_ytd"] = m.group(1)
                break

        return result if result else None

    @staticmethod
    def _extract_total_amount_from_asset_row(line: str, row_key: str | None = None) -> Decimal | None:
        """
        Toma una fila tipo:
        - Bonds 52 991 545 420 089 53 411 634 58.06
        - Liquidity 951 473 951 473 1.03
        y devuelve la última columna monetaria (Total), no el porcentaje.

        NOTA – Por qué en algunos meses (ej. 2025-05, 2025-08, 2025-11) la caja sale mal:
        La página "Total assets" no tiene el mismo layout en todos los PDFs. Cuando UBS cambia
        la maqueta (tabla, fuentes, posiciones), pdfplumber extract_text() puede devolver
        la fila Liquidity con todos los números pegados en un solo token (ej. "5408515154236")
        en lugar de separados ("54 236 54 236 1.03"). Entonces el parser interpreta un solo
        número gigante. En los meses que se leen bien, el mismo PDF devuelve espacios entre
        cifras. Solución robusta: usar extracción por tablas (extract_tables) en esa página
        cuando se detecte un solo token numérico enorme en la fila Liquidity.
        """
        parts = [p.replace(",", "").strip() for p in line.split() if p.strip()]
        if not parts:
            return None

        first_numeric_idx = None
        for idx, token in enumerate(parts):
            if re.fullmatch(r"\d+(?:\.\d+)?%?", token):
                first_numeric_idx = idx
                break
        if first_numeric_idx is None:
            return None

        number_tokens = parts[first_numeric_idx:]
        if not number_tokens:
            return None

        # Remove trailing percentage token if present.
        last = number_tokens[-1]
        if re.fullmatch(r"\d+\.\d+%?", last):
            number_tokens = number_tokens[:-1]
        if not number_tokens:
            return None

        key = (row_key or "").lower()
        take_n = 1
        if key in {"liquidity", "equities"}:
            if len(number_tokens) >= 2 and len(number_tokens) % 2 == 0:
                take_n = min(max(len(number_tokens) // 2, 1), 3)
            elif len(number_tokens) >= 3:
                take_n = 3
        elif key in {"bonds", "net_assets"}:
            if len(number_tokens) >= 7:
                take_n = 3
            elif len(number_tokens) >= 4:
                take_n = 2

        take_n = min(take_n, len(number_tokens))
        for n in range(take_n, 0, -1):
            candidate = " ".join(number_tokens[-n:])
            val = _parse_usd(candidate)
            if val is not None:
                return val
        return None

    def _extract_currency_allocation(self, text: str) -> dict[str, dict[str, Any]] | None:
        """Extract currency allocation from page 3.

        Pattern: USD USDollar 71324799 77.49%
                 EUR Euro 5671525 6.16%  etc.
        """
        currencies: dict[str, dict[str, Any]] = {}
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        row_pattern = re.compile(
            r"^(USD|EUR|GBP|GLB|JPY|CHF)\s+([A-Za-z\s]+?)\s+(\d[\d\s]*)\s+([\d.]+)\s*%$",
            re.IGNORECASE,
        )
        various_pattern = re.compile(r"^Various\s+(\d[\d\s]*)\s+([\d.]+)\s*%$", re.IGNORECASE)

        for line in lines:
            m = row_pattern.match(line)
            if m:
                code = m.group(1).upper()
                name = m.group(2).strip()
                val = _parse_usd(m.group(3))
                pct = m.group(4)
                currencies[code] = {
                    "name": name,
                    "market_value": val,
                    "percentage": pct,
                }
                continue

            m_var = various_pattern.match(line)
            if m_var:
                currencies["Various"] = {
                    "name": "Various",
                    "market_value": _parse_usd(m_var.group(1)),
                    "percentage": m_var.group(2),
                }

        # Fallback for fully concatenated extraction (no line breaks/spaces).
        if not currencies:
            no_space = text.replace(" ", "")
            fallback_patterns = [
                (r"USDUSDollar(\d+)([\d.]+)%", "USD", "US Dollar"),
                (r"EUREuro(\d+)([\d.]+)%", "EUR", "Euro"),
                (r"GBPPoundSterling(\d+)([\d.]+)%", "GBP", "Pound Sterling"),
                (r"GLBGlobal(\d+)([\d.]+)%", "GLB", "Global"),
                (r"JPYJapaneseYen(\d+)([\d.]+)%", "JPY", "Japanese Yen"),
                (r"CHFSwissFranc(\d+)([\d.]+)%", "CHF", "Swiss Franc"),
                (r"Various(\d+)([\d.]+)%", "Various", "Various"),
            ]
            for pattern, code, name in fallback_patterns:
                m = re.search(pattern, no_space)
                if not m:
                    continue
                currencies[code] = {
                    "name": name,
                    "market_value": _parse_usd(m.group(1)),
                    "percentage": m.group(2),
                }

        return currencies if currencies else None

    def _extract_portfolio_overview(self, text: str) -> dict[str, Any] | None:
        """Extract portfolio overview from page 4 (net assets, performance, dates)."""
        result: dict[str, Any] = {}

        # Net assets and performance by period
        # Pattern: 31.12.2024-31.12.2025 52974 2.63% 1397
        periods = re.findall(
            r"(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4})\s+(\d+)\s+([\d.]+%)\s+(\d+)",
            text,
        )
        if periods:
            annual_perf = []
            for start, end, net_assets, twr, value in periods:
                annual_perf.append({
                    "period_start": start,
                    "period_end": end,
                    "net_assets": _parse_usd(net_assets),
                    "twr": twr,
                    "performance_value": _parse_usd(value),
                })
            result["periods"] = annual_perf

        # Reference currency
        m = re.search(r"Referencecurrency\s+(\w+)", text)
        if not m:
            m = re.search(r"Referencecurrency(\w+)", text.replace(" ", ""))
        if m:
            result["reference_currency"] = m.group(1)

        return result if result else None

    def _extract_asset_allocation(self, text: str) -> dict[str, Any] | None:
        """Extract asset allocation from pages 5-7."""
        result: dict[str, Any] = {}

        # By asset class: "Liquidity 52974 100.00 ..."
        # By currency: "USD USDollar 52974 100.00 ..."
        # By instrument category: "Directinvestments 50000 94.39"

        # Instrument category
        instruments: dict[str, dict[str, Any]] = {}
        instr_patterns = [
            (r"Directinvestments\s+(\d+)\s+([\d.]+)", "Direct investments"),
            (r"Account\s+(\d+)\s+([\d.]+)", "Account"),
        ]
        for pattern, name in instr_patterns:
            m = re.search(pattern, text)
            if m:
                instruments[name] = {
                    "amount": _parse_usd(m.group(1)),
                    "percentage": m.group(2),
                }
        if instruments:
            result["by_instrument"] = instruments

        return result if result else None

    def _extract_performance(self, text: str) -> dict[str, Any] | None:
        """Extract performance data from pages 8-9.

        Targets:
        - Monthly TWR values
        - Performance summary (current month, quarter, YTD, since inception)
        - Monthly detail table
        """
        result: dict[str, Any] = {}

        # Performance summary line:
        # "PerformanceTWR(NetbeforeTax) 0.18% 0.59% 2.63% 2.63% -18.66%"
        m = re.search(
            r"PerformanceTWR\(NetbeforeTax\)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)",
            text.replace(" ", ""),
        )
        if not m:
            # Try with spaces
            m = re.search(
                r"PerformanceTWR\s*\(Net\s*before\s*Tax\)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)\s+([-\d.]+%)",
                text,
            )
        if m:
            result["summary"] = {
                "current_month": m.group(1),
                "current_quarter": m.group(2),
                "year_to_date": m.group(3),
                "last_12_months": m.group(4),
                "since_inception": m.group(5),
            }

        # Average annual performance
        m = re.search(r"AverageannualperformanceTWR\(NetbeforeTax\)\s+([-\d.]+%)", text.replace(" ", ""))
        if m:
            result["average_annual_twr"] = m.group(1)

        # Monthly performance rows (handles both concatenated and spaced variants).
        months_data = self._extract_monthly_table_rows(text)
        if months_data:
            result["monthly"] = months_data

        return result if result else None

    @staticmethod
    def _extract_monthly_table_rows(text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        in_monthly_table = False
        seen: set[str] = set()

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            no_space = line.replace(" ", "")

            if "MonthlyperformancebeforetaxvaluedinUSD" in no_space:
                in_monthly_table = True
                continue
            if in_monthly_table and "AnnualperformancebeforetaxvaluedinUSD" in no_space:
                in_monthly_table = False
                continue
            if not in_monthly_table:
                continue
            if line.lower().startswith("reference currency") or line.lower().startswith("cumulative"):
                continue

            parsed = UBSSwitzerlandCustodyParser._parse_monthly_row_line(line)
            if not parsed:
                continue
            if parsed["period_iso"] in seen:
                continue
            seen.add(parsed["period_iso"])
            rows.append(parsed)

        rows.sort(key=lambda r: r["period_iso"], reverse=True)
        return rows

    @staticmethod
    def _parse_monthly_row_line(line: str) -> dict[str, Any] | None:
        # Preferred path: explicit row parsing for "31 March 2025 81 278 864 7 -1 435 ..."
        # 1) Date parsing: both "31December2025 ..." and "31 December 2025 ...".
        m = re.match(r"^(\d{1,2})([A-Za-z]+)(\d{4})\s+(.*)$", line)
        if m:
            day_s, month_s, year_s, tail = m.group(1), m.group(2), m.group(3), m.group(4)
        else:
            m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+(.*)$", line)
            if not m:
                return None
            day_s, month_s, year_s, tail = m.group(1), m.group(2), m.group(3), m.group(4)

        month_n = _MONTHS.get(month_s.lower())
        if not month_n:
            return None
        try:
            dt = date(int(year_s), month_n, int(day_s))
        except ValueError:
            return None

        # 2) Parse numeric columns using TWR percentages as anchors.
        pct_values = re.findall(r"-?\d+(?:\.\d+)?%", tail)
        final_value = None
        inflows = None
        outflows = None
        performance_value = None
        cumulative_value = None

        if pct_values:
            twr_monthly = pct_values[0]
            split = tail.split(twr_monthly, 1)
            left = split[0].strip()
            right = split[1].strip() if len(split) > 1 else ""

            left_tokens = re.findall(r"-?\d+(?:\.\d+)?", left)
            groups = UBSSwitzerlandCustodyParser._split_four_numeric_groups(left_tokens)
            if groups:
                final_value = UBSSwitzerlandCustodyParser._compose_group_value(groups[0])
                inflows = UBSSwitzerlandCustodyParser._compose_group_value(groups[1])
                outflows = UBSSwitzerlandCustodyParser._compose_group_value(groups[2])
                performance_value = UBSSwitzerlandCustodyParser._compose_group_value(groups[3])

            right_no_pct = re.sub(r"-?\d+(?:\.\d+)?%", "", right).strip()
            right_tokens = re.findall(r"-?\d+(?:\.\d+)?", right_no_pct)
            if right_tokens:
                cumulative_value = UBSSwitzerlandCustodyParser._compose_group_value(right_tokens)
        else:
            tail_tokens = re.findall(r"-?\d+(?:\.\d+)?", tail)
            if tail_tokens:
                final_value = UBSSwitzerlandCustodyParser._compose_group_value(tail_tokens)

        if final_value is None:
            return None

        return {
            "period": f"{dt.day:02d} {month_s.title()} {dt.year}",
            "period_iso": dt.isoformat(),
            "final_value": final_value,
            "inflows": inflows,
            "outflows": outflows,
            "performance_value": performance_value,
            "twr_monthly": pct_values[0] if len(pct_values) > 0 else None,
            "cumulative_value": cumulative_value,
            "twr_cumulative": pct_values[1] if len(pct_values) > 1 else None,
        }

    @staticmethod
    def _is_valid_group(tokens: list[str]) -> bool:
        if not tokens:
            return False
        if len(tokens) == 1:
            return re.fullmatch(r"-?\d+(?:\.\d+)?", tokens[0]) is not None
        first = tokens[0]
        first_unsigned = first[1:] if first.startswith("-") else first
        if re.fullmatch(r"\d{1,3}", first_unsigned) is None:
            return False
        for tok in tokens[1:]:
            if re.fullmatch(r"\d{3}", tok) is None:
                return False
        return True

    @staticmethod
    def _compose_group_value(tokens: list[str]) -> Decimal | None:
        if not tokens:
            return None
        if len(tokens) == 1:
            return _parse_usd(tokens[0])
        sign = "-" if tokens[0].startswith("-") else ""
        first = tokens[0][1:] if tokens[0].startswith("-") else tokens[0]
        raw = sign + first + "".join(tokens[1:])
        return _parse_usd(raw)

    @classmethod
    def _split_four_numeric_groups(
        cls,
        tokens: list[str],
    ) -> tuple[list[str], list[str], list[str], list[str]] | None:
        n = len(tokens)
        if n < 4:
            return None

        best: tuple[list[str], list[str], list[str], list[str]] | None = None
        best_key: tuple[int, int, int, int] | None = None

        for i in range(1, n - 2):
            for j in range(i + 1, n - 1):
                for k in range(j + 1, n):
                    groups = (tokens[:i], tokens[i:j], tokens[j:k], tokens[k:])
                    if not all(cls._is_valid_group(g) for g in groups):
                        continue
                    expected_value_tokens = 2 if n >= 6 else 1
                    key = (
                        -abs(len(groups[3]) - expected_value_tokens),
                        -abs(len(groups[1]) - 1),  # inflow usually compact
                        -abs(len(groups[2]) - 1),  # outflow usually compact
                        len(groups[0]),            # then maximize final-value width
                    )
                    if best_key is None or key > best_key:
                        best_key = key
                        best = groups
        return best

    @staticmethod
    def _build_historical_activity_from_performance(
        account_number: str | None,
        performance: dict[str, Any],
        statement_date: date | None,
    ) -> list[dict[str, Any]]:
        if not account_number or not statement_date:
            return []

        monthly = performance.get("monthly", []) if performance else []
        if not monthly:
            return []

        rows: list[dict[str, Any]] = []
        for row in monthly:
            period_iso = row.get("period_iso")
            if not period_iso:
                continue
            try:
                yr, mo, dy = (int(x) for x in str(period_iso).split("-"))
                dt = date(yr, mo, dy)
            except (TypeError, ValueError):
                continue

            # Keep statement year + previous Dec bridge (for January utility calc).
            if dt.year not in {statement_date.year, statement_date.year - 1}:
                continue

            rows.append({
                "date": dt,
                "final_value": _parse_usd(row.get("final_value")),
                "inflows": _parse_usd(row.get("inflows")),
                "outflows": _parse_usd(row.get("outflows")),
                "performance_value": _parse_usd(row.get("performance_value")),
                "performance_cumulative_value": _parse_usd(row.get("cumulative_value")),
            })

        if not rows:
            return []

        rows.sort(key=lambda r: r["date"])

        outflow_is_signed = any(
            r["outflows"] is not None and r["outflows"] < 0 for r in rows
        )

        output: list[dict[str, Any]] = []
        prev_final: Decimal | None = None
        for row in rows:
            dt = row["date"]
            final_value = row["final_value"]
            inflows = row["inflows"]
            outflows = row["outflows"]

            net_contributions: Decimal | None = None
            if inflows is not None or outflows is not None:
                infl = inflows or Decimal("0")
                outf = outflows or Decimal("0")
                if (
                    outflow_is_signed
                    and outf < 0
                    and infl > 0
                    and infl <= Decimal("10")
                    and abs(outf) >= Decimal("100")
                ):
                    # UBS occasionally shows tiny technical inflows (e.g. 7) in the same
                    # row as a real cash withdrawal; reporting movement should reflect the
                    # withdrawal amount to match account cash control used in summary tables.
                    net_contributions = outf
                else:
                    net_contributions = infl + outf if outflow_is_signed else infl - outf

            utilidad = row["performance_value"]
            if utilidad is None and final_value is not None and prev_final is not None:
                if net_contributions is not None:
                    utilidad = final_value - prev_final - net_contributions
                else:
                    utilidad = final_value - prev_final

            if dt.year == statement_date.year:
                last_day = calendar.monthrange(dt.year, dt.month)[1]
                output.append({
                    "account_number": account_number,
                    "period_year": dt.year,
                    "period_month": dt.month,
                    "period_end": f"{dt.year:04d}-{dt.month:02d}-{last_day:02d}",
                    "ending_value_with_accrual": (
                        str(final_value) if final_value is not None else None
                    ),
                    "ending_value_without_accrual": (
                        str(final_value) if final_value is not None else None
                    ),
                    "net_contributions": (
                        str(net_contributions) if net_contributions is not None else None
                    ),
                    "utilidad": str(utilidad) if utilidad is not None else None,
                    "utilidad_ytd": (
                        str(row.get("performance_cumulative_value"))
                        if row.get("performance_cumulative_value") is not None
                        else None
                    ),
                    "source": "ubs_performance_monthly_table",
                })

            if final_value is not None:
                prev_final = final_value

        return output

    def _extract_holdings(
        self, pdf: pdfplumber.PDF, warnings: list[str]
    ) -> list[ParsedRow]:
        """Extract detailed positions from pages 10+.

        Portfolio 01 has minimal holdings:
        - Liquidity-Accounts: USD current account
        - Liquidity-Call deposits: Fiduciary call investment

        For larger portfolios (Portfolio 02), this would include bonds and equities.
        """
        rows: list[ParsedRow] = []

        # Scan pages for "Detailed positions" or "Positions overview"
        for page_idx in range(len(pdf.pages)):
            page_text = _extract_page_text(pdf, page_idx)
            no_space = page_text.replace(" ", "")

            # ── Liquidity – Accounts ──
            if "Liquidity-Accounts" in no_space or "Liquidity-Accounts" in page_text:
                acct_rows = self._parse_account_holdings(page_text, no_space)
                rows.extend(acct_rows)

            # ── Liquidity – Call Deposits ──
            if "Liquidity-Calldeposits" in no_space or "CallDeposits" in no_space:
                deposit_rows = self._parse_call_deposit_holdings(page_text, no_space)
                rows.extend(deposit_rows)

            # ── Bonds ──
            if "Bonds-" in no_space or "Fixedincome" in no_space:
                bond_rows = self._parse_bond_holdings(page_text, no_space)
                rows.extend(bond_rows)

            # ── Equities ──
            if "Equities-" in no_space:
                equity_rows = self._parse_equity_holdings(page_text, no_space)
                rows.extend(equity_rows)

        if not rows:
            warnings.append("No detailed position rows extracted")

        return rows

    def _parse_account_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse liquidity account holdings.

        Pattern: USD 2974.10 UBSCurrentAccountforPrivateClients USD 4509.04 2974 5.61
                 CH850020620656055260F 31.12.2024 0.00
        """
        rows: list[ParsedRow] = []

        # Match account lines - currency amount description
        pattern = re.compile(
            r"(USD|EUR|CHF|GBP)\s+([\d.]+)\s+(.*?)\s+(USD|EUR|CHF|GBP)\s+([\d.]+)\s+(\d+)\s+([\d.]+)"
        )
        for m in pattern.finditer(text):
            data = {
                "asset_class": "Liquidity",
                "sub_class": "Accounts",
                "currency": m.group(1),
                "amount": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "opening_currency": m.group(4),
                "opening_balance": str(_parse_usd(m.group(5))),
                "market_value": str(_parse_usd(m.group(6))),
                "percentage": m.group(7),
            }
            rows.append(ParsedRow(data=data, confidence=0.80,
                                  warnings=["Concatenated text – verify description"]))

        # Also try to find IBAN
        iban_match = re.search(r"(CH\d{2}\d{16,20}\w*)", no_space)
        if iban_match and rows:
            rows[0].data["iban"] = iban_match.group(1)

        return rows

    def _parse_call_deposit_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse call deposit holdings.

        Pattern: USD 50000 FiduciaryCallInvestment 100% 100%I 50000 94.39
        """
        rows: list[ParsedRow] = []

        # Fiduciary call investment
        m = re.search(
            r"(USD|EUR|CHF|GBP)\s+(\d+)\s+(Fiduciary\s*Call\s*Investment|FiduciaryCallInvestment)",
            text,
        )
        if not m:
            m = re.search(r"(USD|EUR|CHF)\s+(\d+)\s+Fiduciary", text)

        if m:
            # Find the market value and percentage
            market_val = _parse_usd(m.group(2))
            # Look for interest rate
            rate_match = re.search(r"Interestrate:\s*([\d.]+%)", text)

            data = {
                "asset_class": "Liquidity",
                "sub_class": "Call deposits",
                "currency": m.group(1),
                "description": "Fiduciary Call Investment",
                "face_value": str(market_val) if market_val else None,
                "market_value": str(market_val) if market_val else None,
            }
            if rate_match:
                data["interest_rate"] = rate_match.group(1)

            # Serial number
            serial = re.search(r"Serialno\.\s*(\d+)", text.replace(" ", ""))
            if serial:
                data["serial_number"] = serial.group(1)

            rows.append(ParsedRow(data=data, confidence=0.85, warnings=[]))

        # Total call deposits as verification
        total_match = re.search(r"TotalCalldeposits\s+(\d+)", no_space)
        if total_match and rows:
            rows[-1].data["total_call_deposits"] = str(_parse_usd(total_match.group(1)))

        return rows

    def _parse_bond_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse bond holdings (for Portfolio 02 PDFs with fixed income).

        Expected pattern per holding (multi-line):
        CCY FACE_VALUE DESCRIPTION RATING COST_PRICE MKT_PRICE GAIN MARKET_VALUE %
                       SECTOR                     EXCH_RATE  EXCH_GAIN ACCR_INT
                       DURATION                   COST_VALUE DATE      UNREAL_PL
                       YIELD                      LAST_PURCHASE
        """
        rows: list[ParsedRow] = []

        # Generic bond line: Currency, face value, and market value
        bond_pattern = re.compile(
            r"(USD|EUR|GBP|CHF|JPY)\s+([\d,]+)\s+(.+?)\s+"
            r"([\d.]+%?)\s+([\d.]+%?)\s+([-\d,]+)\s+([\d,]+)\s+([\d.]+)"
        )
        for m in bond_pattern.finditer(text):
            data = {
                "asset_class": "Bonds",
                "currency": m.group(1),
                "face_value": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "cost_price": m.group(4),
                "market_price": m.group(5),
                "market_gain": str(_parse_usd(m.group(6))),
                "market_value": str(_parse_usd(m.group(7))),
                "percentage": m.group(8),
            }
            rows.append(ParsedRow(data=data, confidence=0.70,
                                  warnings=["Bond extract from concatenated text"]))

        return rows

    def _parse_equity_holdings(self, text: str, no_space: str) -> list[ParsedRow]:
        """Parse equity holdings (for Portfolio 02 PDFs with equities)."""
        rows: list[ParsedRow] = []

        # Generic equity line pattern
        equity_pattern = re.compile(
            r"(USD|EUR|GBP|CHF|JPY)\s+([\d,]+)\s+(.+?)\s+"
            r"([\d.]+)\s+([\d.]+)\s+([-\d,]+)\s+([\d,]+)\s+([\d.]+)"
        )
        for m in equity_pattern.finditer(text):
            data = {
                "asset_class": "Equities",
                "currency": m.group(1),
                "quantity": str(_parse_usd(m.group(2))),
                "description": m.group(3).strip(),
                "avg_cost": str(_parse_usd(m.group(4))),
                "market_price": str(_parse_usd(m.group(5))),
                "market_gain": str(_parse_usd(m.group(6))),
                "market_value": str(_parse_usd(m.group(7))),
                "percentage": m.group(8),
            }
            rows.append(ParsedRow(data=data, confidence=0.70,
                                  warnings=["Equity extract from concatenated text"]))

        return rows

