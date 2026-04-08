"""
Parser: Goldman Sachs – Mandato / Wrap Statement (PDF).
v2.1.0 – Real extraction using PyMuPDF (fitz).

CRITICAL: pdfplumber CANNOT extract text from GS PDFs.
Must use ``fitz`` (PyMuPDF).

TESTED AGAINST
==============
- 202512 Boatview - Mandato - GoldmanSachs.pdf  (222 pages)
  Portfolio XXX-XX451-9 (group)
  9 individual sub-portfolios:
    Advisory (XXX-XX063-1) — $117.4M
    Aristotle LCV (XXX-XX064-9)
    Eastern Shore (XXX-XX065-6)
    Brokerage (XXX-XX066-4)
    Corporate Fixed Income (XXX-XX067-2)
    Harding Loevner (XXX-XX069-8)
    Wellington Non-US EQ (XXX-XX072-2)
    Brokerage 2 (XXX-XX147-6)
    Brokerage (XXX-XX195-0)
  Total: $273,860,843.16
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from typing import Any

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus
from parsers.goldman_sachs._gs_common import (
    parse_usd,
    extract_all_text_fitz,
    extract_page_texts_fitz,
    extract_detection_text,
    extract_period,
    extract_portfolio_number,
    extract_overview,
    extract_tax_summary,
    extract_asset_strategy,
    extract_holdings,
    extract_sub_portfolios,
)

logger = logging.getLogger(__name__)


class GoldmanSachsCustodyParser(BaseParser):
    BANK_CODE = "goldman_sachs"
    ACCOUNT_TYPE = "custody"
    VERSION = "2.1.2"
    DESCRIPTION = "Parser para cartolas Goldman Sachs Mandato/Wrap (PDF)"
    SUPPORTED_EXTENSIONS = [".pdf"]

    # ── detection ──────────────────────────────────────────────────────
    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            text, n_pages = extract_detection_text(str(filepath))
            if n_pages == 0:
                return 0.0

            text_lower = text.lower()
            score = 0.0

            # Goldman Sachs markers
            if "goldman sachs" in text_lower:
                score += 0.30
            # Wrap/Mandato marker (this distinguishes from ETF)
            if "ex brokerage" in text_lower or "statement wrap" in text_lower:
                score += 0.30
            # Multiple sub-portfolios
            if text_lower.count("xxx-") >= 3:
                score += 0.15
            # Portfolio number pattern
            if re.search(r"xxx-\w+-\d+", text_lower):
                score += 0.10
            # File name bonus
            fname = filepath.stem.lower()
            if "goldmansachs" in fname or "goldman" in fname:
                score += 0.10
            if "mandato" in fname:
                score += 0.15

            return max(0.0, min(score, 1.0))
        except Exception:
            return 0.0

    # ── parse ─────────────────────────────────────────────────────────
    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        warnings: list[str] = []
        rows: list[ParsedRow] = []
        balances: dict[str, Any] = {}
        qualitative: dict[str, Any] = {}
        statement_date = None
        period_start = None
        period_end = None
        opening_balance = None
        closing_balance = None

        try:
            page_texts = extract_page_texts_fitz(filepath)
            all_text = "\n".join(page_texts[:10])  # First 10 pages for summary data

            # 1) Period & master portfolio number
            period = extract_period(all_text)
            acct = extract_portfolio_number(all_text)
            if period:
                balances["period"] = period
                period_start = self._parse_period_date(period.get("start"))
                period_end = self._parse_period_date(period.get("end"))
                statement_date = period_end or period_start
            if acct:
                balances["account_number"] = acct
            balances["currency"] = "USD"

            # 2) Sub-portfolios (page 2)
            if len(page_texts) >= 2:
                sub_ports = extract_sub_portfolios(page_texts[1])
                if sub_ports:
                    balances["sub_portfolios"] = sub_ports

            # 3) Overview (legacy GS wraps may place it on page 4+)
            overview = self._extract_primary_overview(page_texts)
            if overview:
                balances.update(overview)
                if overview.get("asset_allocation"):
                    qualitative["asset_allocation"] = self._json_safe(overview["asset_allocation"])
                activity = overview.get("portfolio_activity", {})
                inv_results = overview.get("investment_results", {})
                opening_balance = (
                    activity.get("opening_value")
                    or inv_results.get("beginning_market_value")
                )
                closing_balance = (
                    activity.get("closing_value")
                    or inv_results.get("ending_market_value")
                    or overview.get("total_portfolio")
                )
                closing_balance = self._reconcile_overview_total_with_asset_allocation(
                    balances=balances,
                    qualitative=qualitative,
                    closing_balance=closing_balance,
                )
            # 4) Tax summary (pages 4-5)
            tax_text = ""
            for i in range(3, min(6, len(page_texts))):
                tax_text += page_texts[i] + "\n"
            tax = extract_tax_summary(tax_text)
            if tax:
                qualitative["tax_summary"] = tax

            # 5) Asset strategy analysis (pages 6-8)
            strategy_text = ""
            for i in range(5, min(9, len(page_texts))):
                strategy_text += page_texts[i] + "\n"
            strategy = extract_asset_strategy(strategy_text)
            if strategy:
                qualitative["asset_strategy"] = strategy

            # 6) Holdings from all sub-portfolio sections
            holdings = extract_holdings(page_texts)
            for h in holdings:
                confidence = 0.80
                w: list[str] = []
                if not h.get("market_value"):
                    w.append("Missing market_value")
                    confidence = 0.55
                rows.append(ParsedRow(
                    data={k: str(v) if isinstance(v, Decimal) else v for k, v in h.items()},
                    confidence=confidence,
                    warnings=w,
                ))

            # 7) Extract per-sub-portfolio overviews
            sub_overviews = self._extract_sub_portfolio_overviews(page_texts, warnings)
            if sub_overviews:
                qualitative["sub_portfolio_overviews"] = sub_overviews
                opening_balance, closing_balance = self._apply_sub_portfolio_summary_fallback(
                    balances=balances,
                    qualitative=qualitative,
                    sub_overviews=sub_overviews,
                    opening_balance=opening_balance,
                    closing_balance=closing_balance,
                )

            monthly = self._build_account_monthly_activity(
                account_number=balances.get("account_number"),
                opening_balance=opening_balance,
                closing_balance=closing_balance,
                investment_results=balances.get("investment_results", {}),
            )
            if monthly:
                qualitative["account_monthly_activity"] = monthly

        except Exception as exc:
            logger.exception("Goldman Sachs Custody parse error: %s", exc)
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
            currency="USD",
            opening_balance=opening_balance,
            closing_balance=closing_balance,
            warnings=warnings,
        )

    def _reconcile_overview_total_with_asset_allocation(
        self,
        *,
        balances: dict[str, Any],
        qualitative: dict[str, Any],
        closing_balance: Decimal | None,
    ) -> Decimal | None:
        """
        Some legacy GS wrap overviews expose an inflated top-line total while the
        asset-allocation table on the same page carries the correct TOTAL PORTFOLIO.
        When both disagree materially, trust the asset-allocation total.
        """
        asset_allocation = qualitative.get("asset_allocation")
        if not isinstance(asset_allocation, dict):
            return closing_balance

        total_payload = asset_allocation.get("TOTAL PORTFOLIO")
        alloc_total = None
        if isinstance(total_payload, dict):
            alloc_total = self._to_decimal(
                total_payload.get("market_value")
                or total_payload.get("value")
                or total_payload.get("amount")
            )
        if alloc_total is None or alloc_total <= 0:
            return closing_balance
        if closing_balance is None or closing_balance <= 0:
            balances["total_portfolio"] = alloc_total
            return alloc_total

        gap = abs(closing_balance - alloc_total)
        rel_gap = gap / alloc_total if alloc_total else Decimal("0")
        if rel_gap <= Decimal("0.10"):
            return closing_balance

        balances["total_portfolio"] = alloc_total

        investment_results = balances.get("investment_results")
        if isinstance(investment_results, dict):
            investment_results["ending_market_value"] = alloc_total

        portfolio_activity = balances.get("portfolio_activity")
        if isinstance(portfolio_activity, dict):
            portfolio_activity["closing_value"] = alloc_total

        return alloc_total

    def _extract_primary_overview(self, page_texts: list[str]) -> dict[str, Any]:
        """
        Find the account-level overview page without assuming a fixed page index.

        Legacy GS wraps may insert "Special Messages" before the main overview, so
        the audited overview can start on page 4 and continue into the next page.
        """
        max_scan = min(6, len(page_texts))
        for idx in range(max_scan):
            page_text = page_texts[idx]
            if "TOTAL PORTFOLIO" not in page_text or "PORTFOLIO ASSET ALLOCATION" not in page_text:
                continue

            combined_text = page_text
            has_investment_results = "INVESTMENT RESULTS" in page_text
            next_text = page_texts[idx + 1] if idx + 1 < len(page_texts) else ""
            next_lines = [line.strip() for line in next_text.splitlines() if line.strip()]
            next_is_overview_continued = any(
                line == "Overview" or line == "Overview (Continued)"
                for line in next_lines[-8:]
            )

            if not has_investment_results and "INVESTMENT RESULTS" in next_text and next_is_overview_continued:
                combined_text = page_text + "\n" + next_text

            overview = extract_overview(combined_text)
            if (
                overview.get("asset_allocation")
                or overview.get("investment_results")
                or overview.get("total_portfolio")
            ):
                return overview

        return {}

    @staticmethod
    def _parse_period_date(raw: str | None) -> date | None:
        if not raw:
            return None
        cleaned = raw.strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d,%Y", "%b %d,%Y"):
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return parse_usd(str(value))

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {k: GoldmanSachsCustodyParser._json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [GoldmanSachsCustodyParser._json_safe(v) for v in value]
        return value

    def _build_account_monthly_activity(
        self,
        account_number: str | None,
        opening_balance: Decimal | None,
        closing_balance: Decimal | None,
        investment_results: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Estandariza movimientos/utilidad para carga a monthly_closings."""
        if not account_number:
            return []

        net_contributions = self._to_decimal(
            investment_results.get("net_deposits_withdrawals")
        )
        utilidad = self._to_decimal(investment_results.get("investment_results"))

        if (
            utilidad is None
            and opening_balance is not None
            and closing_balance is not None
            and net_contributions is not None
        ):
            utilidad = closing_balance - opening_balance - net_contributions

        if (
            net_contributions is None
            and opening_balance is not None
            and closing_balance is not None
            and utilidad is not None
        ):
            net_contributions = closing_balance - opening_balance - utilidad

        if utilidad is None and net_contributions is None:
            return []

        return [
            {
                "account_number": account_number,
                "beginning_value": (
                    str(opening_balance) if opening_balance is not None else None
                ),
                "ending_value_with_accrual": (
                    str(closing_balance) if closing_balance is not None else None
                ),
                "ending_value_without_accrual": (
                    str(closing_balance) if closing_balance is not None else None
                ),
                "net_contributions": (
                    str(net_contributions) if net_contributions is not None else None
                ),
                "utilidad": str(utilidad) if utilidad is not None else None,
                "source": "gs_investment_results",
            }
        ]

    # ── validate ──────────────────────────────────────────────────────
    def _apply_sub_portfolio_summary_fallback(
        self,
        *,
        balances: dict[str, Any],
        qualitative: dict[str, Any],
        sub_overviews: list[dict[str, Any]],
        opening_balance: Decimal | None,
        closing_balance: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Fill missing account-level monthly data from sub-portfolio overviews."""
        summary = self._aggregate_sub_portfolio_overviews(sub_overviews)
        if not summary:
            return opening_balance, closing_balance

        if opening_balance is None:
            opening_balance = summary.get("opening_balance")
        if closing_balance is None:
            closing_balance = summary.get("closing_balance")

        if balances.get("total_portfolio") is None and closing_balance is not None:
            balances["total_portfolio"] = closing_balance

        if not balances.get("investment_results") and summary.get("investment_results"):
            balances["investment_results"] = summary["investment_results"]

        if not balances.get("portfolio_activity") and summary.get("portfolio_activity"):
            balances["portfolio_activity"] = summary["portfolio_activity"]

        if not qualitative.get("asset_allocation") and summary.get("asset_allocation"):
            qualitative["asset_allocation"] = self._json_safe(summary["asset_allocation"])

        return opening_balance, closing_balance

    def _aggregate_sub_portfolio_overviews(
        self,
        sub_overviews: list[dict[str, Any]],
    ) -> dict[str, Any]:
        totals = {
            "opening_balance": Decimal("0"),
            "closing_balance": Decimal("0"),
            "net_contributions": Decimal("0"),
            "utilidad": Decimal("0"),
        }
        seen = {key: False for key in totals}
        asset_allocation: dict[str, dict[str, Decimal | str]] = {}

        for overview in sub_overviews:
            if not isinstance(overview, dict):
                continue

            investment_results = overview.get("investment_results")
            if not isinstance(investment_results, dict):
                investment_results = {}
            portfolio_activity = overview.get("portfolio_activity")
            if not isinstance(portfolio_activity, dict):
                portfolio_activity = {}

            opening_value = self._coalesce_decimal(
                investment_results.get("beginning_market_value"),
                portfolio_activity.get("opening_value"),
            )
            closing_value = self._coalesce_decimal(
                investment_results.get("ending_market_value"),
                portfolio_activity.get("closing_value"),
                overview.get("total_portfolio"),
            )
            net_contributions = self._coalesce_decimal(
                investment_results.get("net_deposits_withdrawals"),
            )
            utilidad = self._coalesce_decimal(
                investment_results.get("investment_results"),
            )

            if opening_value is not None:
                totals["opening_balance"] += opening_value
                seen["opening_balance"] = True
            if closing_value is not None:
                totals["closing_balance"] += closing_value
                seen["closing_balance"] = True
            if net_contributions is not None:
                totals["net_contributions"] += net_contributions
                seen["net_contributions"] = True
            if utilidad is not None:
                totals["utilidad"] += utilidad
                seen["utilidad"] = True

            sub_asset_alloc = overview.get("asset_allocation")
            if not isinstance(sub_asset_alloc, dict):
                continue
            for label, payload in sub_asset_alloc.items():
                if not isinstance(payload, dict):
                    continue
                market_value = self._coalesce_decimal(
                    payload.get("market_value"),
                    payload.get("value"),
                    payload.get("amount"),
                )
                if market_value is None:
                    continue
                existing = asset_allocation.setdefault(
                    str(label),
                    {"market_value": Decimal("0")},
                )
                existing["market_value"] += market_value

        if not any(seen.values()) and not asset_allocation:
            return {}

        closing_total = totals["closing_balance"] if seen["closing_balance"] else None
        if closing_total not in (None, Decimal("0")):
            for payload in asset_allocation.values():
                market_value = payload["market_value"]
                payload["percentage"] = f"{(market_value / closing_total * Decimal('100')):.2f}%"

        summary: dict[str, Any] = {}
        if seen["opening_balance"]:
            summary["opening_balance"] = totals["opening_balance"]
        if seen["closing_balance"]:
            summary["closing_balance"] = totals["closing_balance"]
        if seen["net_contributions"] or seen["utilidad"]:
            summary["investment_results"] = {
                "beginning_market_value": (
                    totals["opening_balance"] if seen["opening_balance"] else None
                ),
                "net_deposits_withdrawals": (
                    totals["net_contributions"] if seen["net_contributions"] else None
                ),
                "investment_results": totals["utilidad"] if seen["utilidad"] else None,
                "ending_market_value": (
                    totals["closing_balance"] if seen["closing_balance"] else None
                ),
            }
        if seen["opening_balance"] or seen["closing_balance"]:
            summary["portfolio_activity"] = {
                "opening_value": totals["opening_balance"] if seen["opening_balance"] else None,
                "closing_value": totals["closing_balance"] if seen["closing_balance"] else None,
            }
        if asset_allocation:
            summary["asset_allocation"] = asset_allocation
        return summary

    def _coalesce_decimal(self, *values: Any) -> Decimal | None:
        for value in values:
            decimal_value = self._to_decimal(value)
            if decimal_value is not None:
                return decimal_value
        return None

    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        bal = result.balances or {}

        # Cross-check: total portfolio vs investment results ending
        total = bal.get("total_portfolio")
        results = bal.get("investment_results", {})
        ending = results.get("ending_market_value")
        if total and ending and abs(Decimal(str(total)) - Decimal(str(ending))) > Decimal("1"):
            errors.append(
                f"GS Custody: total {total} != investment results ending {ending}"
            )

        return errors

    # ── sub-portfolio overviews ───────────────────────────────────────
    def _extract_sub_portfolio_overviews(
        self, page_texts: list[str], warnings: list[str],
    ) -> list[dict[str, Any]]:
        """Scan for sub-portfolio Overview pages and extract their summaries.

        Each sub-portfolio has an Overview page with TOTAL PORTFOLIO and
        PORTFOLIO ASSET ALLOCATION.  We look for pages where the footer
        says 'Overview' and 'Statement Detail'.
        """
        overviews: list[dict[str, Any]] = []

        for i, pt in enumerate(page_texts):
            lines = [l.strip() for l in pt.splitlines() if l.strip()]
            # Check if this is an Overview page for a sub-portfolio
            has_overview = any(
                l == "Overview" or l == "Overview (Continued)" for l in lines[-8:]
            ) if len(lines) > 8 else False
            has_detail = any(l == "Statement Detail" for l in lines[-8:]) if len(lines) > 8 else False
            has_total = any("TOTAL PORTFOLIO" in l for l in lines[:10])

            if has_overview and has_detail and has_total:
                # Extract portfolio name and number from the page
                port_num = None
                port_name = None
                for l in lines[-10:]:
                    m = re.search(r"Portfolio No:\s*([\w-]+)", l)
                    if m:
                        port_num = m.group(1)
                # Name from the page header area (first few lines typically)
                for l in lines[:5]:
                    if "BOATVIEW" in l and l != "BOATVIEW LIMITED":
                        port_name = l

                overview_data = extract_overview(pt)
                if overview_data:
                    overview_data["portfolio_number"] = port_num
                    overview_data["portfolio_name"] = port_name
                    overview_data["page"] = i + 1
                    overviews.append(overview_data)

        return overviews
