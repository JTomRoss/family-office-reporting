import pytest

from parsers.jpmorgan.report_mandato import JPMorganMandateReportParser


def test_jpm_report_mandato_uses_breakdown_table_instead_of_donut_artifact():
    parser = JPMorganMandateReportParser()
    text = "\n".join(
        [
            "Total Asset Allocation",
            "Allocation Summary",
            "Cash 43.00%",
            "57.00%",
            "Allocation Breakdown By Account Type (% of Market Value)",
            "Discretionary Total",
            "Asset Class (100.00%) Asset Allocation",
            "Total 100.00% 100.00%",
            "Fixed Income & Cash 57.00% 57.00%",
            "US Fixed Income 43.16% 43.16%",
            "Cash 1.53% 1.53%",
            "Asia Fixed Income 0.84% 0.84%",
            "Extended Fixed Income 11.47% 11.47%",
            "Equity 43.00% 43.00%",
            "US Large Cap Equity 31.24% 31.24%",
            "European Large Cap Equity 6.85% 6.85%",
            "Japanese Large Cap Equity 2.69% 2.69%",
            "Asia ex-Japan Equity 1.05% 1.05%",
            "Other Equity 1.17% 1.17%",
            "Please see Important Information",
        ]
    )

    alloc = parser._extract_allocation(text=text)
    assert "Cash, Deposits & Money Market" not in alloc
    assert "Fixed Income" not in alloc
    assert "Equities" not in alloc
    assert alloc["US Equities"]["value"] == 31.24
    assert alloc["Non US Equities"]["value"] == 11.76


def test_jpm_report_mandato_extracts_hg_hy_from_complementario_ocr_text():
    parser = JPMorganMandateReportParser()
    text = "\n".join(
        [
            "Portfolio Positioning (%) EQ HG HY Cash",
            "40% 50% 10% 0%",
            "43% 43% 12% 2%",
            "3.00% -6.71% 1.58% 1.61%",
            "Duration (years) HG HY Total Duration Blended Duration JPM CIO",
            "4.25 3.82 4.18",
            "3.95 2.70 3.69 6.08",
            "-0.30 -1.12 -0.49",
            "Yield (%) HG HY",
            "4.79% 7.01%",
            "4.50% 6.25%",
            "-0.29% -0.76%",
        ]
    )

    alloc = parser._extract_allocation_from_complementario_ocr_text(text=text)
    assert alloc["Investment Grade Fixed Income"]["value"] == 43.0
    assert alloc["High Yield Fixed Income"]["value"] == 12.0

    metrics = parser._extract_fixed_income_metrics_from_complementario_ocr_text(
        text=text,
        allocation=alloc,
    )
    assert metrics["fixed_income_duration"]["value"] == 3.69
    assert metrics["fixed_income_yield"]["value"] == pytest.approx(4.881818181818182)


def test_jpm_report_mandato_handles_noisy_complementario_ocr_lines():
    parser = JPMorganMandateReportParser()
    text = "\n".join(
        [
            "Portfolio Positioning (%",
            "EQ 40c 40% 0.005",
            "HG 50% 47c 22.795",
            "HY 10% 125 1.795",
            "Cash 0% 18 005",
            "Duration (years)",
            "HG 418 402 -0.16",
            "HY 3.08 293 0.15",
            "Total Duration Blended 100 3.80 0.19",
            "Duration JPM CIO 6.04",
            "Yield 96",
            "HG 4609 4609 0.009",
            "HY 7.169 6550 -0.618",
            "Source. JPMorgan 4 Bloomberg as of 913002025",
        ]
    )

    alloc = parser._extract_allocation_from_complementario_ocr_text(text=text)
    assert alloc["Investment Grade Fixed Income"]["value"] == 47.0
    assert alloc["High Yield Fixed Income"]["value"] == 12.0

    metrics = parser._extract_fixed_income_metrics_from_complementario_ocr_text(
        text=text,
        allocation=alloc,
    )
    assert metrics["fixed_income_duration"]["value"] == 3.8
    assert metrics["fixed_income_yield"]["value"] == pytest.approx(5.0, abs=0.02)
