import pytest

from parsers.bbh.report_mandato import BBHMandateReportParser
from parsers.ubs.report_mandato import UBSMandateReportParser
from parsers.ubs_miami.report_mandato import UBSMiamiMandateReportParser


def test_bbh_report_mandato_emits_only_subasset_splits():
    parser = BBHMandateReportParser()
    text = "\n".join(
        [
            "Equity 105,000 42.10%",
            "Cash & Fixed Income 144,000 57.90%",
            "Cash 2,000 0.90%",
            "% High Yield (of total portfolio) 3.50%",
            "U.S. Large Cap 50,000 20.00%",
            "U.S. Mid/Small Cap 25,000 10.00%",
            "Non-U.S. Developed 12,500 5.00%",
            "Emerging Markets 5,000 2.00%",
            "Global 12,750 5.10%",
        ]
    )

    alloc = parser._extract_allocation(text=text)

    assert "Cash, Deposits & Money Market" not in alloc
    assert "Fixed Income" not in alloc
    assert "Equities" not in alloc
    assert alloc["Investment Grade Fixed Income"] == {"value": 53.5, "unit": "%"}
    assert alloc["High Yield Fixed Income"] == {"value": 3.5, "unit": "%"}
    assert alloc["US Equities"] == {"value": 30.0, "unit": "%"}
    assert alloc["Non US Equities"] == {"value": 7.0, "unit": "%"}
    assert alloc["Global Equity"] == {"value": 5.1, "unit": "%"}


def test_ubs_report_mandato_emits_global_separately_and_no_macros():
    parser = UBSMandateReportParser()
    text = "\n".join(
        [
            "Liquidity 1,000 1.94%",
            "High Grade Bonds 10,000 25.00%",
            "Corporate Bonds 8,000 18.00%",
            "High Yield Bonds 4,000 9.00%",
            "Equities US 6,000 15.00%",
            "Equities EMU 1,000 2.50%",
            "Equities EMMA 900 2.25%",
            "Equities UK 800 2.00%",
            "Equities Japan 700 1.75%",
            "Equities Global 2,400 6.00%",
            "Equities Switzerland 600 1.50%",
        ]
    )

    alloc = parser._extract_allocation(text=text)

    assert "Cash, Deposits & Money Market" not in alloc
    assert "Fixed Income" not in alloc
    assert "Equities" not in alloc
    assert alloc["Investment Grade Fixed Income"] == {"value": 43.0, "unit": "%"}
    assert alloc["High Yield Fixed Income"] == {"value": 9.0, "unit": "%"}
    assert alloc["US Equities"] == {"value": 15.0, "unit": "%"}
    assert alloc["Non US Equities"] == {"value": 10.0, "unit": "%"}
    assert alloc["Global Equity"] == {"value": 6.0, "unit": "%"}


def test_ubs_miami_report_mandato_emits_only_subasset_splits():
    parser = UBSMiamiMandateReportParser()
    text = "\n".join(
        [
            "Cash 200.00 0.20",
            "Fixed Income 59,990.00 59.99",
            "Corporate IG Credit 44,130.00 44.13",
            "Corporate High Yield 5,000.00 5.00",
            "Emerging Markets 10,860.00 10.86",
            "Equity 39,810.00 39.81",
            "US 23,950.00 23.95",
        ]
    )

    alloc = parser._extract_allocation(text=text)

    assert "Cash, Deposits & Money Market" not in alloc
    assert "Fixed Income" not in alloc
    assert "Equities" not in alloc
    assert alloc["Investment Grade Fixed Income"] == {"value": 44.13, "unit": "%"}
    assert alloc["High Yield Fixed Income"] == {"value": 15.86, "unit": "%"}
    assert alloc["US Equities"] == {"value": 23.95, "unit": "%"}
    assert alloc["Non US Equities"]["unit"] == "%"
    assert alloc["Non US Equities"]["value"] == pytest.approx(15.86)
