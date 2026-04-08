from parsers.goldman_sachs.report_mandato import GoldmanSachsMandateReportParser


def test_goldman_report_mandato_emits_only_subasset_splits_and_no_report_macros():
    parser = GoldmanSachsMandateReportParser()
    text = "\n".join(
        [
            "Asset Allocation Performance",
            "Cash, Deposits & Money Market Funds 26,818 0.0%",
            "Fixed Income 135,183,885 56.2%",
            "Investment Grade Fixed Income 119,090,907 49.5%",
            "Other Fixed Income 16,092,978 6.7%",
            "Public Equity 105,452,802 43.8%",
            "US Equity 77,639,840 32.3%",
            "Global Equity 4,850,333 2.0%",
            "Non-US Equity 22,962,630 9.5%",
        ]
    )

    alloc = parser._extract_allocation(text=text)

    assert "Cash, Deposits & Money Market" not in alloc
    assert "Fixed Income" not in alloc
    assert "Equities" not in alloc
    assert alloc["Investment Grade Fixed Income"] == {"value": 49.5, "unit": "%"}
    assert alloc["High Yield Fixed Income"] == {"value": 6.7, "unit": "%"}
    assert alloc["US Equities"] == {"value": 32.3, "unit": "%"}
    assert alloc["Global Equity"] == {"value": 2.0, "unit": "%"}
    assert alloc["Non US Equities"] == {"value": 9.5, "unit": "%"}
