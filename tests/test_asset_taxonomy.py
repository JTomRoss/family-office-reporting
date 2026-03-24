from asset_taxonomy import (
    asset_bucket_order,
    asset_bucket_detail_label,
    classify_etf_asset_bucket,
    coarse_asset_bucket_series,
    default_chart_color_sequence,
)


def test_asset_bucket_order_matches_visual_stack_bottom_to_top():
    assert asset_bucket_order() == [
        "Caja",
        "RF IG Short",
        "RF IG Long",
        "HY",
        "Non US RF",
        "Alternativos",
        "Real Estate",
        "RV EM",
        "RV DM",
    ]


def test_taxonomy_exposes_default_visual_palette_for_generic_and_coarse_asset_charts():
    assert default_chart_color_sequence()[:2] == ["#B53639", "#2D6FB7"]
    assert coarse_asset_bucket_series() == [
        ("Cash, Deposits & Money Market", "Caja", "#D5DEE9"),
        ("Fixed Income", "Renta Fija", "#2D6FB7"),
        ("Equities", "Renta Variable", "#B53639"),
    ]


def test_excel_taxonomy_classifies_new_bucket_and_preserves_special_character_aliases():
    assert classify_etf_asset_bucket("non us fixed income") == "Non US RF"
    assert classify_etf_asset_bucket("1-3yr") == "RF IG Short"
    assert classify_etf_asset_bucket("short-duration") == "RF IG Short"
    assert classify_etf_asset_bucket("SPDR BLOOMBERG 1-10 YEAR U.S.") == "RF IG Short"
    assert classify_etf_asset_bucket("SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG") == "RF IG Short"


def test_excel_taxonomy_exposes_detail_labels_for_personal_asset_table():
    assert asset_bucket_detail_label("RF IG Short") == "IG Fixed income"
    assert asset_bucket_detail_label("RV DM") == "Global Equity"
    assert asset_bucket_detail_label("PE") == "PE"
