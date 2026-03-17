from asset_taxonomy import (
    asset_bucket_order,
    coarse_asset_bucket_series,
    default_chart_color_sequence,
)


def test_asset_bucket_order_matches_visual_stack_bottom_to_top():
    assert asset_bucket_order() == [
        "Caja",
        "RF IG Short",
        "RF IG Long",
        "HY",
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
