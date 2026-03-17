from __future__ import annotations

from pathlib import Path

import pandas as pd

from parsers.excel.alternatives import AlternativesExcelParser


def _write_alternatives_workbook(path: Path) -> None:
    nav_rows = [
        ["Moneda", "USD", "EUR", "USD", "USD", "USD", "USD"],
        ["Dato", "NAV", "NAV", "NAV", "NAV", "NAV", "NAV"],
        ["Estratgegia", "Buyout", "Buyout", "Buyout", "Buyout", "Buyout", "Buyout"],
        ["Clase de activo", "PE", "PE", "PE", "PE", "PE", "RE"],
        ["Nemo", "FUND_A", "FUND_B", "FUND_B", "FUND_D", "FUND_C", "FUND_E"],
        ["Sigla", "FUND_A", "FUND_B", "FUND_B", "FUND_D", "FUND_C", "FUND_E"],
        ["Sociedad", "Boatview", "Telmar", "Telmar", "Telmar", "Ecoterra", "Ect RE"],
        [None, 1, 2, 3, 4, 5, 6],
        ["Fecha", "FUND_A", "FUND_B", "FUND_B USD", "FUND_D", "FUND_C", "FUND_E"],
        ["2025-01-01", 0, 0, 0, 0, 0, 0],
        ["2025-01-31", 100, 999, 200, 50, 300, 400],
    ]
    mov_rows = [
        ["Moneda", "USD", "EUR", "USD", "USD", "USD", "USD"],
        ["Dato", "Movimiento", "Movimiento", "Movimiento", "Movimiento", "Movimiento", "Movimiento"],
        ["Estratgegia", "Buyout", "Buyout", "Buyout", "Buyout", "Buyout", "Buyout"],
        ["Clase de activo", "PE", "PE", "PE", "PE", "PE", "RE"],
        ["Nemo", "FUND_A", "FUND_B", "FUND_B", "FUND_D", "FUND_C", "FUND_E"],
        ["Sigla", "FUND_A", "FUND_B", "FUND_B", "FUND_D", "FUND_C", "FUND_E"],
        ["Sociedad", "Boatview", "Telmar", "Telmar", "Telmar", "Ecoterra", "Ect RE"],
        [None, 1, 2, 3, 4, 5, 6],
        ["Fecha", "FUND_A", "FUND_B", "FUND_B USD", "FUND_D", "FUND_C", "FUND_E"],
        ["2025-01-01", 0, 0, 0, 0, 0, 0],
        ["2025-01-31", -100, -999, -150, -30, -300, -40],
    ]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(nav_rows).to_excel(writer, sheet_name="NAV", header=False, index=False)
        pd.DataFrame(mov_rows).to_excel(writer, sheet_name="Movimientos", header=False, index=False)


def test_alternatives_parser_excludes_eur_duplicates_and_inverts_movement_sign(tmp_dir):
    workbook = tmp_dir / "Alternativos.xlsx"
    _write_alternatives_workbook(workbook)

    parser = AlternativesExcelParser()
    result = parser.safe_parse(workbook)

    assert result.is_success
    assert result.bank_code == "alternativos"
    assert result.warnings == ["Sociedades excluidas en parser de alternativos: Ecoterra, El Faro."]

    rows = {
        (row.data["entity_name"], row.data["asset_class"], row.data["strategy"], row.data["currency"]): row.data
        for row in result.rows
    }

    assert ("Ecoterra", "PE", "Buyout", "USD") not in rows
    assert ("Telmar", "PE", "Buyout", "EUR") not in rows

    boatview = rows[("Boatview", "PE", "Buyout", "USD")]
    telmar = rows[("Telmar", "PE", "Buyout", "USD")]
    ect_re = rows[("Ecoterra RE", "RE", "Buyout", "USD")]

    assert boatview["closing_date"] == "2025-01-31"
    assert boatview["ending_value"] == 100.0
    assert boatview["movements_net"] == 100.0
    assert boatview["profit_period"] == 0.0
    assert boatview["nemo_reference"] == "FUNDA"

    assert telmar["ending_value"] == 250.0
    assert telmar["movements_net"] == 180.0
    assert telmar["profit_period"] == 70.0
    assert telmar["nemo_reference"] == "FUNDB"
    assert ect_re["ending_value"] == 400.0
    assert ect_re["movements_net"] == 40.0
    assert ect_re["profit_period"] == 360.0
    assert ect_re["nemo_reference"] == "FUNDE"
