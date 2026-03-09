"""
Reporte: Caja por cuenta Boatview, periodo 2025-12.

Usa la misma lógica que el backend (_resolve_cash_value) para que los valores
sean comparables con la UI. Solo lectura de BD; no modifica parsers.

Ejecutar desde la raíz del proyecto:
  python scripts/report_caja_boatview_202512.py
"""

import sys
from pathlib import Path

# Raíz del proyecto en path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.session import get_engine, get_session_factory
from backend.schemas import FilterParams
from backend.routers.data import (
    _query_closing_rows,
    _resolve_cash_value,
)

# Nombres de banco para mostrar
BANK_DISPLAY = {
    "jpmorgan": "JP Morgan",
    "ubs": "UBS Suiza",
    "ubs_miami": "UBS Miami",
    "goldman_sachs": "Goldman Sachs",
    "bbh": "BBH",
    "bice": "BICE",
}


def main():
    engine = get_engine()
    session_factory = get_session_factory(engine)
    db = session_factory()

    filters = FilterParams(
        entity_names=["Boatview"],
        years=[2025],
        months=[12],
    )
    from backend.db.models import Account

    query = _query_closing_rows(
        db=db,
        filters=filters,
        years={2025},
        months=[12],
    )
    all_rows = query.order_by(Account.bank_code, Account.account_type).all()

    etf_cash_cache: dict[int, dict[tuple[int, int], float]] = {}
    table = []
    for mc, acct, norm in all_rows:
        cache = etf_cash_cache.setdefault(acct.id, {})
        caja = _resolve_cash_value(
            db=db,
            acct=acct,
            mc=mc,
            norm=norm,
            etf_cash_cache=cache,
        )
        bank_label = BANK_DISPLAY.get(acct.bank_code, acct.bank_code.replace("_", " ").title())
        table.append({
            "Banco": bank_label,
            "Tipo cuenta": acct.account_type,
            "ID": acct.identification_number or acct.account_number,
            "Moneda": mc.currency or acct.currency,
            "Caja": caja,
        })

    db.close()

    # Imprimir tabla en markdown
    if not table:
        print("Sin datos: no hay cierres 2025-12 para entity_name = Boatview.")
        print("(Verifica que existan cuentas Boatview y que estén cargadas cartolas dic 2025).")
        return

    # Formatear caja como número para la tabla
    for r in table:
        r["Caja_str"] = f"{r['Caja']:,.2f}" if isinstance(r["Caja"], (int, float)) else str(r["Caja"])
    keys = ["Banco", "Tipo cuenta", "ID", "Moneda", "Caja"]
    col_widths = {}
    for k in keys:
        if k == "Caja":
            col_widths[k] = max(len(k), max(len(r["Caja_str"]) for r in table))
        else:
            col_widths[k] = max(len(k), max(len(str(r[k])) for r in table))
    sep = "|" + "|".join("-" * (col_widths[k] + 2) for k in keys) + "|"
    header = "|" + "|".join(f" {k} ".ljust(col_widths[k] + 2) for k in keys) + "|"
    print(header)
    print(sep)
    for r in table:
        cells = [
            str(r["Banco"]),
            str(r["Tipo cuenta"]),
            str(r["ID"]),
            str(r["Moneda"]),
            r["Caja_str"],
        ]
        row_str = "|" + "|".join(f" {c} ".ljust(col_widths[k] + 2) for k, c in zip(keys, cells)) + "|"
        print(row_str)

    print()
    total_caja = sum(r["Caja"] for r in table)
    print(f"Total caja (todas las cuentas Boatview 2025-12): {total_caja:,.2f}")


if __name__ == "__main__":
    main()
