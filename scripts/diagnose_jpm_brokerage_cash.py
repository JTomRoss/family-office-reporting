"""
Diagnóstico: qué filas de holdings tiene guardadas la cuenta JPM brokerage 9001
para 2025-12 (parsed_data_json). Sirve para ver si el segundo instrumento de caja
está en BD y con qué nombre, o si falta (parser no lo capturó).

Ejecutar desde la raíz del proyecto con venv activado:
  python scripts/diagnose_jpm_brokerage_cash.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.session import get_engine, get_session_factory
from backend.db.models import Account, ParsedStatement
from sqlalchemy import extract

def main():
    engine = get_engine()
    session_factory = get_session_factory(engine)
    db = session_factory()

    acct = db.query(Account).filter(
        Account.entity_name == "Boatview",
        Account.bank_code == "jpmorgan",
        Account.account_type == "brokerage",
    ).first()
    if not acct:
        print("No se encontró cuenta Boatview / jpmorgan / brokerage.")
        db.close()
        return

    stmt = (
        db.query(ParsedStatement)
        .filter(
            ParsedStatement.account_id == acct.id,
            extract("year", ParsedStatement.statement_date) == 2025,
            extract("month", ParsedStatement.statement_date) == 12,
        )
        .order_by(ParsedStatement.id.desc())
        .first()
    )
    if not stmt:
        print(f"No hay ParsedStatement para cuenta {acct.identification_number} (id={acct.id}) 2025-12.")
        db.close()
        return

    import json
    data = json.loads(stmt.parsed_data_json or "{}")
    rows = data.get("rows") or []
    print(f"Cuenta: {acct.account_number} (ID {acct.identification_number}), 2025-12.")
    print(f"Total filas en parsed_data_json (excl. totales): {len(rows)}")
    print()
    print("Filas con instrument + market_value (candidatos a caja si el nombre coincide):")
    print("-" * 80)
    for i, r in enumerate(rows):
        if r.get("is_total"):
            print(f"  [{i}] (TOTAL) instrument={r.get('instrument')!r} market_value={r.get('market_value')}")
            continue
        name = r.get("instrument") or ""
        mv = r.get("market_value")
        print(f"  [{i}] instrument={name!r}  market_value={mv}")
    db.close()


if __name__ == "__main__":
    main()
