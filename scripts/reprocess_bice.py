"""
Reprocesa todos los documentos BICE en BD usando el parser actualmente instalado.

Pasos:
  1. Borra todos los registros de bice_monthly_snapshot.
  2. Por cada RawDocument de banco bice_inversiones/bice, re-parsea el PDF
     desde disco y re-carga el snapshot con la nueva logica.

Uso:
  .venv\Scripts\python.exe scripts/reprocess_bice.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.db.models import Account, BiceMonthlySnapshot, RawDocument
from backend.db.session import get_engine
from backend.services.data_loading_service import DataLoadingService
from parsers.bice.brokerage import BICEBrokerageParser
from sqlalchemy.orm import Session

BICE_BANK_CODES = {"bice_inversiones", "bice"}


def main(dry_run: bool = False) -> None:
    engine = get_engine()
    db = Session(engine)
    svc = DataLoadingService(db)
    parser = BICEBrokerageParser()

    docs: list[tuple[RawDocument, Account]] = (
        db.query(RawDocument, Account)
        .join(Account, RawDocument.account_id == Account.id)
        .filter(Account.bank_code.in_(BICE_BANK_CODES))
        .order_by(RawDocument.id)
        .all()
    )

    snap_count = db.query(BiceMonthlySnapshot).count()
    print(f"Documentos BICE encontrados : {len(docs)}")
    print(f"Snapshots actuales en BD    : {snap_count}")

    if dry_run:
        print("\n[DRY-RUN] No se realizaran cambios.")
        for d, a in docs:
            exists = Path(d.filepath).exists()
            print(f"  id={d.id:5} | {a.entity_name[:30]:30} | {'OK' if exists else 'FALTA'} | {d.filepath[:60]}")
        db.close()
        return

    # 1. Borrar snapshots existentes
    print("\nBorrando snapshots existentes...")
    deleted = db.query(BiceMonthlySnapshot).delete()
    db.commit()
    print(f"  {deleted} filas eliminadas de bice_monthly_snapshot.")

    # 2. Reprocesar cada documento
    ok = 0
    errors = []
    for d, a in docs:
        filepath = Path(d.filepath)
        if not filepath.exists():
            msg = f"id={d.id} | ARCHIVO NO ENCONTRADO: {d.filepath}"
            print(f"  ERROR: {msg}")
            errors.append(msg)
            continue

        try:
            result = parser.safe_parse(str(filepath))
            if result.status.value == "error":
                msg = f"id={d.id} | {a.entity_name[:25]} | parse error: {result.errors}"
                print(f"  ERROR: {msg}")
                errors.append(msg)
                continue

            svc._load_bice_snapshot(result, d, a)
            db.commit()
            ok += 1
            sd = result.statement_date
            period = f"{sd.year}-{sd.month:02d}" if sd else "???"
            print(
                f"  OK    id={d.id:5} | {a.entity_name[:30]:30} | {period}"
                f" | warns={len(result.warnings)}"
            )
            if result.warnings:
                for w in result.warnings:
                    print(f"        WARN: {w}")

        except Exception as exc:
            db.rollback()
            msg = f"id={d.id} | {a.entity_name[:25]} | excepcion: {exc}"
            print(f"  ERROR: {msg}")
            errors.append(msg)

    snap_final = db.query(BiceMonthlySnapshot).count()
    print(f"\nReproceso completado: {ok}/{len(docs)} OK, {len(errors)} errores.")
    print(f"Snapshots en BD ahora: {snap_final}")

    if errors:
        print("\nErrores:")
        for e in errors:
            print(f"  - {e}")

    db.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Solo listar, no modificar BD")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
