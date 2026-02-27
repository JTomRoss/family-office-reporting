"""
FO Reporting – Router de cuentas.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.schemas import AccountResponse, AccountCreate
from backend.services.account_service import AccountService

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("/", response_model=list[AccountResponse])
def list_accounts(
    active_only: bool = True,
    db: Session = Depends(get_db),
):
    """Lista todas las cuentas del maestro."""
    service = AccountService(db)
    return service.get_all(active_only=active_only)


@router.get("/filter-options")
def get_filter_options(db: Session = Depends(get_db)):
    """Retorna opciones de filtro disponibles para la UI."""
    service = AccountService(db)
    return service.get_filter_options()


@router.get("/classification-errors")
def check_classification(db: Session = Depends(get_db)):
    """Detecta errores de clasificación en el maestro."""
    service = AccountService(db)
    return service.detect_classification_errors()


@router.get("/auto-fill")
def auto_fill_by_id(
    identification_number: str,
    bank_code: str | None = None,
    entity_name: str | None = None,
    db: Session = Depends(get_db),
):
    """Auto-completa metadata buscando por dígito verificador + banco + sociedad."""
    service = AccountService(db)
    metadata = service.auto_fill_by_identification(
        identification_number, bank_code, entity_name
    )
    if not metadata:
        raise HTTPException(
            status_code=404,
            detail="Cuenta no encontrada en maestro con esos datos",
        )
    return metadata


@router.get("/{account_number}")
def get_account(account_number: str, db: Session = Depends(get_db)):
    """Obtiene una cuenta por número."""
    service = AccountService(db)
    account = service.get_by_number(account_number)
    if not account:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada")
    return account


@router.get("/{account_number}/auto-fill")
def auto_fill(account_number: str, db: Session = Depends(get_db)):
    """Auto-completa metadata desde el maestro de cuentas (por account_number)."""
    service = AccountService(db)
    metadata = service.auto_fill_metadata(account_number)
    if not metadata:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada en maestro")
    return metadata


@router.delete("/")
def delete_all_accounts(db: Session = Depends(get_db)):
    """Elimina TODAS las cuentas del maestro y datos dependientes."""
    from backend.db.models import Account, DailyPosition, DailyMovement, MonthlyClosing
    db.query(DailyPosition).delete()
    db.query(DailyMovement).delete()
    db.query(MonthlyClosing).delete()
    # Desvincular documentos de las cuentas (no borrar los docs)
    from backend.db.models import RawDocument
    db.query(RawDocument).filter(RawDocument.account_id.isnot(None)).update(
        {"account_id": None}, synchronize_session=False
    )
    count = db.query(Account).count()
    db.query(Account).delete()
    db.commit()
    return {"status": "deleted_all_accounts", "count": count}
