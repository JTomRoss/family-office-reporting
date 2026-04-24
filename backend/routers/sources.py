"""
FO Reporting – Router "sources" (trazabilidad: metadata de documentos fuente).

Alimenta el drawer "Ver fuente" del frontend nuevo. Para cualquier número
renderizado en la UI, el usuario debe poder ver qué archivo/parser/hash lo
originó (§1.5 §10.2 RULES_INHERITED).
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.db.models import Account, RawDocument, ParserVersion

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("/{document_id}")
def get_source(document_id: int, db: Session = Depends(get_db)) -> dict:
    """
    Metadata de un documento fuente: archivo, tipo, hash, parser usado,
    versión, período, estado y cuenta asociada.
    """
    doc = db.query(RawDocument).filter(RawDocument.id == document_id).first()
    if not doc:
        raise HTTPException(status_code=404, detail=f"Documento {document_id} no encontrado")

    pv = None
    if doc.parser_version_id:
        pv = db.query(ParserVersion).filter(ParserVersion.id == doc.parser_version_id).first()

    acct = None
    if doc.account_id:
        a = db.query(Account).filter(Account.id == doc.account_id).first()
        if a:
            acct = {
                "id": f"A{a.id:04d}",
                "account_number": a.account_number,
                "society": a.entity_name,
                "bank": a.bank_code,
                "type": a.account_type,
                "currency": a.currency,
            }

    return {
        "id": doc.id,
        "filename": doc.filename,
        "filepath": doc.filepath,
        "file_type": doc.file_type,
        "sha256_hash": doc.sha256_hash,
        "file_size_bytes": doc.file_size_bytes,
        "bank_code": doc.bank_code,
        "period_year": doc.period_year,
        "period_month": doc.period_month,
        "status": doc.status,
        "error_message": doc.error_message,
        "uploaded_at": doc.uploaded_at.isoformat() if doc.uploaded_at else None,
        "processed_at": doc.processed_at.isoformat() if doc.processed_at else None,
        "parser": (
            {
                "name": pv.parser_name,
                "version": pv.version,
                "source_hash": pv.source_hash,
            } if pv else None
        ),
        "account": acct,
    }
