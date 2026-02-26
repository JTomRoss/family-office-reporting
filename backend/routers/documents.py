"""
FO Reporting – Router de documentos (upload, process, list, delete).
"""

import tempfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.schemas import DocumentUploadResponse, DocumentListItem
from backend.services.document_service import DocumentService

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    file_type: str = Form(...),
    bank_code: Optional[str] = Form(None),
    account_id: Optional[int] = Form(None),
    period_year: Optional[int] = Form(None),
    period_month: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    """
    Sube un documento al sistema.

    Idempotente: si el SHA-256 ya existe, retorna el existente sin duplicar.

    file_type: "pdf_cartola", "pdf_report", "excel_positions",
               "excel_movements", "excel_prices", "excel_master"
    """
    service = DocumentService(db)

    # Guardar en temp
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        doc, is_duplicate = service.upload_document(
            temp_filepath=tmp_path,
            original_filename=file.filename,
            file_type=file_type,
            bank_code=bank_code,
            account_id=account_id,
            period_year=period_year,
            period_month=period_month,
        )

        return DocumentUploadResponse(
            id=doc.id,
            filename=doc.filename,
            sha256_hash=doc.sha256_hash,
            file_type=doc.file_type,
            status=doc.status,
            is_duplicate=is_duplicate,
            message="Documento duplicado, no se procesó" if is_duplicate else "Cargado correctamente",
        )
    finally:
        tmp_path.unlink(missing_ok=True)


@router.post("/upload-batch")
async def upload_batch(
    files: list[UploadFile] = File(...),
    file_type: str = Form(...),
    bank_code: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Carga masiva de documentos."""
    service = DocumentService(db)
    results = []

    for file in files:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=Path(file.filename).suffix
        ) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            doc, is_dup = service.upload_document(
                temp_filepath=tmp_path,
                original_filename=file.filename,
                file_type=file_type,
                bank_code=bank_code,
            )
            results.append({
                "filename": file.filename,
                "id": doc.id,
                "is_duplicate": is_dup,
                "status": doc.status,
            })
        except Exception as e:
            results.append({
                "filename": file.filename,
                "error": str(e),
            })
        finally:
            tmp_path.unlink(missing_ok=True)

    return {"results": results, "total": len(results)}


@router.post("/{document_id}/process")
def process_document(
    document_id: int,
    db: Session = Depends(get_db),
):
    """Procesa un documento con el parser adecuado."""
    service = DocumentService(db)
    return service.process_document(document_id)


@router.get("/", response_model=list[DocumentListItem])
def list_documents(
    file_type: Optional[str] = None,
    bank_code: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista documentos con filtros opcionales."""
    service = DocumentService(db)
    return service.list_documents(file_type=file_type, bank_code=bank_code, status=status)


@router.delete("/{document_id}")
def delete_document(document_id: int, db: Session = Depends(get_db)):
    """Elimina un documento."""
    service = DocumentService(db)
    success = service.delete_document(document_id)
    if not success:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return {"status": "deleted", "id": document_id}


@router.delete("/")
def delete_all_documents(db: Session = Depends(get_db)):
    """Elimina TODOS los documentos. Usar con cuidado."""
    from backend.db.models import RawDocument
    count = db.query(RawDocument).count()
    db.query(RawDocument).delete()
    db.commit()
    return {"status": "deleted_all", "count": count}
