"""
FO Reporting – Router de documentos (upload, process, list, delete).
"""

import tempfile
import re
import unicodedata
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy import and_
from sqlalchemy.orm import Session

from backend.db.models import (
    Account,
    MonthlyClosing,
    MonthlyMetricNormalized,
    ParsedStatement,
    RawDocument,
)
from backend.db.session import get_db
from backend.schemas import DocumentUploadResponse, DocumentListItem
from backend.services.document_service import DocumentService

router = APIRouter(prefix="/documents", tags=["documents"])

_BANK_HINTS = {
    "jpmorgan": ("jpmorgan", "jpm", "jp morgan"),
    "ubs": ("ubs suiza", "ubs swiss", "ubs"),
    "ubs_miami": ("ubs miami",),
    "goldman_sachs": ("goldman sachs", "goldman", "gs"),
    "bbh": ("bbh", "brown brothers harriman"),
    "alternativos": ("alternativos",),
}

_ACCOUNT_TYPE_HINTS = {
    "mandato": ("mandato", "mandate", "custody"),
    "brokerage": ("brokerage",),
    "etf": ("etf",),
    "bonds": ("bond", "bonds", "bono", "bonos", "bo"),
    "checking": ("checking",),
    "current": ("current",),
    "savings": ("savings",),
    "investment": ("investment",),
}


def _normalize_text(value: str | None) -> str:
    raw = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = raw.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", ascii_text).strip()


def _filename_contains_token(filename_norm: str, token: str) -> bool:
    compact_filename = filename_norm.replace(" ", "")
    compact_token = _normalize_text(token).replace(" ", "")
    if not compact_token:
        return False
    return compact_token in compact_filename


def _preview_filename_score(account: Account, filename: str) -> tuple[int, list[str]]:
    filename_norm = _normalize_text(filename)
    compact_filename = filename_norm.replace(" ", "")
    score = 0
    reasons: list[str] = []

    for hint in _BANK_HINTS.get(account.bank_code or "", ()):
        if _filename_contains_token(filename_norm, hint):
            score += 3
            reasons.append(f"banco:{hint}")
            break

    for hint in _ACCOUNT_TYPE_HINTS.get(account.account_type or "", ()):
        hint_norm = _normalize_text(hint)
        if not hint_norm:
            continue
        if hint_norm == "bo":
            if re.search(r"(?:^|[^a-z0-9])bo(?:[^a-z0-9]|$)", filename_norm):
                score += 3
                reasons.append("tipo:bo")
                break
            continue
        if _filename_contains_token(filename_norm, hint):
            score += 4
            reasons.append(f"tipo:{hint}")
            break

    entity_name = str(account.entity_name or "").strip()
    if entity_name:
        entity_norm = _normalize_text(entity_name)
        if entity_norm and _filename_contains_token(filename_norm, entity_norm):
            score += 6
            reasons.append("sociedad")

    identification_number = str(account.identification_number or "").strip()
    if identification_number:
        ident_compact = re.sub(r"[^a-z0-9]", "", identification_number.lower())
        if ident_compact and ident_compact in compact_filename:
            score += 10
            reasons.append("id")

    account_number = str(account.account_number or "").strip()
    if account_number:
        account_compact = re.sub(r"[^a-z0-9]", "", account_number.lower())
        if account_compact and account_compact in compact_filename:
            score += 12
            reasons.append("cuenta")

    return score, reasons


def _recognition_confidence(score: int) -> str:
    if score >= 14:
        return "Alta"
    if score >= 8:
        return "Media"
    if score >= 3:
        return "Baja"
    return ""


def _account_match_payload(
    *,
    filename: str,
    account: Account | None,
    status: str,
    confidence: str,
    reason_text: str,
    bank_code: str = "",
    entity_name: str = "",
    account_type: str = "",
) -> dict:
    return {
        "filename": filename,
        "status": status,
        "confidence": confidence,
        "recognition_reason": reason_text,
        "account_id": account.id if account else None,
        "account_number": account.account_number if account else "",
        "identification_number": account.identification_number if account else "",
        "bank_code": account.bank_code if account else bank_code,
        "entity_name": account.entity_name if account else entity_name,
        "account_type": account.account_type if account else account_type,
        "entity_type": account.entity_type if account else "",
        "person_name": account.person_name if account else "",
        "internal_code": account.internal_code if account else "",
        "currency": account.currency if account else "",
    }


def _preview_batch_recognition_rows(
    *,
    db: Session,
    filenames: list[str],
    bank_code: Optional[str] = None,
    entity_name: Optional[str] = None,
    account_type: Optional[str] = None,
) -> list[dict]:
    query = db.query(Account).filter(Account.is_active == True)
    if bank_code:
        query = query.filter(Account.bank_code == bank_code)
    if entity_name:
        query = query.filter(Account.entity_name == entity_name)
    if account_type:
        query = query.filter(Account.account_type == account_type)
    accounts = query.order_by(Account.entity_name, Account.bank_code, Account.account_type, Account.account_number).all()

    rows: list[dict] = []
    for filename in filenames:
        scored: list[tuple[int, list[str], Account]] = []
        for account in accounts:
            score, reasons = _preview_filename_score(account, filename)
            context_only = (
                score == 0
                and bank_code
                and entity_name
                and account_type
            )
            if score > 0 or context_only:
                scored.append((score, reasons, account))

        scored.sort(
            key=lambda item: (
                -item[0],
                str(item[2].entity_name or "").lower(),
                str(item[2].bank_code or "").lower(),
                str(item[2].account_type or "").lower(),
                str(item[2].account_number or "").lower(),
            )
        )

        best_account: Account | None = None
        confidence = ""
        status = "sin_match"
        reason_text = ""
        if scored:
            top_score, top_reasons, top_account = scored[0]
            second_score = scored[1][0] if len(scored) > 1 else None
            unique_context_match = (
                top_score == 0
                and len(scored) == 1
                and bank_code
                and entity_name
                and account_type
            )
            if unique_context_match or (top_score >= 3 and (second_score is None or top_score >= second_score + 2)):
                best_account = top_account
                confidence = "Contexto" if unique_context_match else _recognition_confidence(top_score)
                status = "reconocido"
                reason_text = ", ".join(top_reasons) if top_reasons else "contexto unico"
            else:
                status = "ambiguo"
                confidence = _recognition_confidence(top_score)
                reason_text = "coincidencias multiples"

        rows.append(
            _account_match_payload(
                filename=filename,
                account=best_account,
                status=status,
                confidence=confidence,
                reason_text=reason_text,
                bank_code=(bank_code or ""),
                entity_name=(entity_name or ""),
                account_type=(account_type or ""),
            )
        )
    return rows


def _manual_batch_context_rows(
    *,
    db: Session,
    filenames: list[str],
    bank_code: Optional[str] = None,
    entity_name: Optional[str] = None,
    account_type: Optional[str] = None,
) -> list[dict]:
    cleaned_filenames = [str(name).strip() for name in filenames if str(name).strip()]
    bank_code_value = str(bank_code or "").strip()
    entity_name_value = str(entity_name or "").strip()
    account_type_value = str(account_type or "").strip()

    if not cleaned_filenames:
        return []

    if not (bank_code_value or entity_name_value or account_type_value):
        return [
            _account_match_payload(
                filename=filename,
                account=None,
                status="sin_match",
                confidence="Manual",
                reason_text="sin datos seleccionados",
            )
            for filename in cleaned_filenames
        ]

    query = db.query(Account).filter(Account.is_active == True)
    if bank_code_value:
        query = query.filter(Account.bank_code == bank_code_value)
    if entity_name_value:
        query = query.filter(Account.entity_name == entity_name_value)
    if account_type_value:
        query = query.filter(Account.account_type == account_type_value)
    matches = query.order_by(Account.entity_name, Account.bank_code, Account.account_type, Account.account_number).all()

    if len(matches) == 1:
        matched_account = matches[0]
        return [
            _account_match_payload(
                filename=filename,
                account=matched_account,
                status="reconocido",
                confidence="Manual",
                reason_text="contexto confirmado manualmente",
            )
            for filename in cleaned_filenames
        ]

    if len(matches) > 1:
        return [
            _account_match_payload(
                filename=filename,
                account=None,
                status="ambiguo",
                confidence="Manual",
                reason_text="faltan datos para elegir una sola cuenta",
                bank_code=bank_code_value,
                entity_name=entity_name_value,
                account_type=account_type_value,
            )
            for filename in cleaned_filenames
        ]

    return [
        _account_match_payload(
            filename=filename,
            account=None,
            status="sin_match",
            confidence="Manual",
            reason_text="sin coincidencia en maestro",
            bank_code=bank_code_value,
            entity_name=entity_name_value,
            account_type=account_type_value,
        )
        for filename in cleaned_filenames
    ]


def _resolve_upload_account(
    *,
    db: Session,
    account_id: Optional[int] = None,
    account_number: Optional[str] = None,
    identification_number: Optional[str] = None,
    bank_code: Optional[str] = None,
    entity_name: Optional[str] = None,
    account_type: Optional[str] = None,
) -> Account | None:
    if account_id is not None:
        return db.query(Account).filter(Account.id == account_id).first()

    if account_number:
        account = (
            db.query(Account)
            .filter(Account.account_number == account_number.strip())
            .first()
        )
        if account:
            return account

    if not identification_number:
        return None

    query = db.query(Account).filter(Account.identification_number == identification_number.strip())
    if bank_code:
        query = query.filter(Account.bank_code == bank_code.strip())
    if entity_name:
        query = query.filter(Account.entity_name == entity_name.strip())
    if account_type:
        query = query.filter(Account.account_type == account_type.strip())

    matches = query.order_by(Account.is_active.desc(), Account.id.asc()).all()
    if len(matches) == 1:
        return matches[0]
    return None


def _document_account_display_fields(
    *,
    db: Session,
    doc: RawDocument,
) -> tuple[Optional[str], Optional[str]]:
    if doc.account is not None:
        return doc.account.entity_name, doc.account.account_type

    account_ids: set[int] = set()

    parsed_ids = [
        account_id
        for (account_id,) in (
            db.query(ParsedStatement.account_id)
            .filter(
                ParsedStatement.raw_document_id == doc.id,
                ParsedStatement.account_id.isnot(None),
            )
            .distinct()
            .all()
        )
        if account_id is not None
    ]
    account_ids.update(int(account_id) for account_id in parsed_ids)

    closing_ids = [
        account_id
        for (account_id,) in (
            db.query(MonthlyClosing.account_id)
            .filter(
                MonthlyClosing.source_document_id == doc.id,
                MonthlyClosing.account_id.isnot(None),
            )
            .distinct()
            .all()
        )
        if account_id is not None
    ]
    account_ids.update(int(account_id) for account_id in closing_ids)

    normalized_ids = [
        account_id
        for (account_id,) in (
            db.query(MonthlyMetricNormalized.account_id)
            .filter(
                MonthlyMetricNormalized.source_document_id == doc.id,
                MonthlyMetricNormalized.account_id.isnot(None),
            )
            .distinct()
            .all()
        )
        if account_id is not None
    ]
    account_ids.update(int(account_id) for account_id in normalized_ids)

    if len(account_ids) == 1:
        account = db.query(Account).filter(Account.id == next(iter(account_ids))).first()
        if account is not None:
            return account.entity_name, account.account_type

    if len(account_ids) > 1:
        return "Multiple", "multiple"

    return None, None


@router.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    file_type: str = Form(...),
    bank_code: Optional[str] = Form(None),
    account_id: Optional[int] = Form(None),
    account_number: Optional[str] = Form(None),
    entity_name: Optional[str] = Form(None),
    account_type: Optional[str] = Form(None),
    entity_type: Optional[str] = Form(None),
    person_name: Optional[str] = Form(None),
    internal_code: Optional[str] = Form(None),
    currency: Optional[str] = Form(None),
    sub_accounts: Optional[str] = Form(None),  # Comma-separated for multi-account docs
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
    resolved_account = _resolve_upload_account(
        db=db,
        account_id=account_id,
        account_number=account_number,
        identification_number=identification_number,
        bank_code=bank_code,
        entity_name=entity_name,
        account_type=account_type,
    )

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
            bank_code=(resolved_account.bank_code if resolved_account else bank_code),
            account_id=(resolved_account.id if resolved_account else account_id),
            period_year=period_year,
            period_month=period_month,
        )

        # Si es duplicado, incluir metadata existente para que la UI pueda mostrarla
        existing_meta = None
        if is_duplicate:
            existing_meta = {
                "bank_code": doc.bank_code,
                "account_number": getattr(doc, "account_number", None),
                "entity_name": getattr(doc, "entity_name", None),
                "account_type": getattr(doc, "account_type", None),
                "entity_type": getattr(doc, "entity_type", None),
                "currency": getattr(doc, "currency", None),
                "file_type": doc.file_type,
                "status": doc.status,
            }

        return DocumentUploadResponse(
            id=doc.id,
            filename=doc.filename,
            sha256_hash=doc.sha256_hash,
            file_type=doc.file_type,
            status=doc.status,
            is_duplicate=is_duplicate,
            message="Documento duplicado, no se procesó" if is_duplicate else "Cargado correctamente",
            existing_metadata=existing_meta,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


@router.post("/upload-and-process")
async def upload_and_process(
    file: UploadFile = File(...),
    file_type: str = Form(...),
    bank_code: Optional[str] = Form(None),
    account_id: Optional[int] = Form(None),
    account_number: Optional[str] = Form(None),
    identification_number: Optional[str] = Form(None),
    entity_name: Optional[str] = Form(None),
    account_type: Optional[str] = Form(None),
    entity_type: Optional[str] = Form(None),
    person_name: Optional[str] = Form(None),
    internal_code: Optional[str] = Form(None),
    currency: Optional[str] = Form(None),
    sub_accounts: Optional[str] = Form(None),
    period_year: Optional[int] = Form(None),
    period_month: Optional[int] = Form(None),
    db: Session = Depends(get_db),
):
    """
    Sube Y procesa un documento en un solo paso.
    Útil para Excel maestro y otros archivos que deben procesarse inmediatamente.
    """
    service = DocumentService(db)
    resolved_account = _resolve_upload_account(
        db=db,
        account_id=account_id,
        account_number=account_number,
        identification_number=identification_number,
        bank_code=bank_code,
        entity_name=entity_name,
        account_type=account_type,
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        doc, is_duplicate = service.upload_document(
            temp_filepath=tmp_path,
            original_filename=file.filename,
            file_type=file_type,
            bank_code=(resolved_account.bank_code if resolved_account else bank_code),
            account_id=(resolved_account.id if resolved_account else account_id),
            period_year=period_year,
            period_month=period_month,
        )

        if is_duplicate:
            # Para tipos "operativos" que se reemplazan con cada carga, reprocesar
            # aunque el hash coincida con un documento anterior.
            _always_reprocess = {"excel_master", "excel_alternatives"}
            if file_type in _always_reprocess:
                process_result = service.process_document(doc.id)
                return {
                    "id": doc.id,
                    "filename": doc.filename,
                    "is_duplicate": True,
                    "message": "Reprocesado correctamente",
                    "process_result": process_result,
                }
            return {
                "id": doc.id,
                "filename": doc.filename,
                "is_duplicate": True,
                "message": "Documento duplicado",
            }

        # Procesar inmediatamente
        process_result = service.process_document(doc.id)

        return {
            "id": doc.id,
            "filename": doc.filename,
            "is_duplicate": False,
            "message": "Cargado y procesado correctamente",
            "process_result": process_result,
        }
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


@router.post("/preview-batch-recognition")
def preview_batch_recognition(
    body: dict,
    db: Session = Depends(get_db),
):
    filenames = [
        str(name).strip()
        for name in body.get("filenames", [])
        if str(name).strip()
    ]
    rows = _preview_batch_recognition_rows(
        db=db,
        filenames=filenames,
        bank_code=(body.get("bank_code") or None),
        entity_name=(body.get("entity_name") or None),
        account_type=(body.get("account_type") or None),
    )
    return {"rows": rows, "total": len(rows)}


@router.post("/apply-batch-context")
def apply_batch_context(
    body: dict,
    db: Session = Depends(get_db),
):
    filenames = [
        str(name).strip()
        for name in body.get("filenames", [])
        if str(name).strip()
    ]
    rows = _manual_batch_context_rows(
        db=db,
        filenames=filenames,
        bank_code=(body.get("bank_code") or None),
        entity_name=(body.get("entity_name") or None),
        account_type=(body.get("account_type") or None),
    )
    return {"rows": rows, "total": len(rows)}


@router.get("/cartola-coverage")
def get_cartola_coverage(
    year: int = 2026,
    entity_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    account_query = db.query(Account.entity_name, Account.account_type)
    if entity_name:
        account_query = account_query.filter(Account.entity_name == entity_name)
    base_combos = {
        (str(entity or "").strip(), str(account_type or "").strip())
        for entity, account_type in account_query.distinct().all()
        if str(entity or "").strip() and str(account_type or "").strip()
    }

    coverage_query = (
        db.query(Account.entity_name, Account.account_type, MonthlyClosing.month)
        .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
        .join(
            RawDocument,
            and_(
                RawDocument.id == MonthlyClosing.source_document_id,
                RawDocument.file_type == "pdf_cartola",
            ),
        )
        .filter(MonthlyClosing.year == year)
    )
    if entity_name:
        coverage_query = coverage_query.filter(Account.entity_name == entity_name)

    coverage_by_combo: dict[tuple[str, str], set[int]] = {}
    for entity, account_type, month in coverage_query.all():
        entity_key = str(entity or "").strip()
        type_key = str(account_type or "").strip()
        if not entity_key or not type_key or month is None:
            continue
        base_combos.add((entity_key, type_key))
        coverage_by_combo.setdefault((entity_key, type_key), set()).add(int(month))

    years_query = (
        db.query(MonthlyClosing.year)
        .join(
            RawDocument,
            and_(
                RawDocument.id == MonthlyClosing.source_document_id,
                RawDocument.file_type == "pdf_cartola",
            ),
        )
        .distinct()
        .order_by(MonthlyClosing.year.desc())
    )
    available_years = [int(row[0]) for row in years_query.all() if row[0] is not None]
    entities = sorted({entity for entity, _ in base_combos})

    rows = [
        {
            "entity_name": entity,
            "account_type": account_type,
            "loaded_months": sorted(coverage_by_combo.get((entity, account_type), set())),
        }
        for entity, account_type in sorted(
            base_combos,
            key=lambda item: (item[0].lower(), item[1].lower()),
        )
    ]

    return {
        "selected_year": year,
        "available_years": available_years,
        "entities": entities,
        "months": [f"{year}-{month:02d}" for month in range(1, 13)],
        "rows": rows,
    }


@router.post("/{document_id}/process")
def process_document(
    document_id: int,
    db: Session = Depends(get_db),
):
    """Procesa un documento con el parser adecuado."""
    service = DocumentService(db)
    return service.process_document(document_id)


@router.post("/{document_id}/reclassify")
def reclassify_document(
    document_id: int,
    body: dict,
    db: Session = Depends(get_db),
):
    """
    Reclasifica un documento existente con nueva metadata.
    Usado cuando el usuario carga un duplicado con clasificación diferente.
    """
    service = DocumentService(db)
    result = service.reclassify_document(document_id, body)
    if not result:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return result


@router.get("/", response_model=list[DocumentListItem])
def list_documents(
    file_type: Optional[str] = None,
    bank_code: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Lista documentos con filtros opcionales."""
    service = DocumentService(db)
    docs = service.list_documents(file_type=file_type, bank_code=bank_code, status=status)
    rows = []
    for doc in docs:
        entity_name, account_type = _document_account_display_fields(db=db, doc=doc)
        rows.append(
            {
                "id": doc.id,
                "filename": doc.filename,
                "file_type": doc.file_type,
                "bank_code": doc.bank_code,
                "entity_name": entity_name,
                "account_type": account_type,
                "period_year": doc.period_year,
                "period_month": doc.period_month,
                "status": doc.status,
                "uploaded_at": doc.uploaded_at,
            }
        )
    return rows


@router.delete("/{document_id}")
def delete_document(document_id: int, db: Session = Depends(get_db)):
    """Elimina un documento."""
    service = DocumentService(db)
    success = service.delete_document(document_id)
    if not success:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return {"status": "deleted", "id": document_id}


@router.delete("/")
def delete_all_documents(
    include_accounts: bool = True,
    db: Session = Depends(get_db),
):
    """Elimina TODOS los documentos y sus registros relacionados.
    
    Si include_accounts=True (default), también elimina todas las cuentas
    del maestro, ya que fueron cargadas desde un documento Excel.
    """
    from backend.db.models import (
        RawDocument, ParsedStatement, ValidationLog,
        Account, DailyPosition, DailyMovement, DailyPrice, MonthlyClosing,
    )
    # Eliminar dependientes primero para evitar FK constraints
    db.query(ValidationLog).filter(ValidationLog.raw_document_id.isnot(None)).delete()
    db.query(ParsedStatement).delete()
    doc_count = db.query(RawDocument).count()
    db.query(RawDocument).delete()

    acct_count = 0
    if include_accounts:
        db.query(DailyPosition).delete()
        db.query(DailyMovement).delete()
        db.query(DailyPrice).delete()
        db.query(MonthlyClosing).delete()
        acct_count = db.query(Account).count()
        db.query(Account).delete()

    db.commit()
    return {
        "status": "deleted_all",
        "documents_deleted": doc_count,
        "accounts_deleted": acct_count,
    }
