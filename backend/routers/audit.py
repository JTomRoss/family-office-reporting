"""Endpoints del agente de auditoría Revisión (solo lectura)."""

import os

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.db.session import get_db
from backend.schemas import AuditRevisionParams, AuditRevisionResponse
from backend.services.audit.audit_service import run_audit_revision

router = APIRouter(prefix="/data", tags=["audit"])


@router.get("/audit-revision-config")
def get_audit_revision_config() -> dict:
    """Indica si el backend ve OPENAI_API_KEY (sin exponer el valor)."""
    return {"openai_configured": bool(os.getenv("OPENAI_API_KEY", "").strip())}


@router.post("/audit-revision-run", response_model=AuditRevisionResponse)
def post_audit_revision_run(
    params: AuditRevisionParams,
    db: Session = Depends(get_db),
) -> AuditRevisionResponse:
    """Auditoría por LLM (PDF + reglas) vs capa normalizada; solo lectura."""
    return run_audit_revision(db, params)
