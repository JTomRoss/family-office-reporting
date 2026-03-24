"""
Cliente HTTP para comunicarse con el backend FastAPI.

TODA la comunicación UI → Backend pasa por aquí.
La UI nunca debe importar nada de backend/ directamente.
"""

import httpx
import os
from typing import Any, Optional

# URL base del backend
BACKEND_URL = os.getenv("FO_BACKEND_API_URL", "http://localhost:8000/api/v1")

_client = httpx.Client(base_url=BACKEND_URL, timeout=30.0)


def get(endpoint: str, params: Optional[dict] = None) -> Any:
    """GET request al backend."""
    response = _client.get(endpoint, params=params)
    response.raise_for_status()
    return response.json()


def post(endpoint: str, json: Optional[dict] = None, **kwargs) -> Any:
    """POST request al backend."""
    response = _client.post(endpoint, json=json, **kwargs)
    response.raise_for_status()
    return response.json()


def upload_file(
    endpoint: str,
    filepath: str,
    filename: str,
    file_type: str,
    extra_data: Optional[dict] = None,
) -> Any:
    """Upload de archivo al backend."""
    with open(filepath, "rb") as f:
        files = {"file": (filename, f)}
        data = {"file_type": file_type}
        if extra_data:
            data.update(extra_data)
        response = _client.post(endpoint, files=files, data=data)
    response.raise_for_status()
    return response.json()


def delete(endpoint: str) -> Any:
    """DELETE request al backend."""
    response = _client.delete(endpoint)
    response.raise_for_status()
    return response.json()


def health_check() -> dict:
    """Verifica que el backend esté corriendo."""
    try:
        return get("/health")
    except Exception as e:
        return {"status": "error", "message": str(e)}


def run_audit_revision(params: dict) -> Any:
    """Ejecuta el agente de auditoría Revisión (LLM vs capa normalizada)."""
    response = _client.post("/data/audit-revision-run", json=params, timeout=120.0)
    if response.status_code >= 400:
        try:
            body = response.json()
            detail = body.get("detail")
            if isinstance(detail, list):
                msg = "; ".join(str(x) for x in detail)
            else:
                msg = str(detail) if detail else (response.text or response.reason_phrase)
        except Exception:
            msg = response.text or response.reason_phrase
        raise ValueError(msg) from None
    return response.json()


def get_audit_revision_config() -> dict:
    """Indica si OPENAI_API_KEY está configurada en el backend (sin ver la clave)."""
    return get("/data/audit-revision-config")
