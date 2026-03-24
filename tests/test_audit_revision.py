"""Tests del agente de auditoría Revisión."""

import json
import os
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.main import app
from backend.schemas import AuditRevisionParams
from backend.services.audit import audit_service as audit_service_mod
from backend.services.audit.audit_comparator import within_tolerance
from backend.services.audit.audit_deterministic import _sum_asset_allocation_normalized_json
from backend.services.audit.audit_sampling import sample_rows
from backend.services.audit.audit_business_rules import enrich_hallazgo
from backend.db.session import get_session_factory


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_sample_rows_pct_and_cap():
    rows = list(range(100))
    out = sample_rows(rows, sample_pct=50, max_docs=30, sample_mode="recentes")
    assert len(out) == 30


def test_sample_rows_empty():
    assert sample_rows([], sample_pct=25, max_docs=50, sample_mode="recentes") == []


def test_sum_asset_allocation_json_dict_nonlocal_total():
    """Regresión: nested add_val debe usar nonlocal total (evita 500 en foco instrumentos/todos)."""
    payload = json.dumps({"A": {"value": "100"}, "B": {"value": "200.5"}})
    s = _sum_asset_allocation_normalized_json(payload)
    assert s is not None
    assert float(s) == pytest.approx(300.5)


def test_within_tolerance_small_diff():
    from decimal import Decimal

    assert within_tolerance(Decimal("100.0"), Decimal("100.005"))
    assert not within_tolerance(Decimal("100.0"), Decimal("101.0"))


def test_enrich_bbh_ytd_note():
    """Si la brecha YTD coincide con prior adjustments, baja nota y nivel."""
    json_blob = '{"movements_ytd": "1000", "prior_period_adjustments": "50"}'
    nota, nivel = enrich_hallazgo(
        bank_code="bbh",
        elemento_revisado="Movimientos YTD",
        parsed_data_json=json_blob,
        nota="Diferencia",
        nivel="alta",
        norm_movements_ytd=__import__("decimal").Decimal("1050"),
    )
    assert "prior adjustments" in nota.lower()
    assert nivel == "baja"


def test_audit_revision_run_requires_openai_when_muestra_no_vacia(monkeypatch):
    """Si hay documentos en la muestra y no hay OPENAI_API_KEY, debe fallar 400."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    fake = MagicMock()
    monkeypatch.setattr(audit_service_mod, "fetch_universe", lambda *a, **k: [fake])
    monkeypatch.setattr(audit_service_mod, "sample_rows", lambda *a, **k: [fake])
    db = get_session_factory()()
    try:
        with pytest.raises(HTTPException) as ei:
            audit_service_mod.run_audit_revision(db, AuditRevisionParams())
        assert ei.value.status_code == 400
    finally:
        db.close()


def test_audit_revision_focus_todos_expands(client):
    r = client.post(
        "/api/v1/data/audit-revision-run",
        json={
            "focus": "todos",
            "sample_pct": 25,
            "max_docs": 1,
            "sample_mode": "recentes",
            "bank_codes": ["__nonexistent_bank__"],
        },
    )
    assert r.status_code == 200


def test_audit_revision_run_empty_universe(client):
    r = client.post(
        "/api/v1/data/audit-revision-run",
        json={
            "focus": "valor_cierre",
            "sample_pct": 25,
            "max_docs": 5,
            "sample_mode": "recentes",
            "bank_codes": ["__nonexistent_bank__"],
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["total_candidatos"] == 0
    assert data["revisados"] == 0
    assert data["hallazgos"] == []


def test_audit_revision_use_llm_without_key_removed_legacy(client):
    """Endpoint no requiere `use_llm`; universo vacío no exige API key."""
    old = os.environ.pop("OPENAI_API_KEY", None)
    try:
        r = client.post(
            "/api/v1/data/audit-revision-run",
            json={
                "focus": "valor_cierre",
                "sample_pct": 25,
                "max_docs": 1,
                "sample_mode": "recentes",
                "bank_codes": ["__nonexistent_bank__"],
            },
        )
        assert r.status_code == 200
    finally:
        if old is not None:
            os.environ["OPENAI_API_KEY"] = old
