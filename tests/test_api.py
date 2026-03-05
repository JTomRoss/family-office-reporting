"""
Tests de integración para la API FastAPI.
"""

import pytest
from fastapi.testclient import TestClient

from backend.main import app


@pytest.fixture
def client():
    """TestClient de FastAPI."""
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health(self, client):
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "parsers_loaded" in data

    def test_root(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "app" in data


class TestParsersEndpoint:
    def test_list_parsers(self, client):
        response = client.get("/api/v1/parsers")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Debe tener al menos los parsers base
        bank_codes = {p["bank_code"] for p in data}
        assert "jpmorgan" in bank_codes


class TestDocumentsEndpoint:
    def test_list_empty(self, client):
        response = client.get("/api/v1/documents/")
        assert response.status_code == 200

    def test_delete_nonexistent(self, client):
        response = client.delete("/api/v1/documents/99999")
        assert response.status_code == 404


class TestAccountsEndpoint:
    def test_list_accounts(self, client):
        response = client.get("/api/v1/accounts/")
        assert response.status_code == 200

    def test_filter_options(self, client):
        response = client.get("/api/v1/accounts/filter-options")
        assert response.status_code == 200

    def test_classification_errors(self, client):
        response = client.get("/api/v1/accounts/classification-errors")
        assert response.status_code == 200


class TestDataEndpoint:
    def test_summary(self, client):
        response = client.post("/api/v1/data/summary", json={})
        assert response.status_code == 200

    def test_mandates(self, client):
        response = client.post("/api/v1/data/mandates", json={})
        assert response.status_code == 200

    def test_etf(self, client):
        response = client.post("/api/v1/data/etf", json={})
        assert response.status_code == 200

    def test_reconciliation(self, client):
        response = client.post("/api/v1/data/reconciliation", json={})
        assert response.status_code == 200

    def test_personal(self, client):
        response = client.post("/api/v1/data/personal", json={})
        assert response.status_code == 200

    def test_asset_allocation_report(self, client):
        response = client.post("/api/v1/data/asset-allocation-report", json={})
        assert response.status_code == 200

    def test_validation_logs(self, client):
        response = client.get("/api/v1/data/validation-logs")
        assert response.status_code == 200

    def test_parser_quality(self, client):
        response = client.get("/api/v1/data/parser-quality")
        assert response.status_code == 200
