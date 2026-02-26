"""
conftest.py – Fixtures compartidas para tests.

Principios:
- Cada test usa su propia BD en memoria (aislamiento total).
- El engine de producción NUNCA se toca en tests.
- Fixtures crean y destruyen datos sin efectos secundarios.
"""

import pytest
import tempfile
from pathlib import Path
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.db.models import Base, Account


@pytest.fixture
def db_engine():
    """Engine SQLite en memoria para tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    """Session de BD en memoria para tests. Rollback automático."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def tmp_dir():
    """Directorio temporal para tests de archivos."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_csv(tmp_dir):
    """CSV de ejemplo para tests de parsers."""
    csv_path = tmp_dir / "test_positions.csv"
    csv_path.write_text(
        "account_number,position_date,instrument_code,quantity,market_value,currency\n"
        "ACC001,2025-01-31,AAPL,100,15000.00,USD\n"
        "ACC001,2025-01-31,MSFT,50,20000.00,USD\n"
    )
    return csv_path


@pytest.fixture
def sample_account(db_session):
    """Cuenta de ejemplo creada en la BD de tests."""
    account = Account(
        account_number="TEST-001",
        bank_code="jpmorgan",
        bank_name="JP Morgan",
        account_type="custody",
        entity_name="Test Entity",
        entity_type="sociedad",
        currency="USD",
        country="US",
    )
    db_session.add(account)
    db_session.commit()
    db_session.refresh(account)
    return account
