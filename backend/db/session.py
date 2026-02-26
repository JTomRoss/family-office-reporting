"""
Conexión a base de datos y session factory.

El engine se crea LAZY (no al importar el módulo) para que:
- Los tests puedan inyectar su propia BD sin monkey-patching.
- No se abra conexión a BD de producción durante imports transitivos.
"""

from functools import lru_cache
from typing import Generator, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from backend.config import get_settings


@lru_cache
def get_engine(database_url: Optional[str] = None) -> Engine:
    """
    Crea el engine de forma lazy (primera llamada) y lo cachea.

    Args:
        database_url: URL de BD. Si None, usa la de settings.
                      Pasar un valor explícito es útil para tests.
    """
    settings = get_settings()
    url = database_url or settings.database_url

    engine = create_engine(
        url,
        echo=settings.debug,
        connect_args={"check_same_thread": False},  # SQLite
    )
    # Habilitar WAL mode y foreign keys para SQLite
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def get_session_factory(engine: Optional[Engine] = None) -> sessionmaker:
    """Obtiene session factory, opcionalmente con un engine específico."""
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine or get_engine(),
    )


def get_db() -> Generator[Session, None, None]:
    """Dependency injection para FastAPI."""
    factory = get_session_factory()
    db = factory()
    try:
        yield db
    finally:
        db.close()
