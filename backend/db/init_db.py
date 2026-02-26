"""
Inicialización de la base de datos.

Estrategia:
- En producción: usar Alembic migrations (alembic upgrade head).
- En desarrollo: create_all() como fallback rápido.
- init_database() detecta automáticamente cuál usar.

Ejecutar manualmente:
    python -m backend.db.init_db
"""

import logging
from pathlib import Path

from backend.config import ensure_dirs, PROJECT_ROOT
from backend.db.session import get_engine
from backend.db.models import Base

logger = logging.getLogger(__name__)


def _alembic_available() -> bool:
    """Verifica si Alembic está instalado y configurado."""
    try:
        import alembic  # noqa: F401
        ini_path = PROJECT_ROOT / "alembic.ini"
        return ini_path.exists()
    except ImportError:
        return False


def _run_alembic_upgrade() -> bool:
    """Ejecuta alembic upgrade head."""
    try:
        from alembic.config import Config
        from alembic import command

        ini_path = str(PROJECT_ROOT / "alembic.ini")
        alembic_cfg = Config(ini_path)
        command.upgrade(alembic_cfg, "head")
        logger.info("✓ Alembic migrations aplicadas correctamente.")
        return True
    except Exception as e:
        logger.warning(f"Alembic upgrade falló: {e}. Usando create_all() como fallback.")
        return False


def init_database() -> None:
    """
    Inicializa la BD.
    Intenta Alembic primero; si falla, cae a create_all().
    """
    ensure_dirs()

    if _alembic_available():
        if _run_alembic_upgrade():
            return

    # Fallback: create_all (útil en tests y desarrollo rápido)
    Base.metadata.create_all(bind=get_engine())
    logger.info("✓ Base de datos inicializada con create_all().")
    print("✓ Base de datos inicializada correctamente.")
    print(f"  Tablas: {list(Base.metadata.tables.keys())}")


if __name__ == "__main__":
    init_database()
