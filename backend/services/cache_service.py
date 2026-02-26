"""
FO Reporting – Servicio de cache.

Pre-calcula y persiste resultados en Parquet para que
la UI no recalcule en cada interacción.

Estrategia:
- Al ingestar datos → calcular resúmenes → guardar Parquet.
- Al pedir datos → leer Parquet filtrado.
- Invalidar cuando hay nueva ingesta.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from backend.config import CACHE_DIR
from backend.db.models import CacheMetadata


class CacheService:
    """Gestión de cache Parquet pre-calculado."""

    def __init__(self, db: Session):
        self.db = db
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def get_cached(self, cache_key: str) -> Optional[pd.DataFrame]:
        """
        Retorna DataFrame cacheado, o None si no existe o es inválido.
        """
        meta = (
            self.db.query(CacheMetadata)
            .filter(
                CacheMetadata.cache_key == cache_key,
                CacheMetadata.is_valid == True,
            )
            .first()
        )

        if meta is None:
            return None

        filepath = Path(meta.filepath)
        if not filepath.exists():
            meta.is_valid = False
            self.db.commit()
            return None

        return pd.read_parquet(filepath)

    def save_cache(
        self,
        cache_key: str,
        df: pd.DataFrame,
        data_hash: Optional[str] = None,
    ) -> None:
        """Guarda DataFrame como Parquet y registra metadata."""
        filepath = CACHE_DIR / f"{cache_key}.parquet"
        df.to_parquet(filepath, index=False)

        if data_hash is None:
            data_hash = hashlib.sha256(
                df.to_json().encode()
            ).hexdigest()

        # Upsert metadata
        existing = (
            self.db.query(CacheMetadata)
            .filter(CacheMetadata.cache_key == cache_key)
            .first()
        )

        if existing:
            existing.filepath = str(filepath)
            existing.created_at = datetime.now(timezone.utc)
            existing.data_hash = data_hash
            existing.is_valid = True
        else:
            meta = CacheMetadata(
                cache_key=cache_key,
                filepath=str(filepath),
                data_hash=data_hash,
                is_valid=True,
            )
            self.db.add(meta)

        self.db.commit()

    def invalidate(self, pattern: Optional[str] = None) -> int:
        """
        Invalida cache.
        Si pattern es None, invalida todo.
        Si se da un pattern, invalida los que contengan ese string.
        """
        query = self.db.query(CacheMetadata).filter(CacheMetadata.is_valid == True)
        if pattern:
            query = query.filter(CacheMetadata.cache_key.contains(pattern))

        items = query.all()
        count = 0
        for item in items:
            item.is_valid = False
            count += 1

        self.db.commit()
        return count

    def list_cache(self) -> list[dict]:
        """Lista toda la metadata de cache."""
        items = self.db.query(CacheMetadata).order_by(CacheMetadata.created_at.desc()).all()
        return [
            {
                "cache_key": m.cache_key,
                "filepath": m.filepath,
                "is_valid": m.is_valid,
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in items
        ]
