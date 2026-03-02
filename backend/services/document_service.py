"""
FO Reporting – Servicio de documentos.

Maneja:
- Upload con idempotencia (SHA-256)
- Clasificación automática
- Procesamiento delegado a parsers
- Almacenamiento de raw files
"""

import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from backend.config import RAW_DIR, PROJECT_ROOT
from backend.db.models import RawDocument, ParserVersion, ValidationLog
from backend.services.cache_service import CacheService
from parsers.registry import get_registry


def _to_relative(path: Path) -> str:
    """Convierte path absoluto a relativo respecto a PROJECT_ROOT para guardar en BD."""
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _to_absolute(relative_path: str) -> Path:
    """Reconstruye path absoluto desde el relativo guardado en BD."""
    p = Path(relative_path)
    if p.is_absolute():
        return p
    return PROJECT_ROOT / p


class DocumentService:
    """Servicio para gestión de documentos."""

    def __init__(self, db: Session):
        self.db = db

    def compute_hash(self, filepath: Path) -> str:
        """SHA-256 del archivo."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def check_duplicate(self, file_hash: str) -> Optional[RawDocument]:
        """Verifica si el documento ya existe (idempotencia)."""
        return (
            self.db.query(RawDocument)
            .filter(RawDocument.sha256_hash == file_hash)
            .first()
        )

    def upload_document(
        self,
        temp_filepath: Path,
        original_filename: str,
        file_type: str,
        bank_code: Optional[str] = None,
        account_id: Optional[int] = None,
        period_year: Optional[int] = None,
        period_month: Optional[int] = None,
    ) -> tuple[RawDocument, bool]:
        """
        Sube un documento al sistema.

        Returns:
            (document, is_duplicate)
        """
        file_hash = self.compute_hash(temp_filepath)

        # ── Idempotencia ─────────────────────────────────────────
        existing = self.check_duplicate(file_hash)
        if existing:
            self._log_validation(
                "idempotency", "info",
                f"Documento duplicado detectado: {original_filename} (hash={file_hash[:16]}...)",
                raw_document_id=existing.id,
            )
            return existing, True

        # ── Guardar archivo raw ──────────────────────────────────
        dest_dir = RAW_DIR / (bank_code or "unclassified") / file_type
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / original_filename

        # Si ya existe el nombre, agregar timestamp
        if dest_path.exists():
            stem = dest_path.stem
            suffix = dest_path.suffix
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            dest_path = dest_dir / f"{stem}_{ts}{suffix}"

        shutil.copy2(str(temp_filepath), str(dest_path))

        # ── Crear registro en BD ─────────────────────────────────
        doc = RawDocument(
            filename=original_filename,
            filepath=_to_relative(dest_path),
            file_type=file_type,
            sha256_hash=file_hash,
            file_size_bytes=temp_filepath.stat().st_size,
            bank_code=bank_code,
            account_id=account_id,
            period_year=period_year,
            period_month=period_month,
            status="uploaded",
        )
        self.db.add(doc)
        self.db.commit()
        self.db.refresh(doc)

        self._log_validation(
            "load", "info",
            f"Documento cargado: {original_filename} (hash={file_hash[:16]}...)",
            raw_document_id=doc.id,
        )

        return doc, False

    def process_document(self, document_id: int) -> dict:
        """
        Procesa un documento usando el parser adecuado.

        Returns:
            Dict con status del procesamiento.
        """
        doc = self.db.query(RawDocument).filter(RawDocument.id == document_id).first()
        if not doc:
            return {"status": "error", "message": "Documento no encontrado"}

        doc.status = "processing"
        self.db.commit()

        filepath = _to_absolute(doc.filepath)
        if not filepath.exists():
            doc.status = "error"
            doc.error_message = f"Archivo no encontrado: {filepath}"
            self.db.commit()
            return {"status": "error", "message": doc.error_message}

        # ── Buscar parser adecuado ───────────────────────────────
        registry = get_registry()

        parser = None
        if doc.bank_code and doc.file_type:
            # Intentar parser específico
            account_type = doc.file_type.replace("pdf_", "").replace("excel_", "")
            parser = registry.get_parser(doc.bank_code, account_type)
        # Para excel_master, buscar el parser de sistema
        if parser is None and doc.file_type == "excel_master":
            parser = registry.get_parser("system", "master_accounts")
        if parser is None:
            # Auto-detectar
            parser = registry.get_parser_for_file(filepath)

        if parser is None:
            doc.status = "error"
            doc.error_message = "No se encontró parser adecuado"
            self.db.commit()
            self._log_validation(
                "parse", "error",
                f"Sin parser para: {doc.filename}",
                raw_document_id=doc.id,
            )
            return {"status": "error", "message": doc.error_message}

        # ── Registrar versión del parser ─────────────────────────
        parser_version = self._ensure_parser_version(parser)
        doc.parser_version_id = parser_version.id

        # ── Ejecutar parsing (safe_parse valida contrato automáticamente) ──
        try:
            result = parser.safe_parse(filepath)

            # Validar resultado
            validation_errors = parser.validate(result)
            if validation_errors:
                for err in validation_errors:
                    self._log_validation(
                        "parse", "warning", err,
                        raw_document_id=doc.id,
                        source_module=parser.get_parser_name(),
                    )

            doc.status = "parsed" if result.is_success else "error"
            doc.processed_at = datetime.now(timezone.utc)
            if result.errors:
                doc.error_message = "; ".join(result.errors)

            self.db.commit()

            # ── Cargar datos parseados a tablas de reporting ────
            loading_stats = None
            if doc.file_type != "excel_master" and result.is_success:
                try:
                    from backend.services.data_loading_service import DataLoadingService
                    loader = DataLoadingService(self.db)
                    loading_stats = loader.load_parse_result(
                        result=result,
                        raw_document=doc,
                        parser_version_id=parser_version.id,
                    )
                    self._log_validation(
                        "load", "info",
                        f"Datos cargados: {loading_stats['parsed_statements']} statements, "
                        f"{loading_stats['monthly_closings']} closings, "
                        f"{loading_stats['etf_compositions']} compositions",
                        raw_document_id=doc.id,
                    )
                except Exception as load_err:
                    self._log_validation(
                        "load", "error",
                        f"Error cargando datos de reporting: {load_err}",
                        raw_document_id=doc.id,
                    )

            # ── Si es excel_master, alimentar AccountService ────
            master_stats = None
            if doc.file_type == "excel_master" and result.is_success:
                try:
                    from backend.services.account_service import AccountService
                    acct_service = AccountService(self.db)
                    # Convertir ParsedRow.data a lista de dicts
                    account_rows = []
                    for row in result.rows:
                        # Limpiar NaN de pandas
                        clean = {}
                        for k, v in row.data.items():
                            try:
                                import math
                                if isinstance(v, float) and math.isnan(v):
                                    continue
                            except (TypeError, ValueError):
                                pass
                            clean[k] = v
                        account_rows.append(clean)

                    master_stats = acct_service.upsert_from_master(
                        rows=account_rows,
                        source_hash=result.source_file_hash,
                    )
                    self._log_validation(
                        "master_check", "info",
                        f"Maestro procesado: {master_stats['created']} creadas, "
                        f"{master_stats['updated']} actualizadas, {len(master_stats['errors'])} errores",
                        raw_document_id=doc.id,
                    )
                except Exception as master_err:
                    self._log_validation(
                        "master_check", "error",
                        f"Error procesando maestro de cuentas: {master_err}",
                        raw_document_id=doc.id,
                    )

            # ── Auto-invalidar cache tras ingesta exitosa ────────
            if result.is_success:
                try:
                    cache = CacheService(self.db)
                    invalidated = cache.invalidate()  # Invalida todo el cache
                    if invalidated > 0:
                        self._log_validation(
                            "load", "info",
                            f"Cache invalidado ({invalidated} entradas) tras procesar {doc.filename}",
                            raw_document_id=doc.id,
                        )
                except Exception as cache_err:
                    self._log_validation(
                        "load", "warning",
                        f"Error invalidando cache: {cache_err}",
                        raw_document_id=doc.id,
                    )

            self._log_validation(
                "parse", "info",
                f"Parsed OK: {doc.filename} ({result.row_count} filas)",
                raw_document_id=doc.id,
                source_module=parser.get_parser_name(),
            )

            resp = {
                "status": result.status.value,
                "rows_parsed": result.row_count,
                "warnings": result.warnings,
                "errors": result.errors,
            }
            if master_stats is not None:
                resp["master_stats"] = master_stats
            if loading_stats is not None:
                resp["loading_stats"] = loading_stats
            return resp

        except Exception as e:
            doc.status = "error"
            doc.error_message = str(e)
            self.db.commit()
            self._log_validation(
                "parse", "critical",
                f"Exception parsing {doc.filename}: {e}",
                raw_document_id=doc.id,
            )
            return {"status": "error", "message": str(e)}

    def list_documents(
        self,
        file_type: Optional[str] = None,
        bank_code: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[RawDocument]:
        """Lista documentos con filtros opcionales."""
        query = self.db.query(RawDocument)
        if file_type:
            query = query.filter(RawDocument.file_type == file_type)
        if bank_code:
            query = query.filter(RawDocument.bank_code == bank_code)
        if status:
            query = query.filter(RawDocument.status == status)
        return query.order_by(RawDocument.uploaded_at.desc()).all()

    def delete_document(self, document_id: int) -> bool:
        """Elimina un documento, sus registros dependientes, y su archivo raw."""
        from backend.db.models import MonthlyClosing, EtfComposition, Reconciliation

        doc = self.db.query(RawDocument).filter(RawDocument.id == document_id).first()
        if not doc:
            return False

        # Eliminar registros que referencian este documento (sin cascade en ORM)
        self.db.query(Reconciliation).filter(
            Reconciliation.monthly_closing_id.in_(
                self.db.query(MonthlyClosing.id).filter(
                    MonthlyClosing.source_document_id == document_id
                )
            )
        ).delete(synchronize_session=False)
        self.db.query(MonthlyClosing).filter(
            MonthlyClosing.source_document_id == document_id
        ).delete(synchronize_session=False)
        self.db.query(EtfComposition).filter(
            EtfComposition.source_document_id == document_id
        ).delete(synchronize_session=False)

        # Eliminar archivo físico
        filepath = _to_absolute(doc.filepath)
        if filepath.exists():
            filepath.unlink()

        # parsed_statements y validation_logs se eliminan por cascade
        self.db.delete(doc)
        self.db.commit()

        self._log_validation(
            "load", "info",
            f"Documento eliminado: {doc.filename} (id={document_id})",
        )
        return True

    def reclassify_document(self, document_id: int, metadata: dict) -> dict | None:
        """
        Reclasifica un documento existente con nueva metadata.
        Actualiza bank_code y resetea status para reprocesamiento.
        """
        doc = self.db.query(RawDocument).filter(RawDocument.id == document_id).first()
        if not doc:
            return None

        old_bank = doc.bank_code
        new_bank = metadata.get("bank_code")

        # Actualizar campos disponibles en RawDocument
        if new_bank:
            doc.bank_code = new_bank

        # Resetear a uploaded para que se pueda reprocesar
        doc.status = "uploaded"
        doc.error_message = None
        doc.processed_at = None

        self.db.commit()
        self.db.refresh(doc)

        self._log_validation(
            "load", "info",
            f"Documento reclasificado: {doc.filename} "
            f"(bank: {old_bank}→{new_bank}, id={document_id})",
            raw_document_id=document_id,
        )

        return {
            "status": "reclassified",
            "id": document_id,
            "filename": doc.filename,
            "bank_code": doc.bank_code,
            "new_status": doc.status,
        }

    def _ensure_parser_version(self, parser) -> ParserVersion:
        """Registra o recupera la versión del parser."""
        existing = (
            self.db.query(ParserVersion)
            .filter(
                ParserVersion.parser_name == parser.get_parser_name(),
                ParserVersion.version == parser.VERSION,
            )
            .first()
        )
        if existing:
            return existing

        pv = ParserVersion(
            parser_name=parser.get_parser_name(),
            version=parser.VERSION,
            source_hash=parser.get_source_hash(),
            description=parser.DESCRIPTION,
        )
        self.db.add(pv)
        self.db.commit()
        self.db.refresh(pv)
        return pv

    def _log_validation(
        self,
        validation_type: str,
        severity: str,
        message: str,
        raw_document_id: Optional[int] = None,
        account_id: Optional[int] = None,
        source_module: Optional[str] = None,
    ) -> None:
        """Registra log de validación."""
        log = ValidationLog(
            raw_document_id=raw_document_id,
            account_id=account_id,
            validation_type=validation_type,
            severity=severity,
            message=message,
            source_module=source_module,
        )
        self.db.add(log)
        self.db.commit()
