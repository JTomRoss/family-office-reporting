"""
FO Reporting – Servicio de cuentas (maestro).

El Excel maestro de cuentas es el SSOT.
Este servicio se encarga de:
- Cargar/actualizar el maestro
- Auto-completar metadata en otros procesos
- Detectar errores de clasificación
"""

from typing import Optional
from sqlalchemy.orm import Session

from backend.db.models import Account, ValidationLog


class AccountService:
    def __init__(self, db: Session):
        self.db = db

    def get_all(self, active_only: bool = True) -> list[Account]:
        query = self.db.query(Account)
        if active_only:
            query = query.filter(Account.is_active == True)
        return query.order_by(Account.bank_code, Account.entity_name).all()

    def get_by_number(self, account_number: str) -> Optional[Account]:
        return (
            self.db.query(Account)
            .filter(Account.account_number == account_number)
            .first()
        )

    def get_by_identification(
        self,
        identification_number: str,
        bank_code: str | None = None,
        entity_name: str | None = None,
    ) -> Optional[Account]:
        """Busca cuenta por dígito verificador, opcionalmente filtrado por banco/sociedad."""
        query = self.db.query(Account).filter(
            Account.identification_number == identification_number
        )
        if bank_code:
            query = query.filter(Account.bank_code == bank_code)
        if entity_name:
            query = query.filter(Account.entity_name == entity_name)
        return query.first()

    def upsert_from_master(self, rows: list[dict], source_hash: str) -> dict:
        """
        Upsert desde Excel maestro.
        Crea cuentas nuevas, actualiza existentes.
        account_number es UNIQUE (número real de cuenta).

        Returns:
            {"created": n, "updated": n, "errors": [...]}
        """
        stats = {"created": 0, "updated": 0, "errors": []}

        _UPDATE_FIELDS = [
            "identification_number",
            "bank_code", "bank_name", "account_type",
            "entity_name", "entity_type", "currency",
            "country", "mandate_type", "is_active",
            "person_name", "internal_code", "metadata_json",
        ]

        for row in rows:
            acct_num = row.get("account_number")
            if not acct_num:
                stats["errors"].append(f"Fila sin account_number: {row}")
                continue
            acct_num = str(acct_num)

            existing = self.get_by_number(acct_num)

            if existing:
                # Actualizar
                for field in _UPDATE_FIELDS:
                    val = row.get(field)
                    if val is not None:
                        setattr(existing, field, val)
                existing.source_file_hash = source_hash
                stats["updated"] += 1
            else:
                # Crear
                account = Account(
                    account_number=acct_num,
                    identification_number=row.get("identification_number"),
                    bank_code=str(row.get("bank_code", "unknown")),
                    bank_name=str(row.get("bank_name", "")),
                    account_type=str(row.get("account_type", "unknown")),
                    entity_name=str(row.get("entity_name", "")),
                    entity_type=str(row.get("entity_type", "sociedad")),
                    currency=str(row.get("currency", "USD")),
                    country=str(row.get("country", "")),
                    mandate_type=row.get("mandate_type"),
                    person_name=row.get("person_name"),
                    internal_code=row.get("internal_code"),
                    is_active=bool(row.get("is_active", True)),
                    metadata_json=row.get("metadata_json"),
                    source_file_hash=source_hash,
                )
                self.db.add(account)
                self.db.flush()  # Flush para que la siguiente query lo encuentre
                stats["created"] += 1

        self.db.commit()

        # Log
        self._log(
            "master_check", "info",
            f"Maestro actualizado: {stats['created']} creadas, "
            f"{stats['updated']} actualizadas, {len(stats['errors'])} errores"
        )

        return stats

    def auto_fill_metadata(self, account_number: str) -> Optional[dict]:
        """
        Auto-completa metadata desde el maestro (por account_number).
        """
        account = self.get_by_number(account_number)
        if not account:
            return None
        return self._account_to_autofill(account)

    def auto_fill_by_identification(
        self,
        identification_number: str,
        bank_code: str | None = None,
        entity_name: str | None = None,
    ) -> Optional[dict]:
        """
        Auto-completa metadata desde el maestro usando dígito verificador.
        Usado en la carga de PDFs donde el usuario ingresa banco + sociedad + ID.
        """
        account = self.get_by_identification(
            identification_number, bank_code, entity_name
        )
        if not account:
            return None
        return self._account_to_autofill(account)

    @staticmethod
    def _account_to_autofill(account: Account) -> dict:
        return {
            "account_number": account.account_number,
            "identification_number": account.identification_number,
            "bank_code": account.bank_code,
            "bank_name": account.bank_name,
            "account_type": account.account_type,
            "entity_name": account.entity_name,
            "entity_type": account.entity_type,
            "currency": account.currency,
            "country": account.country,
            "mandate_type": account.mandate_type,
            "person_name": account.person_name,
            "internal_code": account.internal_code,
        }

    def detect_classification_errors(self) -> list[dict]:
        """
        Detecta errores de clasificación en el maestro.
        Ej: bank_code vacío, account_type inválido, etc.
        """
        errors = []
        valid_types = {"custody", "current", "savings", "investment", "etf",
                       "brokerage", "mandato", "bonds", "checking"}
        valid_entity_types = {"sociedad", "persona"}

        accounts = self.db.query(Account).all()
        for acct in accounts:
            if not acct.bank_code:
                errors.append({
                    "account": acct.account_number,
                    "error": "bank_code vacío",
                })
            if acct.account_type not in valid_types:
                errors.append({
                    "account": acct.account_number,
                    "error": f"account_type inválido: {acct.account_type}",
                })
            if acct.entity_type and acct.entity_type not in valid_entity_types:
                errors.append({
                    "account": acct.account_number,
                    "error": f"entity_type inválido: {acct.entity_type}",
                })

        return errors

    def get_filter_options(self) -> dict[str, list[str]]:
        """Retorna opciones de filtro de cuentas para la UI."""
        accounts = self.get_all()
        return {
            "bank_codes": sorted(set(a.bank_code for a in accounts)),
            "entity_names": sorted(set(a.entity_name for a in accounts)),
            "account_types": sorted(set(a.account_type for a in accounts)),
            "currencies": sorted(set(a.currency for a in accounts)),
            "countries": sorted(set(a.country for a in accounts)),
        }

    def _log(self, vtype: str, severity: str, message: str):
        log = ValidationLog(
            validation_type=vtype,
            severity=severity,
            message=message,
            source_module="services.account_service",
        )
        self.db.add(log)
        self.db.commit()
