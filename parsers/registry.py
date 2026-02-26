"""
FO Reporting – Registro dinámico de parsers (Plugin Registry).

Escanea automáticamente la carpeta parsers/ y registra todos los parsers
que hereden de BaseParser.

Hardened:
- Error isolation: un parser con error de import no rompe el resto.
- Conflict detection: registrar 2 parsers con la misma key lanza error.
- Deterministic auto-detect: en caso de empate, ordena por parser_name.
- Discovery errors se acumulan y se pueden consultar.

Uso:
    registry = ParserRegistry()
    registry.auto_discover()
    parser = registry.get_parser("jpmorgan", "custody")
    result = parser.safe_parse(filepath)
"""

from pathlib import Path
from typing import Optional
import importlib
import pkgutil
import logging

from parsers.base import BaseParser, BaseExcelParser

logger = logging.getLogger(__name__)


class ParserConflictError(Exception):
    """Dos parsers intentan registrarse con la misma (bank_code, account_type)."""
    pass


class ParserRegistry:
    """
    Registro central de parsers disponibles.

    Key: (bank_code, account_type) → Parser class
    """

    def __init__(self):
        self._parsers: dict[tuple[str, str], type[BaseParser]] = {}
        self._discovery_errors: list[str] = []

    def register(self, parser_class: type[BaseParser], allow_override: bool = False) -> None:
        """
        Registra un parser.

        Args:
            parser_class: Clase del parser.
            allow_override: Si False (default), lanza error si la key ya existe.
        """
        key = (parser_class.BANK_CODE, parser_class.ACCOUNT_TYPE)

        if key in self._parsers and not allow_override:
            existing = self._parsers[key]
            if existing is not parser_class:
                raise ParserConflictError(
                    f"Parser key {key} ya registrada por {existing.__name__}. "
                    f"No se puede registrar {parser_class.__name__} con la misma key. "
                    f"Cada parser debe tener un (BANK_CODE, ACCOUNT_TYPE) único."
                )
            return  # Mismo parser, ignorar re-registro silenciosamente

        self._parsers[key] = parser_class
        logger.info(f"Parser registrado: {key} → {parser_class.__name__} v{parser_class.VERSION}")

    def get_parser(self, bank_code: str, account_type: str) -> Optional[BaseParser]:
        """Obtiene instancia de parser por banco y tipo."""
        key = (bank_code, account_type)
        parser_class = self._parsers.get(key)
        if parser_class is None:
            return None
        return parser_class()

    def get_parser_for_file(self, filepath: Path) -> Optional[BaseParser]:
        """
        Auto-detecta qué parser usar para un archivo dado.
        Prueba todos los registrados y retorna el de mayor confianza.

        En caso de empate, elige determinísticamente por nombre del parser
        (orden alfabético) para que el resultado sea reproducible.
        """
        candidates: list[tuple[float, str, BaseParser]] = []

        for parser_class in self._parsers.values():
            try:
                parser = parser_class()
                confidence = parser.detect(filepath)
                if confidence >= 0.3:
                    candidates.append((confidence, parser.get_parser_name(), parser))
            except Exception as e:
                logger.warning(f"Error detectando con {parser_class.__name__}: {e}")

        if not candidates:
            logger.warning(f"Ningún parser detectado con confianza >= 0.3 para {filepath}")
            return None

        # Ordenar por: confianza descendente, luego nombre ascendente (determinista)
        candidates.sort(key=lambda x: (-x[0], x[1]))

        winner = candidates[0]

        # Log si hay empate
        if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
            logger.warning(
                f"Empate en auto-detect para {filepath.name}: "
                f"{[(c[1], c[0]) for c in candidates[:3]]}. "
                f"Seleccionado: {winner[1]} (orden alfabético)."
            )

        return winner[2]

    def list_parsers(self) -> list[dict]:
        """Lista todos los parsers registrados."""
        result = []
        for (bank, atype), cls in sorted(self._parsers.items()):
            result.append({
                "bank_code": bank,
                "account_type": atype,
                "class_name": cls.__name__,
                "version": cls.VERSION,
                "description": cls.DESCRIPTION,
                "supported_extensions": cls.SUPPORTED_EXTENSIONS,
            })
        return result

    def get_discovery_errors(self) -> list[str]:
        """Retorna errores encontrados durante auto_discover."""
        return list(self._discovery_errors)

    def auto_discover(self) -> None:
        """
        Escanea recursivamente parsers/ y registra automáticamente
        todas las clases que hereden de BaseParser.

        Errores de import se aíslan: un parser roto no impide
        que los demás se registren.
        """
        self._discovery_errors.clear()
        parsers_dir = Path(__file__).parent
        package_name = "parsers"

        for module_info in pkgutil.walk_packages(
            [str(parsers_dir)],
            prefix=f"{package_name}.",
        ):
            # Saltar el propio base.py y registry.py
            if module_info.name in (f"{package_name}.base", f"{package_name}.registry"):
                continue

            try:
                module = importlib.import_module(module_info.name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, BaseParser)
                        and attr not in (BaseParser, BaseExcelParser)
                        and attr.BANK_CODE  # Solo clases concretas
                        and attr.ACCOUNT_TYPE
                    ):
                        try:
                            self.register(attr)
                        except ParserConflictError as conflict:
                            error_msg = f"CONFLICTO en {module_info.name}: {conflict}"
                            self._discovery_errors.append(error_msg)
                            logger.error(error_msg)
            except Exception as e:
                error_msg = f"Error importando {module_info.name}: {e}"
                self._discovery_errors.append(error_msg)
                logger.error(error_msg)

        logger.info(
            f"Auto-discovery completado: {len(self._parsers)} parsers registrados, "
            f"{len(self._discovery_errors)} errores"
        )


# ── Singleton global ─────────────────────────────────────────────────
_registry: Optional[ParserRegistry] = None


def get_registry() -> ParserRegistry:
    """Retorna singleton del registry, inicializado con auto-discovery."""
    global _registry
    if _registry is None:
        _registry = ParserRegistry()
        _registry.auto_discover()
    return _registry
