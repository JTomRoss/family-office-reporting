"""
FO Reporting – Tests de contrato para parsers.

Este módulo contiene:
1) BaseParserContractTest: clase base que cualquier parser hereda
   para verificar automáticamente que cumple el contrato.
2) Tests genéricos del sistema de parsers y registry.

USO para tests de un parser específico:
    class TestJPMorganCustody(BaseParserContractTest):
        parser_class = JPMorganCustodyParser
        fixture_path = Path("tests/fixtures/jpmorgan_custody_sample.pdf")

    # Los tests de contrato se ejecutan automáticamente.
"""

import pytest
from pathlib import Path
from decimal import Decimal

from parsers.base import BaseParser, BaseExcelParser, ParseResult, ParserStatus, ParsedRow
from parsers.registry import ParserRegistry


# ═══════════════════════════════════════════════════════════════════
# BASE CONTRACT TEST — Herédala para cada parser concreto
# ═══════════════════════════════════════════════════════════════════

class BaseParserContractTest:
    """
    Clase base de tests de contrato para parsers.

    Cualquier parser que herede de BaseParser DEBE tener tests que
    hereden de esta clase. Los tests verifican automáticamente:

    1. Metadata del parser (BANK_CODE, ACCOUNT_TYPE, VERSION)
    2. Que parse() retorna un ParseResult válido
    3. Que validate() retorna lista de strings
    4. Que detect() retorna float entre 0.0 y 1.0
    5. Que validate_contract() no encuentra errores
    6. Que get_source_hash() retorna hash reproducible
    7. Que get_parser_name() tiene formato correcto

    Subclases deben definir:
        parser_class: type[BaseParser]  — La clase del parser
        fixture_path: Path              — Path a archivo fixture para testing
                                          (puede ser None si no hay fixture aún)
    """

    parser_class: type[BaseParser] = None  # type: ignore
    fixture_path: Path = None  # type: ignore

    @pytest.fixture
    def parser(self):
        """Instancia del parser."""
        if self.parser_class is None:
            pytest.skip("parser_class no definida (clase base)")
        return self.parser_class()

    # ── Tests de metadata ────────────────────────────────────────

    def test_bank_code_defined(self, parser):
        """BANK_CODE debe ser un string no vacío."""
        assert parser.BANK_CODE, "BANK_CODE no puede estar vacío"
        assert isinstance(parser.BANK_CODE, str)

    def test_account_type_defined(self, parser):
        """ACCOUNT_TYPE debe ser un string no vacío."""
        assert parser.ACCOUNT_TYPE, "ACCOUNT_TYPE no puede estar vacío"
        assert isinstance(parser.ACCOUNT_TYPE, str)

    def test_version_format(self, parser):
        """VERSION debe seguir semver (X.Y.Z)."""
        parts = parser.VERSION.split(".")
        assert len(parts) == 3, f"VERSION '{parser.VERSION}' no es semver (X.Y.Z)"
        for part in parts:
            assert part.isdigit(), f"VERSION '{parser.VERSION}' tiene partes no numéricas"

    def test_supported_extensions(self, parser):
        """SUPPORTED_EXTENSIONS debe ser lista no vacía de strings con punto."""
        assert parser.SUPPORTED_EXTENSIONS, "SUPPORTED_EXTENSIONS vacía"
        for ext in parser.SUPPORTED_EXTENSIONS:
            assert ext.startswith("."), f"Extensión '{ext}' debe empezar con '.'"

    # ── Tests de identidad ───────────────────────────────────────

    def test_parser_name_format(self, parser):
        """get_parser_name() debe ser parsers.<banco>.<tipo>"""
        name = parser.get_parser_name()
        assert name.startswith("parsers."), f"Parser name '{name}' no empieza con 'parsers.'"
        parts = name.split(".")
        assert len(parts) == 3, f"Parser name '{name}' debe tener 3 partes"

    def test_source_hash_reproducible(self, parser):
        """get_source_hash() debe ser determinista."""
        hash1 = parser.get_source_hash()
        hash2 = parser.get_source_hash()
        assert hash1 == hash2, "source_hash no es reproducible"
        assert len(hash1) == 64, "source_hash debe ser SHA-256 (64 chars)"

    def test_repr(self, parser):
        """__repr__ debe incluir bank y type."""
        r = repr(parser)
        assert parser.BANK_CODE in r
        assert parser.ACCOUNT_TYPE in r

    # ── Tests de contrato funcional ──────────────────────────────

    def test_detect_returns_float(self, parser, tmp_path):
        """detect() debe retornar float entre 0.0 y 1.0."""
        # Crear archivo dummy
        dummy = tmp_path / "test.pdf"
        dummy.write_bytes(b"%PDF-1.4 dummy content")

        confidence = parser.detect(dummy)
        assert isinstance(confidence, (int, float)), "detect() debe retornar float"
        assert 0.0 <= confidence <= 1.0, f"detect() retornó {confidence}, debe ser 0.0-1.0"

    def test_detect_returns_low_for_wrong_file(self, parser, tmp_path):
        """detect() debe retornar baja confianza para archivos claramente incorrectos."""
        wrong = tmp_path / "not_a_statement.txt"
        wrong.write_text("This is definitely not a bank statement")

        confidence = parser.detect(wrong)
        assert confidence < 0.5, (
            f"detect() retornó {confidence} para archivo de texto plano; "
            "debería ser < 0.5"
        )

    def test_validate_returns_list(self, parser):
        """validate() debe retornar lista de strings."""
        # ParseResult vacío
        result = ParseResult(
            status=ParserStatus.ERROR,
            parser_name=parser.get_parser_name(),
            parser_version=parser.VERSION,
            source_file_hash="0" * 64,
        )
        errors = parser.validate(result)
        assert isinstance(errors, list), "validate() debe retornar list"
        for err in errors:
            assert isinstance(err, str), "Cada error debe ser string"

    # ── Tests con fixture (si disponible) ────────────────────────

    def test_parse_with_fixture(self, parser):
        """Si hay fixture, parse() debe retornar ParseResult con contrato OK."""
        if self.fixture_path is None or not self.fixture_path.exists():
            pytest.skip("No fixture disponible para este parser")

        result = parser.safe_parse(self.fixture_path)

        # Tipo correcto
        assert isinstance(result, ParseResult), "parse() debe retornar ParseResult"

        # Status válido
        assert result.status in ParserStatus, f"Status '{result.status}' no es válido"

        # Contrato
        contract_errors = parser.validate_contract(result)
        assert not contract_errors, (
            f"Errores de contrato: {contract_errors}"
        )

    def test_parse_fixture_has_parser_metadata(self, parser):
        """El ParseResult del fixture debe incluir metadata del parser."""
        if self.fixture_path is None or not self.fixture_path.exists():
            pytest.skip("No fixture disponible")

        result = parser.safe_parse(self.fixture_path)
        assert result.parser_name == parser.get_parser_name()
        assert result.parser_version == parser.VERSION


# ═══════════════════════════════════════════════════════════════════
# TESTS GENÉRICOS DEL SISTEMA DE PARSERS
# ═══════════════════════════════════════════════════════════════════

class TestParserRegistry:
    """Tests del registro de parsers."""

    def test_auto_discover_loads_parsers(self):
        """auto_discover debe encontrar al menos 1 parser."""
        registry = ParserRegistry()
        registry.auto_discover()
        parsers = registry.list_parsers()
        assert len(parsers) > 0, "auto_discover no encontró ningún parser"

    def test_all_parser_keys_are_unique(self):
        """No puede haber dos parsers con el mismo (bank_code, account_type)."""
        registry = ParserRegistry()
        registry.auto_discover()
        parsers = registry.list_parsers()

        keys = [(p["bank_code"], p["account_type"]) for p in parsers]
        assert len(keys) == len(set(keys)), (
            f"Hay parsers con keys duplicadas: "
            f"{[k for k in keys if keys.count(k) > 1]}"
        )

    def test_all_parsers_have_valid_metadata(self):
        """Todos los parsers registrados deben tener metadata válida."""
        registry = ParserRegistry()
        registry.auto_discover()

        for info in registry.list_parsers():
            assert info["bank_code"], f"{info['class_name']}: bank_code vacío"
            assert info["account_type"], f"{info['class_name']}: account_type vacío"
            assert info["version"], f"{info['class_name']}: version vacía"

    def test_all_parsers_instantiate_without_error(self):
        """Todos los parsers registrados deben poder instanciarse."""
        registry = ParserRegistry()
        registry.auto_discover()

        for info in registry.list_parsers():
            parser = registry.get_parser(info["bank_code"], info["account_type"])
            assert parser is not None, (
                f"get_parser({info['bank_code']}, {info['account_type']}) retornó None"
            )

    def test_get_parser_returns_none_for_unknown(self):
        """get_parser con clave inexistente retorna None."""
        registry = ParserRegistry()
        assert registry.get_parser("nonexistent_bank", "unknown_type") is None


class TestBaseParserContract:
    """Tests de validate_contract con datos sintéticos."""

    def test_contract_passes_for_valid_result(self):
        """Un ParseResult completo debe pasar el contrato."""

        class DummyParser(BaseParser):
            BANK_CODE = "test"
            ACCOUNT_TYPE = "dummy"
            VERSION = "1.0.0"

            def parse(self, filepath):
                pass  # pragma: no cover

            def validate(self, result):
                return []

            def detect(self, filepath):
                return 0.0

        parser = DummyParser()
        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name="parsers.test.dummy",
            parser_version="1.0.0",
            source_file_hash="a" * 64,
            account_number="ACC001",
            currency="USD",
            rows=[ParsedRow(data={"instrument": "AAPL", "value": 100})],
        )

        errors = parser.validate_contract(result)
        assert errors == [], f"No debería haber errores: {errors}"

    def test_contract_fails_for_success_without_account(self):
        """SUCCESS sin account_number debe fallar contrato."""

        class DummyParser(BaseParser):
            BANK_CODE = "test"
            ACCOUNT_TYPE = "dummy"
            VERSION = "1.0.0"

            def parse(self, filepath):
                pass  # pragma: no cover

            def validate(self, result):
                return []

            def detect(self, filepath):
                return 0.0

        parser = DummyParser()
        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name="parsers.test.dummy",
            parser_version="1.0.0",
            source_file_hash="a" * 64,
            # account_number deliberadamente omitido
            currency="USD",
            rows=[ParsedRow(data={"x": 1})],
        )

        errors = parser.validate_contract(result)
        assert any("account_number" in e for e in errors)

    def test_contract_fails_for_empty_row_data(self):
        """Filas con data vacía deben fallar contrato."""

        class DummyParser(BaseParser):
            BANK_CODE = "test"
            ACCOUNT_TYPE = "dummy"
            VERSION = "1.0.0"

            def parse(self, filepath):
                pass  # pragma: no cover

            def validate(self, result):
                return []

            def detect(self, filepath):
                return 0.0

        parser = DummyParser()
        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name="parsers.test.dummy",
            parser_version="1.0.0",
            source_file_hash="a" * 64,
            account_number="ACC001",
            currency="USD",
            rows=[ParsedRow(data={})],  # data vacía
        )

        errors = parser.validate_contract(result)
        assert any("data vacía" in e for e in errors)
