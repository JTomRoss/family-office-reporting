"""
Tests para parsers/registry.py y parsers/base.py
"""

import pytest
from pathlib import Path
from decimal import Decimal

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus
from parsers.registry import ParserRegistry, ParserConflictError


class DummyParser(BaseParser):
    BANK_CODE = "test_bank"
    ACCOUNT_TYPE = "test_type"
    VERSION = "1.0.0"
    DESCRIPTION = "Parser de prueba"

    def parse(self, filepath: Path) -> ParseResult:
        return ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash="abc123",
        )

    def validate(self, result: ParseResult) -> list[str]:
        return []

    def detect(self, filepath: Path) -> float:
        return 0.5


class TestBaseParser:
    def test_parser_name(self):
        parser = DummyParser()
        assert parser.get_parser_name() == "parsers.test_bank.test_type"

    def test_source_hash_stable(self):
        parser = DummyParser()
        h1 = parser.get_source_hash()
        h2 = parser.get_source_hash()
        assert h1 == h2
        assert len(h1) == 64  # SHA-256

    def test_repr(self):
        parser = DummyParser()
        r = repr(parser)
        assert "test_bank" in r
        assert "1.0.0" in r

    def test_must_have_bank_and_type(self):
        class BadParser(BaseParser):
            BANK_CODE = ""
            ACCOUNT_TYPE = ""
            VERSION = "1.0.0"

            def parse(self, filepath): ...
            def validate(self, result): ...
            def detect(self, filepath): ...

        with pytest.raises(ValueError):
            BadParser()


class TestParseResult:
    def test_properties(self):
        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name="test",
            parser_version="1.0.0",
            source_file_hash="abc",
            rows=[ParsedRow(data={"a": 1}), ParsedRow(data={"b": 2})],
        )
        assert result.is_success
        assert result.row_count == 2


class TestParserRegistry:
    def test_register_and_retrieve(self):
        registry = ParserRegistry()
        registry.register(DummyParser)
        parser = registry.get_parser("test_bank", "test_type")
        assert parser is not None
        assert parser.BANK_CODE == "test_bank"

    def test_get_nonexistent(self):
        registry = ParserRegistry()
        assert registry.get_parser("no", "exist") is None

    def test_list_parsers(self):
        registry = ParserRegistry()
        registry.register(DummyParser)
        parsers = registry.list_parsers()
        assert len(parsers) == 1
        assert parsers[0]["bank_code"] == "test_bank"

    def test_auto_discover(self):
        """Verifica que auto_discover encuentra los parsers definidos."""
        registry = ParserRegistry()
        registry.auto_discover()
        parsers = registry.list_parsers()
        # Debe encontrar al menos los parsers de JPMorgan, UBS, GS, Excel
        bank_codes = {p["bank_code"] for p in parsers}
        assert "jpmorgan" in bank_codes
        assert "ubs" in bank_codes
        assert "goldman_sachs" in bank_codes
        assert "system" in bank_codes  # Excel parsers

    def test_conflict_detection(self):
        """Registrar 2 parsers con la misma key debe lanzar error."""

        class AnotherParser(BaseParser):
            BANK_CODE = "test_bank"
            ACCOUNT_TYPE = "test_type"
            VERSION = "2.0.0"

            def parse(self, filepath): ...
            def validate(self, result): return []
            def detect(self, filepath): return 0.0

        registry = ParserRegistry()
        registry.register(DummyParser)
        with pytest.raises(ParserConflictError):
            registry.register(AnotherParser)

    def test_same_parser_can_reregister(self):
        """Registrar el mismo parser 2 veces no debe ser error."""
        registry = ParserRegistry()
        registry.register(DummyParser)
        registry.register(DummyParser)  # No lanza error
        assert len(registry.list_parsers()) == 1

    def test_discovery_errors_tracked(self):
        """auto_discover debe trackear errores sin romper."""
        registry = ParserRegistry()
        registry.auto_discover()
        # No debería haber errores en nuestros parsers base
        errors = registry.get_discovery_errors()
        assert isinstance(errors, list)
