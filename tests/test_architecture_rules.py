from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"


def _iter_frontend_files():
    for path in FRONTEND_DIR.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        yield path


def test_frontend_does_not_import_backend_or_parsers():
    violations = []
    for path in _iter_frontend_files():
        text = path.read_text(encoding="utf-8")
        if "from backend" in text or "import backend" in text:
            violations.append(f"{path}: import backend")
        if "from parsers" in text or "import parsers" in text:
            violations.append(f"{path}: import parsers")
    assert not violations, "Violaciones de arquitectura:\n" + "\n".join(violations)


def test_frontend_pages_do_not_call_httpx_directly():
    violations = []
    pages_dir = FRONTEND_DIR / "pages"
    for path in pages_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "import httpx" in text or "from httpx" in text:
            violations.append(f"{path}: uso directo de httpx")
    assert not violations, "Violaciones de cliente HTTP:\n" + "\n".join(violations)
