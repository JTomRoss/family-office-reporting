"""Lectura de texto de PDF para fase LLM (pdfplumber)."""

from __future__ import annotations

import os
from pathlib import Path


def extract_pdf_text(filepath: str, max_pages: int = 8) -> str:
    """
    Extrae texto de las primeras páginas del PDF.
    Si el archivo no existe o falla la lectura, retorna cadena vacía.
    """
    path = Path(filepath)
    if not path.is_file():
        return ""

    try:
        import pdfplumber
    except ImportError:
        return ""

    parts: list[str] = []
    try:
        with pdfplumber.open(str(path)) as pdf:
            for i, page in enumerate(pdf.pages[:max_pages]):
                try:
                    t = page.extract_text() or ""
                except Exception:
                    t = ""
                if t.strip():
                    parts.append(f"--- Página {i + 1} ---\n{t}")
    except Exception:
        return ""

    return "\n".join(parts).strip()


def resolve_raw_path(filepath: str) -> str:
    """Normaliza ruta relativa al cwd del proceso."""
    if os.path.isabs(filepath):
        return filepath
    return str(Path.cwd() / filepath)
