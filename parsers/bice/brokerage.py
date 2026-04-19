"""
Parser: BICE Inversiones Corredores de Bolsa S.A. (Cartola PDF).

Identificadores del documento:
  - "BICE Inversiones Corredores de Bolsa S.A." en pie de página
  - "biceinversiones.cl" en pie de página
  - Formato chileno (puntos=miles, coma=decimal)

Estructura del PDF (variable; secciones detectadas por título, no por número de página):
  Portada              : Resumen general ($, US$, Patrimonio)
  DETALLE DE INVERSIONES EN $   : posiciones CLP + movimientos caja CLP
  DETALLE DE INVERSIONES EN US$ : posiciones USD + movimientos caja USD
  DETALLE DE CARTERAS  : detalle de instrumentos RF / RV / FM (puede ser 1-N páginas)
  DETALLE DE MOVIMIENTOS: movimientos de caja y de títulos (puede ser 1-N páginas)
  GLOSARIO             : se ignora

Clasificación de instrumentos (orden estricto per spec):
  1. Caja       → nombre contiene "LIQUIDEZ" o "TESORERIA"
  2. Renta Fija → aparece en "Detalle Cartera Renta Fija" (pág. 4)
                  O categoría padre es "Renta Fija" / "Depósitos a Plazo"
  3. Equities   → todo lo demás (catch-all)

Output en result.balances:
  "positions" → {
    "CLP": {"Caja": D, "Renta Fija": D, "Equities": D, "Total": D, "unclassified": []},
    "USD": {"Caja": D, "Renta Fija": D, "Equities": D, "Total": D, "unclassified": []},
  }
  "movements" → {
    "CLP": {"aportes": D, "retiros": D, "dividendos_otros": D, "neto": D},
    "USD": {"aportes": D, "retiros": D, "dividendos_otros": D, "neto": D},
  }
  Aportes/retiros: calculados desde pág. 6 "Movimientos de Títulos en $/$US$".
    retiro_neto = sum(RESCATE FM sobre TESORERIA/LIQUIDEZ DOLAR)
               − sum(INVERSION FM sobre TESORERIA/LIQUIDEZ DOLAR)
    >0 → retiro neto; <0 → aporte neto. Excluye VENCIMIENTO RF, CORCUP, etc.
  Dividendos_otros: extraído de pág. 2-3 (regex Dividendos y Otros(F)).
  "total_activos_clp" / "total_activos_usd"  (pág. 1)
  "patrimonio_clp"    / "patrimonio_usd"      (pág. 1)

AISLADO: no importa ni comparte lógica con ningún otro parser.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import pdfplumber

from parsers.base import BaseParser, ParseResult, ParsedRow, ParserStatus


# ── Versión ──────────────────────────────────────────────────────────────────

VERSION = "3.4.0"

# ── Regex ────────────────────────────────────────────────────────────────────

_RE_PERIOD = re.compile(
    r"Per[ií]odo:\s*(\d{2}-\d{2}-\d{4})\s+al\s+(\d{2}-\d{2}-\d{4})"
)
_RE_RUT = re.compile(r"Rut:\s*([\d.]+-[\dkK])")
_RE_CLIENT = re.compile(r"Cliente:\s+(.+?)\s+Rut:")
_RE_APORTES = re.compile(r"Compras/Aportes\(D\)\s+([\d.,]+)")
_RE_RETIROS = re.compile(r"Ventas/Rescates\s*\(E\)\s+([\d.,]+)")
_RE_DIVIDENDOS = re.compile(r"Dividendos y Otros\(F\)\s+([\d.,]+)")
_RE_CODE_PREFIX = re.compile(r"^([A-Z0-9][A-Z0-9\-]+)")
_RE_TITULO_DATE = re.compile(r"^\d{2}-\d{2}-\d{4}")
_RE_TITULO_MONTO = re.compile(r"([\d]+(?:\.[\d]{3})*(?:,[\d]+)?)\s*$")  # código antes del primer espacio/(

# ── Constantes de clasificación ───────────────────────────────────────────────

# Filas padre (categorías) en la tabla resumen de págs. 2-3.
# Cuando el parser las encuentra en col 0, actualiza el contexto pero NO las trata
# como instrumentos individuales.
_ALL_PARENT_ROWS = frozenset({
    "renta fija", "renta fija (2)",
    "depositos a plazo", "depositos a plazo bice",
    "depositos a plazo bice (1)", "depositos a plazo bice (2)",
    "disponible en caja", "libreta de ahorro",
    "acciones", "fondos mutuos",
    "operaciones en transito", "otros activos y derivados",
    "forward (resultado neto)", "venta corta",
    "patrimonio custodia pershing", "simultaneas", "total pasivos",
})

_TOTAL_ROWS = frozenset({"total activos", "patrimonio"})

# Subconjuntos para la función _classify_instrument()
_RF_PARENTS = frozenset({
    "renta fija", "renta fija (2)",
    "depositos a plazo", "depositos a plazo bice",
    "depositos a plazo bice (1)", "depositos a plazo bice (2)",
})
_CAJA_PARENTS = frozenset({
    "disponible en caja", "libreta de ahorro",
})
_OPERATIONAL_PARENTS = frozenset({
    "operaciones en transito", "otros activos y derivados",
    "forward (resultado neto)", "venta corta",
    "patrimonio custodia pershing", "simultaneas",
})


# ── Claves de sección (normalizadas, sin tildes) ─────────────────────────────

_SEC_SUMMARY  = "_summary"                        # portada / cover
_SEC_INV_CLP  = "detalle de inversiones en $"
_SEC_INV_USD  = "detalle de inversiones en us$"
_SEC_CARTERAS = "detalle de carteras"
_SEC_MOV      = "detalle de movimientos"
_SEC_APR      = "aportes y retiros patrimoniales"  # si existe


# ── Helpers locales ──────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """
    Normalización ASCII-lowercase para comparaciones robustas contra
    codificaciones variables (UTF-8 con tildes, Latin-1 con caracteres
    garbled que pdfplumber retorna como '?', etc.).
    """
    nfd = unicodedata.normalize("NFD", s)
    return nfd.encode("ascii", "ignore").decode("ascii").lower().strip()


def _safe_cell(row: list, idx: int) -> str:
    """Extrae celda de tabla de forma segura, retorna '' si fuera de rango o None."""
    if idx < len(row) and row[idx] is not None:
        return str(row[idx]).strip()
    return ""


def _parse_cl(text: str) -> Decimal:
    """
    Parsea número en formato chileno (puntos=miles, coma=decimal).
    Retorna Decimal("0") en caso de fallo o entrada vacía.
    Soporta negativos con '-' al inicio.
    """
    if not text:
        return Decimal("0")
    s = text.strip().replace("$", "").replace("US$", "").strip()
    if not s:
        return Decimal("0")
    negative = s.startswith("-")
    if negative:
        s = s[1:].strip()
    s = s.replace(".", "").replace(",", ".")
    if not s:
        return Decimal("0")
    try:
        val = Decimal(s)
        return -val if negative else val
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _parse_date_cl(text: str) -> Optional[date]:
    """Parsea 'DD-MM-YYYY' → date."""
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", text)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def _classify_instrument(
    name: str,
    parent_norm: str,
    rf_codes: frozenset[str],
) -> str:
    """
    Clasifica un instrumento según las reglas del spec (orden estricto).

    Retorna: 'Caja' | 'Renta Fija' | 'Equities' | '_skip'
    """
    name_upper = name.strip().upper()

    # Regla 1 – Caja: LIQUIDEZ o TESORERIA en el nombre
    if "LIQUIDEZ" in name_upper or "TESORERIA" in name_upper:
        return "Caja"

    # Regla 2a – Renta Fija: categoría padre es RF / Depósitos a Plazo
    if parent_norm in _RF_PARENTS:
        return "Renta Fija"

    # Regla 2b – Renta Fija: código aparece en sección "Detalle Cartera Renta Fija" (pág. 4)
    m = _RE_CODE_PREFIX.match(name_upper)
    if m and m.group(1) in rf_codes:
        return "Renta Fija"

    # Regla 3a – Caja: categoría padre es caja explícita
    if parent_norm in _CAJA_PARENTS:
        return "Caja"

    # Filas operativas / derivados → omitir
    if parent_norm in _OPERATIONAL_PARENTS:
        return "_skip"

    # Catch-all → Equities (incluye Acciones, Fondos Mutuos no identificados)
    return "Equities"


# ── Indexación por sección ────────────────────────────────────────────────────

def _get_section_key(page_text: str) -> Optional[str]:
    """
    Retorna la clave de sección normalizada de una página con barra de
    navegación, o None para la portada (sin barra).

    Barra de navegación: primera línea no vacía empieza con
    'RESUMEN DE SUS INVERSIONES' (normalizado).
    Clave de sección: tercera línea no vacía de la página.
    """
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    if not lines:
        return None
    if _norm(lines[0]).startswith("resumen de sus inversiones"):
        return _norm(lines[2]) if len(lines) >= 3 else None
    return None  # portada


def _build_section_index(
    pages_text: list[str],
    pages_tables: list[list[list]],
) -> dict[str, dict]:
    """
    Agrupa páginas por su título de sección. Las páginas sin barra de
    navegación (portada) se colectan bajo la clave sintética '_summary'.
    Las páginas de la misma sección acumulan tablas como lista plana,
    permitiendo secciones de N páginas.

    Retorna: {section_key: {"texts": [...], "tables": [...]}}
    """
    idx: dict[str, dict] = {}
    for i, text in enumerate(pages_text):
        key = _get_section_key(text)
        bucket = key if key is not None else _SEC_SUMMARY
        if bucket not in idx:
            idx[bucket] = {"texts": [], "tables": []}
        idx[bucket]["texts"].append(text)
        idx[bucket]["tables"].extend(pages_tables[i])
    return idx


# ── Funciones de extracción ──────────────────────────────────────────────────

def _build_rf_codes(page4_tables: list[list]) -> frozenset[str]:
    """
    Recopila los códigos de instrumentos de todas las tablas
    "Detalle Cartera Renta Fija" de la página 4.

    Los instrumentos RF aparecen con riesgo en la misma celda:
      'BSTDW31218 (Riesgo : AAA ; AAA'  →  extrae 'BSTDW31218'
    """
    rf_codes: set[str] = set()
    for table in page4_tables:
        if not table or not table[0]:
            continue
        header = _safe_cell(table[0], 0)
        if "renta fija" not in _norm(header):
            continue
        # Saltar filas de encabezado (hay 3: título + 2 líneas de cabecera de columnas)
        for row in table[3:]:
            name = _safe_cell(row, 0)
            if not name:
                continue
            name_norm = _norm(name)
            if name_norm.startswith("subtotal"):
                continue
            # Continuaciones de fila (ej. ') Fecha Compra :') no empiezan con letra/número
            if not name[0].isalpha() and not name[0].isdigit():
                continue
            m = _RE_CODE_PREFIX.match(name.upper())
            if m:
                rf_codes.add(m.group(1))
    return frozenset(rf_codes)


def _extract_page1_summary(page1_tables: list[list]) -> dict:
    """
    Extrae de la página 1:
      - Total Activos en $ y US$  (tabla "Resumen de sus inversiones")
      - Patrimonio en $ y US$      (tabla standalone "Patrimonio")
      - Desglose de activos por categoría

    Estructura de la tabla "Resumen de sus inversiones" (6 cols):
      [label/vacío, vacío, nombre, $, US$, APV]
    Total Activos: col 0 = 'Total Activos', col 3 = $, col 4 = US$
    Patrimonio:    tabla propia de 4 cols: [label, $, US$, APV]
    """
    data: dict = {
        "total_activos_clp": Decimal("0"),
        "total_activos_usd": Decimal("0"),
        "patrimonio_clp": Decimal("0"),
        "patrimonio_usd": Decimal("0"),
        "asset_breakdown": {},
    }

    for table in page1_tables:
        if not table or not table[0]:
            continue
        first_norm = _norm(_safe_cell(table[0], 0))

        # Tabla "Resumen de sus inversiones"
        if "resumen de sus inversiones" in first_norm:
            for row in table[1:]:  # omitir fila de encabezado del título
                col0 = _safe_cell(row, 0)
                col0_norm = _norm(col0)
                if col0_norm == "total activos":
                    data["total_activos_clp"] = _parse_cl(_safe_cell(row, 3))
                    data["total_activos_usd"] = _parse_cl(_safe_cell(row, 4))
                elif not col0 or col0 in ("", "Activos"):
                    # Fila de ítem: col 2 = nombre, col 3 = $, col 4 = US$
                    name = _safe_cell(row, 2)
                    if name and _norm(name) not in ("$", "us$", "apv ($)", "activos"):
                        data["asset_breakdown"][name] = {
                            "clp": str(_parse_cl(_safe_cell(row, 3))),
                            "usd": str(_parse_cl(_safe_cell(row, 4))),
                        }

        # Tabla standalone "Patrimonio" (1 fila, 4 cols: [label, $, US$, APV])
        elif first_norm == "patrimonio" and len(table) == 1 and len(table[0]) >= 3:
            row = table[0]
            data["patrimonio_clp"] = _parse_cl(_safe_cell(row, 1))
            data["patrimonio_usd"] = _parse_cl(_safe_cell(row, 2))

    return data


def _find_investments_table(
    tables: list[list], expect_usd: bool
) -> Optional[list[list]]:
    """
    Encuentra la tabla "Resumen de sus inversiones en $" o "en US$"
    dentro de las tablas extraídas de la página.
    """
    for table in tables:
        if not table or not table[0]:
            continue
        header_norm = _norm(_safe_cell(table[0], 0))
        if "resumen de sus inversiones" not in header_norm:
            continue
        # "en us$" distingue USD de CLP ("sus" tiene "us" pero no "en us")
        if expect_usd and "en us" in header_norm:
            return table
        if not expect_usd and "en us" not in header_norm:
            return table
    return None


def _extract_investments(
    table: list[list],
    rf_codes: frozenset[str],
    is_usd: bool,
) -> tuple[dict, list[ParsedRow]]:
    """
    Procesa la tabla "Resumen de sus inversiones en $/$US" (7 columnas):
      col 0: Instrumento
      col 1: Saldo inicial (G)
      col 2: Compras/Aportes (H)
      col 3: Ventas/Rescates (I)
      col 4: Cambio de Valor
      col 5: Saldo final (J)  ← valor que se reporta
      col 6: % del activo

    Retorna (buckets, rows):
      buckets: {Caja, Renta Fija, Equities, Total, unclassified, instruments}
      rows:    ParsedRow por instrumento individual
    """
    buckets: dict = {
        "Caja": Decimal("0"),
        "Renta Fija": Decimal("0"),
        "Equities": Decimal("0"),
        "unclassified": [],
    }
    instrument_details: dict = {"Caja": [], "Renta Fija": [], "Equities": []}
    rows: list[ParsedRow] = []
    page_num = 3 if is_usd else 2
    current_parent = ""

    for row in table[3:]:  # las 3 primeras filas son cabeceras
        if not row:
            continue
        col0 = _safe_cell(row, 0)
        if not col0:
            continue
        col0_norm = _norm(col0)

        # Filas de totales → ignorar
        if col0_norm in _TOTAL_ROWS:
            continue

        # Filas de categoría padre → actualizar contexto
        if col0_norm in _ALL_PARENT_ROWS:
            current_parent = col0_norm
            continue

        # Instrumento individual
        closing_val = _parse_cl(_safe_cell(row, 5))
        classification = _classify_instrument(col0, current_parent, rf_codes)

        if classification == "_skip":
            continue

        if classification in buckets and classification != "unclassified":
            buckets[classification] += closing_val
            instrument_details[classification].append({"name": col0, "amount": closing_val})
        else:
            buckets["unclassified"].append(
                {"name": col0, "amount": str(closing_val)}
            )

        rows.append(
            ParsedRow(
                data={
                    "instrument": col0,
                    "currency": "USD" if is_usd else "CLP",
                    "classification": classification,
                    "parent_category": current_parent,
                    "opening_value": str(_parse_cl(_safe_cell(row, 1))),
                    "purchases": str(_parse_cl(_safe_cell(row, 2))),
                    "sales": str(_parse_cl(_safe_cell(row, 3))),
                    "change_in_value": str(_parse_cl(_safe_cell(row, 4))),
                    "closing_value": str(closing_val),
                    "pct_of_portfolio": _safe_cell(row, 6),
                    "section": "usd_investments" if is_usd else "clp_investments",
                },
                row_number=page_num,
                confidence=0.95,
            )
        )

    total = buckets["Caja"] + buckets["Renta Fija"] + buckets["Equities"]

    # Calcular % de cada instrumento sobre el total de su moneda
    for cat_items in instrument_details.values():
        for item in cat_items:
            item["pct_of_total"] = (
                (item["amount"] / total * 100).quantize(Decimal("0.01"))
                if total > 0 else Decimal("0")
            )

    # Omitir categorías sin instrumentos
    instruments_out = {
        cat: items for cat, items in instrument_details.items() if items
    }

    return {
        "Caja": buckets["Caja"],
        "Renta Fija": buckets["Renta Fija"],
        "Equities": buckets["Equities"],
        "Total": total,
        "unclassified": buckets["unclassified"],
        "instruments": instruments_out,
    }, rows


def _extract_movements(page_text: str) -> dict:
    """
    Extrae movimientos de la sección "Resumen de Movimientos Caja" usando
    regex sobre el texto de la página (más robusto que parsear la tabla
    de navegación de 22 columnas).

    Campos buscados:
      Compras/Aportes(D)   → aportes
      Ventas/Rescates (E)  → retiros
      Dividendos y Otros(F)→ dividendos_otros
    """
    aportes = Decimal("0")
    retiros = Decimal("0")
    dividendos = Decimal("0")

    m = _RE_APORTES.search(page_text)
    if m:
        aportes = _parse_cl(m.group(1))

    m = _RE_RETIROS.search(page_text)
    if m:
        retiros = _parse_cl(m.group(1))

    m = _RE_DIVIDENDOS.search(page_text)
    if m:
        dividendos = _parse_cl(m.group(1))

    return {
        "aportes": aportes,
        "retiros": retiros,
        "dividendos_otros": dividendos,
        "neto": aportes - retiros,
    }


def _extract_real_flows(
    mov_text: str, instrument_keyword: str
) -> tuple[dict, list[dict]]:
    """
    Extrae flujos reales de entrada/salida desde la sección "DETALLE DE MOVIMIENTOS",
    considerando SOLO RESCATE FM e INVERSION FM sobre el fondo money market
    identificado por instrument_keyword ('TESORERIA' para CLP, 'LIQUIDEZ DOLAR' para USD).

    Retorna (totals_dict, transactions_list).
      totals_dict: {"aportes", "retiros", "neto"}
      transactions_list: lista de movimientos individuales con
        {fecha, instrumento, monto, moneda, tipo_operacion, categoria_auto, es_warning}

    Convención de signo:
      INVERSION FM → monto > 0 (aporte, dinero ingresó a cartera)
      RESCATE FM   → monto < 0 (retiro, dinero salió de cartera)
    """
    rescate = Decimal("0")
    inversion = Decimal("0")
    transactions: list[dict] = []
    current_matches = False
    current_instrument = ""
    moneda = "USD" if "DOLAR" in instrument_keyword.upper() else "CLP"
    keyword_upper = instrument_keyword.upper()

    for raw_line in mov_text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        upper = line.upper()

        # Cabecera de instrumento money market
        if "BICE" in upper and "INSTITUCIONAL" in upper and not _RE_TITULO_DATE.match(line):
            current_matches = keyword_upper in upper
            if current_matches:
                # Extraer nombre limpio: hasta "INSTITUCIONAL"
                idx = upper.find("INSTITUCIONAL")
                current_instrument = line[:idx].strip() if idx > 0 else line.strip()
            continue

        # Línea de transacción: debe empezar con fecha y pertenecer al instrumento correcto
        if not _RE_TITULO_DATE.match(line) or not current_matches:
            continue

        m_monto = _RE_TITULO_MONTO.search(line)
        if not m_monto:
            continue

        amount = _parse_cl(m_monto.group(1))
        fecha_str = line[:10]  # DD-MM-YYYY

        if "RESCATE FM" in upper:
            rescate += amount
            transactions.append({
                "fecha": fecha_str,
                "instrumento": current_instrument or instrument_keyword,
                "monto": -float(amount),   # negativo = retiro
                "moneda": moneda,
                "tipo_operacion": "RESCATE FM",
                "categoria_auto": "Retiro",
                "es_warning": False,
            })
        elif "INVERSION FM" in upper:
            inversion += amount
            transactions.append({
                "fecha": fecha_str,
                "instrumento": current_instrument or instrument_keyword,
                "monto": float(amount),    # positivo = aporte
                "moneda": moneda,
                "tipo_operacion": "INVERSION FM",
                "categoria_auto": "Aporte",
                "es_warning": False,
            })

    net_withdrawal = rescate - inversion
    aportes = max(Decimal("0"), -net_withdrawal)
    retiros = max(Decimal("0"), net_withdrawal)
    totals = {"aportes": aportes, "retiros": retiros, "neto": aportes - retiros}
    return totals, transactions


def _extract_dap_events(mov_text: str) -> list[dict]:
    """
    Detecta movimientos de Depósitos a Plazo (DAP) en la sección de movimientos.
    Marcados con es_warning=True por ser detección heurística (la cartola no siempre
    los registra explícitamente en esta sección).
    """
    events: list[dict] = []
    current_is_dap = False
    current_instrument = ""

    for raw_line in mov_text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        upper = line.upper()

        # Cabecera de instrumento: no es línea de datos y contiene marcadores DAP
        if not _RE_TITULO_DATE.match(line) and (
            "DEPOSITO" in upper or " DAP " in f" {upper} " or upper.startswith("DAP")
        ):
            # Excluir cabeceras de fondos money market ya procesadas arriba
            if "INSTITUCIONAL" in upper and "TESORERIA" not in upper and "LIQUIDEZ DOLAR" not in upper:
                current_is_dap = False
                continue
            if "BICE" in upper and ("INSTITUCIONAL" not in upper):
                current_is_dap = True
                current_instrument = line.strip()
            elif "DEPOSITO" in upper or "DAP" in upper:
                current_is_dap = True
                current_instrument = line.strip()
            continue

        if not _RE_TITULO_DATE.match(line):
            # Si no empieza con fecha, puede resetear el contexto
            if line and not line[0].isdigit():
                current_is_dap = False
            continue

        if not current_is_dap:
            continue

        m_monto = _RE_TITULO_MONTO.search(line)
        if not m_monto:
            continue

        amount = _parse_cl(m_monto.group(1))
        fecha_str = line[:10]

        if "VENCIMIENTO" in upper or ("RESCATE" in upper and "FM" not in upper):
            cat, tipo, monto = "Retiro", "VENCIMIENTO DAP", -float(amount)
        elif "CAPTACION" in upper or "CONSTITUCION" in upper or "APERTURA" in upper:
            cat, tipo, monto = "Aporte", "CAPTACION DAP", float(amount)
        else:
            cat, tipo, monto = "Sin clasificar", "DAP", float(amount)

        events.append({
            "fecha": fecha_str,
            "instrumento": current_instrument or "DAP",
            "monto": monto,
            "moneda": "CLP",
            "tipo_operacion": tipo,
            "categoria_auto": cat,
            "es_warning": True,
        })

    return events


# ── Parser ───────────────────────────────────────────────────────────────────

class BICEBrokerageParser(BaseParser):
    BANK_CODE = "bice_inversiones"
    ACCOUNT_TYPE = "brokerage"
    VERSION = VERSION
    DESCRIPTION = (
        "Parser para cartolas BICE Inversiones Corredores de Bolsa S.A. "
        "(formato chileno CLP/USD, clasificación Caja / RF / Equities)"
    )
    SUPPORTED_EXTENSIONS = [".pdf"]

    # Marcadores de detección (al menos 2 de los 3 deben estar presentes)
    _DETECT_REQUIRED = "BICE Inversiones Corredores de Bolsa S.A."
    _DETECT_OPTIONAL = ["biceinversiones.cl", "BICE Inversiones"]

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                # Leer páginas 1 y (si existe) última para buscar pie de página
                texts = [pdf.pages[0].extract_text() or ""]
                if len(pdf.pages) >= 2:
                    texts.append(pdf.pages[-1].extract_text() or "")
                combined = "\n".join(texts)

                score = 0.0
                if self._DETECT_REQUIRED in combined:
                    score += 0.6
                for opt in self._DETECT_OPTIONAL:
                    if opt in combined:
                        score += 0.2
                # Bonus por nombre de archivo
                if "bice" in filepath.name.lower():
                    score += 0.1
                return min(score, 1.0)
        except Exception:
            return 0.0

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)

        result = ParseResult(
            status=ParserStatus.SUCCESS,
            parser_name=self.get_parser_name(),
            parser_version=self.VERSION,
            source_file_hash=file_hash,
            bank_code=self.BANK_CODE,
            currency="CLP",
        )

        # ── Abrir PDF ──────────────────────────────────────────────────
        try:
            with pdfplumber.open(filepath) as pdf:
                pages_text: list[str] = []
                pages_tables: list[list[list]] = []
                for page in pdf.pages:
                    pages_text.append(page.extract_text() or "")
                    pages_tables.append(page.extract_tables() or [])
        except Exception as exc:
            result.status = ParserStatus.ERROR
            result.errors.append(f"Error abriendo PDF: {exc}")
            return result

        if not pages_text or not pages_text[0]:
            result.status = ParserStatus.ERROR
            result.errors.append("PDF vacío o ilegible")
            return result

        result.raw_text_preview = pages_text[0][:500]

        # ── Indexar secciones por título (independiente del nº de página) ─
        sections = _build_section_index(pages_text, pages_tables)

        def _sec_tables(key: str) -> list[list]:
            return sections.get(key, {}).get("tables", [])

        def _sec_text(key: str) -> str:
            return "\n".join(sections.get(key, {}).get("texts", []))

        # ── Cabecera (portada) ─────────────────────────────────────────
        self._extract_header(_sec_text(_SEC_SUMMARY), result)

        # ── Resumen general (portada) ──────────────────────────────────
        p1_summary = _extract_page1_summary(_sec_tables(_SEC_SUMMARY))

        # ── Códigos RF desde "DETALLE DE CARTERAS" ────────────────────
        rf_codes: frozenset[str] = frozenset()
        if _SEC_CARTERAS in sections:
            rf_codes = _build_rf_codes(_sec_tables(_SEC_CARTERAS))
        else:
            result.warnings.append("Sección 'DETALLE DE CARTERAS' no encontrada")

        # ── Holdings CLP ("DETALLE DE INVERSIONES EN $") ──────────────
        clp_buckets: dict = {
            "Caja": Decimal("0"),
            "Renta Fija": Decimal("0"),
            "Equities": Decimal("0"),
            "Total": Decimal("0"),
            "unclassified": [],
            "instruments": {},
        }
        if _SEC_INV_CLP in sections:
            clp_table = _find_investments_table(_sec_tables(_SEC_INV_CLP), expect_usd=False)
            if clp_table:
                clp_buckets, clp_rows = _extract_investments(
                    clp_table, rf_codes, is_usd=False
                )
                result.rows.extend(clp_rows)
            else:
                result.warnings.append("No se encontró tabla de inversiones CLP")
        else:
            result.warnings.append("Sección 'DETALLE DE INVERSIONES EN $' no encontrada")

        # ── Holdings USD ("DETALLE DE INVERSIONES EN US$") ────────────
        usd_buckets: dict = {
            "Caja": Decimal("0"),
            "Renta Fija": Decimal("0"),
            "Equities": Decimal("0"),
            "Total": Decimal("0"),
            "unclassified": [],
            "instruments": {},
        }
        if _SEC_INV_USD in sections:
            usd_table = _find_investments_table(_sec_tables(_SEC_INV_USD), expect_usd=True)
            if usd_table:
                usd_buckets, usd_rows = _extract_investments(
                    usd_table, rf_codes, is_usd=True
                )
                result.rows.extend(usd_rows)
            else:
                result.warnings.append("No se encontró tabla de inversiones USD")
        else:
            result.warnings.append("Sección 'DETALLE DE INVERSIONES EN US$' no encontrada")

        # ── Movimientos CLP/USD (dividendos_otros) ─────────────────────
        # Extraídos del texto de las secciones de inversiones respectivas.
        clp_movements: dict = {
            "aportes": Decimal("0"),
            "retiros": Decimal("0"),
            "dividendos_otros": Decimal("0"),
            "neto": Decimal("0"),
        }
        usd_movements: dict = {
            "aportes": Decimal("0"),
            "retiros": Decimal("0"),
            "dividendos_otros": Decimal("0"),
            "neto": Decimal("0"),
        }
        if _SEC_INV_CLP in sections:
            clp_movements = _extract_movements(_sec_text(_SEC_INV_CLP))
        if _SEC_INV_USD in sections:
            usd_movements = _extract_movements(_sec_text(_SEC_INV_USD))

        # ── Flujos reales CLP/USD desde "DETALLE DE MOVIMIENTOS" ──────
        # Reemplaza aportes/retiros (que incluyen mov. intra-cuenta).
        # Fuente: RESCATE FM e INVERSION FM sobre TESORERIA / LIQUIDEZ DOLAR.
        # Concatena todas las páginas de la sección (puede abarcar N páginas).
        all_transactions: list[dict] = []
        if _SEC_MOV in sections:
            mov_text = _sec_text(_SEC_MOV)
            clp_flows, clp_txs = _extract_real_flows(mov_text, "TESORERIA")
            usd_flows, usd_txs = _extract_real_flows(mov_text, "LIQUIDEZ DOLAR")
            dap_events = _extract_dap_events(mov_text)
            clp_movements["aportes"] = clp_flows["aportes"]
            clp_movements["retiros"] = clp_flows["retiros"]
            clp_movements["neto"] = clp_flows["neto"]
            usd_movements["aportes"] = usd_flows["aportes"]
            usd_movements["retiros"] = usd_flows["retiros"]
            usd_movements["neto"] = usd_flows["neto"]
            all_transactions = clp_txs + usd_txs + dap_events
        else:
            result.warnings.append(
                "Sección 'DETALLE DE MOVIMIENTOS' no encontrada; aportes/retiros quedan en 0"
            )

        # ── Poblar result ──────────────────────────────────────────────
        result.closing_balance = p1_summary["patrimonio_clp"] or p1_summary["total_activos_clp"]
        result.total_credits = clp_movements["aportes"]
        result.total_debits = clp_movements["retiros"]

        result.balances = {
            "positions": {
                "CLP": {
                    "Caja": clp_buckets["Caja"],
                    "Renta Fija": clp_buckets["Renta Fija"],
                    "Equities": clp_buckets["Equities"],
                    "Total": clp_buckets["Total"],
                    "unclassified": clp_buckets["unclassified"],
                    "instruments": clp_buckets["instruments"],
                },
                "USD": {
                    "Caja": usd_buckets["Caja"],
                    "Renta Fija": usd_buckets["Renta Fija"],
                    "Equities": usd_buckets["Equities"],
                    "Total": usd_buckets["Total"],
                    "unclassified": usd_buckets["unclassified"],
                    "instruments": usd_buckets["instruments"],
                },
            },
            "movements": {
                "CLP": clp_movements,
                "USD": usd_movements,
            },
            "total_activos_clp": p1_summary["total_activos_clp"],
            "total_activos_usd": p1_summary["total_activos_usd"],
            "patrimonio_clp": p1_summary["patrimonio_clp"],
            "patrimonio_usd": p1_summary["patrimonio_usd"],
        }

        # Usar update() para preservar rut/client_name ya escritos por _extract_header
        result.qualitative_data.update({
            "asset_breakdown_p1": {
                k: v for k, v in p1_summary["asset_breakdown"].items()
            },
            "rf_codes_detected": sorted(rf_codes),
            "unclassified_clp": clp_buckets["unclassified"],
            "unclassified_usd": usd_buckets["unclassified"],
            "sections_found": sorted(k for k in sections if not k.startswith("_")),
            "transactions": all_transactions,
        })

        # Advertir si hay instrumentos sin clasificar
        for item in clp_buckets["unclassified"]:
            result.warnings.append(
                f"Instrumento CLP sin clasificar: {item['name']} ({item['amount']})"
            )
        for item in usd_buckets["unclassified"]:
            result.warnings.append(
                f"Instrumento USD sin clasificar: {item['name']} ({item['amount']})"
            )

        return result

    def _extract_header(self, page1_text: str, result: ParseResult) -> None:
        """Extrae cliente, RUT y período desde el texto de la página 1."""
        m = _RE_RUT.search(page1_text)
        if m:
            result.account_number = m.group(1)
            result.qualitative_data["rut"] = m.group(1)

        m = _RE_CLIENT.search(page1_text)
        if m:
            result.qualitative_data["client_name"] = m.group(1).strip()

        m = _RE_PERIOD.search(page1_text)
        if m:
            result.period_start = _parse_date_cl(m.group(1))
            result.period_end = _parse_date_cl(m.group(2))
            result.statement_date = result.period_end

    def validate(self, result: ParseResult) -> list[str]:
        """
        Valida consistencia interna:
          - Total posiciones CLP ≈ Total Activos CLP de pág. 1
          - Total posiciones USD ≈ Total Activos USD de pág. 1
        Tolerancia: ≤ 1 CLP / 0.01 USD por redondeo.
        """
        errors: list[str] = []
        if not result.balances:
            return errors

        tol_clp = Decimal("1")
        tol_usd = Decimal("0.01")

        ref_clp = result.balances.get("total_activos_clp", Decimal("0"))
        ref_usd = result.balances.get("total_activos_usd", Decimal("0"))

        pos = result.balances.get("positions", {})

        if ref_clp and ref_clp > 0:
            calc_clp = pos.get("CLP", {}).get("Total", Decimal("0"))
            diff_clp = abs(ref_clp - calc_clp)
            if diff_clp > tol_clp:
                errors.append(
                    f"Total CLP: pág.1={ref_clp} vs. suma posiciones={calc_clp} "
                    f"(diff={diff_clp})"
                )

        if ref_usd and ref_usd > 0:
            calc_usd = pos.get("USD", {}).get("Total", Decimal("0"))
            diff_usd = abs(ref_usd - calc_usd)
            if diff_usd > tol_usd:
                errors.append(
                    f"Total USD: pág.1={ref_usd} vs. suma posiciones={calc_usd} "
                    f"(diff={diff_usd})"
                )

        return errors
