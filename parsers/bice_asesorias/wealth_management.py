"""
Parser: BICE Asesorías – Cartola Administración de Activos (Banco BICE / Altos Patrimonios).

Identificadores del documento:
  - "Cartola Administración de Activos" en portada (pág. 1)
  - "Banco BICE" como emisor
  - "bice.cl" en pie de página
  - Número de cuenta con formato "C0000-XXXX"

Estructura del PDF (16 páginas fijas):
  Pág. 1  : Portada (cliente, cuenta, fecha)
  Pág. 2  : Balance de Activos + Flujo Patrimonial + Indicadores Económicos
  Pág. 3  : Balance de Activos Renta Fija (desglose)
  Pág. 4  : Balance de Activos Renta Variable (desglose)
  Págs. 5-8: Diversificación (se ignoran)
  Pág. 9  : Cartera de Inversiones – Renta Fija (bonos UF)
  Pág. 10 : Cartera de Inversiones – Fondos Renta Fija
  Pág. 11 : Cartera de Inversiones – Fondos Renta Variable + resumen
  Pág. 12 : Vencimiento de Cupones (se ignora)
  Pág. 13 : Transacciones del período (RF, IF, Fondos RF, Fondos RV, RV, Forward)
  Pág. 14 : Movimientos de Caja CLP (se ignora en cálculo de flujos)
  Pág. 15 : Movimientos de Caja USD (se ignora en cálculo de flujos)
  Pág. 16 : Aportes y Retiros Patrimoniales

Clasificación de instrumentos (orden estricto):
  1. Caja: nombre contiene "LIQUIDEZ", "TESORERIA" o "FMMML",
           O categoría es "FM MONEY MARKET"
  2. Renta Fija: bonos en pág. 9 + fondos RF en pág. 10 (CFIF, FI deuda privada,
                 FI rescatable RF, FM renta fija, etc.)
  3. Equities: fondos RV en pág. 11 (alternativos) + cualquier no clasificado

Movimientos (flujos reales):
  Fuente primaria: Tabla "FLUJO PATRIMONIAL (Últimos Movimientos)" en pág. 2
  Columnas: Fecha | Tipo | Aporte | Retiro | Moneda
  Filtro: solo filas cuya Fecha caiga dentro del mes/año del estado de cuenta
  Aportes = sum(Aporte) para el período; Retiros = sum(Retiro) para el período
  Moneda: CLP separado de USD según columna Moneda
  Fuente secundaria (transacciones individuales para UI): pág. 13 (se mantiene para
    el Detalle de Transacciones pero ya NO determina los totales de aportes/retiros)

Output en result.balances:
  "summary_p2" → {saldo_caja_clp, saldo_caja_usd, cartera_rf_clp, cartera_rf_uf_clp,
                   cartera_rv_clp, total_cartera}  (valores directos de página 2)
  "positions" → {
    "CLP": {"Caja": D, "Renta Fija": D, "Equities": D, "Total": D,
            "instruments": {"Caja": [...], "Renta Fija": [...], "Equities": [...]},
            "unclassified": []},
    "USD": {...same structure...},
  }
  "movements" → {
    "CLP": {"aportes": D, "retiros": D, "neto": D},
    "USD": {"aportes": D, "retiros": D, "neto": D},
  }

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

VERSION = "1.2.0"

# ── Regex ────────────────────────────────────────────────────────────────────

_RE_ACCOUNT = re.compile(r"\(?(C\d{4}-\d{4})\)?")
_RE_DATE_P1 = re.compile(r"\bal\s+(\d{2}-\d{2}-\d{4})")
_RE_CLIENT_P2 = re.compile(r"^(.+?)\s*-\s*PORTAFOLIO AL:", re.MULTILINE)
_RE_DATE_P2 = re.compile(r"PORTAFOLIO AL:\s*(\d{2}-\d{2}-\d{4})")
_RE_PERIOD_TEXT = re.compile(r"PERIODO\s*\((\d{2}-\d{2}-\d{4})\s*-\s*(\d{2}-\d{2}-\d{4})\)")
_RE_FOLIO = re.compile(r"^\d{5,}$")

# ── Constantes de clasificación ───────────────────────────────────────────────

_CAJA_NAME_KEYWORDS = ("LIQUIDEZ", "TESORERIA", "FMMML")

_RF_FUND_TYPES: tuple[str, ...] = (
    "fm renta fija",
    "fondo mutuo renta fija",
    "fi rescatable rf",
    "fondo de inversion rescatable renta fija",
    "fi no rescatable deuda privada",
    "fondo de inversion no rescatable deuda privada",
    "fi no rescatable rf",
    "fondo de inversion no rescatable renta fija",
    "cuotas fi",
    "cfif",
    "cuotas de fondos de inversion nacionales renta fija",
    "finrfi",
    "renta fija",
)

_EQUITY_FUND_TYPES: tuple[str, ...] = (
    "renta variable",
    "alternativo",
    "fi no rescatable otros",
)

# Etiquetas de balance de activos en página 2 (sin tildes, lowercase)
_LABEL_CAJA_CLP = "saldo caja clp"
_LABEL_CAJA_USD = "saldo caja usd"
_LABEL_RF_CLP = "cartera renta fija en clp"
_LABEL_RF_UF = "cartera renta fija en uf"
_LABEL_RV = "cartera renta variable"
_LABEL_TOTAL_CARTERA = "total cartera"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _dedup(s: str) -> str:
    """Colapsa caracteres duplicados consecutivos: 'TToottaall' → 'Total'."""
    return re.sub(r"(.)\1+", r"\1", s)


def _norm(s: str) -> str:
    """Normalización ASCII-lowercase sin tildes."""
    nfd = unicodedata.normalize("NFD", s)
    return nfd.encode("ascii", "ignore").decode("ascii").lower().strip()


def _clean(s: str) -> str:
    """norm + colapso de duplicados para comparaciones robustas con texto bold del PDF."""
    return _dedup(_norm(s))


def _safe_cell(row: list, idx: int) -> str:
    if idx < len(row) and row[idx] is not None:
        return str(row[idx]).strip()
    return ""


def _parse_cl(text: str) -> Decimal:
    """Parsea número en formato chileno (puntos=miles, coma=decimal)."""
    if not text:
        return Decimal("0")
    s = text.strip().replace("$", "").replace("US$", "").replace("%", "").strip()
    if not s:
        return Decimal("0")
    negative = s.startswith("-")
    if negative:
        s = s[1:].strip()
    s = s.replace(".", "").replace(",", ".")
    if not s or s == ".":
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


# ── Clasificación ────────────────────────────────────────────────────────────

def _classify_by_name(name: str) -> Optional[str]:
    """Override de clasificación por nombre (máxima prioridad)."""
    upper = name.upper()
    if any(kw in upper for kw in _CAJA_NAME_KEYWORDS):
        return "Caja"
    return None


def _classify_fund_type(tipo_raw: str) -> str:
    """
    Clasifica por categoría de fondo según texto de cabecera de grupo.
    Retorna 'Caja' | 'Renta Fija' | 'Equities' | '_unknown'
    """
    t = _clean(tipo_raw)

    if "money market" in t or any(kw.lower() in t for kw in _CAJA_NAME_KEYWORDS):
        return "Caja"

    for kw in _RF_FUND_TYPES:
        if kw in t:
            return "Renta Fija"

    for kw in _EQUITY_FUND_TYPES:
        if kw in t:
            return "Equities"

    return "_unknown"


# ── Extracción: cabecera ──────────────────────────────────────────────────────

def _extract_header(page1_text: str, page2_text: str, result: ParseResult) -> None:
    """Extrae cliente, cuenta y fecha de período desde páginas 1 y 2."""

    # Número de cuenta
    m = _RE_ACCOUNT.search(page1_text)
    if not m:
        m = _RE_ACCOUNT.search(page2_text)
    if m:
        result.account_number = m.group(1)

    # Fecha (fin de período)
    m = _RE_DATE_P1.search(page1_text)
    if m:
        result.statement_date = _parse_date_cl(m.group(1))
        result.period_end = result.statement_date
    if not result.statement_date:
        m = _RE_DATE_P2.search(page2_text)
        if m:
            result.statement_date = _parse_date_cl(m.group(1))
            result.period_end = result.statement_date

    # Período completo desde encabezado de sección de transacciones (backup)
    if not result.period_start:
        m = _RE_PERIOD_TEXT.search(page2_text)
        if m:
            result.period_start = _parse_date_cl(m.group(1))
            if not result.period_end:
                result.period_end = _parse_date_cl(m.group(2))

    # Nombre del cliente
    m = _RE_CLIENT_P2.search(page2_text)
    if m:
        result.qualitative_data["client_name"] = m.group(1).strip()


# ── Extracción: Balance de Activos pág. 2 ────────────────────────────────────

def _extract_balance_p2(page2_tables: list[list]) -> dict:
    """
    Lee la tabla 'Balance de Activos' de página 2 y extrae los Valor Mercado $
    para cada categoría reconocida.

    Col 0 = etiqueta, col 1 = Valor Mercado $.
    Maneja texto con tildes corruptas y chars duplicados (bold PDF).
    """
    out: dict = {
        "saldo_caja_clp": Decimal("0"),
        "saldo_caja_usd": Decimal("0"),
        "cartera_rf_clp": Decimal("0"),
        "cartera_rf_uf_clp": Decimal("0"),
        "cartera_rv_clp": Decimal("0"),
        "total_cartera": Decimal("0"),
        "total_patrimonio": Decimal("0"),
    }

    for table in page2_tables:
        if not table:
            continue
        for row in table:
            if not row or not row[0]:
                continue
            label = _clean(_safe_cell(row, 0))
            val = _parse_cl(_safe_cell(row, 1))

            if _LABEL_CAJA_CLP in label:
                out["saldo_caja_clp"] = val
            elif _LABEL_CAJA_USD in label:
                out["saldo_caja_usd"] = val
            elif _LABEL_RF_UF in label:
                out["cartera_rf_uf_clp"] = val
            elif _LABEL_RF_CLP in label:
                out["cartera_rf_clp"] = val
            elif _LABEL_RV in label:
                out["cartera_rv_clp"] = val
            elif _LABEL_TOTAL_CARTERA in label and "renta" not in label and "patrimonio" not in label:
                out["total_cartera"] = val
            elif "patrimonio" in label and "total" in label:
                out["total_patrimonio"] = val

    if out["total_cartera"] == Decimal("0") and out["total_patrimonio"] > Decimal("0"):
        out["total_cartera"] = out["total_patrimonio"]

    return out


# ── Extracción: Cartera Renta Fija pág. 9 (bonos) ────────────────────────────

def _extract_cartera_p9(page9_tables: list[list]) -> list[dict]:
    """
    Extrae bonos de Renta Fija desde página 9.

    Estructura de tabla (16 cols):
      col 0 = Instrumento, col 1 = Emisor, col 3 = Moneda,
      col 12 = Valor Mercado ($) — ya en CLP aunque el bono sea UF.

    Todos los instrumentos de esta página → Renta Fija,
    salvo override explícito por nombre (TESORERIA/LIQUIDEZ → Caja).
    """
    instruments: list[dict] = []

    for table in page9_tables:
        if not table:
            continue
        for row in table:
            if not row:
                continue
            name = _safe_cell(row, 0)
            if not name:
                continue

            label = _clean(name)

            # Saltar cabeceras, secciones y subtotales
            if any(kw in label for kw in (
                "cartera de inversiones", "instrumento", "subtotal",
                "total", "bef -", "ber -"
            )):
                continue

            # Filas de categoría/sector: sin Emisor
            emisor = _safe_cell(row, 1)
            if not emisor:
                continue

            # Valor Mercado en col 12
            if len(row) < 13:
                continue
            amount = _parse_cl(_safe_cell(row, 12))

            classification = _classify_by_name(name) or "Renta Fija"

            instruments.append({
                "name": name,
                "amount_clp": amount,
                "currency_orig": _safe_cell(row, 3) or "UF",
                "classification": classification,
            })

    return instruments


# ── Extracción: Fondos págs. 10-11 ───────────────────────────────────────────

def _extract_fondos(page_tables: list[list], default_equity: bool) -> list[dict]:
    """
    Extrae fondos de inversión desde páginas 10 u 11.

    Estructura de tablas:
      • TABLE 0: section title + col headers (se omite)
      • TABLE N (N≥1): mini-tabla por categoría de fondo
          row 0: cabecera de categoría → col 1 = None/vacío
          row 1+: datos de instrumento → col 1 = Emisor (no vacío)

    Columnas de datos (12 cols):
      0=Instrumento, 1=Emisor, 2=Cantidad, 3=Precio Prom.,
      4=Moneda, 5=Costo Hist., 6=Monto Act., 7=Precio Cierre,
      8=Valor Mercado ($), 9=Utilidad, 10=%Var, 11=%Pat

    Regla central: col 1 vacío → fila de categoría/subtotal (actualiza clasificación,
    no se agrega como instrumento). Col 1 con Emisor → instrumento real.

    default_equity: True para pág. 11 (fondos RV), False para pág. 10 (fondos RF).
    """
    instruments: list[dict] = []
    in_section = False
    current_classification = "Equities" if default_equity else "Renta Fija"

    for table in page_tables:
        if not table or not table[0]:
            continue

        cell00 = _safe_cell(table[0], 0)
        cell00_c = _clean(cell00)

        # Inicio de sección (excluir Forward que también tiene "cartera de inversiones")
        if "cartera de inversiones" in cell00_c and "forward" not in cell00_c:
            in_section = True
            continue

        # Fin de sección: Forward o resumen de patrimonio
        if "forward" in cell00_c or "saldo caja" in cell00_c or "total patrimonio" in cell00_c:
            in_section = False
            continue

        if not in_section:
            continue

        for row in table:
            if not row:
                continue

            name = _safe_cell(row, 0)
            if not name:
                continue

            label = _clean(name)

            # Skip column headers
            if any(kw in label for kw in ("instrumento", "emisor")):
                continue

            emisor = _safe_cell(row, 1)

            if not emisor:
                # Fila de categoría/subtotal: actualizar clasificación activa
                cls = _classify_fund_type(name)
                if cls != "_unknown":
                    current_classification = cls
                name_override = _classify_by_name(name)
                if name_override:
                    current_classification = name_override
                continue

            # Fila de instrumento real (tiene Emisor en col 1)
            if any(kw in label for kw in ("subtotal", "total")):
                continue
            if len(row) < 9:
                continue

            amount = _parse_cl(_safe_cell(row, 8))
            classification = _classify_by_name(name) or current_classification

            instruments.append({
                "name": name,
                "amount_clp": amount,
                "currency_orig": _safe_cell(row, 4) or "CLP",
                "classification": classification,
            })

    return instruments


# ── Extracción: Transacciones pág. 13 ────────────────────────────────────────

def _subcategory_for_instrument(
    classification: str,
    name: str,
    source: str,
) -> str:
    """
    Devuelve la subcategoría dentro del árbol fijo de activos:
      Caja → Money Market
      Renta Fija → Depósitos a Plazo | Bonos | Fondos Mutuos RF
      Equities   → Fondos de Inversión
    source: 'p9_bond' | 'p10_fondo' | 'p11_fondo'
    """
    if classification == "Caja":
        return "Money Market"
    if classification == "Renta Fija":
        if source == "p10_fondo":
            return "Fondos Mutuos RF"
        # p9_bond: distinguir DAP de bonos por nombre
        name_upper = name.upper()
        if "DAP" in name_upper or "DEPOSITO" in name_upper or "DEPOSIT" in name_upper:
            return "Depósitos a Plazo"
        return "Bonos"
    if classification == "Equities":
        return "Fondos de Inversión"
    return "Otros"


def _extract_transactions_p13(page13_tables: list[list]) -> tuple[dict, list[dict]]:
    """
    Extrae flujos reales desde las tablas de transacciones de página 13.

    Retorna (totals_dict, transactions_list).

    totals_dict: {"CLP": {"aportes": D, "retiros": D, "neto": D}, "USD": {...}}
    transactions_list: lista de filas individuales detectadas como MM para
      el cálculo de aportes/retiros. Convención de signo:
        COMPRA (aporte al portafolio) → monto > 0
        VENTA  (retiro del portafolio) → monto < 0
      categoria_auto:
        COMPRA → "Aporte"
        VENTA  → "Retiro"
    """
    venta_clp = Decimal("0")
    compra_clp = Decimal("0")
    venta_usd = Decimal("0")
    compra_usd = Decimal("0")
    transactions: list[dict] = []

    for table in page13_tables:
        if not table:
            continue

        for row in table:
            if not row or len(row) < 10:
                continue

            folio = _safe_cell(row, 0).strip()
            if not _RE_FOLIO.match(folio):
                continue

            fecha_raw = _safe_cell(row, 1)
            operacion = _safe_cell(row, 3).upper()
            tipo = _safe_cell(row, 4).upper()
            instrumento = _safe_cell(row, 5)
            instrumento_upper = instrumento.upper()
            moneda = _safe_cell(row, 8).upper()
            monto_abs = _parse_cl(_safe_cell(row, 9))

            if operacion not in ("VENTA", "COMPRA"):
                continue

            is_mm = "FMMML" in tipo or any(kw in instrumento_upper for kw in _CAJA_NAME_KEYWORDS)
            if not is_mm:
                continue

            is_usd = "USD" in moneda
            if operacion == "VENTA":
                if is_usd:
                    venta_usd += monto_abs
                else:
                    venta_clp += monto_abs
                # VENTA = retiro del portafolio → monto negativo
                transactions.append({
                    "folio": folio,
                    "fecha": fecha_raw,
                    "operacion": operacion,
                    "tipo": tipo,
                    "instrumento": instrumento,
                    "moneda": "USD" if is_usd else "CLP",
                    "monto": float(-monto_abs),
                    "monto_raw": float(monto_abs),
                    "categoria_auto": "Retiro",
                })
            else:  # COMPRA
                if is_usd:
                    compra_usd += monto_abs
                else:
                    compra_clp += monto_abs
                # COMPRA = aporte al portafolio → monto positivo
                transactions.append({
                    "folio": folio,
                    "fecha": fecha_raw,
                    "operacion": operacion,
                    "tipo": tipo,
                    "instrumento": instrumento,
                    "moneda": "USD" if is_usd else "CLP",
                    "monto": float(monto_abs),
                    "monto_raw": float(monto_abs),
                    "categoria_auto": "Aporte",
                })

    def _net_flows(venta: Decimal, compra: Decimal) -> dict:
        net = venta - compra
        aportes = max(Decimal("0"), -net)
        retiros = max(Decimal("0"), net)
        return {"aportes": aportes, "retiros": retiros, "neto": aportes - retiros}

    totals = {
        "CLP": _net_flows(venta_clp, compra_clp),
        "USD": _net_flows(venta_usd, compra_usd),
    }
    return totals, transactions


def _extract_flujo_patrimonial_p2(
    p2_tables: list[list],
    statement_date: Optional[date],
) -> Optional[tuple[dict, list[dict]]]:
    """
    Extrae aportes y retiros del mes desde la tabla FLUJO PATRIMONIAL en pág. 2.

    Busca la tabla con encabezado: Fecha | Tipo | Aporte | Retiro | Moneda
    Filtra solo las filas cuya fecha corresponde al mes/año del estado de cuenta.

    Retorna (totals, transactions) donde:
      totals: {"CLP": {"aportes": D, "retiros": D, "neto": D}, "USD": {...}}
      transactions: lista de filas individuales del período, en el formato estándar
        del campo qualitative_data["transactions"] para que la UI las muestre.
    Retorna None si la tabla no se encuentra.
    """
    if statement_date is None:
        return None

    # Encontrar la tabla correcta por cabecera
    flujo_table: Optional[list[list]] = None
    for tbl in p2_tables:
        if not tbl or len(tbl) < 2:
            continue
        header = [str(c or "").strip().upper() for c in tbl[0]]
        if len(header) >= 4 and header[0] == "FECHA" and "APORTE" in header and "RETIRO" in header:
            flujo_table = tbl
            break

    if flujo_table is None:
        return None

    zero = Decimal("0")
    target_month = statement_date.month
    target_year = statement_date.year

    aportes_clp = zero
    retiros_clp = zero
    aportes_usd = zero
    retiros_usd = zero
    transactions: list[dict] = []

    for row in flujo_table[1:]:  # saltar cabecera
        if not row or len(row) < 4:
            continue
        fecha_str = str(row[0] or "").strip()
        tipo = str(row[1] or "").strip()
        aporte_raw = str(row[2] or "").strip()
        retiro_raw = str(row[3] or "").strip()
        moneda = str(row[4] or "CLP").strip().upper() if len(row) > 4 else "CLP"

        # Parsear fecha DD-MM-YYYY
        try:
            parts = fecha_str.split("-")
            if len(parts) != 3:
                continue
            row_month, row_year = int(parts[1]), int(parts[2])
        except (ValueError, AttributeError):
            continue

        # Solo filas del período del estado de cuenta
        if row_month != target_month or row_year != target_year:
            continue

        aporte = _parse_cl(aporte_raw)
        retiro = _parse_cl(retiro_raw)
        is_usd = "USD" in moneda

        if is_usd:
            aportes_usd += aporte
            retiros_usd += retiro
        else:
            aportes_clp += aporte
            retiros_clp += retiro

        # Generar fila de transacción individual por cada aporte o retiro detectado
        if aporte > zero:
            transactions.append({
                "fecha": fecha_str,
                "operacion": "APORTE",
                "instrumento": tipo or "CAJA",
                "descripcion": f"Flujo Patrimonial — {tipo or 'CAJA'} — Aporte",
                "monto": float(aporte),
                "monto_raw": float(aporte),
                "moneda": "USD" if is_usd else "CLP",
                "categoria_auto": "Aporte",
            })
        if retiro > zero:
            transactions.append({
                "fecha": fecha_str,
                "operacion": "RETIRO",
                "instrumento": tipo or "CAJA",
                "descripcion": f"Flujo Patrimonial — {tipo or 'CAJA'} — Retiro",
                "monto": float(-retiro),
                "monto_raw": float(retiro),
                "moneda": "USD" if is_usd else "CLP",
                "categoria_auto": "Retiro",
            })

    totals = {
        "CLP": {
            "aportes": aportes_clp,
            "retiros": retiros_clp,
            "neto": aportes_clp - retiros_clp,
        },
        "USD": {
            "aportes": aportes_usd,
            "retiros": retiros_usd,
            "neto": aportes_usd - retiros_usd,
        },
    }
    return totals, transactions


# ── Parser principal ──────────────────────────────────────────────────────────

class BICEAsesoriasParser(BaseParser):
    BANK_CODE = "bice_asesorias"
    ACCOUNT_TYPE = "wealth_management"
    VERSION = VERSION
    DESCRIPTION = (
        "Parser para cartolas Administración de Activos de Banco BICE "
        "(formato chileno, clasificación Caja / Renta Fija / Equities)"
    )
    SUPPORTED_EXTENSIONS = [".pdf"]

    # "Cartola Administraci" cubre "Administración" con encoding corrupto
    _DETECT_REQUIRED = "CARTOLA ADMINISTRACI"
    _DETECT_SIGNALS = ["Banco BICE", "bice.cl", "C0000-"]

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() != ".pdf":
            return 0.0
        try:
            with pdfplumber.open(filepath) as pdf:
                if not pdf.pages:
                    return 0.0
                texts: list[str] = []
                for page in pdf.pages[:2]:
                    texts.append(page.extract_text() or "")
                if len(pdf.pages) >= 2:
                    texts.append(pdf.pages[-1].extract_text() or "")
                combined = "\n".join(texts)
                combined_upper = combined.upper()

                score = 0.0
                if self._DETECT_REQUIRED in combined_upper:
                    score += 0.5
                for sig in self._DETECT_SIGNALS:
                    if sig in combined:
                        score += 0.15
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

        try:
            with pdfplumber.open(filepath) as pdf:
                n_pages = len(pdf.pages)
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

        # ── Cabecera (págs. 1-2) ───────────────────────────────────
        p1_text = pages_text[0]
        p2_text = pages_text[1] if n_pages >= 2 else ""
        _extract_header(p1_text, p2_text, result)

        # ── Balance de Activos resumen (pág. 2) ────────────────────
        summary_p2: dict = {}
        if n_pages >= 2:
            summary_p2 = _extract_balance_p2(pages_tables[1])

        # ── Cartera bonos RF (pág. 9) ──────────────────────────────
        bonds_rf: list[dict] = []
        if n_pages >= 9:
            bonds_rf = _extract_cartera_p9(pages_tables[8])
            if not bonds_rf:
                result.warnings.append("No se extrajo ningún bono de pág. 9")

        # ── Fondos RF (pág. 10) ────────────────────────────────────
        fondos_rf: list[dict] = []
        if n_pages >= 10:
            fondos_rf = _extract_fondos(pages_tables[9], default_equity=False)
            if not fondos_rf:
                result.warnings.append("No se extrajeron fondos RF de pág. 10")

        # ── Fondos RV (pág. 11) ────────────────────────────────────
        fondos_rv: list[dict] = []
        if n_pages >= 11:
            fondos_rv = _extract_fondos(pages_tables[10], default_equity=True)
            if not fondos_rv:
                result.warnings.append("No se extrajeron fondos RV de pág. 11")

        # ── Aportes/retiros + transacciones individuales del mes (pág. 2) ──────
        _zero_mov: dict = {
            "CLP": {"aportes": Decimal("0"), "retiros": Decimal("0"), "neto": Decimal("0")},
            "USD": {"aportes": Decimal("0"), "retiros": Decimal("0"), "neto": Decimal("0")},
        }
        flujo_transactions: list[dict] = []
        fp_result = _extract_flujo_patrimonial_p2(pages_tables[1], result.statement_date) if n_pages >= 2 else None
        if fp_result is not None:
            movements, flujo_transactions = fp_result
        else:
            movements = _zero_mov
            result.warnings.append(
                "Tabla FLUJO PATRIMONIAL no encontrada en pág. 2; aportes/retiros quedan en 0"
            )

        # ── Transacciones de fondos (pág. 13) → no se usan para totales ───────
        transactions_p13: list[dict] = []
        if n_pages >= 13:
            _, transactions_p13 = _extract_transactions_p13(pages_tables[12])

        # ── Etiquetar fuente para subcategoría ────────────────────
        bonds_rf_tagged = [(inst, "p9_bond") for inst in bonds_rf]
        fondos_rf_tagged = [(inst, "p10_fondo") for inst in fondos_rf]
        fondos_rv_tagged = [(inst, "p11_fondo") for inst in fondos_rv]
        all_instruments_tagged = bonds_rf_tagged + fondos_rf_tagged + fondos_rv_tagged
        all_instruments = [inst for inst, _ in all_instruments_tagged]

        # ── Construir posiciones clasificadas ──────────────────────

        clp_buckets: dict = {
            "Caja": Decimal("0"),
            "Renta Fija": Decimal("0"),
            "Equities": Decimal("0"),
        }
        clp_detail: dict = {"Caja": [], "Renta Fija": [], "Equities": []}
        unclassified_clp: list = []

        usd_buckets: dict = {
            "Caja": Decimal("0"),
            "Renta Fija": Decimal("0"),
            "Equities": Decimal("0"),
        }
        usd_detail: dict = {"Caja": [], "Renta Fija": [], "Equities": []}
        unclassified_usd: list = []

        for inst, source in all_instruments_tagged:
            cls = inst["classification"]
            amount = inst["amount_clp"]
            name = inst["name"]
            # USD bucket: solo si el instrumento es explícitamente USD (LIQUIDEZ DOLAR)
            is_usd_inst = "LIQUIDEZ DOLAR" in name.upper()

            if is_usd_inst:
                buckets, detail, unclass = usd_buckets, usd_detail, unclassified_usd
            else:
                buckets, detail, unclass = clp_buckets, clp_detail, unclassified_clp

            if cls in buckets:
                buckets[cls] += amount
                detail[cls].append({"name": name, "amount": amount})
            else:
                unclass.append({"name": name, "amount": str(amount)})

        total_clp = sum(clp_buckets.values())
        total_usd = sum(usd_buckets.values())

        # Fallback a resumen pág. 2 si no se extrajeron instrumentos
        if total_clp == Decimal("0") and summary_p2.get("total_cartera", Decimal("0")) > 0:
            total_clp = summary_p2["total_cartera"]
            result.warnings.append(
                "Instrumentos detallados no extraídos; total_clp tomado de resumen pág. 2"
            )

        # ── Formatear con porcentajes ──────────────────────────────
        def _fmt(instruments: list, total: Decimal) -> list:
            out = []
            for inst in instruments:
                if total and total > 0:
                    pct = (inst["amount"] / total * 100).quantize(Decimal("0.01"))
                else:
                    pct = Decimal("0")
                out.append({
                    "name": inst["name"],
                    "amount": inst["amount"],
                    "pct_of_total": f"{pct}%",
                })
            return out

        # ── Poblar ParsedRows ──────────────────────────────────────
        for inst, src in all_instruments_tagged:
            subcategory = _subcategory_for_instrument(
                inst["classification"], inst["name"], src
            )
            result.rows.append(ParsedRow(
                data={
                    "instrument": inst["name"],
                    "classification": inst["classification"],
                    "subcategory": subcategory,
                    "currency": "USD" if "LIQUIDEZ DOLAR" in inst["name"].upper() else "CLP",
                    "currency_orig": inst.get("currency_orig", "CLP"),
                    "amount_clp": str(inst["amount_clp"]),
                },
                confidence=0.90,
            ))

        # ── Poblar result ──────────────────────────────────────────
        result.closing_balance = (
            total_clp if total_clp > 0
            else summary_p2.get("total_cartera")
        )
        result.total_credits = movements["CLP"]["aportes"]
        result.total_debits = movements["CLP"]["retiros"]

        result.balances = {
            "summary_p2": summary_p2,
            "positions": {
                "CLP": {
                    "Caja": clp_buckets["Caja"],
                    "Renta Fija": clp_buckets["Renta Fija"],
                    "Equities": clp_buckets["Equities"],
                    "Total": total_clp,
                    "instruments": {
                        "Caja": _fmt(clp_detail["Caja"], total_clp),
                        "Renta Fija": _fmt(clp_detail["Renta Fija"], total_clp),
                        "Equities": _fmt(clp_detail["Equities"], total_clp),
                    },
                    "unclassified": unclassified_clp,
                },
                "USD": {
                    "Caja": usd_buckets["Caja"],
                    "Renta Fija": usd_buckets["Renta Fija"],
                    "Equities": usd_buckets["Equities"],
                    "Total": total_usd,
                    "instruments": {
                        "Caja": _fmt(usd_detail["Caja"], total_usd),
                        "Renta Fija": _fmt(usd_detail["Renta Fija"], total_usd),
                        "Equities": _fmt(usd_detail["Equities"], total_usd),
                    },
                    "unclassified": unclassified_usd,
                },
            },
            "movements": movements,
        }

        result.qualitative_data.update({
            "unclassified_clp": unclassified_clp,
            "unclassified_usd": unclassified_usd,
            "n_instruments": len(all_instruments),
            "transactions": flujo_transactions,      # filas individuales de FLUJO PATRIMONIAL
            "transactions_p13": transactions_p13,    # transacciones de fondos (referencia)
        })

        for item in unclassified_clp:
            result.warnings.append(
                f"Instrumento CLP sin clasificar: {item['name']} ({item['amount']})"
            )
        for item in unclassified_usd:
            result.warnings.append(
                f"Instrumento USD sin clasificar: {item['name']} ({item['amount']})"
            )

        return result

    def validate(self, result: ParseResult) -> list[str]:
        """
        Valida consistencia interna:
          - Suma de posiciones CLP ≈ Total Cartera de pág. 2
        Tolerancia: 1.000 CLP (redondeos menores).
        """
        errors: list[str] = []
        if not result.balances:
            return errors

        summary = result.balances.get("summary_p2", {})
        ref = summary.get("total_cartera", Decimal("0"))
        if not ref or ref == Decimal("0"):
            return errors

        calc = result.balances.get("positions", {}).get("CLP", {}).get("Total", Decimal("0"))
        diff = abs(ref - calc)
        tol = Decimal("1000")

        if diff > tol:
            errors.append(
                f"Total CLP: pág.2={ref} vs. suma posiciones={calc} (diff={diff})"
            )

        return errors
