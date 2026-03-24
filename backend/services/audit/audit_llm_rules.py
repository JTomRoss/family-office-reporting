"""
Reglas de los motores de lectura (parsers + normalización) para inyectar en el prompt del LLM.
Objetivo: que el modelo interprete el PDF con el mismo criterio que la app, no como lectura genérica.
"""

from __future__ import annotations


def build_engine_rules_prompt(
    *,
    bank_code: str,
    account_type: str,
    account_number: str,
    identification_short: str,
) -> str:
    bc = (bank_code or "").strip().lower()
    at = (account_type or "").strip().lower()
    an = (account_number or "").strip()
    ident = (identification_short or "").strip()

    blocks: list[str] = [
        "### Rol",
        "Eres un auditor asistente. Debes leer el TEXTO de la cartola PDF siguiendo las mismas reglas de negocio "
        "que usa la aplicación Family Office Reporting al parsear y normalizar datos. "
        "No inventes cifras: si no hay un valor claro y único para el foco pedido, indícalo en JSON.",
        "",
        "### Qué compara el sistema (capa normalizada)",
        "- **Valor de cierre**: patrimonio neto / valor final del MES en USD (en BD: `ending_value_with_accrual`, incluye accrual cuando aplica).",
        "- **Movimientos netos**: movimiento neto del MES (aportes/retiros netos del periodo), no el acumulado YTD salvo que el documento solo muestre YTD y el foco sea explícitamente YTD.",
        "- **Caja**: liquidez / efectivo / sweep / money market asignado a caja según la cartola; en BD suele venir de `cash_value` o asignación de activos.",
        "- **Instrumentos**: total de cartera en instrumentos o suma coherente con la asignación por buckets que la app usa en reporting (no dobles conteos de totales + subtotales).",
        "",
        "### Reglas por banco / tipo (prioridad sobre lectura literal genérica)",
    ]

    # UBS Suiza
    if bc == "ubs":
        blocks.extend(
            [
                "**UBS Suiza (custody / brokerage según cartola):**",
                "- Si hay varios portafolios, el valor de cierre debe ser el del portafolio que corresponde al **sufijo de cuenta** (p. ej. -01 vs -02), no la suma de todos si la cartola separa líneas por portafolio.",
                "- Valores de posición negativos pueden ser válidos.",
                "- Continuidad: si el *beginning* del mes no coincide con el cierre del mes anterior, en la app **prevalece el ending auditado** del mes previo; eso no es un error de datos en BD por sí solo.",
                "- Rentabilidad mensual en vistas solo-UBS puede mostrarse 0% con posiciones negativas (regla de visualización; no cambia el valor de cierre en BD).",
                "",
            ]
        )

    # JPMorgan
    if bc == "jpmorgan":
        blocks.append("**JPMorgan:**")
        if at in ("brokerage", "etf"):
            blocks.extend(
                [
                "- En brokerage/ETF, campos mensuales en blanco en la cartola se interpretan como **0** a nivel mensual; **YTD** es solo control, no sustituye el mensual.",
                "- Caja puede derivarse de líneas tipo sweep / liquidity en holdings cuando la cartola no trae línea explícita de caja.",
                "",
                ]
            )
        if at == "bonds" and "1531100" in an:
            blocks.extend(
                [
                "- Cuenta **1531100** (Ecoterra Internacional): la caja en asignación debe tomarse de **Total Cash Holdings**, no mezclar con *Short Term Investments* como caja.",
                "",
                ]
            )
        if at == "custody":
            blocks.extend(
                [
                "- Incluir **Net Security Contributions** dentro de movimientos del mes cuando la cartola los muestra.",
                "",
                ]
            )
        blocks.append("")

    # Goldman Sachs
    if bc == "goldman_sachs":
        blocks.extend(
            [
            "**Goldman Sachs:**",
            "- Algunas cartolas usan OCR o layouts legacy; prioriza totales de resumen / overview cuando existan.",
            "- Puede haber consolidación de sub-portafolios cuando falta overview superior.",
            "",
            ]
        )

    # BBH
    if bc == "bbh":
        blocks.extend(
            [
            "**BBH:**",
            "- Los movimientos YTD pueden incluir **prior period adjustments**; una diferencia en YTD puede explicarse por ese ajuste sin implicar error en el neto mensual.",
            "",
            ]
        )

    # UBS Miami (lighter)
    if bc == "ubs_miami":
        blocks.extend(
            [
            "**UBS Miami:**",
            "- Respetar bloques de actividad y valores de cierre tal como aparecen en la sección de la cuenta.",
            "",
            ]
        )

    # Alternativos
    if bc == "alternativos":
        blocks.extend(
            [
            "**Alternativos (Excel):**",
            "- Origen distinto a PDF; valores vienen de Excel cargado a la capa normalizada.",
            "",
            ]
        )

    blocks.extend(
        [
        "### Cuenta actual (metadata)",
        f"- bank_code: `{bc}`",
        f"- account_type: `{at}`",
        f"- account_number: `{an}`",
        f"- identification (corto): `{ident}`",
        "",
        "### Salida obligatoria",
        "Responde **solo** un objeto JSON con las claves:",
        '`{"amount_usd": <number|null>, "could_not_extract": <true|false>, "short_note": "<una frase en español>"}`',
        "- `amount_usd`: el valor en USD para el foco pedido, o null si no es extraíble con certeza.",
        "- `could_not_extract`: true si el PDF no permite un único número fiable para ese foco.",
        "- `short_note`: breve (ej. de qué línea o sección tomaste el valor, o por qué no se pudo).",
        ]
    )

    return "\n".join(blocks)
