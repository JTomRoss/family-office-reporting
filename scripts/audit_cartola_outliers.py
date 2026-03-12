"""
Auditoría de cartolas: detecta valores anómalos (outliers) y rentabilidades extremas.

Solo lectura. No modifica datos.
Uso: python scripts/audit_cartola_outliers.py

Usa FO_DATABASE_URL si está definido; si no, data/db/fo_reporting.db
"""

from collections import defaultdict
from decimal import Decimal
from pathlib import Path

# Permitir ejecutar desde raíz del proyecto
import sys
if Path(__file__).resolve().parent.parent not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import Session

from backend.db.session import get_engine, get_session_factory
from backend.db.models import (
    MonthlyClosing,
    MonthlyMetricNormalized,
    Account,
    RawDocument,
)


def _dec(v) -> Decimal | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _float_safe(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# Umbrales (USD) para family office
NET_VALUE_ALTO = Decimal("100000000")   # 100 M
NET_VALUE_MUY_ALTO = Decimal("500000000")  # 500 M
PROFIT_ABSOLUTO_ALTO = Decimal("50000000")  # 50 M en un mes
MOVIMIENTOS_VS_ENDING = Decimal("5")  # movimientos > 5x ending (posible error de escala/signo)
RENTABILIDAD_MENSUAL_MAX = Decimal("2")   # 200% en un mes = sospechoso
RENTABILIDAD_MENSUAL_MIN = Decimal("-0.90")  # -90% en un mes = sospechoso


def run_audit(db: Session) -> list[dict]:
    issues: list[dict] = []

    # ── 1. monthly_closings: net_value gigante o negativo ──
    q = (
        db.query(
            MonthlyClosing.id,
            MonthlyClosing.account_id,
            MonthlyClosing.year,
            MonthlyClosing.month,
            MonthlyClosing.net_value,
            MonthlyClosing.change_in_value,
            MonthlyClosing.source_document_id,
            Account.identification_number,
            Account.account_number,
            Account.entity_name,
            Account.bank_code,
            RawDocument.filename,
            RawDocument.id.label("doc_id"),
        )
        .join(Account, MonthlyClosing.account_id == Account.id)
        .outerjoin(RawDocument, MonthlyClosing.source_document_id == RawDocument.id)
        .filter(MonthlyClosing.net_value.isnot(None))
    )
    for row in q:
        nv = _dec(row.net_value)
        if nv is None:
            continue
        doc_id = row.doc_id
        filename = row.filename or "(sin documento)"
        acct = row.identification_number or row.account_number or str(row.account_id)
        entity = row.entity_name or ""
        bank = row.bank_code or ""
        period = f"{row.year}-{row.month:02d}"

        if nv < 0:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "net_value",
                "valor": str(nv),
                "motivo": "net_value NEGATIVO",
            })
        elif nv >= NET_VALUE_MUY_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "net_value",
                "valor": f"{nv:,.0f}",
                "motivo": "net_value muy alto (>= 500 M)",
            })
        elif nv >= NET_VALUE_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "net_value",
                "valor": f"{nv:,.0f}",
                "motivo": "net_value alto (>= 100 M)",
            })

        chg = _dec(row.change_in_value)
        if chg is not None and abs(chg) >= PROFIT_ABSOLUTO_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "change_in_value",
                "valor": f"{chg:,.0f}",
                "motivo": "change_in_value muy alto en valor absoluto (>= 50 M)",
            })

    # ── 2. monthly_metrics_normalized: ending gigante, profit/movimientos extremos, rentabilidad extrema ──
    qn = (
        db.query(
            MonthlyMetricNormalized.id,
            MonthlyMetricNormalized.account_id,
            MonthlyMetricNormalized.year,
            MonthlyMetricNormalized.month,
            MonthlyMetricNormalized.ending_value_with_accrual,
            MonthlyMetricNormalized.ending_value_without_accrual,
            MonthlyMetricNormalized.movements_net,
            MonthlyMetricNormalized.profit_period,
            MonthlyMetricNormalized.source_document_id,
            Account.identification_number,
            Account.account_number,
            Account.entity_name,
            Account.bank_code,
            RawDocument.filename,
            RawDocument.id.label("doc_id"),
        )
        .join(Account, MonthlyMetricNormalized.account_id == Account.id)
        .outerjoin(RawDocument, MonthlyMetricNormalized.source_document_id == RawDocument.id)
    )
    for row in qn:
        ending = _dec(row.ending_value_with_accrual) or _dec(row.ending_value_without_accrual)
        mov = _dec(row.movements_net)
        profit = _dec(row.profit_period)
        doc_id = row.doc_id
        filename = row.filename or "(sin documento)"
        acct = row.identification_number or row.account_number or str(row.account_id)
        entity = row.entity_name or ""
        bank = row.bank_code or ""
        period = f"{row.year}-{row.month:02d}"

        if ending is not None and ending < 0:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "ending_value",
                "valor": str(ending),
                "motivo": "ending_value NEGATIVO (capa normalizada)",
            })
        elif ending is not None and ending >= NET_VALUE_MUY_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "ending_value",
                "valor": f"{ending:,.0f}",
                "motivo": "ending_value muy alto (>= 500 M)",
            })
        elif ending is not None and ending >= NET_VALUE_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "ending_value",
                "valor": f"{ending:,.0f}",
                "motivo": "ending_value alto (>= 100 M)",
            })

        if profit is not None and ending is not None and ending > 0:
            # Rentabilidad aproximada: profit / (ending - profit - movements)
            base = ending - (profit or 0) - (mov or 0)
            if base and base > 0:
                rent = (profit or 0) / base
                if rent >= RENTABILIDAD_MENSUAL_MAX:
                    issues.append({
                        "doc_id": doc_id,
                        "filename": filename,
                        "bank": bank,
                        "account": acct,
                        "entity": entity,
                        "period": period,
                        "campo": "rentabilidad_mensual_aprox",
                        "valor": f"{float(rent)*100:.1f}%",
                        "motivo": "rentabilidad mensual muy alta (>= 200%)",
                    })
                elif rent <= RENTABILIDAD_MENSUAL_MIN:
                    issues.append({
                        "doc_id": doc_id,
                        "filename": filename,
                        "bank": bank,
                        "account": acct,
                        "entity": entity,
                        "period": period,
                        "campo": "rentabilidad_mensual_aprox",
                        "valor": f"{float(rent)*100:.1f}%",
                        "motivo": "rentabilidad mensual muy negativa (<= -90%)",
                    })

        if profit is not None and abs(profit) >= PROFIT_ABSOLUTO_ALTO:
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "profit_period",
                "valor": f"{profit:,.0f}",
                "motivo": "profit_period muy alto en valor absoluto (>= 50 M)",
            })

        if mov is not None and ending is not None and ending > 0 and abs(mov) > MOVIMIENTOS_VS_ENDING * abs(ending):
            issues.append({
                "doc_id": doc_id,
                "filename": filename,
                "bank": bank,
                "account": acct,
                "entity": entity,
                "period": period,
                "campo": "movements_net",
                "valor": f"{mov:,.0f} (ending={ending:,.0f})",
                "motivo": "movimientos netos > 5x ending (revisar escala/signo)",
            })

    # ── 3. monthly_closings: rentabilidad extrema vía change_in_value / net_value ──
    # (solo si no tenemos capa normalizada para ese periodo)
    for row in db.query(
        MonthlyClosing.id,
        MonthlyClosing.account_id,
        MonthlyClosing.year,
        MonthlyClosing.month,
        MonthlyClosing.net_value,
        MonthlyClosing.change_in_value,
        MonthlyClosing.source_document_id,
        Account.identification_number,
        Account.account_number,
        Account.entity_name,
        Account.bank_code,
        RawDocument.filename,
        RawDocument.id.label("doc_id"),
    ).join(Account, MonthlyClosing.account_id == Account.id).outerjoin(
        RawDocument, MonthlyClosing.source_document_id == RawDocument.id
    ).filter(
        MonthlyClosing.net_value.isnot(None),
        MonthlyClosing.net_value > 0,
        MonthlyClosing.change_in_value.isnot(None),
    ):
        nv = _dec(row.net_value)
        chg = _dec(row.change_in_value)
        if nv and chg is not None and nv > 0:
            # Aproximación: rent ≈ change_in_value / (net_value - change_in_value)
            base = nv - chg
            if base and base > 0:
                rent = chg / base
                if rent >= RENTABILIDAD_MENSUAL_MAX or rent <= RENTABILIDAD_MENSUAL_MIN:
                    # Evitar duplicar si ya salió por capa normalizada
                    period = f"{row.year}-{row.month:02d}"
                    if not any(
                        i.get("doc_id") == row.doc_id and i.get("period") == period
                        and "rentabilidad" in (i.get("motivo") or "")
                        for i in issues
                    ):
                        issues.append({
                            "doc_id": row.doc_id,
                            "filename": row.filename or "(sin documento)",
                            "bank": row.bank_code or "",
                            "account": row.identification_number or row.account_number or str(row.account_id),
                            "entity": row.entity_name or "",
                            "period": period,
                            "campo": "rentabilidad_mensual_aprox (closings)",
                            "valor": f"{float(rent)*100:.1f}%",
                            "motivo": "rentabilidad mensual extrema (>= 200% o <= -90%) desde monthly_closings",
                        })

    return issues


def main() -> None:
    engine = get_engine()
    factory = get_session_factory(engine)
    db = factory()
    try:
        issues = run_audit(db)
    finally:
        db.close()

    # Agrupar por documento para lista única de cartolas
    by_doc: dict[int | None, list[dict]] = defaultdict(list)
    for i in issues:
        by_doc[i["doc_id"]].append(i)

    # Ordenar por doc_id (None al final) y por periodo
    doc_ids_sorted = sorted((k for k in by_doc if k is not None), key=lambda x: (0, x))
    if None in by_doc:
        doc_ids_sorted.append(None)

    print("=" * 80)
    print("AUDITORÍA DE CARTOLAS – Valores anómalos y rentabilidades extremas")
    print("(Solo lectura; no se modificó ningún dato)")
    print("=" * 80)
    print()
    print(f"Total de hallazgos: {len(issues)}")
    print(f"Cartolas con al menos un hallazgo: {len([k for k in by_doc if k is not None])}")
    if None in by_doc:
        print(f"  (+ {len(by_doc[None])} filas sin source_document_id asociado)")
    print()
    print("-" * 80)
    print("LISTA DE CARTOLAS CON POSIBLES PROBLEMAS")
    print("-" * 80)
    print()
    print(f"{'Doc ID':<8} {'Archivo':<45} {'Banco':<12} {'Cuenta':<10} {'Sociedad':<25} {'Periodo':<8} {'Problema(s)'}")
    print("-" * 80)

    for doc_id in doc_ids_sorted:
        items = by_doc[doc_id]
        # Una línea por documento con resumen; si hay varios hallazgos, agrupar por (doc, filename, account, period)
        seen_row = set()
        for i in items:
            key = (i.get("doc_id"), i.get("filename"), i.get("account"), i.get("entity"), i.get("period"))
            if key in seen_row:
                continue
            seen_row.add(key)
            doc_id_str = str(i.get("doc_id") or "")
            filename = (i.get("filename") or "")[:44]
            bank = (i.get("bank") or "")[:11]
            account = (i.get("account") or "")[:9]
            entity = (i.get("entity") or "")[:24]
            period = i.get("period") or ""
            motivos = "; ".join({x["motivo"] for x in items if x.get("filename") == i.get("filename") and x.get("period") == i.get("period") and x.get("account") == i.get("account")})
            if len(motivos) > 55:
                motivos = motivos[:52] + "..."
            print(f"{doc_id_str:<8} {filename:<45} {bank:<12} {account:<10} {entity:<25} {period:<8} {motivos}")

    print()
    print("-" * 80)
    print("DETALLE POR HALLAZGO (campo, valor, motivo)")
    print("-" * 80)
    for i in issues:
        print(f"  Doc {i.get('doc_id')} | {i.get('filename')} | {i.get('entity')} | {i.get('account')} | {i.get('period')} | {i.get('campo')} = {i.get('valor')} | {i.get('motivo')}")
    print()
    print("Fin del reporte.")


if __name__ == "__main__":
    main()
