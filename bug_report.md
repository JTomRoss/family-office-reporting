# Bug Report — FO Reporting System

**Fecha:** 2026-04-07  
**Autor:** Revisión estática (Claude Sonnet 4.6)  
**Rama:** master  
**Alcance:** Revisión de errores lógicos, casos borde no manejados, fallos silenciosos y posible corrupción de datos.

---

## Contexto: por qué `1531100` tiene tratamiento especial de cash

La cuenta `1531100` (Ecoterra Internacional, JPMorgan bonds) incluye en su sección `Cash, Deposits & Short Term` dos subcategorías de naturaleza distinta:

- **Total Cash Holdings** → depósitos reales → `Cash, Deposits & Money Market`
- **Total Short Term Investments** → fondos money market ultra-cortos (letras de tesorería, repos) → `Investment Grade Fixed Income`

Para todas las demás cuentas JPM bonds, ambos se cuentan como caja. Para `1531100`, los Short Term Investments son contablemente Renta Fija IG. Sin el override (introducido en `v2.0.2` el 2026-03-31), el cash estaba inflado y el FI subestimado por el mismo monto.

El override en `_apply_cash_holdings_override_for_ecoterra_1100` (bonds.py:258) reemplaza el ending de caja con `Total Cash Holdings` y suma `Total Short Term Investments` a Fixed Income, de modo que `cash + fixed_income = ending_value`.

Documentado en `SESSION_STATE.md §6`.

---

## BUG 1 — `_extract_period` en `bonds.py:117` — año incorrecto en periodos de enero

**Severidad: Alta (silencioso, latente)**  
**Archivo:** `parsers/jpmorgan/bonds.py:117`

### Descripción

```python
m = re.search(
    r"(\d{1,2})\s+(\w+)\s*-\s*(\d{1,2})\s+(\w+)\s+(\d{4})",
    text,
)
year = int(m.group(5))  # ← un solo año capturado, siempre el del END
result.period_start = date(year, start_month, start_day)  # ← INCORRECTO para enero
result.period_end   = date(year, end_month,   end_day)
```

Para los statements de enero, el texto del PDF es `"01 December - 31 January 2026"`. El regex captura un solo año (el del período final). Resultado:

- `period_end = date(2026, 1, 31)` ✓
- `period_start = date(2026, 12, 1)` ✗ (debería ser `2025-12-01`)

`statement_date = period_end` es correcto, por lo que la carga a DB usa la fecha correcta. Pero `period_start` queda asociado al mes incorrecto. El riesgo real es si alguna lógica futura usa `period_start` para inferir el mes anterior o para reconciliación.

### Fix sugerido

Detectar cuando `start_month > end_month` (cruce de año) y restar 1 al año del start:

```python
start_year = year - 1 if start_month > end_month else year
result.period_start = date(start_year, start_month, start_day)
result.period_end   = date(year, end_month, end_day)
```

---

## BUG 2 — `_extract_total_short_term_investments` en `bonds.py:230` — fallback captura la columna equivocada

**Severidad: Alta (silencioso, corrompe datos de la cuenta 1531100)**  
**Archivo:** `parsers/jpmorgan/bonds.py:230`

### Descripción

```python
pattern = re.compile(
    r"Total\s+Short\s+Term\s+Investments\s+[\d,]+\.\d{2}\s+([\d,]+\.\d{2})",
)  # Captura el SEGUNDO número (beginning  →  ending)

fallback_pattern = re.compile(
    r"Total\s+Short\s+Term\s+Investments\s+([\d,]+\.\d{2})",
)  # Captura el PRIMER número (siempre)
```

En el layout normal del PDF, la línea tiene formato `beginning  ending`:
```
Total Short Term Investments    1,314,748.12    1,217,852.19
```

El patrón primario captura `1,217,852.19` (ending ✓). Si el patrón primario falla por una variación de whitespace o cambio de layout en el PDF, el fallback captura `1,314,748.12` (el beginning ✗) y lo suma a Fixed Income como si fuera el valor de cierre del período. El IG FI queda inflado con el valor del mes anterior. No hay ninguna alerta ni warning.

### Fix sugerido

Alinear el fallback para que también capture el segundo número, o usar el tercero como fallback solo si el layout tiene una sola columna:

```python
# Si hay dos números, tomar el segundo (ending)
pattern_two_cols = re.compile(
    r"Total\s+Short\s+Term\s+Investments\s+[\d,]+\.\d{2}\s+([\d,]+\.\d{2})",
)
# Si hay un solo número, tomarlo directamente (ending)
pattern_one_col = re.compile(
    r"Total\s+Short\s+Term\s+Investments\s+([\d,]+\.\d{2})(?!\s+[\d,]+\.\d{2})",
)
```

---

## BUG 3 — `_apply_bbh_prior_adjustments` en `data_loading_service.py:3445` — prior_adj silently dropped si el mes previo no existe

**Severidad: Media**  
**Archivo:** `backend/services/data_loading_service.py:3445`

### Descripción

```python
def _apply_bbh_prior_adjustments(self, account, year, month, account_values):
    prior_adj = account_values.get("prior_period_adjustments")
    if prior_adj is None:
        return
    ...
    prev = self.db.query(MonthlyClosing).filter(year=prev_year, month=prev_month).first()
    if prev is None:
        return  # ← prior_adj silently discarded, no warning logged
    base = prev.change_in_value or Decimal("0")
    prev.change_in_value = base + prior_adj
```

Si se carga el primer statement BBH de una cuenta (sin historial previo), o si se cargan statements fuera de orden cronológico, `prior_adj` se descarta sin ningún log ni warning. El `change_in_value` del mes previo queda incorrecto sin traza.

Adicionalmente, la mutación es **acumulativa**: si el documento BBH del mes actual es reprocesado y `prev` ya fue commiteado con el ajuste anterior, el adjustment se aplica dos veces (`original + 2 * prior_adj`).

### Fix sugerido

Agregar un log cuando `prev is None` y `prior_adj` es descartado:

```python
if prev is None:
    self._log(
        "load", "warning",
        f"BBH prior_period_adjustment descartado: no existe cierre previo para "
        f"{account.account_number} {prev_year}-{prev_month:02d}",
        account_id=account.id,
    )
    return
```

Para la doble aplicación, verificar si el adjustment ya fue aplicado antes de sumar.

---

## BUG 4 — `_recompute_ubs_income_from_identity` en `data_loading_service.py:3538` — absorbe errores de parsing como utilidad sin alerta

**Severidad: Media (peligroso)**  
**Archivo:** `backend/services/data_loading_service.py:3538`

### Descripción

```python
current.income = current.net_value - current.change_in_value - prev.net_value
```

Esta fórmula absorbe **cualquier discrepancia**, incluyendo errores del parser. Si el parser extrae un `net_value` incorrecto (error de OCR, columna equivocada, ruido en texto), el error no genera un warning de identidad — se convierte automáticamente en "utilidad del período". Solo se detecta si alguien compara los valores absolutos contra la fuente.

Esto ya ocurrió en producción: los statements UBS de Telmar `2023-10` y `2023-11` produjeron `ending_values` incorrectos (`-10` por axis-noise del gráfico), y el sistema los persistió sin alerta, requiriendo una investigación manual para detectarlos (ver `SESSION_STATE.md §9`).

### Fix sugerido

Agregar un check de rango de sanidad antes de persistir el income recomputado:

```python
recomputed = current.net_value - current.change_in_value - prev.net_value
# Si el income implícito es más del 50% del ending value, es sospechoso
if current.net_value > 0 and abs(recomputed) > 0.5 * abs(current.net_value):
    self._log("load", "warning",
        f"UBS income recomputado sospechoso: {account.account_number} "
        f"{year}-{month:02d}: income={recomputed}, ending={current.net_value}")
current.income = recomputed
```

---

## BUG 5 — `_validate_ytd_consistency` en `data_loading_service.py:3473` — consulta con `autoflush=False` puede producir comparaciones inconsistentes

**Severidad: Media**  
**Archivo:** `backend/services/data_loading_service.py:3473`

### Descripción

```python
rows = self.db.query(MonthlyClosing).filter(
    MonthlyClosing.account_id == account.id,
    MonthlyClosing.year == year,
    MonthlyClosing.month <= month,
).all()
sum_mov = sum(row.change_in_value or Decimal("0") for row in rows)
```

La sesión usa `autoflush=False`. Dentro del loop de `load_parse_result`, el upsert del mes actual modifica el objeto ORM en memoria pero no lo ha flusheado al momento de llamar `_validate_ytd_consistency`. Dependiendo de si el objeto ya existía en la sesión (update) o es nuevo (insert), el query puede o no incluir los valores del mes actual. El resultado es una comparación YTD inconsistente que puede generar:
- Warnings falsos (si incluye el mes actual dos veces)
- Fallos de detección silenciosos (si no incluye el mes actual)

### Fix sugerido

Hacer un `self.db.flush()` antes de la consulta dentro del método, o excluir explícitamente el mes actual del query de suma YTD y sumarle el valor en memoria separadamente:

```python
sum_mov = sum(row.change_in_value or Decimal("0") for row in rows if row.month != month)
current_mov = account_values.get("change_investment") or Decimal("0")
sum_mov += current_mov
```

---

## BUG 6 — `_reconcile_mandates_asset_breakdown_to_target` en `data.py:521` — residual positivo siempre se atribuye a Equities

**Severidad: Media (clasifica activos incorrectamente)**  
**Archivo:** `backend/routers/data.py:521`

### Descripción

```python
if residual > 0:
    breakdown["Equities"] = breakdown.get("Equities", 0.0) + residual
    return breakdown
```

Si la suma de los buckets del breakdown es menor que `target_total` (el ending value), la diferencia entera se añade a `Equities`. Esto ocurre cuando la asignación de activos del PDF no cubre el 100% del portfolio (categorías no mapeadas, nuevas categorías del banco, ruido de parsing). El efecto: activos no clasificados aparecen como Equity en los reportes sin ningún indicador de que ocurrió una reclasificación forzada.

### Fix sugerido

Loguear cuando el residual supere un umbral razonable (ej. > 1% del ending value), y considerar atribuirlo a una categoría "Unclassified" en lugar de Equities:

```python
if residual > 0:
    if target_total > 0 and residual / target_total > 0.01:
        logger.warning(f"Mandate breakdown residual > 1%: {residual:.2f} assigned to Equities")
    breakdown["Equities"] = breakdown.get("Equities", 0.0) + residual
```

---

## BUG 7 — Cantidades negativas silenciadas en `canonical_breakdown_from_payload`

**Severidad: Baja-Media**  
**Archivo:** `backend/services/normalized_reporting_payload.py:268`

### Descripción

```python
amount = _convert_amount_by_unit(value=amount_raw, unit=unit, ending_value=ending_value)
if amount is None or amount <= 0:
    continue  # ← descarta negativos
```

Y en `_accumulate_by_category`:
```python
if amount <= 0:
    return
```

Una posición de caja en negativo (sobregiro), un ajuste de prior period negativo, o un hedge corto reportado con signo negativo se descarta sin traza. El canonical breakdown muestra ese bucket como cero, pero el ending value incluye el valor negativo, generando una discrepancia entre la suma del breakdown y el ending.

---

## BUG 8 — Doble implementación del extractor de cash con lógica divergente

**Severidad: Media (riesgo de mantenimiento e inconsistencia)**  
**Archivos:**
- `backend/services/data_loading_service.py`: `_cash_from_asset_allocation_json()` → retorna `Decimal`
- `backend/routers/data.py`: `_extract_cash_from_asset_allocation()` → retorna `float`

### Descripción

Ambas funciones implementan la misma heurística (encontrar el cash umbrella en el JSON de asset allocation) con lógica casi idéntica pero diferencias sutiles en el manejo de múltiples umbrellas:

```python
# En data.py (router):
if umbrella_values:
    return max(max(umbrella_values), 0.0)  # ← max de umbrellas

# En data_loading_service.py:
# lógica equivalente pero con detección de umbrella distinta
```

Si una de las dos implementaciones actualiza su heurística de detección y la otra no, los reportes mostrarán cash diferente dependiendo del path que los calcula. No hay test que verifique que ambas coinciden para el mismo input.

---

## BUG 9 — `_get_filter_options` en `data.py:166` — cuentas Alternativos invisibles en filtros

**Severidad: Media (UX y reporting)**  
**Archivo:** `backend/routers/data.py:166`

### Descripción

```python
bank_codes = [
    row[0]
    for row in db.query(Account.bank_code)
        .join(MonthlyClosing, MonthlyClosing.account_id == Account.id)
        .distinct().all()
]
```

Los filtros de UI se populan haciendo join con `MonthlyClosing`. Pero según el contrato documentado en `AGENT_CONTEXT.md`:
> *"`Alternativos.xlsx` is an independent source and loads only into `monthly_metrics_normalized`."*

Las cuentas de Alternativos no tienen filas en `monthly_closings`. Por lo tanto, si una cuenta de Alternativos nueva no tiene ningún `MonthlyClosing`, no aparece en las opciones de filtro del UI. El usuario no puede filtrar por ella y el dato existe en el sistema sin ser visible.

### Fix sugerido

Incluir también cuentas con datos en `monthly_metrics_normalized` en las opciones de filtro, o hacer la consulta directamente sobre la tabla `accounts` filtrada por cuentas que tengan al menos un row en cualquiera de las dos tablas de reporting.

---

## BUG 10 — Filtro de tipo cuenta para Alternativos usa LIKE con formato JSON asumido

**Severidad: Baja (frágil ante cambios de serialización)**  
**Archivo:** `backend/routers/data.py:143`

### Descripción

```python
Account.metadata_json.like(f'%\"asset_class\": \"{asset_class}\"%'),
```

Este patrón asume que el JSON fue serializado con:
- Espacio después de `:`
- Dobles comillas sin escaping especial

Si `json.dumps` es llamado con `separators=(',', ':')` (formato compacto, sin espacios), la cadena resultante sería `"asset_class":"PE"` y el LIKE fallaría silenciosamente, filtrando fuera todas las cuentas Alternativos del resultado. No hay error, no hay warning — simplemente no aparecen.

### Fix sugerido

Usar `JSON_EXTRACT` si la DB lo soporta, o asegurar que la serialización de `metadata_json` siempre usa el mismo formato (documentado y testeado).

---

## BUG 11 — `load_parse_result` en `data_loading_service.py:809` — sin rollback ante IntegrityError

**Severidad: Media-Alta (sesión corrompida)**  
**Archivo:** `backend/services/data_loading_service.py:809`

### Descripción

```python
for account in accounts:
    try:
        self._upsert_parsed_statement(...)
        self._upsert_monthly_closing(...)
        self._upsert_etf_compositions(...)
    except Exception as exc:
        stats["errors"].append(msg)
        self._log("load", "error", msg, raw_document.id, account.id)
        # ← NO hay self.db.rollback() aquí

self.db.flush()   # ← puede lanzar PendingRollbackError si la sesión está inválida
self.db.commit()  # ← puede comprometer datos parciales de las cuentas previas
```

Si `_upsert_monthly_closing` lanza un `sqlalchemy.exc.IntegrityError` (violación de unique constraint, columna NOT NULL, etc.), SQLAlchemy marca la transacción como inválida (`in rollback`). El código captura la excepción pero no hace `self.db.rollback()`. El `self.db.flush()` posterior lanza `sqlalchemy.exc.PendingRollbackError`, lo que resulta en un error 500 al frontend. Dependiendo del punto de fallo, los datos de las cuentas procesadas antes del error pueden haber quedado en un estado inconsistente.

### Fix sugerido

Agregar `self.db.rollback()` en el handler, o usar `savepoint` por cuenta para poder hacer rollback granular sin perder las cuentas ya procesadas exitosamente:

```python
except Exception as exc:
    self.db.rollback()  # ← restaurar sesión a estado válido
    stats["errors"].append(msg)
    self._log(...)
```

---

## Resumen de severidades

| # | Archivo | Descripción | Severidad |
|---|---------|-------------|-----------|
| 2 | `parsers/jpmorgan/bonds.py:230` | Fallback STI captura beginning en lugar de ending → FI inflado en 1531100 | **Alta** |
| 11 | `backend/services/data_loading_service.py:809` | Sin rollback ante IntegrityError → sesión corrompida | **Alta** |
| 1 | `parsers/jpmorgan/bonds.py:117` | `period_start` incorrecto en statements de enero | **Alta** (latente) |
| 4 | `backend/services/data_loading_service.py:3538` | UBS absorbe errores de parsing como utilidad sin alerta | **Media** |
| 5 | `backend/services/data_loading_service.py:3473` | YTD validation inconsistente con autoflush=False | **Media** |
| 6 | `backend/routers/data.py:521` | Residual de breakdown va a Equities sin log | **Media** |
| 8 | `data_loading_service.py` + `data.py` | Doble implementación de extractor de cash con lógica divergente | **Media** |
| 9 | `backend/routers/data.py:166` | Alternativos invisibles en filter options | **Media** |
| 3 | `backend/services/data_loading_service.py:3445` | BBH prior_adj silently dropped + acumulativo en reproceso | **Media** |
| 10 | `backend/routers/data.py:143` | LIKE asume formato JSON específico para Alternativos | **Baja** |
| 7 | `backend/services/normalized_reporting_payload.py:268` | Amounts ≤ 0 silenciados en canonical breakdown | **Baja** |

---

*Generado por revisión estática. Ningún archivo fue modificado.*
