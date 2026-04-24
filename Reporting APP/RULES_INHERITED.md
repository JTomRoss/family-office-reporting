# RULES_INHERITED.md

Reglas de negocio, invariantes y metodologías heredadas del repositorio real
`JTomRoss/family-office-reporting` (Streamlit + FastAPI, SQLite) — consolidadas
para el prototipo de UI en HTML. Este documento NO describe la UI; describe la
lógica que la UI debe respetar y reflejar fielmente.

Fecha de corte del repo leído: `master @ 8f6d131cc6fd`.

---

## 0. Glosario rápido

| Término | Significado |
|---|---|
| **Cartola** | Statement mensual del custodio (PDF normalmente). Manda como verdad de cierre. |
| **Custodio / Banco** | Institución que custodia los activos (BICE, JPM NY, UBS SW, UBS Miami, GS, BBH, Wellington, Altos Patrimonios / BICE Asesorías). |
| **Sociedad** | Entidad legal del family office (Boatview Limited, Ecoterra Internacional SpA, Inversiones Las Raíces SCC, Telmar, Ecoterra SpA). |
| **Cuenta** | Portafolio dentro de un custodio (ej. JPM `9001` Brokerage, UBS SW `206-560552-01`). |
| **Capa normalizada** | Tabla canónica `monthly_metrics_normalized` — única fuente autorizada para lectura desde la UI. |
| **SSOT** | Single Source of Truth. |

---

## 1. Principios inviolables (Arquitectura)

Copiados literalmente de `ARCHITECTURE.md` del repo. Son **reglas de oro** que
la UI y cualquier refactor deben respetar:

| # | Principio | Implicación para la UI |
|---|-----------|------------------------|
| 1 | **UI sin lógica de negocio** | La UI solo consume endpoints/tablas; **jamás** recalcula profit, ending_value, YTD, asset allocation ni movimientos netos. |
| 2 | **Parser aislado** | Un archivo `.py` por banco × tipo de cuenta. No hay lógica compartida entre parsers de distintos bancos. |
| 3 | **Plugin architecture** | Interfaz `BaseParser`, registro dinámico. `ParserConflictError` si dos parsers declaran la misma `(bank_code, account_type)`. |
| 4 | **Idempotencia** | SHA-256 del documento antes de procesar. Mismo archivo nunca duplica. |
| 5 | **Trazabilidad completa** | `raw → parsed → validated → reconciled`, todo con timestamps. Cada valor en UI debe poder "ir a la fuente" (archivo, página, fila). |
| 6 | **Correcciones upstream** | Una corrección nunca vive en la UI. Se persiste como regla y el pipeline la aplica en parsing o cálculo. |
| 7 | **Tests obligatorios** | Cada fórmula en `calculations/` tiene test unitario. |
| 8 | **SOLID estricto** | Single Responsibility por módulo. |
| 9 | **Freeze atómico** | Git tag + snapshot de datos + verificación de integridad. |

### Regla de consumo (no negociable)
1. Los routers de reporting leen **primero** `monthly_metrics_normalized`.
2. Si falta fila/campo normalizado → fallback a `monthly_closings`.
3. La UI nunca hace cálculos financieros de interpretación de cartola;
   solo renderiza.

Campos obligatorios de la capa normalizada (por cuenta × mes):
- `ending_value_with_accrual`
- `ending_value_without_accrual`
- `accrual_ending`
- `cash_value`
- `movements_net`
- `profit_period`

---

## 2. Contrato de Parsers (`BaseParser`)

Todo parser hereda de `BaseParser` y **debe** declarar:

```python
BANK_CODE: str          # ej. "bice_inversiones", "ubs", "jpmorgan"
ACCOUNT_TYPE: str       # ej. "brokerage", "custody", "etf", "report_mandato"
VERSION: str            # semver, obligatorio
DESCRIPTION: str
SUPPORTED_EXTENSIONS: list[str]   # normalmente [".pdf"]
```

Métodos obligatorios: `parse()`, `validate()`, `detect()`.

### `ParseResult` — contrato mínimo
- `parser_name`, `parser_version`, `source_file_hash`: SIEMPRE.
- Si `status == SUCCESS` y no es parser `system`:
  - `account_number` **o** `account_numbers` (multi-cuenta, ej. JPM Mandatos).
  - `currency`.
  - `statement_date` **o** ≥ 1 fila.
- Cada `ParsedRow.data` no puede estar vacía.
- Validación automática vía `safe_parse()` → si falla contrato, status pasa a `PARTIAL`.

### Auto-detect
- `detect(filepath)` retorna `float ∈ [0, 1]`.
- En empate, se elige por **orden alfabético** del nombre del parser (determinismo).

### Hash
- `compute_file_hash(path)` → SHA-256 del archivo (idempotencia).
- `get_source_hash()` → SHA-256 del código fuente del parser (versión efectiva).

---

## 3. Inventario de parsers (banco × tipo de cuenta)

Lista completa según `parsers/` del repo:

| Custodio | Tipo cuenta | Archivo |
|---|---|---|
| **BICE** Inversiones CdB | Brokerage | `parsers/bice/brokerage.py` |
| **BICE Asesorías** (Altos Patrimonios) | Wealth Management | `parsers/bice_asesorias/wealth_management.py` |
| **BBH** | Custody | `parsers/bbh/custody.py` |
| **BBH** | Report Mandato | `parsers/bbh/report_mandato.py` |
| **Goldman Sachs** | Custody | `parsers/goldman_sachs/custody.py` |
| **Goldman Sachs** | ETF | `parsers/goldman_sachs/etf.py` |
| **Goldman Sachs** | Report Mandato | `parsers/goldman_sachs/report_mandato.py` |
| **JP Morgan** | Brokerage | `parsers/jpmorgan/brokerage.py` |
| **JP Morgan** | Bonds | `parsers/jpmorgan/bonds.py` |
| **JP Morgan** | Custody | `parsers/jpmorgan/custody.py` |
| **JP Morgan** | ETF | `parsers/jpmorgan/etf.py` |
| **JP Morgan** | Report Mandato | `parsers/jpmorgan/report_mandato.py` |
| **UBS Suiza** | Custody | `parsers/ubs/custody.py` |
| **UBS Suiza** | Report Mandato | `parsers/ubs/report_mandato.py` |
| **UBS Miami** | Custody | `parsers/ubs_miami/custody.py` |
| **UBS Miami** | Report Mandato | `parsers/ubs_miami/report_mandato.py` |
| **Wellington** | Custody | `parsers/wellington/custody.py` |
| **Excel** (sistema) | positions, movements, prices, master_accounts, alternatives | `parsers/excel/*.py` |
| **System** | Asset Allocation Report | `parsers/system/report_asset_allocation.py` |

La UI **debe** enumerar estos parsers en filtros y en el Maestro de cuentas; no
debe inventar combinaciones inexistentes.

---

## 4. Reglas específicas por parser

### 4.1 BICE Brokerage (`parsers/bice/brokerage.py`, v3.4.0)

#### Detección
- `"BICE Inversiones Corredores de Bolsa S.A."` en el texto (obligatorio, +0.6).
- `"biceinversiones.cl"` o `"BICE Inversiones"` (+0.2 c/u).
- Nombre de archivo contiene `bice` (+0.1).

#### Secciones del PDF (por título, **no** por número de página)
- Portada (`_summary`)
- `DETALLE DE INVERSIONES EN $` (CLP)
- `DETALLE DE INVERSIONES EN US$` (USD)
- `DETALLE DE CARTERAS` (detalle RF / RV / FM)
- `DETALLE DE MOVIMIENTOS`
- `GLOSARIO` (se ignora)

#### Clasificación de instrumentos (orden estricto)
1. **Caja**: nombre contiene `LIQUIDEZ` o `TESORERIA`.
2. **Renta Fija**: código aparece en "Detalle Cartera Renta Fija" **o** categoría padre es `Renta Fija` / `Depósitos a Plazo` / `DAP BICE`.
3. **Caja** (regla 3a): categoría padre es `Disponible en Caja` / `Libreta de Ahorro`.
4. **`_skip`**: `Operaciones en tránsito`, `Otros activos y derivados`, `Forward (resultado neto)`, `Venta Corta`, `Patrimonio Custodia Pershing`, `Simultáneas`.
5. **Equities**: catch-all (incluye Acciones, FM no identificados).

#### Aportes / retiros (crítico)
- **No** usar `Compras/Aportes(D)` / `Ventas/Rescates(E)` del resumen — incluyen movimientos intra-cuenta.
- Fuente real: sección `DETALLE DE MOVIMIENTOS`.
  - `RESCATE FM` sobre `TESORERIA` (CLP) o `LIQUIDEZ DOLAR` (USD) → retiro.
  - `INVERSION FM` sobre `TESORERIA` / `LIQUIDEZ DOLAR` → aporte.
  - `neto = aportes − retiros`.
- Detección de DAP es **heurística** (`es_warning=True` en transacción).

#### Formato de números
- Chileno: puntos = miles, coma = decimal. Soporta negativo con `-` prefijo.

#### Validación interna (tolerancias)
- CLP: suma posiciones ≈ `total_activos_clp`, tolerancia ≤ 1 CLP.
- USD: suma posiciones ≈ `total_activos_usd`, tolerancia ≤ 0.01 USD.

---

### 4.2 UBS Suiza Custody (`parsers/ubs/custody.py`, v2.3.3)

#### Formato
- Texto **concatenado** (sin espacios): `UBSSwitzerlandAG`, `Portfolio206-560552-01`.
- Page 3 "Total assets" contiene **ambos** portafolios 01 y 02 + asignación por moneda (página más rica).
- Páginas posteriores son específicas del portfolio actual.

#### Detección
- `"ubsswitzerlandag"` (sin espacios) +0.35.
- `"statementofassets"` +0.25.
- Patrón `portfolio 206-\d{6}-\d{2}` +0.20.
- Nombre con `suiza`/`switzerland` +0.15, `portfolio` +0.05.

#### Regla estructural
- **Selecciona el bloque de portfolio que matchea el sufijo** de la cuenta (`-01` / `-02`).
  El "net assets" de ese bloque es la **verdad de cierre** (`ending_with = selected_net_assets`).

#### Asset classes
- Canónicos: `Liquidity`, `Bonds`, `Equities`. Se usa la columna **Total** (con accruals).

#### Ending value (con/sin accrual)
Orden de intentos de extracción desde texto concatenado:
1. `Netassets <wo> <accrual> <with> 100.00` → 3 valores.
2. `Netassets <wo> <with> 100.00` → 2 valores.
3. `Netassets <with> 100.00` → 1 valor (wo = with, accrual = 0).
4. Fallback: `Totalmarketvalue` + `Totalaccruedinterest`.

#### Guardrail multi-portfolio
- Si `|ending_wo − ending_with| > max(1000, 10% × ending_with)` → sobrescribe `ending_wo = ending_with` (evita capturar total del otro portfolio).

#### Utilidad (profit) — fallback definido
```
utilidad = performance_table_value  si existe en tabla Monthly performance
         = ending_current − ending_previous  si no hay fuente de movimientos
```

#### Fecha de cierre
- Primero: parsing de `Statement of assets as of DD Month YYYY`.
- Fallback: prefijo `YYYYMM` en nombre de archivo → último día del mes.

#### Nota de layout conocida (bug aceptado)
- Meses 2025-05, 2025-08, 2025-11: `pdfplumber` concatena números de la fila Liquidity en un solo token → caja sale mal. **Solución pendiente**: `extract_tables()` en esa página cuando se detecte un token numérico anómalo.

---

### 4.3 JP Morgan — multi-parser

JPM tiene **cinco** parsers distintos (Brokerage, Bonds, Custody, ETF, Report Mandato). Cada cuenta JPM mapea a uno; el número de cuenta **4-dígitos** ayuda a decidir (ver Maestro). Casos mapeados en el Maestro Boatview:

| Nº | Tipo | Parser |
|---|---|---|
| `0007` | ETF | `jpmorgan/etf.py` |
| `1100` | Mandato Bonos | `jpmorgan/bonds.py` (o `report_mandato` según formato) |
| `2600` | Mandato | `jpmorgan/report_mandato.py` |
| `9001` | Brokerage | `jpmorgan/brokerage.py` |

Los parsers JPM son los más grandes (20–28 KB); declaran `account_numbers` (lista) porque una cartola puede traer **múltiples** cuentas.

---

### 4.4 UBS Miami Custody, BBH, Goldman Sachs, Wellington

Parsers separados por archivo. Cada uno:
- Tiene su propio `detect()` con marcadores de pie de página / logo / patrón de número de cuenta.
- Declara su `VERSION` y `BANK_CODE`.
- Produce `ParseResult` con `balances`, `qualitative_data.asset_allocation`, y filas por instrumento.
- **UBS Miami** tiene override específico para taxonomía de mandatos (ver §6).

---

### 4.5 BICE Asesorías / Altos Patrimonios (`bice_asesorias/wealth_management.py`)

Parser separado (no comparte con BICE Brokerage). Formato Wealth Management es distinto al Corredores de Bolsa.

---

## 5. Cálculos financieros canónicos (`calculations/`)

**Solo `Decimal`** — nunca `float` para dinero. Cada función tiene test.

### 5.1 Profit

#### JPM ETF
```
Profit = Income + Change_in_Value + (Accrual_mes − Accrual_mes_prev)
```

#### UBS Suiza
```
Profit = Total_Assets_mes − Movimientos_mes − Total_Assets_mes_prev
```

### 5.2 Rentabilidades

```
Return_mensual% = (Profit / Total_Assets_mes_prev) × 100    # None si denom == 0

YTD% (chain-linking) = [ Π (1 + rᵢ/100) − 1 ] × 100         # compuesto mensual
```

### 5.3 Allocation

```
weight%  = (part / total) × 100
valid_allocation = |Σ weights − 100| ≤ 0.01      # tolerancia 1 bp
etf_composition_check = |Σ instr_values − reported_total| ≤ 0.01
```

### 5.4 Conciliación diario vs mensual (`reconciliation.py`)

- **La cartola (`monthly_total`) manda como verdad.**
- Thresholds:
  - `MATCHED` — diferencia exacta = 0.
  - `MINOR_DIFF` — diferencia ≤ threshold (default 0.01%).
  - `MAJOR_DIFF` — diferencia > threshold → **ALERTA**.
  - `MISSING_DAILY` / `MISSING_MONTHLY` — falta un lado.
- Se reconcilia también **por instrumento** (`reconcile_by_instrument`): marca `in_daily_only` / `in_monthly_only`.

---

## 6. Diccionarios canónicos

### 6.1 `asset_bucket_dictionary.json` — buckets visuales (ETF / mandatos directos)

Orden fijo para gráficos stacked (de abajo a arriba en el repo):

```
Caja → RF IG Short → RF IG Long → HY → Non US RF →
Alternativos → Real Estate → RV EM → RV DM
```

Colores (usar **estos**, no reinterpretar):

| Bucket | Tone | Color |
|---|---|---|
| RV DM | rv | `#B53639` |
| RV EM | rv | `#D85759` |
| RF IG Short | rf | `#2D6FB7` |
| RF IG Long | rf | `#4D92D9` |
| HY | rf | `#8AB8EB` |
| Non US RF | rf | `#A8CBF0` |
| Alternativos | alt | `#2E7D5A` |
| Real Estate | alt | `#6AA56A` |
| PE | alt | `#2E7D5A` |
| RE | alt | `#6AA56A` |
| Caja | cash | `#D5DEE9` |

**Bucket por defecto** cuando no hay match: `RV DM`.

**Alias de compatibilidad** (exactos → bucket):
```
"SPDR"                                  → RF IG Short
"ISHARES MSCI EM-ACC"                   → RV EM
"EMERGING MARKET EQUITIES"              → RV EM
"EMERGING MARKETS EQUITIES"             → RV EM
"NON-US EQUITY" / "NON US EQUITY"       → RV EM
"TOTAL NON-US EQUITY"                   → RV EM
"VAND USDCP1-3 USDA"                    → RF IG Short
"VANG USDCPBD USDA"                     → RF IG Long
"VANG USDCPBD USDA ACC"                 → RF IG Long
"RF IG" (alias)                         → RF IG Short
```

**Tokens de resolución:**
- `alt`, `hy`, `re`, `rf`, `rv` solo permiten **match exacto** (no keyword).
- Keywords exigen longitud ≥ 4 tras quitar espacios.
- Paleta de charts por defecto (orden): `RV DM, RF IG Short, RV EM, RF IG Long, HY, Non US RF, Caja, Alternativos, Real Estate`.

### 6.2 `etf_instrument_dictionary.py` — instrumentos ETF canónicos

**Orden canónico:** `IWDA, IEMA, VDCA, VDPA, SPDR, IHYA, Money Market`
**Instrumentos cash:** `{Money Market}`

Mapeos clave (el parser normaliza a uno de los 7):

| Alias | Canónico |
|---|---|
| `IWDA`, `ISHARES CORE MSCI WORLD`, `P ISHARES CORE MSCI WORLD`, `MSCI WORLD INDEX FUND (ISHARES)` | **IWDA** |
| `IEMA`, `ISHARES MSCI EM-ACC` (y variantes), `MSCI EMERGING MARKETS INDEX FUND (ISHARES)`, `ISHARES III PLC-ISHARES MSCI EMERGING MARKETS ETF` | **IEMA** |
| `IHYA`, `ISHARES USD HY CORP USD ACC` (y variantes), `MARKIT IBOXX USD LIQUID HY CAPPED INDEX FUND (ISHARES)`, `ISHARES II PLC-ISHARES $ HIGH YIELD CORP BOND UCITS ETF` | **IHYA** |
| `VDCA`, `VAND USDCP1-3 USDA`, `VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF`, `VANGUARD FUNDS PLC-VANGUARD US CMN CLASS ETF` | **VDCA** |
| `VDPA`, `VANG USDCPBD USDA` (± ACC), `VANGUARD USD CORPORATE BOND UCITS ETF`, `VUCP`, `USD CORPORATE BOND UCITS ETF` | **VDPA** |
| `SPDR`, `SPDR BLOOMBERG 1-10 YEAR U.S.`, `SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG` | **SPDR** |
| `JPM LI-LIQ LVNAV FD - USD - W -`, `PROCEEDS FROM PENDING SALES` | **Money Market** |

**Reglas extra (fuzzy sobre upper sin no-alfanuméricos):**
- `VDPA` si contiene `VDPA`.
- `VDPA` si contiene `USDCPBD` **y** `USDA`.
- `SPDR` si contiene `SPDR` **y** `BLOOMBERG`.
- `Money Market` si contiene (lowercase): `sweep`, `liquidity`, `money market`, `cash`, `depósito`/`deposito`/`deposit`/`deposits`, `li-liq`.
- Default si no matchea: **`Other`**.

### 6.3 `mandate_report_dictionary.json` + `mandate_taxonomy.py` — categorías de mandatos

Categorías canónicas (tokens):
```
cash, ig_fixed_income, hy_fixed_income, fixed_income,
us_equities, non_us_equities, global_equities, equities,
private_equity, real_estate, other_investments
```

**Ignores** (nunca clasificar como mandato): `totalportfolio`, `totalnetmarketvalue`, `netassets`, `totalassets`, `totalmarketvalue`.

**Shortcut:** cualquier label con `otherinvestment`, `assetallocationinvestment`, `miscellaneous`, `hedgefund` → `other_investments`.

**Reglas `contains_rules`** (orden de evaluación importa):

| # | contains_any | contains_all | exclude_any | → categoría |
|---|---|---|---|---|
| 1 | cash, deposit, money market, liquidity | — | fixed income, bond, equity, stock | `cash` |
| 2 | private equity | — | — | `private_equity` |
| 3 | other investment(s), asset allocation investment(s), miscellaneous, hedge fund(s) | — | — | `other_investments` |
| 4 | real estate, real assets, property | — | — | `real_estate` |
| 5 | high yield, non investment grade, us high yield, other fixed income | — | — | `hy_fixed_income` |
| 6 | investment grade, high grade, corporate, government, treasury, tips, short duration | — | — | `ig_fixed_income` |
| 7 | fixed income, bond(s), credit, loan | — | — | `fixed_income` |
| 8 | non us, non-us, international, emerging, eafe, europe, japan, switzerland, uk, emu | equit | — | `non_us_equities` |
| 9 | us equity/equities, us large, s&p, sp500, russell, nasdaq | — | — | `us_equities` |
| 10 | global equity | — | — | `global_equities` |
| 11 | equity/equities, stock(s) | — | — | `equities` |

**Override por banco — `ubs_miami`:**
- `contains_any: [emerging]` **AND** `contains_all: [fixed, income]` → `hy_fixed_income`.

---

## 7. Data Loading Service (`backend/services/data_loading_service.py`)

- Al cargar una cartola:
  1. Persiste documento + hash SHA-256.
  2. Invoca parser aislado.
  3. **Upsert** en `monthly_metrics_normalized` con los 6 campos canónicos.
  4. **Resincroniza** después de ajustes YTD / prior-period.
  5. **Resincroniza** cuando llega `pdf_report` (asset allocation).
- Endpoint de observabilidad: `GET /api/v1/data/normalization-quality` → cobertura, meses sin normalizar, diferencias relevantes vs `monthly_closings`.
- Backfill histórico: `scripts/backfill_normalized_metrics.py`.

---

## 8. Hardening (post-auditoría)

Aspectos que la UI debe respetar / reflejar:

- `datetime.now(timezone.utc)` (no `utcnow`).
- `RawDocument.filepath` relativo a `PROJECT_ROOT`.
- Cada `*_json` tiene schema Pydantic (`serialize_json` / `deserialize_json`).
- `CheckConstraints` en SQLite para campos enum (`status`, `file_type`, `entity_type`, `account_type`, `severity`, `validation_type`, `reconciliation_status`).
- `ParserConflictError` si registro duplicado.
- `error isolation`: import fallido de un parser no rompe discovery.
- `CORS` configurable vía `Settings.cors_origins`.
- `cache.invalidate()` automático tras cada carga → nunca servir datos obsoletos.

---

## 9. Maestro de cuentas (Boatview + entorno real leído de `uploads/`)

Lista curada de cuentas reales (usar en la UI como mock base):

| Sociedad | Banco | Cuenta | Tipo | Divisa |
|---|---|---|---|---|
| Ecoterra Internacional SpA | BICE | (por nº) | Brokerage / Cartola | USD/CLP |
| Ecoterra Internacional SpA | BICE Asesorías (Altos) | C0000-0893 | Cartola | CLP |
| Inversiones Las Raíces SCC | BICE | 038 | Inversiones | CLP |
| Inversiones Las Raíces SCC | Altos Patrimonios | 237 | Cartola | CLP |
| Boatview Limited | JPM NY | 1100 | Mandato Bonos | USD |
| Boatview Limited | JPM NY | 2600 | Mandato | USD |
| Boatview Limited | JPM NY | 0007 | ETF | USD |
| Boatview Limited | JPM NY | 9001 | Brokerage | USD |
| Boatview Limited | UBS Suiza | 206-560552-01 / -02 | Mandato | USD |
| Boatview Limited | UBS Miami | 432 | Mandato | USD |
| Boatview Limited | GS | GS-9912 | ETF | USD |
| Boatview Limited | GS | GS-9914 | Mandato | USD |
| Boatview Limited | BBH | BBH-4412 | Cartola | USD |
| Boatview Limited | Wellington | 576371 | Cartola | USD |

---

## 10. Implicaciones para el prototipo HTML

1. **Dashboard / Mandatos / ETF / Posiciones** → toda tabla y KPI lee de la capa normalizada. Si un valor falta, mostrar "—" + ícono de fallback, no improvisar.
2. **Drawer de trazabilidad** → para cualquier número renderizado, debe existir botón "Ver fuente" con: archivo, página, fila, parser + versión, hash del parser, hash del archivo.
3. **Correcciones** → UI propone; pipeline aplica. El botón "Guardar como regla" persiste; el botón "Aplicar a similares" dispara re-proceso upstream.
4. **Diccionario** → usar **exactamente** los buckets, colores y orden de §6. Cualquier instrumento sin match cae a `RV DM` y debe aparecer en Alertas.
5. **Calidad** → alertas derivan de `monthly_closings` vs `normalized` y de `reconciliation.reconcile_monthly` (MATCHED / MINOR_DIFF / MAJOR_DIFF / MISSING_*).
6. **Filtros** → patrón "pendiente de aplicar" (estado dirty hasta `Aplicar`). Filtros globales reales: Sociedad, Banco, Cuenta, Tipo de cuenta, Persona/Holder, Período.
7. **Parsers en UI** → enumerar exactamente los de §3. No inventar `jpm-ny-v4`, `bbh-v1`, etc., sin anclar a `BANK_CODE` + `ACCOUNT_TYPE` + `VERSION` reales.
8. **Ingesta** → mostrar SHA-256, detección con score, versión de parser, status (`SUCCESS`/`PARTIAL`/`ERROR`), warnings de contrato.
9. **Números** → tabular `JetBrains Mono`, CLP con puntos-miles, USD con comas, sin reinterpretar: el parser ya normalizó.
10. **Cartola Ecoterra (PDF generado)** → bloque de "Metodologías" al final debe referenciar §5 (fórmulas explícitas de Profit JPM ETF, Profit UBS Suiza, YTD compuesto).

---

## 11. Cosas que la UI NO debe hacer (checklist anti-errores)

- ❌ Sumar posiciones en el cliente para calcular `ending_value`.
- ❌ Inventar un parser que no exista en §3.
- ❌ Aplicar correcciones solo visualmente.
- ❌ Usar colores de buckets distintos a §6.1.
- ❌ Calcular YTD con suma aritmética (es chain-linking: `Π(1+rᵢ) − 1`).
- ❌ Tratar `Compras/Aportes(D)` de BICE como aportes reales.
- ❌ Mezclar portfolios UBS 01/02 (la cuenta manda vía sufijo).
- ❌ Convertir float↔Decimal en el navegador para dinero.

---

_Fin de documento. Si detectas un conflicto entre este `RULES_INHERITED.md` y el código del repo, el repo manda y este documento se actualiza._
