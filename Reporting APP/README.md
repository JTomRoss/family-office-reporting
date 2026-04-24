# Reporting APP — Ecoterra Family Office

Prototipo de alta fidelidad (HTML / CSS / JS vanilla, sin build) del reporting
financiero interno del family office. Consolida posiciones, movimientos,
rentabilidades y asset allocation de ~19 cuentas × 5 sociedades × 8 custodios.

Este prototipo **no** lleva lógica de negocio: toda fórmula vive en el backend
real ([`JTomRoss/family-office-reporting`](https://github.com/JTomRoss/family-office-reporting),
FastAPI + SQLite). Ver `RULES_INHERITED.md` para invariantes y `ARCHITECTURE.md`
para el modelo de datos.

---

## 1. Correr el frontend mock (5 segundos)

No hay build, ni `npm install`, ni bundler.

```bash
# Opción A — cualquier servidor estático
python3 -m http.server 5173
# → abrir http://localhost:5173

# Opción B — con Node
npx serve . -p 5173

# Opción C — abrir directamente (funciona, pero sin hash-routing limpio)
open index.html
```

El estado (tema, moneda, filtros, pestañas) persiste en `localStorage`.

### Cache-busting
`index.html` carga `assets/styles.css?v=N`, `mock.js?v=N`, `app.js?v=N`.
**Si editas CSS/JS, sube `N`** o el navegador servirá versión vieja.

---

## 2. Estructura

```
index.html               # shell, topbar, sidebar, view
assets/
  styles.css             # un único archivo — tokens, layout, componentes
  mock.js                # window.MOCK — todos los datasets y helpers
  app.js                 # SPA: router hash, páginas, filtros globales
RULES_INHERITED.md       # invariantes de negocio (no tocar sin conversar)
DESIGN_SYSTEM.md         # direcciones visuales A (claro) / B (oscuro)
ARCHITECTURE.md          # módulos y modelo de datos
_archive/originals/      # HTMLs originales — dirección visual final
```

### Páginas implementadas
Análisis: `/dashboard · /posiciones · /movimientos · /rentabilidades · /alternativos`
Calidad: `/normalizada · /correcciones · /alertas`
Ingesta: `/importar · /archivos · /maestro · /diccionario`
Sistema: `/auditoria · /config`

---

## 3. Conectar al backend real

El prototipo consume `window.MOCK` como si fuera la respuesta de una API REST.
Migrar a backend real = reemplazar cada lectura de `window.MOCK` por un `fetch`.

### 3.1 Shape esperado (ver `assets/mock.js`)

```
MOCK.societies   [{id, name, jur, rut}]
MOCK.banks       [{code, name, country, short}]
MOCK.accounts    [{id, number, society, bank, type, currency, holder, parser}]
MOCK.parsers     [{name, bank, account_type, version}]
MOCK.buckets     [{id, color, order, css}]       // §6.1 canónicos
MOCK.etfDictionary       [{canonical, bucket, aliases[]}]
MOCK.mandateCategories   [string]                // §6.3
MOCK.months/monthLabels  [YYYY-MM / 'ene 25']
MOCK.fxSeries / MOCK.fx  serie + spot CLP/USD
MOCK.kpis        {patrimonio_usd, variacion_usd, twr_mom, twr_ytd, …}
MOCK.totalUSDSeries      número[13]
MOCK.retMonthly          número[13] en %
MOCK.allocation / bySociety / byBank / byCurrency
MOCK.positions   [{acct, isin, instr, ccy, qty, price, mv, accrual, bucket}]
MOCK.movements   [{acct, date, kind, desc, ccy, amount}]
MOCK.files       [{name, uploaded_at, parser, status, hash, pages}]
MOCK.alerts      [{id, sev, rule, detail, acct, period, status}]
MOCK.corrections [{id, field, before, after, rule_proposed, status, …}]
MOCK.audit       [{ts, user, event, obj, detail}]
MOCK.ingestQueue [{id, pdf, parser, society, account, period, scores, status, warnings}]
MOCK.altFunds / altAggregate / strategies
```

Todos los montos monetarios son **USD** salvo campos con `_clp` explícito o
`ccy === 'CLP'`. El prototipo **nunca** recalcula nada que no sea presentación.

### 3.2 Endpoints esperados del backend

| UI consume | Endpoint backend (propuesto) | Notas |
|---|---|---|
| `MOCK.kpis`, `totalUSDSeries`, `retMonthly` | `GET /api/v1/reporting/dashboard?period=YYYY-MM&society=&bank=&account=` | Lee de `monthly_metrics_normalized` |
| `MOCK.positions` | `GET /api/v1/reporting/positions?period=&account=` | Una fila por posición/mes |
| `MOCK.movements` | `GET /api/v1/reporting/movements?period=&account=` | |
| `MOCK.allocation / bySociety / byBank / byCurrency` | `GET /api/v1/reporting/allocation?period=&groupby=` | Buckets §6.1 |
| `MOCK.accounts / societies / banks` | `GET /api/v1/master/accounts`, `/master/societies`, `/master/banks` | Maestro §9 |
| `MOCK.parsers` | `GET /api/v1/parsers` | §3 |
| `MOCK.buckets / etfDictionary / mandateCategories` | `GET /api/v1/dictionary/{buckets,etf,mandates}` | §6 |
| `MOCK.files` | `GET /api/v1/sources?status=&period=` | |
| `MOCK.alerts` | `GET /api/v1/quality/alerts` | §5.4 + heurísticas |
| `MOCK.corrections` | `GET /api/v1/corrections` | |
| `MOCK.audit` | `GET /api/v1/audit/log?limit=` | §8 |
| `MOCK.ingestQueue` | `GET /api/v1/ingest/queue` | post-fingerprint |
| `MOCK.altFunds / altAggregate` | `GET /api/v1/reporting/alternatives?period=` | |
| **Upload cartolas** | `POST /api/v1/sources` multipart | SHA-256 idempotente (§1.4) |
| **Upload Excel maestro** | `POST /api/v1/master/accounts/bulk` | preview-diff → apply |
| **Upload Excel diccionario** | `POST /api/v1/dictionary/bulk` | |
| **Correcciones** | `POST /api/v1/corrections` · `POST /api/v1/corrections/{id}/apply` | upstream, no solo UI |
| **Auditor independiente** | `POST /api/v1/audit/runs` · `GET /api/v1/audit/sample?period=&n=` | container aislado, ver `/auditoria` |
| **Observabilidad** | `GET /api/v1/data/normalization-quality` | Coverage vs monthly_closings |

### 3.3 Dónde cambiar `window.MOCK` por `fetch`

El prototipo accede a los datos vía `const D = window.MOCK` en la línea 7 de
`assets/app.js`. La estrategia recomendada:

1. Crear `assets/api.js` que exponga `window.API` con la misma forma que
   `window.MOCK` pero backeada por `fetch()`. Cada getter devuelve una promesa.
2. En `app.js` reemplazar `const D = window.MOCK` por una inicialización async:
   ```js
   const D = await window.API.bootstrap();  // resuelve societies/banks/accounts/parsers/buckets
   // luego cada página hace fetches puntuales:
   //   await window.API.positions({period, account}) en lugar de D.positions
   ```
3. Introducir capa de caché en memoria invalidada por evento `upload.done`
   (backend emite; UI escucha). Ver §1.9 `cache.invalidate()`.
4. Mantener `window.MOCK` como fallback offline con flag `?mock=1` en la URL.

El Dashboard, Posiciones, Movimientos y Rentabilidades son los primeros
candidatos a cablear, porque leen **directamente** de
`monthly_metrics_normalized` (§1 regla de consumo).

### 3.4 Variables de entorno (propuesto)

```
# .env (leído por un futuro assets/config.js)
ECOTERRA_API_BASE=http://localhost:8000/api/v1
ECOTERRA_MODE=LIVE            # MOCK | LIVE
ECOTERRA_FX_SOURCE=backend    # backend | bcch
```

Hoy `assets/app.js` pintea `Base URL = http://localhost:8000/api/v1` y
`Modo datos = MOCK` en `/config` — esos son los hooks a leer desde env.

---

## 4. Reglas que el prototipo ya respeta

Ver `RULES_INHERITED.md` para el detalle completo. En particular:

- Toda celda consolidada cita fuente (§1.5 trazabilidad) — botón "Ver fuente".
- Correcciones son **reglas**, no parches visuales (§1.6).
- Diccionario usa **exactamente** los buckets, colores y orden de §6.1.
- Filtros globales se aplican con botón Aplicar, nunca auto (§5 perf).
- Parsers enumerados provienen literalmente del inventario §3.
- Alternativos aparece como **custodio virtual** en la distribución por
  custodio del Dashboard (NAV consolidado PE/RE/VC).

---

## 5. Deploy del frontend

Cualquier host estático:

```bash
# Vercel / Netlify / Cloudflare Pages / GitHub Pages / S3+CloudFront
# No hay paso de build. Publica la raíz del repo.
```

Para proteger detrás de SSO (recomendado para uso interno): Cloudflare Access,
Tailscale Funnel, o un reverse proxy con OAuth2 delante.

### CORS para el backend
`Settings.cors_origins` del backend debe incluir el origen del frontend
deployado (ver §8 del documento de reglas).

---

## 6. Próximos pasos

- [ ] Cablear Dashboard + Posiciones al backend real (`api.js`).
- [ ] Integrar carga de cartolas con el pipeline de fingerprinting (§7).
- [ ] Conectar "Auditor independiente" al container aislado — ver `/auditoria`.
- [ ] Migrar `window.MOCK` a fixtures para tests de UI (cypress / playwright).
- [ ] Export de cartola Ecoterra PDF usando los mismos datos del dashboard.

---

_Última actualización: Abr 2026. Cambios de lógica de negocio requieren revisar
RULES_INHERITED.md y, si procede, actualizar el repo real._
