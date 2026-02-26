# FO Reporting – Arquitectura del Sistema

## 1. Decisión Tecnológica y Justificación

### Stack Seleccionado

| Componente        | Tecnología        | Justificación |
|-------------------|-------------------|---------------|
| **Backend API**   | FastAPI (Python)  | Async nativo, validación Pydantic, OpenAPI auto, tipado estricto, mejor separación UI/lógica que Dash/Streamlit |
| **Frontend**      | Streamlit          | Rápido de iterar, Python puro, componentes financieros suficientes, pero 100% desacoplado vía API REST |
| **Base de datos** | SQLite + SQLAlchemy | Cero infraestructura, suficiente para volumen de Family Office (<100K registros/año), portable, respaldable como archivo |
| **Cache**         | Disco (JSON/Parquet) | Sin Redis. Cache de resultados pre-calculados en archivos Parquet para tablas pesadas |
| **PDF parsing**   | pdfplumber / tabula-py | Mejor extracción de tablas que PyPDF2. Por banco/tipo aislado |
| **Excel parsing** | openpyxl / pandas | Estándar industria |
| **Testing**       | pytest + hypothesis | Tests obligatorios para cálculos financieros |
| **Container**     | Docker Compose     | Backend + Frontend + volúmenes de datos persistentes |
| **Hashing**       | SHA-256            | Idempotencia de documentos |

### ¿Por qué FastAPI + Streamlit y NO solo Streamlit?

1. **Streamlit puro viola la regla "UI sin lógica de negocio"**. Su modelo reactivo tienta a poner cálculos en callbacks.
2. **FastAPI como API gateway** fuerza la separación: Streamlit solo consume endpoints REST.
3. **FastAPI permite futuras UIs** (React, Grafana, Excel con Power Query) sin reescribir lógica.
4. **Testing de lógica** se hace contra la API, independiente de UI.

### ¿Por qué SQLite y no PostgreSQL?

1. Volumen estimado: <50 cuentas, <500 documentos/año, <100K filas posiciones → SQLite sobra.
2. Backup = copiar 1 archivo. Freeze/restore trivial.
3. Sin servidor de BD que mantener.
4. Migración a PostgreSQL futura: solo cambiar connection string (SQLAlchemy abstrae).

---

## 2. Arquitectura de Capas (estricta)

```
┌─────────────────────────────────────────────────┐
│                 STREAMLIT UI                      │
│  (Solo presentación. Consume API REST)            │
│  Cero lógica de negocio. Cero parsing.            │
└─────────────────┬───────────────────────────────┘
                  │ HTTP REST (JSON)
┌─────────────────▼───────────────────────────────┐
│              FASTAPI BACKEND                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ Routers  │ │ Services │ │ Schemas (Pydantic)│ │
│  └────┬─────┘ └────┬─────┘ └──────────────────┘ │
│       │            │                              │
│  ┌────▼────────────▼──────────────────────────┐  │
│  │           DOMAIN LAYER                      │  │
│  │  ┌────────────┐  ┌──────────────────────┐   │  │
│  │  │ Parsers    │  │ Calculations         │   │  │
│  │  │ (plugins)  │  │ (fórmulas aisladas)  │   │  │
│  │  └────────────┘  └──────────────────────┘   │  │
│  │  ┌────────────┐  ┌──────────────────────┐   │  │
│  │  │ Reconciler │  │ Validators           │   │  │
│  │  └────────────┘  └──────────────────────┘   │  │
│  └─────────────────────────────────────────────┘  │
│       │                                           │
│  ┌────▼────────────────────────────────────────┐  │
│  │          DATA LAYER (SQLAlchemy + SQLite)    │  │
│  │  models/ repositories/ migrations/           │  │
│  └──────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────┘
         │
    ┌────▼────┐
    │ SQLite  │  + /data/raw/ (documentos originales)
    │  .db    │  + /data/cache/ (parquet pre-calculados)
    └─────────┘
```

---

## 3. Principios Inviolables

| # | Principio | Implementación |
|---|-----------|----------------|
| 1 | UI sin lógica | Streamlit solo llama API y renderiza |
| 2 | Parser aislado | 1 archivo .py por banco×tipo_cuenta |
| 3 | Plugin architecture | Interfaz `BaseParser`, registro dinámico |
| 4 | Idempotencia | SHA-256 de cada documento antes de procesar |
| 5 | Trazabilidad | raw → parsed → validated → reconciled, todo con timestamps |
| 6 | Correcciones upstream | Siempre en parser/cálculo, nunca en UI |
| 7 | Tests obligatorios | Cada fórmula en `calculations/` tiene test |
| 8 | SOLID estricto | Single Responsibility en cada módulo |
| 9 | Freeze atómico | Git tag + snapshot datos + verificación integridad |

---

## 4. Plan de Performance

| Problema | Solución |
|----------|----------|
| Recalcular todo en cada interacción | Cache Parquet de resultados por (año, cuenta, periodo) |
| Filtros lentos | Pre-computar tablas resumen en ingesta, no en request |
| Cambio de pestaña lento | Cada pestaña carga datos independientes desde cache |
| Gráficos pesados | Datos agregados pre-calculados, no raw |
| Upload grande | Procesamiento async con progress tracking |

**Estrategia de cache:**
- Al ingestar datos → calcular y guardar resúmenes en Parquet.
- Al cambiar filtros → leer Parquet filtrado (microsegundos).
- Invalidar cache solo cuando hay nueva ingesta o re-procesamiento.

---

## 5. Plan de Testing

| Capa | Tipo | Herramienta |
|------|------|-------------|
| Calculations | Unit tests obligatorios | pytest |
| Calculations | Property-based | hypothesis |
| Parsers | Unit con fixtures PDF/Excel de ejemplo | pytest |
| API | Integration tests | pytest + httpx (TestClient) |
| Reconciliation | Tests con datasets conocidos | pytest |
| UI | Smoke test HTTP 200 | requests en script restore |
| E2E | Freeze → modify → restore → verify | Script bash/py |

---

## 6. Checklist Definition of Done V1

- [ ] Carga de PDFs con detección automática de banco/tipo
- [ ] Carga de 4 tipos Excel (posiciones, movimientos, precios, maestro)
- [ ] Idempotencia verificada (mismo doc no duplica)
- [ ] Maestro de cuentas como SSOT funcional
- [ ] Parser JPMorgan cartola implementado
- [ ] Parser UBS Suiza cartola implementado
- [ ] Parser Goldman Sachs cartola implementado
- [ ] Cálculo Profit JPM ETF con test
- [ ] Cálculo Profit UBS Suiza con test
- [ ] Conciliación diario vs mensual operativa
- [ ] Pestaña Resumen con filtros multi-selección
- [ ] Pestaña Mandatos con asset allocation
- [ ] Pestaña ETF con composición instrumentos
- [ ] Pestaña Personal con consolidado
- [ ] Sistema Freeze funcional
- [ ] Sistema Restore funcional
- [ ] Docker Compose operativo
- [ ] Tests unitarios calculations: 100% cobertura
- [ ] Tests unitarios parsers: al menos 1 fixture por parser
- [ ] Documentación README completa

---

## 7. Medidas de Hardening (post-auditoría)

Cambios aplicados para fortalecer confiabilidad, trazabilidad y mantenibilidad:

### 7.1 Compatibilidad futura
- **`datetime.utcnow` eliminado** → `datetime.now(timezone.utc)` en todos los archivos (compatible Python 3.12+).
- **Alembic** integrado desde día 1 con migración inicial. Protege la BD contra cambios de schema sin migración.

### 7.2 Confiabilidad de datos
- **Paths relativos en BD** → `RawDocument.filepath` ahora guarda paths relativos a PROJECT_ROOT. Si el proyecto se mueve de directorio, los paths no se rompen.
- **JSON schemas validados** → `backend/db/json_schemas.py` contiene Pydantic models para cada campo `*_json`. Funciones `serialize_json()` / `deserialize_json()` garantizan que solo se persiste JSON válido.
- **CheckConstraints en BD** → Los campos `status`, `file_type`, `entity_type`, `account_type`, `severity`, `validation_type` y `reconciliation_status` tienen `CHECK` constraints que rechazan valores inválidos a nivel SQLite.
- **Enums Python** → Todos los valores válidos documentados en `backend/db/models.py` como `str, Enum`.

### 7.3 Robustez de parsers
- **validate_contract()** → Método automático en `BaseParser` que verifica que el `ParseResult` tiene campos mínimos (account_number, currency, parser_name, etc.). Se ejecuta siempre via `safe_parse()`.
- **BaseParserContractTest** → Clase base de tests (`tests/test_parser_contracts.py`) que cualquier parser hereda para auto-verificar metadata, detect(), y contrato. Reduce el riesgo de romper cosas al agregar parsers nuevos.
- **ParserConflictError** → El registry ahora rechaza registrar 2 parsers con la misma `(bank_code, account_type)` key. Antes sobreescribía silenciosamente.
- **Auto-detect determinista** → En caso de empate de confianza, el parser se elige por orden alfabético del nombre (reproducible).
- **Error isolation** → Un parser con error de import no rompe el discovery de los demás. Los errores se acumulan y se pueden consultar.

### 7.4 Testabilidad
- **Engine lazy** → `session.py` ya no crea el engine al importar. Usa `@lru_cache` + `get_engine()`. Los tests pueden inyectar su propia BD sin efectos secundarios.
- **conftest.py mejorado** → Session con rollback automático, fixture `sample_account`, engine aislado por test.

### 7.5 Seguridad
- **CORS configurable** → Orígenes permitidos definidos en `Settings.cors_origins`, sobreescribibles vía `.env`. Ya no es `allow_origins=["*"]`.

### 7.6 Cache
- **Auto-invalidación** → Al procesar un documento exitosamente, `document_service` llama `cache.invalidate()` automáticamente. El cache nunca sirve datos obsoletos.
