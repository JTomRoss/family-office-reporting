# Ecoterra Reporting — Arquitectura de datos y módulos

> Documento de referencia para futuras conversaciones con Claude Code.
> Refleja el diseño de la interfaz y reglas acordadas con el CIO.

## Reglas de oro (inmutables)

1. **Aislamiento total entre motores de lectura de PDF.** Cada parser (Bice, JPM, UBS SW, UBS Miami, GS, BBH, Altos Patrimonios, Boatview, etc.) vive en su propio módulo sin dependencias compartidas más allá de una interfaz común de salida.
2. **Tabla normalizada única** como destino obligatorio de TODO dato extraído. Meta (cuando exista API FX): una sola tabla `normalized` con columna `currency`. Transición: mantener `normalized_local` (CLP+USD Chile) y `normalized_intl` (USD offshore) hasta activar FX.
3. **Toda visualización (tabla, gráfico, KPI) consulta únicamente la tabla normalizada.** Cero recomputación en vistas. Cero fuentes alternativas.
4. **Trazabilidad hasta la fuente.** Cada fila normalizada carga `source_file_id`, `source_page`, `source_row`, `parser_version`, `ingested_at`, `edited_by`, `edit_history[]`.
5. **Aprendizaje de correcciones.** Cada corrección manual se persiste como regla en `correction_rules`. El pipeline consulta esas reglas antes de escribir en normalized.

## Módulos de la aplicación

### 1. Ingesta
- **Importar → Manual:** drag & drop, usuario completa sociedad/banco/cuenta/periodo, parser se ejecuta, preview, confirma.
- **Importar → Automática:** drag & drop masivo. El router detecta parser por fingerprint (hash de layout + keywords). Genera tabla de revisión con chips de confianza por campo (alto/medio/bajo) + bulk-apply a cartolas similares.
- **Importar → Masiva CSV/Excel:** upload de posiciones diarias o movimientos diarios con mapeo de columnas.
- **Archivos:** browser con filtros + selección múltiple + eliminar en bulk. Muestra estado (parsed / error / pending review).
- **Maestro de cuentas:** tabla editable con columnas: sociedad, titular, banco, cuenta, custodio, divisa base, país, grupo familiar, etiquetas.
- **Diccionario de instrumentos:** ISIN, ticker, nombre, clase de activo, subclase, emisor, moneda, país, benchmark, liquidez.

### 2. Análisis
- **Dashboard A — Por entidad:** filtros por sociedad / banco / cuenta / persona / fecha con botón Aplicar. KPIs, asset allocation, movimientos, alertas.
- **Dashboard B — Vistas ejecutivas:** layouts predefinidos tipo comité familiar (replica Excel actual).
- **Posiciones:** tabla fija cuenta × instrumento con columnas estables. Celdas vacías → `—`.
- **Movimientos:** log con búsqueda, filtros, export.
- **Rentabilidades:** TWR, MWR, periodo configurable, benchmark comparado, drill-down por activo.
- **Descarga PDF:** genera cartola Ecoterra con el mismo contenido del dashboard.

### 3. Calidad de datos
- **Tabla normalizada:** vista técnica CIO/auditor con TODAS las filas + metadata fuente.
- **Correcciones:** pestaña dedicada con campos editables. Cada edit → regla aplicable a similares.
- **Alertas de calidad:** heurísticas automáticas:
  - Rentabilidad fuera de rango (|r| > 15% mensual)
  - Saldos con variación > X% sin movimientos que lo expliquen
  - Identidades rotas: saldo_inicial + movimientos ≠ saldo_final
  - Cartolas esperadas y no cargadas (matriz cuenta × mes)
  - Instrumentos nuevos sin diccionario
  - Duplicados potenciales
  - Campos vacíos en cartolas parseadas

### 4. Sistema
- **Auditoría:** log inmutable de cada carga, edición, eliminación. Quién, cuándo, desde qué IP, qué cambió.
- **Configuración:** usuarios, roles (CIO, familia), FX API, formato números, tema.

## Modelo de datos (high level)

```
sources
  id, filename, uploaded_at, uploaded_by, parser_id, parser_version,
  status {pending|parsed|error|reviewed},
  fingerprint, pdf_url, thumbnail_url

accounts_master
  id, sociedad_id, banco_id, cuenta_numero, custodio, divisa_base,
  titular, grupo_familiar, pais, etiquetas[]

instruments_dict
  id, isin, ticker, nombre, asset_class, asset_subclass, emisor,
  moneda, pais, benchmark, liquidez_clase

normalized  -- una fila por posición o movimiento
  id, fecha, tipo {posicion|movimiento}, cuenta_id, instrumento_id,
  cantidad, precio, valor_moneda, moneda, valor_usd, valor_clp,
  movimiento_tipo, contraparte,
  source_file_id, source_page, source_row, parser_version,
  edited_by, edit_count, edit_history jsonb

correction_rules
  id, pattern jsonb, action jsonb, created_by, created_at,
  applied_count, active

audit_log
  id, ts, user_id, action, target_table, target_id,
  diff jsonb, ip, ua
```

## Flujo de carga automática (detalle UX)

1. Usuario arrastra N PDFs a la dropzone.
2. Router ejecuta fingerprinting paralelo por archivo.
3. Aparece **tabla de revisión** con una fila por PDF:
   - PDF (thumbnail + nombre)
   - Parser detectado (dropdown editable)
   - Sociedad (chip confianza + editable)
   - Banco (chip confianza + editable)
   - Cuenta (chip confianza + editable)
   - Periodo (chip + editable)
   - Estado: ✓ listo · ⚠ revisar · ✗ error
4. Selección múltiple → bulk actions:
   - Aplicar valor a seleccionadas
   - Aplicar valor a todas las similares (por fingerprint)
   - Aprobar e ingestar
5. Al aprobar → parser corre → filas a `normalized` → sources.status = parsed.

## Estados visuales de tablas

- **Loading:** skeletons con estructura visible, nunca spinner centrado que tapa todo.
- **Empty por filtro:** mantener headers, fila "Sin datos para los filtros aplicados", acción "Limpiar filtros".
- **Empty genuino:** misma estructura con `—` en todas las celdas.
- **Error:** banner arriba, datos parciales visibles con badge de warning.

## Reglas de performance de UI

- Debounce de búsquedas a 250ms, pero **filtros de panel no aplican hasta presionar "Aplicar"**.
- Virtualizar tablas > 200 filas.
- Memoizar agregaciones por sociedad/banco/mes en capa de datos.
- Prefetch de drawer de trazabilidad al hover.
