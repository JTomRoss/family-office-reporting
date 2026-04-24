# Ecoterra — Sistema de diseño

## Principios
1. **Confiabilidad visible.** Cada número tiene procedencia. Hover/click muestra fuente (archivo, página, fila).
2. **Densidad editorial, no terminal de Bloomberg.** Espacio generoso, jerarquía clara, números protagonistas.
3. **Tablas estables.** Columnas y filas fijas. Sin datos → `0` o `—`. Nunca colapsar.
4. **Filtros con "Aplicar".** Nunca auto-recomputar al seleccionar. Evita lags percibidos.
5. **Estados de carga explícitos.** Todo botón que procesa muestra spinner + texto.

## Direcciones visuales
Exploramos **2 direcciones** bajo el mismo sistema (toggleables en Tweaks):

### A — Editorial Claro (default)
- Background: `#f7f5ef` (hueso cálido, chroma ≤ 0.01)
- Tinta: `#1a1d1a` / `#3a3d3a`
- Acento primario: `#1a3a2e` (verde bosque profundo)
- Acento secundario: `#8a7a4e` (bronce apagado, solo para trazos finos y ornamentos)
- Positivo: `#2d5a3e` · Negativo: `#7a2d2d` (ambos desaturados)
- Líneas: `#d8d4c7`

### B — Grafito Oscuro
- Background: `#15171a`
- Surface: `#1d2024`
- Tinta: `#e8e6df` / `#9a9890`
- Acento primario: `#c9a961` (dorado muy tenue)
- Acento secundario: `#6b8a7a`
- Positivo: `#7aa889` · Negativo: `#c07575`
- Líneas: `#2d3138`

## Tipografía
- **Display / Titulares:** `"Instrument Serif"` (Google Fonts) — serif de transición, editorial, para números hero y headers de sección.
- **UI / Cuerpo:** `"Inter"` con variante tabular — no, mejor `"IBM Plex Sans"` para evitar slop. Decisión: usamos **"Söhne"-like → fallback a `"Inter"` con `font-feature-settings: 'ss01', 'cv11', 'tnum'`**. Pragmáticamente: `Inter` tabular.
- **Mono para números en tablas:** `"JetBrains Mono"` tabular.

Pairings seleccionados:
- Default: **Instrument Serif** (headers/KPIs hero) + **Inter** (UI) + **JetBrains Mono** (cifras en tablas).
- Alt: **Fraunces** no (slop). → **Newsreader** + **Inter**.

## Escala tipográfica
- Hero KPI: 56px / serif
- H1 sección: 28px / serif
- H2: 18px / sans medium
- Body: 14px / sans
- Caption / meta: 11.5px / sans uppercase tracking 0.08em
- Mono datos: 13px tabular

## Espaciado
Grid base 4px. Padding estándar card: 24px. Gap entre cards: 16px. Sidebar: 248px. Topbar: 56px.

## Radios
Muy sutiles. `4px` cards, `2px` botones, `0` tablas. Nada redondeado tipo SaaS.

## Sombra
Apenas existente. `0 1px 0 rgba(0,0,0,0.04)` en cards sobre fondo claro. En oscuro, usar borders en vez de shadows.

## Números
- Tabular nums siempre.
- Signo negativo con paréntesis opcional (toggle en Tweaks).
- Separadores CLP: punto miles, coma decimal. USD: coma miles, punto decimal.
- Abreviaciones: K, M, MM configurable.

## Iconografía
- Stroke 1.5px, lineal, estilo Phosphor Light o custom.
- Nada de emoji.
- Placeholders de imagen: SVG rayado con caption mono.

## Trazabilidad — patrón UX
- Cualquier número consolidado: hover muestra tooltip con `Banco · Cuenta · Cartola · p.X`.
- Click en celda de tabla: abre **drawer lateral** de 480px con:
  1. Ruta del dato (cartola fuente → fila normalizada → agregación)
  2. Thumbnail + botón "Abrir PDF"
  3. Historial de ediciones manuales sobre ese dato
- Badge `●` verde/amarillo/rojo en esquina superior de celda si hubo corrección manual.

## Navegación
Sidebar izquierda fija, 4 grupos:
1. **Análisis** — Dashboard, Posiciones, Movimientos, Rentabilidades
2. **Datos** — Tabla normalizada, Correcciones, Alertas de calidad
3. **Ingesta** — Importar, Archivos, Maestro de cuentas, Diccionario
4. **Sistema** — Auditoría, Configuración

## Componentes clave
- **KPI hero card** con serif grande + delta + sparkline sutil
- **Filter bar** con chips + botón Aplicar sticky
- **Tabla fija** con columnas congeladas y placeholders `—`
- **Drawer de trazabilidad** lateral derecho 480px
- **Drop zone** grande con estados: idle / dragging / processing / review
- **Review table** post-ingesta con chips de confianza y bulk-apply
- **PDF export preview** — modal con preview del PDF generado
