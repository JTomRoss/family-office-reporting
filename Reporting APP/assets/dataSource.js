/* ============================================================
 * dataSource.js — Swap de window.MOCK → datos reales del backend
 *
 * Se ejecuta ANTES que app.js registre su listener DOMContentLoaded.
 * Publica una promesa window.__DATA_READY que app.js debe await-ear
 * antes de renderRoute().
 *
 * Qué se reemplaza (Fase 1 + 2):
 *   - Master: societies, banks, accounts, parsers
 *   - Diccionarios: buckets, etfDictionary, mandateCategories
 *   - Dashboard: kpis (campos disponibles), totalUSDSeries, retMonthly,
 *                monthLabels, months, bySociety, byBank, byCurrency
 *   - allocation (pie chart por bucket §6.1)
 *   - Positions (vacío hoy; se llenará cuando haya Excel diario)
 *   - Normalized (vista canónica de auditoría)
 *   - retMonthly + por-sociedad (Rentabilidades)
 *   - altFunds, altAggregate, strategies (Alternativos)
 *   - alerts (ValidationLog + heurísticas)
 *   - audit log
 *
 * Qué NO se reemplaza todavía:
 *   - movements (tabla daily_movements vacía hoy)
 *   - files, corrections, ingestQueue
 *   - fxSeries, fx
 * ============================================================ */
(function(){
  if (!window.API){
    // Sin API cargada (ej. si se abrió index.html por file://). Deja MOCK intacto.
    window.__DATA_READY = Promise.resolve({mode:'MOCK', reason:'API-missing'});
    window.__LIVE_DATA = false;
    return;
  }

  if (window.API.config.mode === 'MOCK'){
    window.__DATA_READY = Promise.resolve({mode:'MOCK', reason:'?mock=1'});
    window.__LIVE_DATA = false;
    return;
  }

  // Convierte USD absolutos a millones (la UI espera M).
  const toM = (x) => (x == null ? null : +(Number(x) / 1e6).toFixed(4));

  function mapAllocationBuckets(d){
    // Backend: d.allocation_buckets = [{bucket:"Caja", value_usd: N}, ...]
    // Frontend espera:             [{bucket, pct, val (en M USD)}, ...]
    const list = Array.isArray(d.allocation_buckets) ? d.allocation_buckets : [];
    const total = list.reduce((s,r) => s + Number(r.value_usd || 0), 0);
    if (!total) return [];
    return list.map(r => ({
      bucket: r.bucket,
      val: +(Number(r.value_usd) / 1e6).toFixed(4),
      pct: +((Number(r.value_usd) / total) * 100).toFixed(1),
    })).sort((a,b) => b.val - a.val);
  }

  function mapDashboard(d, mockFallback){
    const totalsM = (d.totalUSDSeries || []).map(toM);
    const patrimM = toM(d.kpis?.patrimonio_usd);
    const patrimPrevM = totalsM.length >= 2 ? totalsM[totalsM.length - 2] : null;
    const variacionM = toM(d.kpis?.variacion_usd);
    const variacionPct = (patrimPrevM && patrimM != null && patrimPrevM > 0)
      ? +(((patrimM - patrimPrevM) / patrimPrevM) * 100).toFixed(2)
      : null;

    // KPIs: mezclamos lo real con lo que aún no expone el backend (benchmark_diff,
    // liquidez_pct, twr_ytd_prev). Esos permanecen del mock hasta v2.
    const kpis = {
      patrimonio_usd: patrimM,
      patrimonio_clp: toM(d.kpis?.patrimonio_clp),
      variacion_usd: variacionM,
      variacion_pct: variacionPct != null ? variacionPct : (mockFallback?.variacion_pct ?? 0),
      twr_mom: d.kpis?.twr_mom ?? (mockFallback?.twr_mom ?? 0),
      twr_ytd: d.kpis?.twr_ytd ?? (mockFallback?.twr_ytd ?? 0),
      twr_ytd_prev: mockFallback?.twr_ytd_prev ?? 0,
      liquidez_usd: mockFallback?.liquidez_usd ?? 0,
      liquidez_pct: mockFallback?.liquidez_pct ?? 0,
      benchmark_diff: mockFallback?.benchmark_diff ?? 0,
    };

    // bySociety: [{key, value_usd}] → [{id, name, value_usd (M), pct}]
    const sumSoc = (d.bySociety || []).reduce((s,r) => s + Number(r.value_usd || 0), 0);
    const bySociety = (d.bySociety || []).map(r => ({
      id: r.key,
      name: r.key,
      value_usd: toM(r.value_usd),
      pct: sumSoc > 0 ? +(Number(r.value_usd) / sumSoc * 100).toFixed(1) : 0,
    })).sort((a,b) => b.value_usd - a.value_usd);

    // byBank: [{key, value_usd}] → [{code, value_usd (M), pct}]
    const sumBk = (d.byBank || []).reduce((s,r) => s + Number(r.value_usd || 0), 0);
    const byBank = (d.byBank || []).map(r => ({
      code: r.key,
      value_usd: toM(r.value_usd),
      pct: sumBk > 0 ? +(Number(r.value_usd) / sumBk * 100).toFixed(1) : 0,
    })).sort((a,b) => b.value_usd - a.value_usd);

    // byCurrency: convertimos CLP → USD equivalente con el FX del mock (sin
    // FX oficial todavía) para poder calcular pct comparable. El label sigue
    // siendo la moneda nativa; el monto mostrado queda en "equivalente M USD".
    const fxClpUsd = Number((window.MOCK && window.MOCK.fx) || 958);
    const ccyRaw = (d.byCurrency || []).map(r => {
      const native = Number(r.value_native || 0);
      const usdEq = r.currency === 'USD' ? native
                  : r.currency === 'CLP' ? native / fxClpUsd
                  : native; // otras monedas: asumimos 1:1 hasta tener FX real
      return { code: r.currency, value_native: native, usdEq };
    });
    const totalUsdEq = ccyRaw.reduce((s,r) => s + r.usdEq, 0);
    const byCurrency = ccyRaw.map(r => ({
      code: r.code,
      value_usd: +((r.usdEq) / 1e6).toFixed(4),
      pct: totalUsdEq > 0 ? +(r.usdEq / totalUsdEq * 100).toFixed(1) : 0,
    }));

    const allocation = mapAllocationBuckets(d);

    return {
      kpis,
      totalUSDSeries: totalsM,
      retMonthly: d.retMonthly || [],
      months: d.months || [],
      monthLabels: d.monthLabels || [],
      bySociety,
      byBank,
      byCurrency,
      allocation,
      _liveMeta: d.meta || null,
    };
  }

  function mapPositions(p){
    // El backend ya retorna el shape {acct, isin, instr, ccy, qty, price, mv, accrual, bucket}
    // Agregamos `mv` en nativo (el backend ya lo tenía como "mv").
    return (p.positions || []).map(r => ({
      acct: r.acct,
      account_number: r.account_number,
      society: r.society,
      bank: r.bank,
      isin: r.isin,
      instr: r.instr,
      ccy: r.ccy,
      qty: r.qty,
      price: r.price,
      mv: r.mv,
      mv_usd: r.mv_usd,
      accrual: r.accrual,
      bucket: r.bucket,
      as_of: r.as_of,
    }));
  }

  function mapAccounts(list){
    // La UI espera el shape del mock, que ya coincide casi 1:1 con el backend.
    return list.map(a => ({
      id: a.id,
      number: a.number,
      society: a.society,
      bank: a.bank,
      type: a.type,
      currency: a.currency,
      holder: a.holder,
      parser: a.parser,
    }));
  }

  function mapSocieties(list){
    return list.map(s => ({
      id: s.id,
      name: s.name,
      jur: s.jur,
      currency: s.currency,
    }));
  }

  function mapBanks(list){
    return list.map(b => ({
      code: b.code,
      name: b.name,
      short: b.short,
      country: b.country,
    }));
  }

  function mapParsers(list){
    return list.map(p => ({
      name: p.name,
      bank: p.bank,
      account_type: p.account_type,
      version: p.version,
    }));
  }

  async function boot(opts){
    const period = (opts && opts.period) || window.API.defaultPeriod();
    const scope = (opts && opts.scope) || _initialScope();
    const biceCurrency = (opts && opts.biceCurrency) || _initialBiceCurrency();
    try {
      const boot = await window.API.bootstrap({period, scope, biceCurrency});
      const MOCK = window.MOCK || {};
      const mockKpis = MOCK.kpis || {};

      // Swap mínimo: reemplazamos arrays in-place para no romper referencias.
      // app.js capturó `const D = window.MOCK` → `D` sigue siendo el mismo objeto,
      // así que basta con mutar campos.
      MOCK.societies = mapSocieties(boot.societies);
      MOCK.banks = mapBanks(boot.banks);
      MOCK.accounts = mapAccounts(boot.accounts);
      MOCK.parsers = mapParsers(boot.parsers);
      MOCK.buckets = boot.buckets;
      MOCK.etfDictionary = boot.etfDictionary;
      MOCK.mandateCategories = boot.mandateCategories;

      const dash = mapDashboard(boot.dashboard, mockKpis);
      MOCK.kpis = dash.kpis;
      MOCK.totalUSDSeries = dash.totalUSDSeries;
      MOCK.retMonthly = dash.retMonthly;
      MOCK.months = dash.months;
      MOCK.monthLabels = dash.monthLabels;
      MOCK.bySociety = dash.bySociety;
      MOCK.byBank = dash.byBank;
      MOCK.byCurrency = dash.byCurrency;
      // Allocation por bucket §6.1 — pie chart del dashboard. Si el backend no
      // trae datos (ej. no hay asset_allocation_json en ninguna cuenta del período),
      // dejamos el mock para no romper el render.
      if (dash.allocation && dash.allocation.length) {
        MOCK.allocation = dash.allocation;
      }

      MOCK.positions = mapPositions(boot.positions);

      // Tabla normalizada — vista técnica de auditoría (§10.1).
      if (boot.normalized && Array.isArray(boot.normalized.rows)) {
        MOCK.normalized = boot.normalized.rows;
      }

      // Rentabilidades: retMonthly ya viene del dashboard.
      // Guardamos el detalle por sociedad y la serie por sociedad para la página.
      if (boot.returns) {
        window.__RETURNS_BY_SOCIETY = boot.returns.bySociety || [];
      }

      // Alternativos (PE/RE con NAV real).
      if (boot.alternatives) {
        const alt = boot.alternatives;
        MOCK.altFunds = (alt.funds || []).map(f => ({
          id: f.id,
          name: f.name,
          class: f.class,
          strategy: f.strategy,
          society: f.society,
          vintage: f.vintage,
          nav: f.nav,              // ya en M USD
          commit: f.commit,        // null hoy
          distributed: f.distributed, // null hoy
          irr: f.irr,              // null hoy
          tvpi: f.tvpi,            // null hoy
          detail_label: f.detail_label,
        }));
        MOCK.altAggregate = alt.aggregate || {
          PE:{nav:0}, RE:{nav:0}, VC:{nav:0}, global:{nav:0},
        };
        MOCK.strategies = alt.strategies || {PE:[], RE:[], VC:[]};
      }

      // Alertas de calidad.
      if (boot.alerts && Array.isArray(boot.alerts.alerts)) {
        MOCK.alerts = boot.alerts.alerts;
      }

      // Audit log inmutable.
      if (Array.isArray(boot.auditLog)) {
        MOCK.audit = boot.auditLog;
      }

      // Al hacer swap LIVE de accounts, los IDs pasaron de "A01" a "A0001".
      // Los datasets del mock que referencian account IDs viejos ya no matchean.
      // Los vaciamos para evitar crashes al buscar `D.getAccount('A01')`.
      // Cuando exista endpoint real para estos, los swappeamos con datos vivos.
      MOCK.movements = [];            // no tenemos endpoint de movements aún
      MOCK.ingestQueue = [];          // idem
      MOCK.corrections = [];          // tabla correction_rules no existe aún
      // altFunds: si NO vino del backend, lo vacío igual para evitar refs cruzadas.
      if (!boot.alternatives || !Array.isArray(boot.alternatives.funds)) {
        MOCK.altFunds = [];
      }

      // Archivos procesados (raw_documents).
      if (boot.files && Array.isArray(boot.files.files)) {
        MOCK.files = boot.files.files;
        window.__FILES_TOTALS = boot.files.totals || null;
      }

      // Cobertura cuenta × mes (para detectar cartolas faltantes).
      if (boot.coverage) {
        window.__COVERAGE = boot.coverage;
      }

      // Refrescamos getters que capturaron arrays antiguos en closure.
      MOCK.getBank = (code) => MOCK.banks.find(b => b.code === code);
      MOCK.getSociety = (id) => MOCK.societies.find(s => s.id === id);
      MOCK.getAccount = (id) => MOCK.accounts.find(a => a.id === id);
      MOCK.getBucket = (id) => MOCK.buckets.find(b => b.id === id);

      window.__LIVE_DATA = true;
      window.__LIVE_PERIOD = period;
      window.__LIVE_SCOPE = scope;
      window.__LIVE_BICE_CCY = biceCurrency;
      return {mode: 'LIVE', period, scope, biceCurrency, meta: dash._liveMeta};
    } catch (err){
      console.warn('[dataSource] Fallback a MOCK por error en bootstrap:', err);
      window.__LIVE_DATA = false;
      window.__LIVE_ERROR = String(err && err.message || err);
      return {mode: 'MOCK', reason: 'fetch-error', error: window.__LIVE_ERROR};
    }
  }

  function _initialScope(){
    const s = localStorage.getItem('ecoterra.scope');
    return (s === 'national' || s === 'international') ? s : 'international';
  }
  function _initialBiceCurrency(){
    const c = localStorage.getItem('ecoterra.biceCcy');
    return (c === 'USD' || c === 'CLP') ? c : 'CLP';
  }

  // API público para re-fetch con otro scope / currency BICE.
  // El caller debe await-earlo y luego ejecutar renderRoute() para repintar.
  window.swapMockWithScope = async function(scope, biceCurrency){
    if (scope !== 'international' && scope !== 'national'){
      throw new Error(`scope inválido: ${scope}`);
    }
    if (biceCurrency && biceCurrency !== 'CLP' && biceCurrency !== 'USD'){
      throw new Error(`biceCurrency inválido: ${biceCurrency}`);
    }
    const period = (window.__LIVE_PERIOD || window.API.defaultPeriod());
    return boot({period, scope, biceCurrency: biceCurrency || _initialBiceCurrency()});
  };

  window.__DATA_READY = boot();
})();
