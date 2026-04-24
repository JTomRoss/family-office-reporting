/* ============================================================
 * api.js — Capa HTTP contra el backend FastAPI (/api/v1)
 *
 * Expone:
 *   window.API.bootstrap({period}) → resuelve { societies, banks, accounts,
 *                                              parsers, buckets, etfDictionary,
 *                                              mandateCategories, dashboard, positions }
 *   window.API.dashboard({period})
 *   window.API.positions({period})
 *   window.API.parityDashboard({period, toleranceUsd})
 *
 * Config:
 *   - Base URL por defecto: mismo host que sirve el frontend nuevo, puerto 8000.
 *     Puede sobreescribirse con ?api=http://otra.base:8000/api/v1 en la URL.
 *   - Mode:
 *       LIVE (default) = hace fetch al backend.
 *       MOCK (?mock=1) = NO hace fetch, deja que app.js use window.MOCK.
 * ============================================================ */
(function(){
  const qs = new URLSearchParams(location.search);

  function deriveBase(){
    // Si ?api=… override; si no, mismo host + :8000/api/v1.
    const override = qs.get('api');
    if (override) return override.replace(/\/+$/,'');
    const host = location.hostname || 'localhost';
    return `http://${host}:8000/api/v1`;
  }

  const config = {
    baseUrl: deriveBase(),
    mode: qs.get('mock') === '1' ? 'MOCK' : 'LIVE',
  };

  async function get(path){
    const url = `${config.baseUrl}${path}`;
    const res = await fetch(url, { method:'GET', headers:{'Accept':'application/json'} });
    if (!res.ok){
      const text = await res.text().catch(()=>'');
      throw new Error(`API ${res.status} ${res.statusText} en ${url}\n${text.slice(0,300)}`);
    }
    return res.json();
  }

  async function bootstrap(opts){
    const period = (opts && opts.period) || defaultPeriod();
    const scope = (opts && opts.scope) || 'international';
    const biceCurrency = (opts && opts.biceCurrency) || 'CLP';
    const bc = encodeURIComponent(biceCurrency);
    const p = encodeURIComponent(period);
    const s = encodeURIComponent(scope);
    // Paralelismo agresivo: lo que no depende de period + lo que sí.
    const [
      societies, banks, accounts, parsers,
      buckets, etfDictionary, mandateCategories,
      dashboard, positions, normalized,
      returns_, alternatives, alerts, auditLog,
      files, coverage,
    ] = await Promise.all([
      get('/master/societies'),
      get('/master/banks'),
      get('/master/accounts'),
      get('/master/parsers'),
      get('/dictionary/buckets'),
      get('/dictionary/etf'),
      get('/dictionary/mandates'),
      get(`/reporting/dashboard?period=${p}&scope=${s}&bice_currency=${bc}`),
      get(`/reporting/positions?period=${p}`),
      get(`/reporting/normalized?period=${p}&scope=${s}&bice_currency=${bc}`),
      get(`/reporting/returns?period=${p}&scope=${s}`),
      get(`/reporting/alternatives?period=${p}`),
      get(`/quality/alerts?period=${p}&scope=${s}`),
      get('/reporting/audit-log?limit=200'),
      get('/reporting/files?limit=500'),
      get(`/reporting/coverage?months=12&scope=${s}`),
    ]);
    return {
      period, scope, biceCurrency,
      societies, banks, accounts, parsers,
      buckets, etfDictionary, mandateCategories,
      dashboard, positions, normalized,
      returns: returns_, alternatives, alerts, auditLog,
      files, coverage,
    };
  }

  async function source(documentId){
    return get(`/sources/${encodeURIComponent(documentId)}`);
  }

  function defaultPeriod(){
    // Intenta usar el stored en localStorage; si no, usa mes anterior al actual.
    const stored = localStorage.getItem('ecoterra.period');
    if (stored && /^\d{4}-(0[1-9]|1[0-2])$/.test(stored)) return stored;
    const d = new Date();
    d.setDate(1); d.setMonth(d.getMonth() - 1);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2,'0');
    return `${y}-${m}`;
  }

  async function dashboard({period, scope = 'international'}){
    return get(`/reporting/dashboard?period=${encodeURIComponent(period)}&scope=${encodeURIComponent(scope)}`);
  }
  async function positions({period}){
    return get(`/reporting/positions?period=${encodeURIComponent(period)}`);
  }
  async function parityDashboard({period, scope = 'international', toleranceUsd} = {}){
    const t = toleranceUsd != null ? `&tolerance_usd=${toleranceUsd}` : '';
    return get(`/parity/dashboard?period=${encodeURIComponent(period)}&scope=${encodeURIComponent(scope)}${t}`);
  }

  window.API = { config, bootstrap, dashboard, positions, parityDashboard, source, defaultPeriod };
})();
