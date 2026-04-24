/* ============================================================
 * app.js — Router + render de páginas (SPA sin build)
 * Respeta la gramática visual aprobada y los datos de MOCK.
 * ============================================================ */
(function(){
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const D = window.MOCK;

  // ---- Estado global ----
  const _storedScope = localStorage.getItem('ecoterra.scope');
  const _initialScope = (_storedScope === 'national' || _storedScope === 'international')
    ? _storedScope : 'international';
  const state = {
    theme: localStorage.getItem('ecoterra.theme') || 'light',
    currency: localStorage.getItem('ecoterra.cur') || 'USD', // 'USD' | 'CLP'
    filters: { scope:_initialScope, society:[], bank:[], account:[], type:[], holder:[], period:'2026-03' },
    filtersDirty: { scope:_initialScope, society:[], bank:[], account:[], type:[], holder:[], period:'2026-03' },
    route: location.hash.slice(1) || '/dashboard',
    refetching: false,
  };

  // ---- Aplicar tema ----
  function applyTheme(t){
    document.documentElement.setAttribute('data-theme', t);
    localStorage.setItem('ecoterra.theme', t);
    state.theme = t;
    $('#themeLight')?.classList.toggle('active', t==='light');
    $('#themeDark')?.classList.toggle('active', t==='dark');
  }

  // ---- Nav config ----
  const NAV = [
    {group:'Análisis', items:[
      {id:'/dashboard',       label:'Dashboard'},
      {id:'/posiciones',      label:'Posiciones'},
      {id:'/movimientos',     label:'Movimientos'},
      {id:'/rentabilidades',  label:'Rentabilidades'},
      {id:'/alternativos',    label:'Alternativos'},
    ]},
    {group:'Calidad', items:[
      {id:'/normalizada',     label:'Tabla normalizada'},
      {id:'/correcciones',    label:'Correcciones', badge:D.corrections.filter(c=>c.status==='pending').length},
      {id:'/alertas',         label:'Alertas de calidad', badge:D.alerts.length},
    ]},
    {group:'Ingesta', items:[
      {id:'/importar',        label:'Importar'},
      {id:'/archivos',        label:'Archivos'},
      {id:'/maestro',         label:'Maestro de cuentas'},
      {id:'/diccionario',     label:'Diccionario'},
    ]},
    {group:'Sistema', items:[
      {id:'/auditoria',       label:'Auditoría'},
      {id:'/config',          label:'Configuración'},
    ]},
  ];

  function renderSidebar(){
    const side = $('#sidebar');
    side.innerHTML = NAV.map(g => `
      <div class="side-group">
        <div class="side-title">${g.group}</div>
        ${g.items.map(it=>`
          <a class="side-item ${state.route===it.id?'active':''}" href="#${it.id}">
            <span>${it.label}</span>
            ${it.badge?`<span class="badge">${it.badge}</span>`:''}
          </a>`).join('')}
      </div>`).join('');
  }

  // ---- Filtros globales ----
  function filterAccounts(){
    const f = state.filters;
    return D.accounts.filter(a =>
      (!f.society.length || f.society.includes(a.society)) &&
      (!f.bank.length    || f.bank.includes(a.bank)) &&
      (!f.account.length || f.account.includes(a.id)) &&
      (!f.type.length    || f.type.includes(a.type)) &&
      (!f.holder.length  || f.holder.includes(a.holder))
    );
  }

  function filterPositions(){
    const accts = new Set(filterAccounts().map(a=>a.id));
    return D.positions.filter(p => accts.has(p.acct));
  }

  function filterMovements(){
    const accts = new Set(filterAccounts().map(a=>a.id));
    return D.movements.filter(m => accts.has(m.acct));
  }

  function isFiltered(){
    const f = state.filters;
    return f.society.length||f.bank.length||f.account.length||f.type.length||f.holder.length;
  }

  // ---- Helpers formato ----
  const cur = () => state.currency;
  function money(v_usd){
    if (v_usd==null) return '—';
    if (cur()==='USD') return '$'+D.formatUSD(v_usd);
    return '$'+D.formatCLP(v_usd*D.fx);
  }
  function moneyRaw(v, ccy){
    if (v==null) return '—';
    if (ccy==='CLP') return D.formatCLP(v);
    return D.formatUSD(v);
  }
  // Convierte mv a USD (los mv CLP son en CLP nativo)
  function mvToUSD(p){
    return p.ccy==='CLP' ? p.mv / D.fx : (p.ccy==='EUR' ? p.mv*1.08 : (p.ccy==='GBP' ? p.mv*1.26 : (p.ccy==='CHF' ? p.mv*1.12 : p.mv)));
  }

  // ---- Top-bar filter bar (solo para páginas analíticas) ----
  function renderFilterBar(){
    const f = state.filtersDirty;
    const chip = (key, labelKey, label, options, display) => `
      <div class="dropdown filter-chip ${JSON.stringify(state.filters[key])!==JSON.stringify(f[key])?'dirty':''}" data-fkey="${key}">
        <span class="lbl">${label}</span>
        <span class="val">${display}</span>
        <span class="caret">▾</span>
        <div class="dropdown-menu" onclick="event.stopPropagation()">
          ${options.map(o=>`
            <label class="dropdown-opt">
              <input type="checkbox" data-fkey="${key}" value="${o.v}" ${f[key].includes(o.v)?'checked':''}>
              <span>${o.lbl}</span>
              ${o.tag?`<span class="tag">${o.tag}</span>`:''}
            </label>`).join('')}
        </div>
      </div>`;

    const societyOpts = D.societies.map(s=>({v:s.id, lbl:s.name}));
    const bankOpts    = D.banks.map(b=>({v:b.code, lbl:b.name, tag:b.country}));
    const acctOpts    = D.accounts.map(a=>({v:a.id, lbl:`${a.number} · ${D.getBank(a.bank).short}`, tag:a.currency}));
    const typeOpts    = Array.from(new Set(D.accounts.map(a=>a.type))).map(t=>({v:t, lbl:t.replace(/_/g,' ')}));
    const holderOpts  = Array.from(new Set(D.accounts.map(a=>a.holder))).map(h=>({v:h, lbl:h}));

    const disp = (arr, fallback) => arr.length===0 ? fallback : (arr.length===1 ? arr[0] : `${arr.length} sel.`);

    // Chip Ámbito (Nacional/Internacional): selección única, es el primer filtro
    // y dispara re-fetch del backend porque cambia el universo de cuentas.
    const scopeChip = `
      <div class="dropdown filter-chip scope-chip ${state.filters.scope !== f.scope ? 'dirty' : ''}" data-fkey="scope">
        <span class="lbl">Ámbito</span>
        <span class="val">${f.scope === 'national' ? 'BICE' : 'Internacional'}</span>
        <span class="caret">▾</span>
        <div class="dropdown-menu" onclick="event.stopPropagation()">
          <label class="dropdown-opt">
            <input type="radio" name="scope-radio" data-scope="international" ${f.scope==='international'?'checked':''}>
            <span>Internacional</span>
          </label>
          <label class="dropdown-opt">
            <input type="radio" name="scope-radio" data-scope="national" ${f.scope==='national'?'checked':''}>
            <span>BICE</span>
          </label>
        </div>
      </div>`;

    const bar = $('#filterbar');
    bar.innerHTML = `
      <span class="filter-label">Filtros</span>
      ${scopeChip}
      ${chip('society','society','Sociedad', societyOpts, disp(f.society.map(v=>D.getSociety(v)?.name), 'Todas'))}
      ${chip('bank','bank','Banco',          bankOpts, disp(f.bank.map(v=>D.getBank(v)?.short), 'Todos'))}
      ${chip('account','account','Cuenta',   acctOpts, disp(f.account.map(v=>D.getAccount(v)?.number), 'Todas'))}
      ${chip('type','type','Tipo Cuenta',    typeOpts, disp(f.type, 'Todos'))}
      ${chip('holder','holder','Persona',    holderOpts, disp(f.holder, 'Todas'))}
      <div class="filter-chip period"><span class="lbl">Período</span> <span class="val">Mar 2026</span> <span class="caret">▾</span></div>
      <div class="filter-actions">
        <span class="btn-clear">Limpiar</span>
        <button class="btn-apply" ${isDirty()?'':'disabled'}>${state.refetching ? 'Cargando…' : 'Aplicar'}</button>
      </div>`;

    // Bindings
    $$('.dropdown', bar).forEach(dd=>{
      dd.addEventListener('click', e=>{
        if (e.target.closest('.dropdown-menu')) return;
        e.stopPropagation();
        $$('.dropdown.open').forEach(x=>x!==dd && x.classList.remove('open'));
        dd.classList.toggle('open');
      });
    });
    $$('input[type=checkbox]', bar).forEach(cb=>{
      cb.addEventListener('change', () => {
        const k = cb.dataset.fkey, v = cb.value;
        if (cb.checked){ if (!f[k].includes(v)) f[k].push(v); }
        else { f[k] = f[k].filter(x=>x!==v); }
        renderFilterBar();
      });
    });
    $$('input[type=radio][data-scope]', bar).forEach(rb=>{
      rb.addEventListener('change', () => {
        f.scope = rb.dataset.scope;
        renderFilterBar();
      });
    });
    bar.querySelector('.btn-apply').addEventListener('click', async ()=>{
      if (state.refetching) return;
      const scopeChanged = state.filters.scope !== f.scope;
      state.filters = JSON.parse(JSON.stringify(f));
      if (scopeChanged){
        // El scope cambió → re-fetch al backend con nuevo scope antes de renderizar.
        localStorage.setItem('ecoterra.scope', state.filters.scope);
        state.refetching = true;
        renderFilterBar();
        try {
          if (window.swapMockWithScope){
            await window.swapMockWithScope(state.filters.scope);
          }
        } catch (err){
          console.error('[scope-switch] refetch error:', err);
        }
        state.refetching = false;
      }
      renderFilterBar();
      renderRoute();
    });
    bar.querySelector('.btn-clear').addEventListener('click', ()=>{
      const keptScope = state.filters.scope;
      state.filters = {scope:keptScope, society:[],bank:[],account:[],type:[],holder:[],period:'2026-03'};
      state.filtersDirty = {scope:keptScope, society:[],bank:[],account:[],type:[],holder:[],period:'2026-03'};
      renderFilterBar();
      renderRoute();
    });
  }
  function isDirty(){
    const a = state.filters, b = state.filtersDirty;
    if (a.scope !== b.scope) return true;
    for (const k of ['society','bank','account','type','holder']){
      if (JSON.stringify(a[k])!==JSON.stringify(b[k])) return true;
    }
    return false;
  }
  document.addEventListener('click', (e)=>{
    // Close dropdowns when clicking outside any .dropdown
    const insideDropdown = e.target && e.target.closest && e.target.closest('.dropdown');
    if (insideDropdown) return;
    $$('.dropdown.open').forEach(d=>d.classList.remove('open'));
  }, false);

  // ============================================================
  // PÁGINAS
  // ============================================================

  // ---- DASHBOARD ----
  function pageDashboard(){
    const k = D.kpis;
    const allocSorted = [...(D.allocation||[])].sort((a,b)=>b.val-a.val);
    const filtered = filterAccounts();
    const isBice = state.filters.scope === 'national';
    const biceCcy = (window.__LIVE_BICE_CCY || 'CLP');
    const totalUSD = filterPositions().reduce((s,p)=>s+mvToUSD(p),0) / 1e6;

    // En scope=BICE, patrimonio viene en moneda nativa (CLP o USD) según biceCcy.
    // En international, viene en USD.
    const patrim = isBice
      ? (biceCcy === 'CLP' ? (k.patrimonio_clp || 0) : (k.patrimonio_usd || 0))
      : (isFiltered() ? totalUSD : k.patrimonio_usd);

    // Formateador que respeta la moneda del scope (sin convertir).
    // v_in_M = valor en millones de la moneda activa.
    const moneyView = (v_in_M) => {
      if (v_in_M == null) return '—';
      const absVal = Math.abs(v_in_M) * 1e6;
      const sign = v_in_M < 0 ? '-' : '';
      if (isBice) {
        return (biceCcy === 'CLP')
          ? `${sign}$${D.formatCLP(absVal)}`
          : `${sign}$${D.formatUSD(absVal)}`;
      }
      return money(v_in_M * 1e6);
    };
    const currencyLabel = isBice ? biceCcy : cur();

    const variacion_M = isBice
      ? (biceCcy === 'CLP' ? (k.variacion_clp || 0) : (k.variacion_usd || 0))
      : (k.variacion_usd || 0);

    return `
      ${pageHead('Dashboard', 'Visión consolidada del patrimonio. Lectura desde capa normalizada (§1).')}
      <div id="filterbar" class="filter-bar"></div>

      <div class="tabs">
        <div class="tab active">Por entidad</div>
        <div class="tab">Vistas ejecutivas</div>
      </div>

      ${state.filters.scope === 'national' ? `
        <div class="bice-ccy-row" role="tablist" aria-label="Moneda BICE">
          <span class="lbl">Moneda BICE</span>
          <button class="bice-ccy-btn ${(window.__LIVE_BICE_CCY||'CLP')==='CLP'?'active':''}" data-ccy="CLP" ${state.refetching?'disabled':''}>CLP</button>
          <button class="bice-ccy-btn ${(window.__LIVE_BICE_CCY||'CLP')==='USD'?'active':''}" data-ccy="USD" ${state.refetching?'disabled':''}>USD</button>
          <span class="subtle" style="margin-left:12px;font-size:11px">Mundos CLP y USD son independientes · no se convierten entre sí.</span>
        </div>
      ` : ''}

      <div class="kpi-grid">
        ${kpi('Patrimonio Consolidado',
              moneyView(patrim),
              isBice ? `Moneda ${biceCcy}` : (cur()==='USD' ? `CLP ${(patrim*D.fx).toFixed(2)} MMM` : `USD ${patrim.toFixed(2)} M`),
              `${(k.twr_ytd||0)>=0?'+':''}${(k.twr_ytd||0).toFixed(2)}%`, 'YTD', false, 'up')}
        ${kpi('Variación del Período',
              moneyView(variacion_M),
              null,
              D.formatPct(k.variacion_pct||0), 'MoM', (k.variacion_pct||0)<0, 'up')}
        ${kpi('Rentabilidad TWR',
              (k.twr_mom||0).toFixed(2)+'%',
              null,
              isBice ? '' : `+${(k.benchmark_diff||0).toFixed(2)}%`,
              isBice ? 'mensual' : 'vs. benchmark', false, 'flat')}
        ${kpi('Liquidez Disponible',
              isBice ? '—' : moneyView(k.liquidez_usd||0),
              isBice ? `Mundo ${biceCcy} · BICE` : (cur()==='USD'? `${(k.liquidez_pct||0).toFixed(1)}% del total` : null),
              isBice ? '' : '−0.12%', isBice ? '' : 'MoM', true, 'down')}
      </div>

      <div class="row">
        ${cardEvolution()}
        ${cardAllocation()}
      </div>

      <div class="row-3">
        ${cardMini('Por sociedad', (D.bySociety||[]).map(r=>({name:r.name, val:moneyView(r.value_usd), pct:r.pct})))}
        ${cardMini('Por custodio', (() => {
          const rows = (D.byBank||[]).map(r=>({
            name: (D.getBank(r.code)?.short || D.getBank(r.code)?.name || r.code),
            val_usd: r.value_usd,
            pct: r.pct,
          }));
          if (isBice){
            // En BICE no mezclamos Alternativos (que vive en mundo internacional USD).
            return rows.map(r=>({name:r.name, val:moneyView(r.val_usd), pct:r.pct}));
          }
          // Internacional: §6.1 trata Alternativos como custodio virtual.
          const altNav_usd = D.altAggregate?.global?.nav || 0;
          const total_usd = rows.reduce((s,r)=>s+r.val_usd,0) + altNav_usd;
          const rescaled = rows.map(r=>({...r, pct: total_usd ? r.val_usd/total_usd*100 : 0}));
          if (altNav_usd > 0){
            rescaled.push({ name:'Alternativos', val_usd: altNav_usd, pct: altNav_usd/total_usd*100, custom:true });
          }
          rescaled.sort((a,b)=>b.val_usd-a.val_usd);
          return rescaled.map(r=>({name:r.name, val:moneyView(r.val_usd), pct:r.pct, custom:r.custom}));
        })())}
        ${cardMini('Por moneda', (D.byCurrency||[]).map(r=>({
          name: r.code,
          val: moneyView(r.value_usd),
          pct: r.pct,
        })))}
      </div>

      <div class="foot-row">
        ${cardLastMovs()}
        ${cardAlerts()}
      </div>
    `;
  }

  function kpi(label, val, sub, delta, note, neg=false, trend='up'){
    const paths = {
      up:'M0,22 L20,20 L40,21 L60,17 L80,15 L100,13 L120,9 L140,10 L160,7 L180,5 L200,3',
      flat:'M0,14 L20,16 L40,12 L60,14 L80,15 L100,11 L120,13 L140,10 L160,12 L180,9 L200,11',
      down:'M0,10 L20,12 L40,9 L60,14 L80,12 L100,16 L120,14 L140,18 L160,16 L180,20 L200,18',
    };
    const showCur = val.includes('$') && cur();
    return `
      <div class="kpi">
        <div class="kpi-label">${label}</div>
        <div class="kpi-value">${val}${showCur?` <span class="cur">${cur()}</span>`:''}</div>
        ${sub?`<div class="kpi-clp">${sub}</div>`:''}
        <div class="kpi-foot"><span class="kpi-delta ${neg?'neg':''}">${delta}</span><span class="kpi-note">${note}</span></div>
        <svg class="kpi-spark" viewBox="0 0 200 26" preserveAspectRatio="none">
          <path d="${paths[trend]}" fill="none" stroke-width="1"/>
        </svg>
      </div>`;
  }

  function cardEvolution(){
    // Genera path con 12 puntos (últimos 12 meses)
    const series = D.totalUSDSeries.slice(-12);
    const labels = D.monthLabels.slice(-12);
    const min = Math.min(...series)*0.98, max = Math.max(...series)*1.01;
    const W = 720, H = 260, padL=30, padR=30, padT=10, padB=40;
    const step = (W-padL-padR)/(series.length-1);
    const pts = series.map((v,i)=>{
      const x = padL + i*step;
      const y = padT + (1-(v-min)/(max-min)) * (H-padT-padB);
      return [x,y];
    });
    const linePath = pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
    const fillPath = linePath + ` L${pts[pts.length-1][0]},${H-padB} L${pts[0][0]},${H-padB} Z`;
    const grids = [0.25,0.5,0.75].map(p=>{
      const y = padT + p*(H-padT-padB);
      const val = (max - p*(max-min)).toFixed(1)+'M';
      return `<line x1="${padL}" y1="${y}" x2="${W-padR}" y2="${y}"/><text x="0" y="${y+3}" fill="var(--chart-label)" stroke="none">${val}</text>`;
    }).join('');
    const xTicks = [0,3,6,8,11].map(i=>{
      const x = padL + i*step;
      return `<text x="${x-14}" y="${H-18}" fill="var(--chart-label)" stroke="none">${labels[i]||''}</text>`;
    }).join('');

    return `
      <div class="card">
        <div class="card-head">
          <div>
            <div class="card-kicker">Evolución patrimonio</div>
            <div class="card-title">Últimos 12 meses</div>
          </div>
          <div class="segmented" id="curSeg"><span class="${cur()==='USD'?'active':''}" data-c="USD">USD</span><span class="${cur()==='CLP'?'active':''}" data-c="CLP">CLP</span></div>
        </div>
        <div class="chart-wrap">
          <svg viewBox="0 0 ${W} ${H}" width="100%" height="100%" preserveAspectRatio="none">
            <defs>
              <linearGradient id="themed-fill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="var(--chart-fill)" stop-opacity="var(--chart-fill-opacity)"/>
                <stop offset="100%" stop-color="var(--chart-fill)" stop-opacity="0"/>
              </linearGradient>
            </defs>
            <g class="grid" stroke-width="1" font-family="Geist Mono" font-size="9">${grids}</g>
            <path class="fill" d="${fillPath}"/>
            <path class="line" d="${linePath}" fill="none" stroke-width="1.4"/>
            <circle class="halo" cx="${pts[pts.length-1][0]}" cy="${pts[pts.length-1][1]}" r="8"/>
            <circle class="dot"  cx="${pts[pts.length-1][0]}" cy="${pts[pts.length-1][1]}" r="3.5"/>
            <g class="axis" font-family="Geist Mono" font-size="9">${xTicks}</g>
          </svg>
        </div>
      </div>`;
  }

  function cardAllocation(){
    const total = D.allocation.reduce((s,a)=>s+a.val,0);
    const C = 48, stroke=14, circ = 2*Math.PI*C;
    let offset = 0;
    const arcs = D.allocation.map(a=>{
      const len = a.val/total * circ;
      const seg = `<circle cx="60" cy="60" r="${C}" fill="none" stroke="${D.getBucket(a.bucket).color}" stroke-width="${stroke}" stroke-dasharray="${len.toFixed(2)} ${(circ-len).toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}" transform="rotate(-90 60 60)"/>`;
      offset += len;
      return seg;
    }).join('');
    const legend = D.allocation.slice().sort((a,b)=>b.val-a.val).map(a=>`
      <div class="legend-row">
        <span class="sw" style="background:${D.getBucket(a.bucket).color}"></span>
        <span class="name">${a.bucket}</span>
        <span class="val">${money(a.val*1e6).replace('$','')}</span>
        <span class="pct">${a.pct.toFixed(1)}%</span>
      </div>`).join('');
    return `
      <div class="card">
        <div class="card-head">
          <div><div class="card-kicker">Asset allocation</div><div class="card-title">Buckets canónicos §6.1</div></div>
          <div class="card-action" title="Exportar">↓</div>
        </div>
        <div class="alloc-layout">
          <div class="donut">
            <svg viewBox="0 0 120 120" width="150" height="150">
              <circle class="bg" cx="60" cy="60" r="48" fill="none" stroke-width="14"/>
              ${arcs}
            </svg>
            <div class="donut-center">
              <div class="label">Total</div>
              <div class="val">${money(total*1e6).replace('$','')}</div>
            </div>
          </div>
          <div class="alloc-legend">${legend}</div>
        </div>
      </div>`;
  }

  function cardMini(title, rows){
    const safe = rows.map(r => ({
      ...r,
      pct: (typeof r.pct === 'number' && isFinite(r.pct)) ? r.pct : 0,
    }));
    const max = Math.max(1, ...safe.map(r=>r.pct));
    const swCls = ['sw-1','sw-2','sw-3','sw-4','sw-5','sw-6','sw-7'];
    return `
      <div class="card">
        <div class="mini-kicker">${title}</div>
        ${safe.map((r,i)=>`
          <div class="mini-row ${r.custom?'mini-row-alt':''}">
            <span class="sw" style="background:${r.custom?'var(--green)':`var(--${swCls[i%swCls.length]})`}"></span>
            <span class="name" style="--w:${Math.max(4,Math.round(r.pct/max*100))}%">${r.name}</span>
            <span class="val">${r.val}</span>
            <span class="pct">${r.pct.toFixed(1)}%</span>
          </div>`).join('')}
      </div>`;
  }

  function cardLastMovs(){
    const rows = D.movements.slice(0,6).map(m=>{
      const a = D.getAccount(m.acct);
      if (!a) return ''; // movement sin cuenta válida (cross-dataset mismatch)
      const cls = m.amount<0?'neg':'pos';
      const sign = m.amount<0?'−':'+';
      const soc = D.getSociety(a.society);
      const bank = D.getBank(a.bank);
      return `<tr>
        <td class="date">${m.date.slice(5)}</td>
        <td>${(soc?.name || a.society || '—').split(' ')[0]} · ${(bank?.short || a.bank || '—')} · ${(a.number || '').slice(-4)}</td>
        <td>${pillFor(m.type)}</td>
        <td>${m.instr}</td>
        <td class="num ${cls}">${sign}${D.formatUSD(Math.abs(m.amount))}<span class="cur">${m.ccy}</span></td>
      </tr>`;
    }).filter(Boolean).join('');
    const emptyRow = rows ? '' : '<tr><td colspan="5" class="subtle" style="padding:22px;text-align:center">Sin movimientos cargados. Sube un Excel de movimientos diarios desde Importar.</td></tr>';
    return `
      <div class="card">
        <div class="card-head">
          <div><div class="card-title" style="font-size:20px">Últimos movimientos</div></div>
          <a class="card-action" href="#/movimientos">Ver todos →</a>
        </div>
        <table>
          <thead><tr><th>Fecha</th><th>Cuenta</th><th>Tipo</th><th>Instrumento</th><th style="text-align:right">Monto</th></tr></thead>
          <tbody>${rows || emptyRow}</tbody>
        </table>
      </div>`;
  }

  function pillFor(type){
    const map = {
      buy:['buy','Compra'], sell:['sell','Venta'],
      coupon:['coupon','Cupón'], dividend:['div','Dividendo'],
      deposit:['buy','Aporte'], withdraw:['sell','Retiro'],
      redeem:['sell','Rescate'], fee:['neutral','Comisión'],
    };
    const [cls,lbl] = map[type] || ['neutral', type];
    return `<span class="pill ${cls}">${lbl}</span>`;
  }

  function cardAlerts(){
    const top = D.alerts.slice(0,4);
    const acctMeta = (id) => {
      const a = id ? D.getAccount(id) : null;
      if (!a) return '';
      const bank = D.getBank(a.bank);
      return `${bank?.short || a.bank || '—'} · ${a.number || id} · `;
    };
    return `
      <div class="card">
        <div class="card-head">
          <div><div class="card-title" style="font-size:20px">Alertas</div></div>
          <span class="alerts-count">${D.alerts.length}</span>
        </div>
        ${top.map(a=>`
          <div class="alert">
            <div class="alert-bar ${a.sev==='warning'?'warn':(a.sev==='info'?'info':'')}"></div>
            <div>
              <div class="alert-title">${a.title}</div>
              <div class="alert-meta">${acctMeta(a.acct)}${a.month || ''}${a.detail ? ' · ' + a.detail : ''}</div>
            </div>
            <a class="alert-action" href="#/alertas">Ver</a>
          </div>`).join('')}
        ${top.length ? '' : '<div class="subtle" style="padding:16px;text-align:center">Sin alertas activas.</div>'}
      </div>`;
  }

  // ---- POSICIONES ----
  function pagePosiciones(){
    const positions = filterPositions();
    const groupBy = (state.__posGroup || 'bucket');
    const groups = {};
    positions.forEach(p=>{
      const key = groupBy==='bucket' ? p.bucket
                : groupBy==='account' ? p.acct
                : groupBy==='society' ? (p.society || D.getAccount(p.acct)?.society || '—')
                : p.ccy;
      (groups[key] = groups[key] || []).push(p);
    });

    const total = positions.reduce((s,p)=>s+mvToUSD(p),0);
    const keys = Object.keys(groups).sort((a,b)=>{
      if (groupBy==='bucket'){
        const oa = D.getBucket(a)?.order ?? 99, ob = D.getBucket(b)?.order ?? 99;
        return oa-ob;
      }
      const sa = groups[a].reduce((s,p)=>s+mvToUSD(p),0);
      const sb = groups[b].reduce((s,p)=>s+mvToUSD(p),0);
      return sb-sa;
    });

    const rows = keys.map(k=>{
      const g = groups[k];
      const gSum = g.reduce((s,p)=>s+mvToUSD(p),0);
      const gPct = gSum/total*100;
      const label = groupBy==='bucket' ? `<span class="sw" style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${D.getBucket(k)?.color||'#888'};margin-right:8px"></span>${k}`
                  : groupBy==='account' ? `${D.getBank(D.getAccount(k).bank).short} · ${D.getAccount(k).number}`
                  : groupBy==='society' ? D.getSociety(k)?.name
                  : k;
      const gRow = `<tr class="grp"><td colspan="3">${label} <span class="muted mono" style="font-weight:400;margin-left:6px">(${g.length})</span></td><td class="num">${money(gSum)}</td><td class="num">${gPct.toFixed(1)}%</td></tr>`;
      const itemRows = g.slice().sort((a,b)=>mvToUSD(b)-mvToUSD(a)).map(p=>{
        const a = D.getAccount(p.acct);
        const usd = mvToUSD(p);
        return `<tr>
          <td>${p.instr}${p.mat?`<div class="subtle">vcto ${p.mat}${p.yld?` · YTM ${p.yld.toFixed(2)}%`:''}</div>`:''}</td>
          <td><span class="sw" style="display:inline-block;width:7px;height:7px;border-radius:2px;background:${D.getBucket(p.bucket)?.color};margin-right:6px;vertical-align:middle"></span>${p.bucket}</td>
          <td>${D.getBank(a.bank).short} · ${a.number}</td>
          <td class="num">${moneyRaw(p.mv, p.ccy)}<span class="cur">${p.ccy}</span></td>
          <td class="num">${(usd/total*100).toFixed(2)}%</td>
        </tr>`;
      }).join('');
      return gRow + itemRows;
    }).join('');

    return `
      ${pageHead('Posiciones', `Detalle por instrumento · ${positions.length} posiciones · Capa normalizada §1`)}
      <div id="filterbar" class="filter-bar"></div>
      <div class="toolbar">
        <div class="segmented" id="posGroup">
          ${['bucket','account','society','currency'].map(g=>`<span class="${groupBy===g?'active':''}" data-g="${g}">${g==='bucket'?'Por bucket':g==='account'?'Por cuenta':g==='society'?'Por sociedad':'Por moneda'}</span>`).join('')}
        </div>
        <div class="searchbox"><span class="ico">⌕</span><input placeholder="Buscar instrumento…" id="posSearch"></div>
        <div class="spacer"></div>
        <div class="muted mono">Total ${money(total)}</div>
      </div>

      <div class="card" style="padding:0 24px 12px">
        <table>
          <thead><tr>
            <th>Instrumento</th>
            <th>Bucket</th>
            <th>Cuenta</th>
            <th class="num">Market value</th>
            <th class="num">% portfolio</th>
          </tr></thead>
          <tbody id="posBody">${rows}</tbody>
        </table>
      </div>
    `;
  }

  // ---- MOVIMIENTOS ----
  function pageMovimientos(){
    const movs = filterMovements();
    const byType = {};
    movs.forEach(m => byType[m.type] = (byType[m.type]||0)+1);
    const totalUSD = movs.reduce((s,m)=>{
      const v = m.ccy==='CLP'? m.amount/D.fx : (m.ccy==='EUR'? m.amount*1.08 : m.amount);
      return s + v;
    },0);

    const rows = movs.map(m=>{
      const a = D.getAccount(m.acct);
      const cls = m.amount<0?'neg':'pos';
      const sign = m.amount<0?'−':'+';
      return `<tr>
        <td class="date">${m.date}</td>
        <td>${D.getSociety(a.society).name}</td>
        <td>${D.getBank(a.bank).short} · ${a.number}</td>
        <td>${pillFor(m.type)}</td>
        <td>${m.instr}${m.note?`<div class="subtle">${m.note}</div>`:''}</td>
        <td class="num ${cls}">${sign}${D.formatUSD(Math.abs(m.amount))}<span class="cur">${m.ccy}</span></td>
        <td><button class="src-btn">⊕ fuente</button></td>
      </tr>`;
    }).join('');

    return `
      ${pageHead('Movimientos', `${movs.length} movimientos · neto ${D.formatUSD(totalUSD)} USD · Detectados según reglas por parser (§4)`)}
      <div id="filterbar" class="filter-bar"></div>
      <div class="toolbar">
        <div class="searchbox"><span class="ico">⌕</span><input placeholder="Buscar instrumento o cuenta…"></div>
        <div class="spacer"></div>
        <div class="muted">Filtrar por tipo:</div>
        ${Object.entries(byType).map(([t,n])=>`${pillFor(t)} <span class="mono muted" style="font-size:10px;margin-right:10px">${n}</span>`).join('')}
      </div>
      <div class="card" style="padding:0 24px 12px">
        <table>
          <thead><tr>
            <th>Fecha</th><th>Sociedad</th><th>Cuenta</th><th>Tipo</th><th>Instrumento</th>
            <th class="num">Monto</th><th></th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  // ---- RENTABILIDADES ----
  function pageRentabilidades(){
    const mm = D.retMonthly;
    // Si tenemos datos reales por sociedad (cableados vía backend), los usamos;
    // si no, caemos al cálculo proxy del mock.
    let soc;
    if (Array.isArray(window.__RETURNS_BY_SOCIETY) && window.__RETURNS_BY_SOCIETY.length){
      soc = window.__RETURNS_BY_SOCIETY.map(s => ({
        id: s.id,
        name: s.name,
        mom: (s.mom == null ? 0 : s.mom),
        ytd: (s.ytd == null ? 0 : s.ytd),
      }));
    } else {
      soc = D.societies.map(s=>{
        const proxy = mm.map((r,i)=> (r ?? 0) + (Math.sin((i+(s.id||'X').charCodeAt(0))/2)*0.15));
        let p=1; for (const r of proxy.slice(10)) p *= (1+r/100);
        const ytd = (p-1)*100;
        return {id:s.id, name:s.name, mom:proxy[12], ytd:ytd};
      });
    }

    const benchmark = 1.28; // YTD benchmark
    const ytd = D.kpis.twr_ytd ?? 0;

    // path sparkline — algunos valores del backend pueden ser null (primer mes sin prev).
    const mmSafe = mm.map(v => (v == null ? 0 : v));
    const W=720,H=240,padL=28,padR=18,padT=16,padB=34;
    const step = (W-padL-padR)/(Math.max(1,mmSafe.length-1));
    const min = Math.min(...mmSafe)-.3, max=Math.max(...mmSafe)+.3;
    const pts = mmSafe.map((v,i)=>[padL+i*step, padT+(1-(v-min)/(max-min))*(H-padT-padB)]);
    const line = pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
    const bars = mmSafe.map((v,i)=>{
      const base = H-padB;
      const zero = padT+(1-(0-min)/(max-min))*(H-padT-padB);
      const y = padT+(1-(v-min)/(max-min))*(H-padT-padB);
      const h = Math.abs(y-zero);
      const x = padL+i*step-5;
      return `<rect x="${x}" y="${Math.min(y,zero)}" width="10" height="${h}" fill="${v<0?'var(--red)':'var(--green)'}" opacity=".75"/>`;
    }).join('');

    return `
      ${pageHead('Rentabilidades', `TWR mensual + YTD chain-linked (§5.2) · Fórmula: YTD% = [Π(1+rᵢ) − 1]·100`)}
      <div id="filterbar" class="filter-bar"></div>

      <div class="kpi-grid" style="grid-template-columns:repeat(3,1fr)">
        ${kpi('TWR YTD 2026', D.formatPct(ytd), `vs. benchmark ${D.formatPct(benchmark)}`, D.formatPct(ytd-benchmark), 'alpha', ytd<benchmark, 'up')}
        ${kpi('TWR MoM', D.formatPct(mm[12]), null, D.formatPct(mm[12]-mm[11]), 'vs. mes anterior', mm[12]<mm[11], 'flat')}
        ${kpi('TWR 12M', D.formatPct(ytd + D.kpis.twr_ytd_prev), 'base 31-Mar-2025', '+0.48%', 'vs. benchmark', false, 'up')}
      </div>

      <div class="row">
        <div class="card">
          <div class="card-head">
            <div><div class="card-kicker">Rentabilidad mensual</div><div class="card-title">Últimos 12 meses · consolidado</div></div>
          </div>
          <div class="chart-wrap">
            <svg viewBox="0 0 ${W} ${H}" width="100%" height="100%" preserveAspectRatio="none">
              <g class="grid" stroke-width="1">
                <line x1="${padL}" y1="${padT+(H-padT-padB)/2}" x2="${W-padR}" y2="${padT+(H-padT-padB)/2}"/>
              </g>
              ${bars}
              <path class="line" d="${line}" fill="none" stroke-width="1.4"/>
              ${pts.map(p=>`<circle class="dot" cx="${p[0]}" cy="${p[1]}" r="2.5"/>`).join('')}
              <g class="axis" font-family="Geist Mono" font-size="9">
                ${[0,3,6,9,12].map(i=>`<text x="${pts[i][0]-14}" y="${H-14}">${D.monthLabels[i]}</text>`).join('')}
              </g>
            </svg>
          </div>
        </div>

        <div class="card">
          <div class="card-head">
            <div><div class="card-kicker">Por sociedad</div><div class="card-title">TWR YTD 2026</div></div>
          </div>
          <table>
            <thead><tr><th>Sociedad</th><th class="num">MoM</th><th class="num">YTD</th></tr></thead>
            <tbody>
              ${soc.map(s=>`<tr>
                <td>${s.name}</td>
                <td class="num ${s.mom<0?'neg':'pos'}">${D.formatPct(s.mom)}</td>
                <td class="num ${s.ytd<0?'neg':'pos'}">${D.formatPct(s.ytd)}</td>
              </tr>`).join('')}
            </tbody>
          </table>
        </div>
      </div>

      <div class="card gap-16">
        <div class="card-head">
          <div><div class="card-kicker">Metodología</div><div class="card-title">Fórmulas canónicas §5</div></div>
        </div>
        <div style="font-size:12.5px;color:var(--text-2);line-height:1.7">
          <div><b>Profit JPM ETF:</b> <span class="mono">Income + Change_in_Value + (Accrual − Accrual_prev)</span></div>
          <div><b>Profit UBS Suiza:</b> <span class="mono">Total_Assets − Movimientos − Total_Assets_prev</span></div>
          <div><b>Rentabilidad mensual %:</b> <span class="mono">(Profit / Total_Assets_prev) × 100</span>  — <i>None si denom = 0</i></div>
          <div><b>YTD % (chain-linking):</b> <span class="mono">[ Π(1 + rᵢ/100) − 1 ] × 100</span> — nunca suma aritmética.</div>
          <div><b>Validación allocation:</b> <span class="mono">|Σ weights − 100| ≤ 0.01</span> (1 bp)</div>
        </div>
      </div>
    `;
  }

  // ============================================================
  // ALTERNATIVOS — dashboard con sub-tabs PE · RE · VC · Global · Detalle
  // ============================================================
  const ALT_COLORS = {
    PE: 'var(--blue)',
    RE: 'var(--green)',
    VC: 'var(--gold)',
  };
  const ALT_STRAT_COLORS = [
    'var(--blue)', 'var(--green)', 'var(--gold)', 'var(--red)',
    'color-mix(in oklab, var(--blue) 60%, var(--text-3))',
    'color-mix(in oklab, var(--green) 60%, var(--text-3))',
    'color-mix(in oklab, var(--gold) 70%, var(--text-3))',
    'color-mix(in oklab, var(--red) 60%, var(--text-3))',
  ];

  function altFmt(v, opts={}){
    const {m=true, suffix='', decimals=1, sign=false} = opts;
    if (v==null || isNaN(v)) return '—';
    const s = (sign && v>0 ? '+' : '') + (m ? '$' : '') + v.toFixed(decimals) + (m ? 'M' : '') + suffix;
    return s;
  }
  function altPct(v, opts={}){
    if (v==null || isNaN(v)) return '—';
    const {decimals=1, sign=false} = opts;
    return (sign && v>0?'+':'') + (v*100).toFixed(decimals)+'%';
  }
  function altMult(v, d=2){ if (v==null) return '—'; return v.toFixed(d)+'x'; }

  function altKpiStrip(agg, kind){
    const label = kind==='global'?'Total fondos':'Fondos';
    return `
      <div class="alt-kpis">
        <div class="alt-kpi"><div class="lbl">${label}</div><div class="val">${agg.count}</div></div>
        <div class="alt-kpi"><div class="lbl">Compromiso total</div><div class="val">${altFmt(agg.commit)}</div></div>
        <div class="alt-kpi"><div class="lbl">NAV total</div><div class="val">${altFmt(agg.nav)}</div></div>
        <div class="alt-kpi"><div class="lbl">Aportes</div><div class="val ${agg.contributions<0?'neg':''}">${altFmt(agg.contributions)}</div></div>
        <div class="alt-kpi"><div class="lbl">Distribuido</div><div class="val">${altFmt(agg.distributed)}</div></div>
        <div class="alt-kpi"><div class="lbl">TVPI portfolio</div><div class="val">${altMult(agg.tvpi)}</div></div>
        <div class="alt-kpi"><div class="lbl">IRR portfolio</div><div class="val ${agg.irr<0?'neg':'pos'}">${altPct(agg.irr)}</div></div>
        <div class="alt-kpi"><div class="lbl">DPI total</div><div class="val">${altMult(agg.dpi)}</div></div>
      </div>
    `;
  }

  // Donut SVG reutilizable — retorna arcs + legend
  function altDonut(data, {activeKey=null, dataKey='value', labelKey='label', colors=ALT_STRAT_COLORS, size=170, filterKey=null}={}){
    const total = data.reduce((s,d)=>s+d[dataKey],0);
    if (total<=0) return '<div class="alt-empty">Sin datos</div>';
    const C=60, stroke=18, circ=2*Math.PI*C;
    let offset = 0;
    const arcs = data.map((d,i)=>{
      const len = d[dataKey]/total * circ;
      const color = d.color || colors[i%colors.length];
      const dim = activeKey && activeKey!==d[labelKey];
      const seg = `<circle class="alt-arc ${dim?'dim':''}" cx="75" cy="75" r="${C}" fill="none" stroke="${color}" stroke-width="${stroke}" stroke-dasharray="${len.toFixed(2)} ${(circ-len).toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}" transform="rotate(-90 75 75)" ${filterKey?`data-alt-filter="${filterKey}" data-alt-filter-val="${d[labelKey]}" style="cursor:pointer"`:''}/>`;
      offset += len;
      return seg;
    }).join('');
    const legend = data.map((d,i)=>{
      const color = d.color || colors[i%colors.length];
      const pct = (d[dataKey]/total*100).toFixed(0);
      const active = activeKey===d[labelKey];
      return `<div class="alt-legend-row ${activeKey && !active?'dim':''} ${active?'on':''}" ${filterKey?`data-alt-filter="${filterKey}" data-alt-filter-val="${d[labelKey]}" style="cursor:pointer"`:''}>
        <span class="sw" style="background:${color}"></span>
        <span class="name">${d[labelKey]}</span>
        <span class="pct">${pct}%</span>
      </div>`;
    }).join('');
    return `
      <div class="alt-donut-wrap">
        <svg viewBox="0 0 150 150" width="${size}" height="${size}">
          <circle class="bg" cx="75" cy="75" r="60" fill="none" stroke-width="18"/>
          ${arcs}
        </svg>
        <div class="alt-legend">${legend}</div>
      </div>
      ${filterKey ? `<div class="alt-filter-hint">${activeKey?`Filtrando por <b>${activeKey}</b> · <a data-alt-clear-filter href="#">limpiar</a>`:'Haz clic en una categoría para filtrar'}</div>`:''}
    `;
  }

  // Line chart simple (NAV anual por ejemplo)
  function altLineChart(points, {height=180, color='var(--blue)', area=true, yFormat=v=>'$'+v.toFixed(0)+'M'}={}){
    if (!points.length) return '<div class="alt-empty">Sin datos</div>';
    const W=600, H=height, PAD={l:52, r:12, t:18, b:26};
    // Si x es string/no-numérico, usar índice
    const numericX = points.every(p => typeof p.x === 'number' && !isNaN(p.x));
    const xs = numericX ? points.map(p=>p.x) : points.map((_,i)=>i);
    const ys = points.map(p=>p.y);
    const xMin=Math.min(...xs), xMax=Math.max(...xs);
    const yMin=Math.min(0,...ys), yMax=Math.max(...ys)*1.1 || 1;
    const X = x => PAD.l + (x-xMin)/(xMax-xMin||1)*(W-PAD.l-PAD.r);
    const Y = y => H-PAD.b - (y-yMin)/(yMax-yMin||1)*(H-PAD.t-PAD.b);
    const line = points.map((p,i)=>`${i?'L':'M'}${X(xs[i]).toFixed(1)},${Y(p.y).toFixed(1)}`).join(' ');
    const fill = `${line} L${X(xMax).toFixed(1)},${Y(yMin).toFixed(1)} L${X(xMin).toFixed(1)},${Y(yMin).toFixed(1)} Z`;
    // Eje Y — 4 ticks
    const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => yMin + t*(yMax-yMin));
    const yLines = yTicks.map(t=>`<line x1="${PAD.l}" x2="${W-PAD.r}" y1="${Y(t).toFixed(1)}" y2="${Y(t).toFixed(1)}" class="grid"/><text x="${PAD.l-6}" y="${(Y(t)+3).toFixed(1)}" class="axis" text-anchor="end">${yFormat(t)}</text>`).join('');
    // Eje X — labels (cada N puntos)
    const step = Math.max(1, Math.ceil(points.length/10));
    const xTicks = points.map((p,i)=>({p,i})).filter(({i})=> i%step===0 || i===points.length-1);
    const xLines = xTicks.map(({p,i})=>`<text x="${X(xs[i]).toFixed(1)}" y="${H-8}" class="axis" text-anchor="middle">${p.label||p.x}</text>`).join('');
    const dots = points.map((p,i)=>`<circle cx="${X(xs[i]).toFixed(1)}" cy="${Y(p.y).toFixed(1)}" r="3" fill="${color}"/>`).join('');
    return `
      <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" class="alt-chart">
        ${yLines}
        ${area?`<path d="${fill}" fill="${color}" opacity="0.1"/>`:''}
        <path d="${line}" fill="none" stroke="${color}" stroke-width="1.6"/>
        ${dots}
        ${xLines}
      </svg>
    `;
  }

  // Bar chart vertical
  function altBars(data, {height=180, colorFn=(d,i)=>d.color||'var(--green)', yFormat=v=>'$'+v.toFixed(0)+'M', labelKey='label', valueKey='value'}={}){
    if (!data.length) return '<div class="alt-empty">Sin datos</div>';
    const W=600, H=height, PAD={l:52, r:12, t:14, b:26};
    const vals = data.map(d=>d[valueKey]);
    const yMin=Math.min(0,...vals), yMax=Math.max(...vals)*1.1 || 1;
    const Y = y => H-PAD.b - (y-yMin)/(yMax-yMin||1)*(H-PAD.t-PAD.b);
    const bw = (W-PAD.l-PAD.r) / data.length;
    const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => yMin + t*(yMax-yMin));
    const yLines = yTicks.map(t=>`<line x1="${PAD.l}" x2="${W-PAD.r}" y1="${Y(t).toFixed(1)}" y2="${Y(t).toFixed(1)}" class="grid"/><text x="${PAD.l-6}" y="${(Y(t)+3).toFixed(1)}" class="axis" text-anchor="end">${yFormat(t)}</text>`).join('');
    const bars = data.map((d,i)=>{
      const x = PAD.l + i*bw + bw*0.15;
      const w = bw*0.7;
      const y0 = Y(0), y1 = Y(d[valueKey]);
      return `<rect x="${x.toFixed(1)}" y="${Math.min(y0,y1).toFixed(1)}" width="${w.toFixed(1)}" height="${Math.abs(y1-y0).toFixed(1)}" fill="${colorFn(d,i)}" rx="2"/>
              <text x="${(x+w/2).toFixed(1)}" y="${H-8}" class="axis" text-anchor="middle">${d[labelKey]}</text>`;
    }).join('');
    return `
      <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" class="alt-chart">
        ${yLines}
        ${bars}
      </svg>
    `;
  }

  // Combo: bars (TVPI) + line (IRR) en ejes duales
  function altComboChart(data, {height=200}={}){
    if (!data.length) return '<div class="alt-empty">Sin datos</div>';
    const W=600, H=height, PAD={l:52, r:52, t:14, b:26};
    const tvpis = data.map(d=>d.tvpi);
    const irrs  = data.map(d=>d.irr);
    const yMinL = Math.min(0.8, ...tvpis)*0.95, yMaxL = Math.max(...tvpis)*1.1;
    const yMinR = Math.min(-0.15, ...irrs), yMaxR = Math.max(...irrs)*1.2;
    const YL = y => H-PAD.b - (y-yMinL)/(yMaxL-yMinL||1)*(H-PAD.t-PAD.b);
    const YR = y => H-PAD.b - (y-yMinR)/(yMaxR-yMinR||1)*(H-PAD.t-PAD.b);
    const bw = (W-PAD.l-PAD.r) / data.length;
    const yTicks = [0, 0.25, 0.5, 0.75, 1];
    const yLinesL = yTicks.map(t=>{const v=yMinL+t*(yMaxL-yMinL);return `<line x1="${PAD.l}" x2="${W-PAD.r}" y1="${YL(v).toFixed(1)}" y2="${YL(v).toFixed(1)}" class="grid"/><text x="${PAD.l-6}" y="${(YL(v)+3).toFixed(1)}" class="axis" text-anchor="end">${v.toFixed(1)}x</text>`}).join('');
    const yLinesR = yTicks.map(t=>{const v=yMinR+t*(yMaxR-yMinR);return `<text x="${W-PAD.r+6}" y="${(YR(v)+3).toFixed(1)}" class="axis" text-anchor="start">${(v*100).toFixed(0)}%</text>`}).join('');
    const bars = data.map((d,i)=>{
      const x = PAD.l + i*bw + bw*0.15;
      const w = bw*0.7;
      const y0 = YL(yMinL), y1 = YL(d.tvpi);
      return `<rect x="${x.toFixed(1)}" y="${Math.min(y0,y1).toFixed(1)}" width="${w.toFixed(1)}" height="${Math.abs(y1-y0).toFixed(1)}" fill="var(--blue)" rx="2" opacity="0.85"/>`;
    }).join('');
    const lineP = data.map((d,i)=>{const x=PAD.l+(i+0.5)*bw; return `${i?'L':'M'}${x.toFixed(1)},${YR(d.irr).toFixed(1)}`}).join(' ');
    const dots = data.map((d,i)=>{const x=PAD.l+(i+0.5)*bw; return `<circle cx="${x.toFixed(1)}" cy="${YR(d.irr).toFixed(1)}" r="3" fill="var(--red)"/>`}).join('');
    const xLabels = data.map((d,i)=>`<text x="${(PAD.l+(i+0.5)*bw).toFixed(1)}" y="${H-8}" class="axis" text-anchor="middle">${d.label}</text>`).join('');
    return `
      <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" class="alt-chart">
        ${yLinesL}${yLinesR}
        ${bars}
        <path d="${lineP}" fill="none" stroke="var(--red)" stroke-width="1.6" stroke-dasharray="3 2"/>
        ${dots}
        ${xLabels}
      </svg>
      <div class="alt-chart-legend">
        <span><span class="sw" style="background:var(--blue)"></span>TVPI</span>
        <span><span class="sw" style="background:var(--red)"></span>IRR</span>
      </div>
    `;
  }

  function pageAlternativos(){
    const st = state.alt || (state.alt = {tab:'PE', strategyFilter:null, society:'', vintage:'', detailFund:null});
    if (!st.detailFund) st.detailFund = D.altFunds[0].id;
    const tabs = [
      ['PE','Private Equity'],
      ['RE','Real Estate'],
      ['VC','Venture Capital'],
      ['global','Vista Global'],
      ['detail','Detalle Fondo'],
    ];
    let body = '';
    if (st.tab==='PE' || st.tab==='RE' || st.tab==='VC'){
      body = altClassView(st.tab);
    } else if (st.tab==='global'){
      body = altGlobalView();
    } else {
      body = altDetailView();
    }
    return `
      ${pageHead('Alternativos', `Portfolio de fondos alternativos · PE · RE · VC`)}
      <div class="alt-tabs">
        ${tabs.map(([v,l])=>`<div class="alt-tab ${st.tab===v?'active':''}" data-alt-tab="${v}">${l}</div>`).join('')}
      </div>
      ${body}
    `;
  }

  function altClassView(cls){
    const st = state.alt;
    // Filtro por estrategia (desde donut)
    let funds = D.altFunds.filter(f => f.class===cls);
    if (st.strategyFilter) funds = funds.filter(f => f.strategy===st.strategyFilter);
    if (st.society) funds = funds.filter(f => f.society===st.society);
    if (st.vintage) funds = funds.filter(f => String(f.vintage)===String(st.vintage));

    const agg = (() => {
      const f = funds;
      if (!f.length) return {count:0, commit:0, nav:0, contributions:0, distributed:0, tvpi:0, irr:0, dpi:0};
      const commit = f.reduce((s,x)=>s+x.commitment,0);
      const nav    = f.reduce((s,x)=>s+x.nav,0);
      const apo    = f.reduce((s,x)=>s+x.contributions,0);
      const dist   = f.reduce((s,x)=>s+x.distributed,0);
      return {count:f.length, commit, nav, contributions:apo, distributed:dist,
              tvpi:(nav+dist)/Math.abs(apo||1), dpi:dist/Math.abs(apo||1),
              irr: f.reduce((s,x)=>s+x.irr*x.nav,0)/Math.max(nav,0.01)};
    })();

    // Donut por estrategia
    const byStrat = D.strategies[cls].map(s=>{
      const f = D.altFunds.filter(x=>x.class===cls && x.strategy===s);
      return { label:s, value: f.reduce((acc,x)=>acc+x.nav,0) };
    }).filter(d=>d.value>0);

    // NAV anual agregado de la clase (sum de todos los fondos)
    const years = [];
    for (let y=2016; y<=2026; y++) years.push(y);
    const navYearly = years.map(y=>{
      const f = D.altFunds.filter(x=>x.class===cls && (!st.strategyFilter || x.strategy===st.strategyFilter));
      const nav = f.reduce((s,fund)=>{
        const pt = fund.navSeries.find(p=>p.year===y);
        return s + (pt?pt.nav:0);
      }, 0);
      return { x:y, y:nav, label:String(y) };
    });

    // Capital llamado neto anual (sum de flujos negativos aportes)
    const capCalled = years.map(y=>{
      const f = D.altFunds.filter(x=>x.class===cls && (!st.strategyFilter || x.strategy===st.strategyFilter));
      const v = f.reduce((s,fund)=>{
        return s + fund.flows.filter(fl=>fl.year===y && fl.amount<0).reduce((a,fl)=>a+(-fl.amount),0);
      }, 0);
      return { label:String(y), value: v/1e6 };
    });

    // TVPI/IRR histórico anual del programa — aproximado por promedio NAV-weighted por año
    const tvpiIrrYearly = years.map(y=>{
      const f = D.altFunds.filter(x=>x.class===cls && x.vintage<=y && (!st.strategyFilter || x.strategy===st.strategyFilter));
      if (!f.length) return { label:String(y), tvpi:1.0, irr:0 };
      // TVPI simulado: crece con vintage edad
      const avgAge = f.reduce((s,x)=>s+(y-x.vintage),0)/f.length;
      const tvpi = 0.95 + Math.min(0.55, avgAge*0.08);
      const irr  = Math.max(-0.15, Math.min(0.32, -0.05 + avgAge*0.05));
      return { label:String(y), tvpi, irr };
    });

    // Filtros disponibles
    const societies = [...new Set(D.altFunds.filter(f=>f.class===cls).map(f=>f.society))];
    const vintages  = [...new Set(D.altFunds.filter(f=>f.class===cls).map(f=>f.vintage))].sort();

    const rows = funds.map(f=>`
      <tr data-alt-fund-row="${f.id}" style="cursor:pointer">
        <td class="alt-fund-name">${f.name}</td>
        <td><span class="alt-strat-chip" style="background:color-mix(in oklab, ${ALT_STRAT_COLORS[D.strategies[cls].indexOf(f.strategy)%ALT_STRAT_COLORS.length]} 15%, transparent); color:${ALT_STRAT_COLORS[D.strategies[cls].indexOf(f.strategy)%ALT_STRAT_COLORS.length]}">${f.strategy}</span></td>
        <td>${D.getSociety(f.society)?.name.split(' ')[0] || f.society}</td>
        <td class="mono">${f.vintage}</td>
        <td class="mono subtle">${f.currency}</td>
        <td class="mono">${altFmt(f.commitment)}</td>
        <td class="mono">${(f.pct_called*100).toFixed(1)}%</td>
        <td class="mono">${altFmt(f.nav)}</td>
        <td class="mono ${f.contributions<0?'neg':''}">${altFmt(f.contributions)}</td>
        <td class="mono">${altFmt(f.distributed)}</td>
        <td><span class="alt-mult ${f.tvpi>=1?'pos':'neg'}">${altMult(f.tvpi)}</span></td>
        <td><span class="alt-mult ${f.irr>=0?'pos':'neg'}">${altPct(f.irr)}</span></td>
        <td class="mono">${altMult(f.dpi)}</td>
      </tr>
    `).join('');

    return `
      ${altKpiStrip(agg, 'class')}

      <div class="alt-row">
        <div class="alt-card">
          <div class="alt-card-head">NAV por estrategia${st.strategyFilter?` · <b>${st.strategyFilter}</b>`:' — clic para filtrar'}</div>
          ${altDonut(byStrat, {activeKey: st.strategyFilter, filterKey:'strategy'})}
        </div>
        <div class="alt-card">
          <div class="alt-card-head">NAV anual del programa</div>
          ${altLineChart(navYearly, {color: ALT_COLORS[cls]})}
        </div>
      </div>

      <div class="alt-row">
        <div class="alt-card">
          <div class="alt-card-head">Capital llamado neto anual</div>
          ${altBars(capCalled, {colorFn:()=>ALT_COLORS[cls]})}
        </div>
        <div class="alt-card">
          <div class="alt-card-head">Evolución anual TVPI + IRR del programa</div>
          ${altComboChart(tvpiIrrYearly)}
        </div>
      </div>

      <div class="alt-card" style="margin-top:14px">
        <div class="alt-card-head alt-table-head">
          <div>Fondos de ${cls==='PE'?'Private Equity':cls==='RE'?'Real Estate':'Venture Capital'} · ${funds.length}</div>
          <div class="alt-table-filters">
            <label>Estrategia:
              <select data-alt-f="strategyFilter">
                <option value="">Todas</option>
                ${D.strategies[cls].map(s=>`<option ${st.strategyFilter===s?'selected':''} value="${s}">${s}</option>`).join('')}
              </select>
            </label>
            <label>Sociedad:
              <select data-alt-f="society">
                <option value="">Todas</option>
                ${societies.map(s=>`<option ${st.society===s?'selected':''} value="${s}">${D.getSociety(s)?.name.split(' ')[0]||s}</option>`).join('')}
              </select>
            </label>
            <label>Vintage:
              <select data-alt-f="vintage">
                <option value="">Todos</option>
                ${vintages.map(v=>`<option ${String(st.vintage)===String(v)?'selected':''} value="${v}">${v}</option>`).join('')}
              </select>
            </label>
          </div>
        </div>
        <div class="alt-table-wrap">
          <table class="alt-table">
            <thead><tr>
              <th>Fondo</th><th>Estrategia</th><th>Sociedad</th><th>Vintage</th><th>Mon.</th>
              <th>Compromiso</th><th>% llamado</th><th>NAV</th><th>Aporte</th><th>Distribuido</th>
              <th>TVPI</th><th>IRR</th><th>DPI</th>
            </tr></thead>
            <tbody>${rows || `<tr><td colspan="13" class="subtle" style="padding:28px;text-align:center">Sin fondos con estos filtros</td></tr>`}</tbody>
          </table>
        </div>
      </div>
    `;
  }

  function altGlobalView(){
    const agg = D.altAggregate.global;
    // Donut por clase
    const byClass = [
      { label:'Private Equity', value:D.altAggregate.PE.nav, color:ALT_COLORS.PE },
      { label:'Real Estate',    value:D.altAggregate.RE.nav, color:ALT_COLORS.RE },
      { label:'Venture Capital',value:D.altAggregate.VC.nav, color:ALT_COLORS.VC },
    ];
    // Bars compromiso/NAV/dist agrupadas por clase
    // scatter IRR vs TVPI todos los fondos
    const W=600, H=280, PAD={l:52, r:16, t:14, b:36};
    const pts = D.altFunds.map(f=>({x:f.irr, y:f.tvpi, f}));
    const xMin=Math.min(-0.15,...pts.map(p=>p.x)), xMax=Math.max(0.30,...pts.map(p=>p.x));
    const yMin=Math.min(0.8,...pts.map(p=>p.y)), yMax=Math.max(...pts.map(p=>p.y))*1.08;
    const X = x => PAD.l + (x-xMin)/(xMax-xMin||1)*(W-PAD.l-PAD.r);
    const Y = y => H-PAD.b - (y-yMin)/(yMax-yMin||1)*(H-PAD.t-PAD.b);
    const xTicks = [-0.15, 0, 0.1, 0.2, 0.3].filter(t=>t>=xMin && t<=xMax);
    const yTicks = [0.8, 1.0, 1.4, 1.8, 2.2].filter(t=>t>=yMin && t<=yMax);
    const grid = `${xTicks.map(t=>`<line x1="${X(t).toFixed(1)}" x2="${X(t).toFixed(1)}" y1="${PAD.t}" y2="${H-PAD.b}" class="grid"/><text x="${X(t).toFixed(1)}" y="${H-18}" class="axis" text-anchor="middle">${(t*100).toFixed(0)}%</text>`).join('')}
                  ${yTicks.map(t=>`<line x1="${PAD.l}" x2="${W-PAD.r}" y1="${Y(t).toFixed(1)}" y2="${Y(t).toFixed(1)}" class="grid"/><text x="${PAD.l-6}" y="${(Y(t)+3).toFixed(1)}" class="axis" text-anchor="end">${t.toFixed(1)}x</text>`).join('')}`;
    const dots = pts.map(p=>`<circle cx="${X(p.x).toFixed(1)}" cy="${Y(p.y).toFixed(1)}" r="5" fill="${ALT_COLORS[p.f.class]}" opacity="0.75" stroke="var(--bg-2)" stroke-width="1"><title>${p.f.name} · TVPI ${altMult(p.f.tvpi)} · IRR ${altPct(p.f.irr)}</title></circle>`).join('');
    // Axes labels
    const axisLabels = `<text x="${(W/2).toFixed(0)}" y="${H-2}" class="axis" text-anchor="middle">IRR (%)</text>
                        <text x="12" y="${(H/2).toFixed(0)}" class="axis" text-anchor="middle" transform="rotate(-90 12 ${(H/2).toFixed(0)})">TVPI (x)</text>`;

    // Bars agrupadas
    const groups = ['PE','RE','VC'];
    const barsData = groups.map(g=>({class:g, commit:D.altAggregate[g].commit, nav:D.altAggregate[g].nav, dist:D.altAggregate[g].distributed}));
    const Wb=600, Hb=220, PADb={l:52, r:12, t:14, b:26};
    const maxV = Math.max(...barsData.flatMap(d=>[d.commit, d.nav, d.dist]))*1.1;
    const Yb = v => Hb-PADb.b - v/maxV*(Hb-PADb.t-PADb.b);
    const gw = (Wb-PADb.l-PADb.r)/groups.length;
    const sw = gw*0.25;
    const yTicksB = [0, 0.25, 0.5, 0.75, 1].map(t=>t*maxV);
    const yLinesB = yTicksB.map(t=>`<line x1="${PADb.l}" x2="${Wb-PADb.r}" y1="${Yb(t).toFixed(1)}" y2="${Yb(t).toFixed(1)}" class="grid"/><text x="${PADb.l-6}" y="${(Yb(t)+3).toFixed(1)}" class="axis" text-anchor="end">$${t.toFixed(0)}M</text>`).join('');
    const barsSvg = barsData.map((d,i)=>{
      const x0 = PADb.l + i*gw + (gw - sw*3)/2;
      return `
        <rect x="${x0.toFixed(1)}" y="${Yb(d.commit).toFixed(1)}" width="${sw.toFixed(1)}" height="${(Hb-PADb.b-Yb(d.commit)).toFixed(1)}" fill="${ALT_COLORS[d.class]}" opacity="0.35" rx="2"/>
        <rect x="${(x0+sw).toFixed(1)}" y="${Yb(d.nav).toFixed(1)}" width="${sw.toFixed(1)}" height="${(Hb-PADb.b-Yb(d.nav)).toFixed(1)}" fill="${ALT_COLORS[d.class]}" opacity="0.85" rx="2"/>
        <rect x="${(x0+sw*2).toFixed(1)}" y="${Yb(d.dist).toFixed(1)}" width="${sw.toFixed(1)}" height="${(Hb-PADb.b-Yb(d.dist)).toFixed(1)}" fill="${ALT_COLORS[d.class]}" opacity="0.55" rx="2"/>
        <text x="${(x0+sw*1.5).toFixed(1)}" y="${Hb-8}" class="axis" text-anchor="middle">${d.class==='PE'?'Private Equity':d.class==='RE'?'Real Estate':'Venture Capital'}</text>
      `;
    }).join('');

    // Tabla pivote: estrategia × (compromiso, NAV, TVPI, IRR, DPI, count)
    const allStrategies = [...new Set(D.altFunds.map(f=>f.strategy))];
    const pivot = allStrategies.map(s=>{
      const f = D.altFunds.filter(x=>x.strategy===s);
      const commit = f.reduce((a,x)=>a+x.commitment,0);
      const nav    = f.reduce((a,x)=>a+x.nav,0);
      const apo    = f.reduce((a,x)=>a+x.contributions,0);
      const dist   = f.reduce((a,x)=>a+x.distributed,0);
      const irr    = f.reduce((a,x)=>a+x.irr*x.nav,0)/Math.max(nav,0.01);
      return {
        strategy:s, classes:[...new Set(f.map(x=>x.class))].join('+'),
        count:f.length, commit, nav, dist,
        tvpi: (nav+dist)/Math.abs(apo||1),
        irr,
        dpi: dist/Math.abs(apo||1)
      };
    }).sort((a,b)=>b.nav-a.nav);

    return `
      ${altKpiStrip(agg, 'global')}

      <div class="alt-row">
        <div class="alt-card">
          <div class="alt-card-head">NAV por clase de activo</div>
          ${altDonut(byClass, {colors: [ALT_COLORS.PE, ALT_COLORS.RE, ALT_COLORS.VC]})}
        </div>
        <div class="alt-card">
          <div class="alt-card-head">Compromiso · NAV · Distribuido</div>
          <svg viewBox="0 0 ${Wb} ${Hb}" width="100%" height="${Hb}" class="alt-chart">${yLinesB}${barsSvg}</svg>
          <div class="alt-chart-legend">
            <span><span class="sw" style="background:var(--text-3);opacity:.4"></span>Compromiso</span>
            <span><span class="sw" style="background:var(--text-2)"></span>NAV</span>
            <span><span class="sw" style="background:var(--text-3);opacity:.7"></span>Distribuido</span>
          </div>
        </div>
      </div>

      <div class="alt-card" style="margin-top:14px">
        <div class="alt-card-head">IRR vs TVPI por estrategia — todos los fondos (${D.altFunds.length})</div>
        <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" class="alt-chart">
          ${grid}
          ${dots}
          ${axisLabels}
        </svg>
        <div class="alt-chart-legend">
          <span><span class="sw" style="background:${ALT_COLORS.PE}"></span>Private Equity</span>
          <span><span class="sw" style="background:${ALT_COLORS.RE}"></span>Real Estate</span>
          <span><span class="sw" style="background:${ALT_COLORS.VC}"></span>Venture Capital</span>
        </div>
      </div>

      <div class="alt-card" style="margin-top:14px">
        <div class="alt-card-head">Tabla pivote por estrategia</div>
        <div class="alt-table-wrap">
          <table class="alt-table">
            <thead><tr>
              <th>Estrategia</th><th>Clases</th><th># Fondos</th>
              <th>Compromiso</th><th>NAV</th><th>Distribuido</th>
              <th>TVPI</th><th>IRR</th><th>DPI</th>
            </tr></thead>
            <tbody>${pivot.map(p=>`<tr>
              <td><b>${p.strategy}</b></td>
              <td class="subtle">${p.classes}</td>
              <td class="mono">${p.count}</td>
              <td class="mono">${altFmt(p.commit)}</td>
              <td class="mono">${altFmt(p.nav)}</td>
              <td class="mono">${altFmt(p.dist)}</td>
              <td><span class="alt-mult ${p.tvpi>=1?'pos':'neg'}">${altMult(p.tvpi)}</span></td>
              <td><span class="alt-mult ${p.irr>=0?'pos':'neg'}">${altPct(p.irr)}</span></td>
              <td class="mono">${altMult(p.dpi)}</td>
            </tr>`).join('')}</tbody>
          </table>
        </div>
      </div>
    `;
  }

  function altDetailView(){
    const st = state.alt;
    const f = D.altFunds.find(x=>x.id===st.detailFund) || D.altFunds[0];

    // Trimestral: TVPI/IRR/DPI simulados por trimestre desde vintage hasta 2026Q1
    const quarters = [];
    for (let y=f.vintage; y<=2026; y++) for (let q=1; q<=4; q++){
      if (y===2026 && q>1) break;
      quarters.push({y,q, label:`${y}-${String(q*3).padStart(2,'0')}`});
    }
    const trims = quarters.map((pt,i)=>{
      const age = i+1;
      const t = age/quarters.length;
      return {
        label: pt.label,
        tvpi: 0.9 + t*(f.tvpi-0.9) + (Math.sin(i*0.3)*0.05),
        irr:  (t*f.irr) + (i>2 ? Math.cos(i*0.2)*0.03 : -0.08*(1-t*2)),
        dpi:  Math.min(f.dpi, t*t*f.dpi*1.2),
      };
    });

    // NAV anual del fondo
    const navYearly = f.navSeries.map(p=>({ x:p.year, y:p.nav, label:String(p.year) }));

    // Flujo de caja: agrupado anual, expandible a trimestres
    const flowsByYear = {};
    f.flows.forEach(fl => {
      if (!flowsByYear[fl.year]) flowsByYear[fl.year] = {year:fl.year, total:0, quarters:[]};
      flowsByYear[fl.year].total += fl.amount;
      flowsByYear[fl.year].quarters.push(fl);
    });
    let navRunning = 0;
    const flowRows = Object.values(flowsByYear).sort((a,b)=>a.year-b.year).map(yg => {
      navRunning += yg.total;
      const expanded = (st.expandedYears||[]).includes(yg.year);
      const amtClass = yg.total<0?'neg':'pos';
      return `
        <tr class="flow-year" data-alt-flow-year="${yg.year}" style="cursor:pointer">
          <td><span class="flow-caret ${expanded?'open':''}">▸</span> <b>${yg.year}</b></td>
          <td class="mono ${amtClass}">${yg.total<0?'-':''}$${Math.abs(yg.total/1000).toFixed(0)}K</td>
          <td class="mono">$${(navRunning/1000).toFixed(0)}K</td>
        </tr>
        ${expanded ? yg.quarters.map(q=>`
          <tr class="flow-q">
            <td style="padding-left:44px" class="subtle">${yg.year}-Q${q.quarter}</td>
            <td class="mono ${q.amount<0?'neg':'pos'}">${q.amount<0?'-':''}$${Math.abs(q.amount/1000).toFixed(0)}K</td>
            <td></td>
          </tr>`).join('') : ''}
      `;
    }).join('');

    return `
      <div class="alt-detail-picker alt-card">
        <div class="alt-card-head">Detalle por fondo</div>
        <label>Fondo:
          <select data-alt-f="detailFund">
            ${D.altFunds.map(x=>`<option ${x.id===f.id?'selected':''} value="${x.id}">${x.name} · ${x.class} · ${x.vintage}</option>`).join('')}
          </select>
        </label>
      </div>

      <div class="alt-detail-kpis">
        <div class="alt-kpi"><div class="lbl">Estrategia</div><div class="val sm">${f.strategy}</div></div>
        <div class="alt-kpi"><div class="lbl">Vintage</div><div class="val">${f.vintage}</div></div>
        <div class="alt-kpi"><div class="lbl">Compromiso</div><div class="val">${altFmt(f.commitment)}</div></div>
        <div class="alt-kpi"><div class="lbl">NAV actual</div><div class="val">${altFmt(f.nav)}</div></div>
        <div class="alt-kpi"><div class="lbl">Aporte neto</div><div class="val ${f.contributions<0?'neg':''}">${altFmt(f.contributions)}</div></div>
        <div class="alt-kpi"><div class="lbl">Distribuido</div><div class="val">${altFmt(f.distributed)}</div></div>
      </div>
      <div class="alt-detail-kpis">
        <div class="alt-kpi"><div class="lbl">Sociedad</div><div class="val sm">${D.getSociety(f.society)?.name || f.society}</div></div>
        <div class="alt-kpi"><div class="lbl">TVPI</div><div class="val ${f.tvpi>=1?'pos':'neg'}">${altMult(f.tvpi)}</div></div>
        <div class="alt-kpi"><div class="lbl">IRR</div><div class="val ${f.irr>=0?'pos':'neg'}">${altPct(f.irr)}</div></div>
        <div class="alt-kpi"><div class="lbl">DPI</div><div class="val">${altMult(f.dpi)}</div></div>
        <div class="alt-kpi"><div class="lbl">% llamado</div><div class="val">${(f.pct_called*100).toFixed(1)}%</div></div>
        <div class="alt-kpi"><div class="lbl">Moneda</div><div class="val sm">${f.currency}</div></div>
      </div>

      <div class="alt-card" style="margin-top:14px">
        <div class="alt-card-head">Evolución NAV (anual)</div>
        ${altLineChart(navYearly, {color: ALT_COLORS[f.class], yFormat:v=>'$'+v.toFixed(1)+'M'})}
      </div>

      <div class="alt-row-3">
        <div class="alt-card">
          <div class="alt-card-head">Múltiplo (TVPI) trimestral</div>
          ${altLineChart(trims.map(t=>({x:t.label,y:t.tvpi,label:t.label})), {color:'var(--blue)', yFormat:v=>v.toFixed(2)+'x'})}
        </div>
        <div class="alt-card">
          <div class="alt-card-head">IRR trimestral</div>
          ${altLineChart(trims.map(t=>({x:t.label,y:t.irr,label:t.label})), {color:'var(--red)', yFormat:v=>(v*100).toFixed(0)+'%'})}
        </div>
        <div class="alt-card">
          <div class="alt-card-head">DPI trimestral</div>
          ${altLineChart(trims.map(t=>({x:t.label,y:t.dpi,label:t.label})), {color:'var(--green)', yFormat:v=>v.toFixed(2)+'x'})}
        </div>
      </div>

      <div class="alt-card" style="margin-top:14px">
        <div class="alt-card-head">Flujo de caja</div>
        <table class="alt-table alt-flow-table">
          <thead><tr><th>Período</th><th>Movimiento</th><th>NAV fin período</th></tr></thead>
          <tbody>
            ${flowRows}
            <tr class="flow-current">
              <td>2026-04 — NAV actual</td>
              <td class="mono pos">${altFmt(f.nav, {decimals:1})}</td>
              <td class="mono"><b>${altFmt(f.nav, {decimals:1})}</b></td>
            </tr>
          </tbody>
        </table>
      </div>
    `;
  }

  function bindAlternativos(){
    const st = state.alt;
    if (!st) return;
    document.querySelectorAll('[data-alt-tab]').forEach(el => el.addEventListener('click', () => {
      st.tab = el.dataset.altTab;
      st.strategyFilter = null; // reset al cambiar tab
      renderRoute();
    }));
    document.querySelectorAll('[data-alt-filter]').forEach(el => el.addEventListener('click', (e) => {
      e.preventDefault();
      const key = el.dataset.altFilter, val = el.dataset.altFilterVal;
      if (key==='strategy') st.strategyFilter = (st.strategyFilter===val) ? null : val;
      renderRoute();
    }));
    document.querySelectorAll('[data-alt-clear-filter]').forEach(el => el.addEventListener('click', e => {
      e.preventDefault(); st.strategyFilter = null; renderRoute();
    }));
    document.querySelectorAll('[data-alt-f]').forEach(el => el.addEventListener('change', e => {
      st[el.dataset.altF] = e.target.value || null;
      renderRoute();
    }));
    document.querySelectorAll('[data-alt-fund-row]').forEach(el => el.addEventListener('click', () => {
      st.detailFund = el.dataset.altFundRow;
      st.tab = 'detail';
      renderRoute();
    }));
    document.querySelectorAll('[data-alt-flow-year]').forEach(el => el.addEventListener('click', () => {
      const y = +el.dataset.altFlowYear;
      st.expandedYears = st.expandedYears || [];
      const i = st.expandedYears.indexOf(y);
      if (i>=0) st.expandedYears.splice(i,1);
      else st.expandedYears.push(y);
      renderRoute();
    }));
  }

  // ---- TABLA NORMALIZADA ----
  function pageNormalizada(){
    const rows = D.normalized.map(r=>{
      const a = D.getAccount(r.account_id);
      const societyName = r.society || (a ? (D.getSociety(a.society)?.name || a.society) : '—');
      const bankShort = r.bank ? (D.getBank(r.bank)?.short || r.bank) : (a ? (D.getBank(a.bank)?.short || a.bank) : '—');
      return `<tr>
        <td>${societyName}</td>
        <td>${bankShort}</td>
        <td class="mono">${r.account_number}</td>
        <td class="mono">${r.month}</td>
        <td class="num">${moneyRaw(r.ending_value_with_accrual, r.currency)}<span class="cur">${r.currency}</span></td>
        <td class="num">${moneyRaw(r.ending_value_without_accrual, r.currency)}</td>
        <td class="num">${moneyRaw(r.accrual_ending, r.currency)}</td>
        <td class="num">${moneyRaw(r.cash_value, r.currency)}</td>
        <td class="num ${r.movements_net<0?'neg':'pos'}">${moneyRaw(r.movements_net, r.currency)}</td>
        <td class="num ${r.profit_period<0?'neg':'pos'}">${moneyRaw(r.profit_period, r.currency)}</td>
        <td><span class="dot ${r.source==='normalized'?'ok':'warn'}"></span><span class="mono subtle">${r.source}</span></td>
        <td>${r.source_document_id ? `<button class="src-btn" data-src-doc="${r.source_document_id}" title="Ver fuente">⊕</button>` : '<span class="subtle">—</span>'}</td>
      </tr>`;
    }).join('');

    const coverage = 98.4;
    return `
      ${pageHead('Tabla normalizada', `Capa canónica <code class="mono">monthly_metrics_normalized</code> · campos obligatorios §1`)}
      <div id="filterbar" class="filter-bar"></div>
      <div class="toolbar">
        <div class="metric"><span class="lbl">Cobertura</span><span class="v">${coverage}%</span></div>
        <div style="width:1px;height:30px;background:var(--line);margin:0 8px"></div>
        <div class="metric"><span class="lbl">Cuentas</span><span class="v mono">${D.normalized.length}</span></div>
        <div style="width:1px;height:30px;background:var(--line);margin:0 8px"></div>
        <div class="metric"><span class="lbl">Mes</span><span class="v mono">Mar 2026</span></div>
        <div class="spacer"></div>
        <button class="src-btn">Exportar CSV</button>
        <button class="src-btn">Ver scripts de backfill</button>
      </div>
      <div class="card" style="padding:0 24px 12px">
        <div class="t-wrap scroll">
          <table>
            <thead><tr>
              <th>Sociedad</th><th>Banco</th><th>Cuenta</th><th>Mes</th>
              <th class="num">Ending con accr.</th>
              <th class="num">Ending sin accr.</th>
              <th class="num">Accrual</th>
              <th class="num">Cash</th>
              <th class="num">Movs net</th>
              <th class="num">Profit</th>
              <th>Fuente</th><th></th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      </div>
    `;
  }

  // ---- CORRECCIONES ----
  function pageCorrecciones(){
    const pills = {pending:'part', applied:'ok', reviewed:'sev-info'};
    const rows = D.corrections.map((c,i)=>`
      <tr data-cidx="${i}">
        <td class="mono">${c.id}</td>
        <td>
          <div class="edit-field" contenteditable="true" data-field="title" spellcheck="false">${c.title}</div>
          <div class="edit-field subtle" contenteditable="true" data-field="detail" spellcheck="false">${c.detail}</div>
        </td>
        <td><span class="pill ${pills[c.status]}">${c.status}</span></td>
        <td><div class="edit-field" contenteditable="true" data-field="scope" spellcheck="false">${c.scope}</div></td>
        <td class="mono">${c.parser}</td>
        <td class="date">${c.proposed}</td>
        <td>
          ${c.status==='pending' ? `<button class="src-btn" data-act="save">Guardar regla</button> <button class="src-btn" data-act="apply">Aplicar a similares</button> <button class="src-btn" data-act="discard">Descartar</button>` : `<button class="src-btn" data-act="trace">Ver trazabilidad</button>`}
        </td>
      </tr>`).join('');
    return `
      ${pageHead('Correcciones', `Flujo upstream (§10.3). Edita en línea: título, descripción y alcance son editables. Al guardar, la corrección se propaga al pipeline (§10.4).`)}
      <div class="toolbar">
        <div class="metric"><span class="lbl">Pendientes</span><span class="v mono">${D.corrections.filter(c=>c.status==='pending').length}</span></div>
        <div style="width:1px;height:30px;background:var(--line);margin:0 8px"></div>
        <div class="metric"><span class="lbl">Aplicadas</span><span class="v mono">${D.corrections.filter(c=>c.status==='applied').length}</span></div>
        <div class="spacer"></div>
        <button class="btn-apply">+ Nueva propuesta</button>
      </div>
      <div class="card" style="padding:0 24px 12px">
        <table class="editable-table">
          <thead><tr><th>ID</th><th>Título / descripción</th><th>Estado</th><th>Alcance</th><th>Parser</th><th>Propuesta</th><th>Acciones</th></tr></thead>
          <tbody id="correcciones-tbody">${rows}</tbody>
        </table>
      </div>
    `;
  }

  // ---- ALERTAS ----
  function pageAlertas(){
    const rows = D.alerts.map(a=>{
      const sevCls = a.sev==='critical'?'sev-crit':a.sev==='warning'?'sev-warn':'sev-info';
      const dotCls = a.sev==='critical'?'err':a.sev==='warning'?'warn':'info';
      let acctLbl = '—';
      if (a.acct){
        const acc = D.getAccount(a.acct);
        if (acc){
          const bank = D.getBank(acc.bank);
          acctLbl = `${bank?.short || acc.bank || '—'} · ${acc.number || ''}`;
        } else {
          acctLbl = a.acct;
        }
      }
      return `<tr>
        <td class="mono">${a.id}</td>
        <td><span class="sev-col"><span class="dot ${dotCls}"></span><span class="pill ${sevCls}">${a.sev}</span></span></td>
        <td>${a.kind}</td>
        <td>${a.title}<div class="subtle">${a.detail || ''}</div></td>
        <td>${acctLbl}</td>
        <td class="mono">${a.month || ''}</td>
        <td><button class="src-btn">Ver</button> <button class="src-btn">Resolver</button></td>
      </tr>`;
    }).join('');

    const cnt = (sev)=>D.alerts.filter(a=>a.sev===sev).length;
    return `
      ${pageHead('Alertas de calidad', `Umbrales §5.4 + reglas de diccionario §6 + contrato parser §2`)}
      <div id="filterbar" class="filter-bar"></div>
      <div class="toolbar">
        <div class="metric"><span class="lbl">Críticas</span><span class="v mono" style="color:var(--red)">${cnt('critical')}</span></div>
        <div style="width:1px;height:30px;background:var(--line);margin:0 8px"></div>
        <div class="metric"><span class="lbl">Warning</span><span class="v mono" style="color:var(--gold-dim)">${cnt('warning')}</span></div>
        <div style="width:1px;height:30px;background:var(--line);margin:0 8px"></div>
        <div class="metric"><span class="lbl">Info</span><span class="v mono" style="color:var(--blue)">${cnt('info')}</span></div>
        <div class="spacer"></div>
        <button class="src-btn">Exportar</button>
      </div>
      <div class="card" style="padding:0 24px 12px">
        <table>
          <thead><tr><th>ID</th><th>Severidad</th><th>Tipo</th><th>Título</th><th>Cuenta</th><th>Mes</th><th></th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  // ---- IMPORTAR ----
  // Inicializa una copia editable de la cola; sobrevive entre renders dentro de la sesión.
  if (!state.__ingest) state.__ingest = JSON.parse(JSON.stringify(D.ingestQueue));
  if (!state.__ingestSel) state.__ingestSel = {};

  function confClass(s){ return s>=0.95?'hi':s>=0.75?'mid':'lo'; }
  function confPct(s){ return Math.round((s||0)*100); }

  // Autofill desde el maestro: al setear society + account,
  // banco/tipo/currency/holder/parser quedan determinados por el maestro §9.
  function resolveFromMaster(row){
    if (!row.account) return null;
    const a = D.getAccount(row.account);
    if (!a) return null;
    const b = D.getBank(a.bank);
    const s = D.getSociety(a.society);
    const p = D.parsers.find(x=>x.name===a.parser);
    return {
      bank: b.name, bank_short: b.short, bank_country: b.country,
      society_name: s.name, society_jur: s.jur,
      type: a.type, currency: a.currency, holder: a.holder,
      number: a.number, parser: a.parser, parser_version: p?.version || row.version,
    };
  }

  function pageImportar(){
    const queue = state.__ingest;
    const sel = state.__ingestSel;
    const total = queue.length;
    const ready = queue.filter(r=>r.status==='ready').length;
    const review = queue.filter(r=>r.status==='review').length;
    const error = queue.filter(r=>r.status==='error').length;
    const dup = queue.filter(r=>r.status==='duplicate').length;
    const selCount = Object.keys(sel).filter(k=>sel[k]).length;

    // Options para los selects inline
    const societyOpts = D.societies.map(s=>`<div class="inline-select-opt" data-v="${s.id}"><span>${s.name}</span><span class="sub">${s.jur}</span></div>`).join('');
    const parserOpts  = D.parsers.map(p=>`<div class="inline-select-opt" data-v="${p.name}"><span>${p.name}</span><span class="sub">v${p.version}</span></div>`).join('');

    // Cuenta: agrupadas por sociedad y filtrables por sociedad seleccionada
    function accountOptsFor(societyId){
      const list = societyId ? D.accounts.filter(a=>a.society===societyId) : D.accounts;
      const byBank = {};
      list.forEach(a=>{ (byBank[a.bank]=byBank[a.bank]||[]).push(a); });
      let html = '';
      Object.keys(byBank).forEach(bk=>{
        const b = D.getBank(bk);
        html += `<div class="inline-select-hdr">${b.short}</div>`;
        byBank[bk].forEach(a=>{
          html += `<div class="inline-select-opt" data-v="${a.id}"><span>${a.number}</span><span class="sub">${a.type.replace(/_/g,' ')}</span></div>`;
        });
      });
      return html || `<div class="inline-select-opt" style="color:var(--text-3)">Sin cuentas para esa sociedad</div>`;
    }

    // Construye cada fila
    const rows = queue.map((r, i) => {
      const resolved = resolveFromMaster(r);
      const society = r.society ? D.getSociety(r.society) : null;
      const account = r.account ? D.getAccount(r.account) : null;
      const parserMeta = r.parser ? D.parsers.find(p=>p.name===r.parser) : null;
      const lowConf = r.status==='review' || r.status==='error';
      const disabled = r.status==='duplicate';
      const isSel = !!sel[r.id];

      const warnHTML = (r.warnings||[]).map(w=>{
        const isErr = r.status==='error' || w.toLowerCase().includes('ningún parser');
        return `<div class="imp-warn ${isErr?'err':''}">⚠ ${w}</div>`;
      }).join('');

      // Cell: Parser (con confianza)
      const parserCell = `
        <div class="conf-cell">
          <div class="conf-top">
            <button class="edit-sel" data-edit="parser" data-row="${r.id}">
              <span class="val ${r.parser?'mono':'missing'}">${r.parser || 'Detectar…'}</span>
              <span class="caret">▾</span>
            </button>
          </div>
          <div class="conf-bar">
            <div class="pbar"><span class="${confClass(r.parser_score)}" style="width:${confPct(r.parser_score)}%"></span></div>
            <span class="pct">${confPct(r.parser_score)}%</span>
          </div>
          <div class="inline-select" data-sel="parser-${r.id}">
            <div class="inline-select-menu">${parserOpts}</div>
          </div>
          ${parserMeta ? `<div class="mono" style="font-size:10px;color:var(--text-3);margin-top:2px">v${parserMeta.version}</div>`:''}
        </div>`;

      // Cell: Sociedad
      const societyCell = `
        <div class="conf-cell">
          <div class="conf-top">
            <button class="edit-sel" data-edit="society" data-row="${r.id}">
              <span class="val ${society?'':'missing'}">${society?society.name:'Seleccionar…'}</span>
              <span class="caret">▾</span>
            </button>
          </div>
          <div class="conf-bar">
            <div class="pbar"><span class="${confClass(r.society_score)}" style="width:${confPct(r.society_score)}%"></span></div>
            <span class="pct">${confPct(r.society_score)}%</span>
          </div>
          <div class="inline-select" data-sel="society-${r.id}">
            <div class="inline-select-menu">${societyOpts}</div>
          </div>
        </div>`;

      // Cell: Cuenta — autocompleta banco/tipo/currency/holder al setearse
      const accountCell = `
        <div class="conf-cell">
          <div class="conf-top">
            <button class="edit-sel" data-edit="account" data-row="${r.id}">
              <span class="val ${account?'mono':'missing'}">${account?account.number:'Seleccionar…'}</span>
              <span class="caret">▾</span>
            </button>
          </div>
          <div class="conf-bar">
            <div class="pbar"><span class="${confClass(r.account_score)}" style="width:${confPct(r.account_score)}%"></span></div>
            <span class="pct">${confPct(r.account_score)}%</span>
          </div>
          <div class="inline-select" data-sel="account-${r.id}">
            <div class="inline-select-menu">${accountOptsFor(r.society)}</div>
          </div>
          ${resolved ? `
            <div class="resolved">
              <span>Banco <b>${resolved.bank_short}</b></span>
              <span>Tipo <b>${resolved.type.replace(/_/g,' ')}</b></span>
              <span>Divisa <b>${resolved.currency}</b></span>
              <span>Holder <b>${resolved.holder}</b></span>
              <span class="autofill">autofill desde maestro §9</span>
            </div>`:''}
        </div>`;

      // Cell: Período
      const periodCell = `
        <div class="conf-cell">
          <div class="conf-top"><span class="val mono">${r.period || '—'}</span></div>
          <div class="conf-bar">
            <div class="pbar"><span class="${confClass(r.period_score)}" style="width:${confPct(r.period_score)}%"></span></div>
            <span class="pct">${confPct(r.period_score)}%</span>
          </div>
          <div class="mono" style="font-size:10px;color:var(--text-3);margin-top:2px">${r.statement_date || 'fallback: nombre'}</div>
        </div>`;

      // Cell: Status
      const statusCell = `
        <div class="imp-status-cell">
          <span class="chip ${r.status}"><span class="dot"></span>${
            r.status==='ready'?'Listo':
            r.status==='review'?'Revisar':
            r.status==='error'?'Error':
            'Duplicado'}</span>
          ${warnHTML}
        </div>`;

      // Actions
      const actionsCell = `
        <div class="imp-row-actions">
          <button class="src-btn ${r.status==='ready'?'primary':''}" data-act="approve" data-row="${r.id}" ${disabled?'disabled':''}>Aprobar</button>
          <button class="src-btn" data-act="preview" data-row="${r.id}">Preview</button>
          <button class="src-btn" data-act="discard" data-row="${r.id}">Descartar</button>
        </div>`;

      return `
        <tr data-row="${r.id}" ${lowConf?'style="background:var(--bg-hover)"':''}>
          <td style="width:26px"><input type="checkbox" class="imp-chk" data-sel="${r.id}" ${isSel?'checked':''} ${disabled?'disabled':''}></td>
          <td style="width:220px">
            <div class="imp-file">
              <div class="imp-thumb"><span class="ext">PDF</span></div>
              <div style="min-width:0">
                <div class="imp-fname">${r.name}</div>
                <div class="imp-fmeta">${r.size} · ${r.pages}p</div>
                <div class="imp-fmeta" title="SHA-256 §1.4">${r.hash}</div>
              </div>
            </div>
          </td>
          <td style="width:200px">${parserCell}</td>
          <td style="width:200px">${societyCell}</td>
          <td>${accountCell}</td>
          <td style="width:130px">${periodCell}</td>
          <td style="width:200px">${statusCell}</td>
          <td style="width:120px">${actionsCell}</td>
        </tr>`;
    }).join('');

    return `
      ${pageHead('Importar cartolas', `Drop de PDFs · auto-detect del parser · SHA-256 idempotencia §1.4 · autofill desde maestro §9`)}

      <div class="dropzone-row">
        <div class="dropzone" id="dz-pdf">
          <div class="dropzone-kicker">Cartolas bancarias · PDF</div>
          <div class="dropzone-title">Arrastra PDFs aquí</div>
          <div class="dropzone-sub">o haz clic para seleccionar. Máx 20 MB por archivo. El router detecta parser, sociedad, cuenta y período por marcadores del documento — filas bajo umbral 95% quedan para revisión.</div>
          <span class="dropzone-btn">Seleccionar PDFs</span>
        </div>
        <div class="dropzone" id="dz-xls">
          <div class="dropzone-kicker">Datos tabulares · XLSX / CSV</div>
          <div class="dropzone-title">Arrastra Excels aquí</div>
          <div class="dropzone-sub">Alternativos · Maestro de cuentas · Diccionario de instrumentos. El router detecta el tipo por headers de columnas (§3 parsers/excel/*).</div>
          <div class="xls-targets">
            <span class="xls-chip" data-xls="alternatives">Alternativos</span>
            <span class="xls-chip" data-xls="master_accounts">Maestro</span>
            <span class="xls-chip" data-xls="instruments">Diccionario</span>
          </div>
          <span class="dropzone-btn ghost">Seleccionar Excel</span>
        </div>
      </div>

      <div class="imp-summary">
        <div class="imp-sum"><div class="lbl">En cola</div><div class="val">${total}</div><div class="sub">sesión 2026-04-03</div></div>
        <div class="imp-sum"><div class="lbl">Listos</div><div class="val ok">${ready}</div><div class="sub">score ≥ 95% en todos</div></div>
        <div class="imp-sum"><div class="lbl">Requieren revisión</div><div class="val warn">${review}</div><div class="sub">al menos un campo &lt; 95%</div></div>
        <div class="imp-sum"><div class="lbl">Errores</div><div class="val err">${error}</div><div class="sub">sin parser o asignación</div></div>
        <div class="imp-sum"><div class="lbl">Duplicados</div><div class="val">${dup}</div><div class="sub">SHA-256 ya procesado</div></div>
      </div>

      <div class="info-banner">
        <span class="lbl">§9 Autofill</span>
        <span class="txt">Al setear <b>Sociedad</b> + <b>ID de cuenta</b>, la app completa automáticamente <b>banco</b>, <b>tipo</b>, <b>divisa</b> y <b>holder</b> leyendo del maestro — no se editan acá. El <b>parser</b> se fija desde <span class="mono" style="font-size:11px">account.parser</span> (§3).</span>
      </div>

      <div class="imp-toolbar">
        <div class="bulk-info">${selCount>0 ? `<b>${selCount}</b> de ${total} seleccionadas` : `<b>${total}</b> archivos en cola`}</div>
        <div class="spacer"></div>
        <button class="btn-ghost" data-bulk="applySimilar" ${selCount?'':'disabled'}>Aplicar a similares (fingerprint)</button>
        <button class="btn-ghost" data-bulk="reassignSociety" ${selCount?'':'disabled'}>Reasignar sociedad…</button>
        <button class="btn-ghost" data-bulk="discard" ${selCount?'':'disabled'}>Descartar</button>
        <button class="btn-apply" data-bulk="approve" ${selCount?'':'disabled'}>Aprobar e ingestar ${selCount?`(${selCount})`:''}</button>
      </div>

      <div class="imp-table">
        <table>
          <thead><tr>
            <th></th>
            <th>Archivo</th>
            <th>Parser detectado</th>
            <th>Sociedad</th>
            <th>Cuenta <span style="color:var(--gold);text-transform:none;letter-spacing:0;font-size:9px;margin-left:4px">autofill</span></th>
            <th>Período</th>
            <th>Estado</th>
            <th></th>
          </tr></thead>
          <tbody id="imp-tbody">${rows}</tbody>
        </table>
      </div>
    `;
  }

  // ---- IMPORTAR: bindings ----
  function bindImportar(){
    if (state.route !== '/importar') return;

    // Checkboxes fila
    $$('.imp-chk[data-sel]').forEach(cb=>{
      cb.addEventListener('change', ()=>{
        state.__ingestSel[cb.dataset.sel] = cb.checked;
        renderRoute();
      });
    });

    // Apertura de selects inline
    $$('.edit-sel[data-edit]').forEach(btn=>{
      btn.addEventListener('click', e=>{
        e.stopPropagation();
        const edit = btn.dataset.edit, rid = btn.dataset.row;
        const key = `${edit}-${rid}`;
        const already = document.querySelector(`.inline-select[data-sel="${key}"].open`);
        $$('.inline-select.open').forEach(s=>s.classList.remove('open'));
        if (!already){
          const sel = document.querySelector(`.inline-select[data-sel="${key}"]`);
          if (sel){
            // Posicionar cerca del botón
            sel.classList.add('open');
          }
        }
      });
    });

    // Selección de una opción dentro del select
    $$('.inline-select').forEach(sel=>{
      sel.addEventListener('click', e=>{
        const opt = e.target.closest('.inline-select-opt[data-v]');
        if (!opt) return;
        e.stopPropagation();
        const [field, rid] = sel.dataset.sel.split('-');
        const row = state.__ingest.find(r=>r.id===rid);
        if (!row) return;
        const v = opt.dataset.v;
        if (field==='parser'){
          row.parser = v; row.parser_score = 1.0;
          const pm = D.parsers.find(p=>p.name===v); if (pm) row.version = pm.version;
        } else if (field==='society'){
          row.society = v; row.society_score = 1.0;
          // Si la cuenta actual no pertenece a la nueva sociedad, limpiar
          if (row.account){
            const a = D.getAccount(row.account);
            if (!a || a.society !== v){ row.account = null; row.account_score = 0; }
          }
        } else if (field==='account'){
          row.account = v; row.account_score = 1.0;
          const a = D.getAccount(v);
          if (a){
            // autofill desde maestro §9
            row.society = a.society;
            row.society_score = Math.max(row.society_score, 0.98);
            row.parser = a.parser;
            row.parser_score = Math.max(row.parser_score, 0.98);
            const pm = D.parsers.find(p=>p.name===a.parser);
            if (pm) row.version = pm.version;
          }
        }
        // Recalcular status: si todos los scores ≥ 0.85 y no hay warnings → ready
        const minScore = Math.min(row.parser_score||0, row.society_score||0, row.account_score||0, row.period_score||0);
        if (row.status !== 'duplicate' && row.status !== 'error'){
          row.status = minScore >= 0.95 ? 'ready' : 'review';
        } else if (row.status === 'error' && minScore >= 0.95 && row.parser && row.society && row.account){
          row.status = 'ready';
          row.warnings = [];
        }
        sel.classList.remove('open');
        renderRoute();
      });
    });

    // Bulk actions
    $$('button[data-bulk]').forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const ids = Object.keys(state.__ingestSel).filter(k=>state.__ingestSel[k]);
        const act = btn.dataset.bulk;
        if (act==='approve'){
          state.__ingest = state.__ingest.filter(r=>!ids.includes(r.id) || r.status==='duplicate' || r.status==='error');
          state.__ingestSel = {};
        } else if (act==='discard'){
          state.__ingest = state.__ingest.filter(r=>!ids.includes(r.id));
          state.__ingestSel = {};
        } else if (act==='applySimilar' || act==='reassignSociety'){
          // noop visual — demo
        }
        renderRoute();
      });
    });

    // Row actions
    $$('button[data-act][data-row]').forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const rid = btn.dataset.row, act = btn.dataset.act;
        if (act==='approve'){
          state.__ingest = state.__ingest.filter(r=>r.id!==rid);
        } else if (act==='discard'){
          state.__ingest = state.__ingest.filter(r=>r.id!==rid);
        } else if (act==='preview'){
          // noop visual
        }
        delete state.__ingestSel[rid];
        renderRoute();
      });
    });

    // Cerrar selects al click fuera
    document.addEventListener('click', closeInlineSelects, { once: true });
  }

  function closeInlineSelects(){
    $$('.inline-select.open').forEach(s=>s.classList.remove('open'));
  }

  // ---- ARCHIVOS ----
  function pageArchivos(){
    const st = state.archivos || (state.archivos = {q:'', bank:'', status:'', from:'', to:'', sort:'date_desc', tab:'procesados'});
    if (!st.tab) st.tab = 'procesados';
    const banks = D.banks;
    const statuses = ['SUCCESS','PARTIAL','ERROR'];

    // Tab: Cobertura (matriz cuenta × mes, detecta gaps)
    if (st.tab === 'cobertura') {
      return pageArchivosCobertura();
    }

    let rows = D.files.slice();
    if (st.q) rows = rows.filter(f => f.name.toLowerCase().includes(st.q.toLowerCase()) || f.hash.includes(st.q));
    if (st.bank) rows = rows.filter(f => { const a=D.getAccount(f.acct); return a && a.bank===st.bank; });
    if (st.status) rows = rows.filter(f => f.status===st.status);
    if (st.from) rows = rows.filter(f => f.date >= st.from);
    if (st.to) rows = rows.filter(f => f.date <= (st.to+' 23:59'));
    rows.sort((a,b)=> st.sort==='date_asc' ? a.date.localeCompare(b.date)
                     : st.sort==='score_desc' ? b.score-a.score
                     : st.sort==='score_asc'  ? a.score-b.score
                     : b.date.localeCompare(a.date));

    // Si tenemos totals del backend (via __FILES_TOTALS), usamos el total real
    // del sistema — no solo los que trajimos en esta página.
    const totals = window.__FILES_TOTALS || {
      total: D.files.length,
      SUCCESS: D.files.filter(f=>f.status==='SUCCESS').length,
      PARTIAL: D.files.filter(f=>f.status==='PARTIAL').length,
      ERROR:   D.files.filter(f=>f.status==='ERROR').length,
    };
    const countOk   = totals.SUCCESS;
    const countPart = totals.PARTIAL;
    const countErr  = totals.ERROR;

    const body = rows.map(f=>{
      const statusCls = f.status==='SUCCESS'?'ok':f.status==='PARTIAL'?'part':'err';
      const a = D.getAccount(f.acct);
      const bank = a ? D.getBank(a.bank) : null;
      return `<tr>
        <td>
          <div>${f.name}</div>
          <div class="subtle mono" style="font-size:10px;margin-top:2px">${f.hash}</div>
        </td>
        <td class="date">${f.date}</td>
        <td>${f.size}</td>
        <td class="mono">${f.parser} <span class="subtle">v${f.version}</span></td>
        <td>${bank ? `${bank.short} · ${a.number}` : '—'}</td>
        <td>${(f.score*100).toFixed(0)}%</td>
        <td><span class="pill ${statusCls}">${f.status}</span>${f.warning?`<div class="subtle" style="margin-top:4px">${f.warning}</div>`:''}</td>
      </tr>`;
    }).join('');

    return `
      ${pageHead('Archivos procesados', `Histórico inmutable · hash SHA-256 · parser + versión · status del contrato §2`)}
      <div class="tabs" style="margin-bottom:14px">
        <div class="tab active" data-archtab="procesados">Procesados</div>
        <div class="tab" data-archtab="cobertura">Cobertura · Cartolas faltantes</div>
      </div>

      <div class="imp-summary" style="margin-bottom:14px">
        <div class="imp-sum"><div class="lbl">Total procesados</div><div class="val">${totals.total}</div><div class="sub">histórico completo</div></div>
        <div class="imp-sum"><div class="lbl">Success</div><div class="val ok">${countOk}</div><div class="sub">parser.load limpio</div></div>
        <div class="imp-sum"><div class="lbl">Partial</div><div class="val warn">${countPart}</div><div class="sub">warnings §4</div></div>
        <div class="imp-sum"><div class="lbl">Error</div><div class="val err">${countErr}</div><div class="sub">ParserError §2</div></div>
      </div>

      <div class="card" style="padding:16px 18px;margin-bottom:14px">
        <div class="arch-filters">
          <div class="arch-field">
            <label>Buscar</label>
            <div class="searchbox"><span class="ico">⌕</span><input id="arch-q" placeholder="Nombre o hash…" value="${st.q}"></div>
          </div>
          <div class="arch-field">
            <label>Custodio</label>
            <select id="arch-bank">
              <option value="">Todos</option>
              ${banks.map(b=>`<option value="${b.code}" ${st.bank===b.code?'selected':''}>${b.short}</option>`).join('')}
            </select>
          </div>
          <div class="arch-field">
            <label>Status</label>
            <select id="arch-status">
              <option value="">Todos</option>
              ${statuses.map(s=>`<option value="${s}" ${st.status===s?'selected':''}>${s}</option>`).join('')}
            </select>
          </div>
          <div class="arch-field">
            <label>Desde</label>
            <input type="date" id="arch-from" value="${st.from}">
          </div>
          <div class="arch-field">
            <label>Hasta</label>
            <input type="date" id="arch-to" value="${st.to}">
          </div>
          <div class="arch-field">
            <label>Ordenar</label>
            <select id="arch-sort">
              <option value="date_desc" ${st.sort==='date_desc'?'selected':''}>Más recientes</option>
              <option value="date_asc" ${st.sort==='date_asc'?'selected':''}>Más antiguas</option>
              <option value="score_desc" ${st.sort==='score_desc'?'selected':''}>Score ↓</option>
              <option value="score_asc" ${st.sort==='score_asc'?'selected':''}>Score ↑</option>
            </select>
          </div>
          <div class="arch-field" style="justify-self:end">
            <label>&nbsp;</label>
            <button class="src-btn" id="arch-export">Exportar CSV</button>
          </div>
        </div>
      </div>

      <div class="card" style="padding:0 24px 12px">
        <div class="arch-count">${rows.length} de ${D.files.length} archivos</div>
        <table>
          <thead><tr>
            <th>Archivo · SHA-256</th><th>Fecha ingesta</th><th>Tamaño</th>
            <th>Parser</th><th>Cuenta</th><th>Score</th><th>Status</th>
          </tr></thead>
          <tbody>${body || `<tr><td colspan="7" class="subtle" style="padding:28px;text-align:center">Sin resultados con los filtros actuales</td></tr>`}</tbody>
        </table>
      </div>
    `;
  }

  function bindArchivos(){
    const st = state.archivos;
    if (!st) return;
    const wire = (id, key, ev='change') => {
      const el = document.getElementById(id);
      if (el) el.addEventListener(ev, e => { st[key] = e.target.value; renderRoute(); });
    };
    wire('arch-q', 'q', 'input');
    wire('arch-bank', 'bank');
    wire('arch-status', 'status');
    wire('arch-from', 'from');
    wire('arch-to', 'to');
    wire('arch-sort', 'sort');

    // Tabs Procesados / Cobertura
    $$('[data-archtab]').forEach(t => {
      t.addEventListener('click', () => { st.tab = t.dataset.archtab; renderRoute(); });
    });

    // Toggle "solo faltantes" en tab Cobertura
    const cg = document.getElementById('cov-only-gaps');
    if (cg) cg.addEventListener('change', e => {
      state.cover = state.cover || {};
      state.cover.showOnlyGaps = e.target.checked;
      renderRoute();
    });
  }

  // ============================================================
  // ARCHIVOS — sub-tab COBERTURA (matriz cuenta × mes, detecta faltantes)
  // ============================================================
  function pageArchivosCobertura(){
    const cov = window.__COVERAGE;
    if (!cov || !Array.isArray(cov.rows) || !cov.rows.length) {
      return `
        ${pageHead('Archivos · Cobertura', `Matriz cuenta × mes · detecta cartolas faltantes`)}
        <div class="tabs" style="margin-bottom:14px">
          <div class="tab" data-archtab="procesados">Procesados</div>
          <div class="tab active" data-archtab="cobertura">Cobertura · Cartolas faltantes</div>
        </div>
        <div class="card" style="padding:24px;text-align:center;color:var(--text-2)">
          Datos de cobertura no disponibles (modo MOCK). Reinicia con backend LIVE para verlos.
        </div>`;
    }

    const st = state.cover || (state.cover = { showOnlyGaps: false });
    const months = cov.months;
    const totals = cov.totals;

    const visibleRows = st.showOnlyGaps
      ? cov.rows.filter(r => r.gaps > 0)
      : cov.rows.slice();
    // Orden: mayor cantidad de gaps primero
    visibleRows.sort((a,b) => b.gaps - a.gaps);

    const head = `<tr>
      <th style="position:sticky;left:0;background:var(--card);z-index:2;min-width:220px">Cuenta</th>
      <th class="num" style="min-width:70px">Gaps</th>
      ${months.map(m => `<th class="num" style="font-size:10px;font-family:'Geist Mono',monospace;min-width:58px">${m.slice(2).replace('-','·')}</th>`).join('')}
    </tr>`;

    const body = visibleRows.map(r => {
      const a = r.account;
      const cells = r.cells.map(c => {
        if (c.covered) {
          const style = 'background:#2D6A4F;color:#fff;opacity:.92';
          const title = `${c.month} · ${c.file_type} · status=${c.status} · doc=${c.doc_id}`;
          const btn = c.doc_id
            ? `<button class="src-btn cov-cell" data-src-doc="${c.doc_id}" style="${style};width:100%;padding:4px 2px;font-size:11px" title="${title}">✓</button>`
            : `<span style="${style};display:inline-block;width:100%;padding:4px 2px;text-align:center" title="${title}">✓</span>`;
          return `<td class="num" style="padding:2px">${btn}</td>`;
        } else {
          return `<td class="num" style="padding:2px;background:color-mix(in oklab, #B53639 18%, transparent);text-align:center;font-size:11px;color:var(--red)" title="${c.month} · sin cartola">—</td>`;
        }
      }).join('');
      const gapCls = r.gaps === 0 ? 'pos' : r.gaps >= 3 ? 'neg' : '';
      return `<tr>
        <td style="position:sticky;left:0;background:var(--card);z-index:1">
          <div style="font-size:12px">${a.society}</div>
          <div class="subtle mono" style="font-size:11px">${a.bank} · ${a.number}</div>
        </td>
        <td class="num ${gapCls}" style="font-family:'Geist Mono',monospace">${r.gaps}</td>
        ${cells}
      </tr>`;
    }).join('');

    const covPct = totals.coverage_pct;
    const covCls = covPct >= 98 ? 'ok' : covPct >= 90 ? 'warn' : 'err';

    return `
      ${pageHead('Archivos · Cobertura', `Matriz cuenta × mes · últimos ${totals.months} meses · detecta cartolas faltantes`)}
      <div id="filterbar" class="filter-bar"></div>
      <div class="tabs" style="margin-bottom:14px">
        <div class="tab" data-archtab="procesados">Procesados</div>
        <div class="tab active" data-archtab="cobertura">Cobertura · Cartolas faltantes</div>
      </div>

      <div class="imp-summary" style="margin-bottom:14px">
        <div class="imp-sum"><div class="lbl">Cobertura</div><div class="val ${covCls}">${covPct}%</div><div class="sub">${totals.cells_covered} / ${totals.cells_total} celdas</div></div>
        <div class="imp-sum"><div class="lbl">Cuentas</div><div class="val">${totals.accounts}</div><div class="sub">scope ${cov.scope || 'intl'}</div></div>
        <div class="imp-sum"><div class="lbl">Meses</div><div class="val">${totals.months}</div><div class="sub">${months[0]} → ${months[months.length-1]}</div></div>
        <div class="imp-sum"><div class="lbl">Faltantes</div><div class="val err">${totals.cells_gap}</div><div class="sub">celdas sin cartola</div></div>
      </div>

      <div class="card" style="padding:14px 18px;margin-bottom:10px;display:flex;align-items:center;gap:14px">
        <label style="display:inline-flex;align-items:center;gap:6px;cursor:pointer">
          <input type="checkbox" id="cov-only-gaps" ${st.showOnlyGaps ? 'checked' : ''}>
          Mostrar solo cuentas con faltantes
        </label>
        <div class="subtle" style="font-size:12px">
          Click en <span style="display:inline-block;background:#2D6A4F;color:#fff;padding:1px 6px;border-radius:2px;font-size:10px">✓</span> para ver la cartola · <span style="color:var(--red)">—</span> = cartola pendiente
        </div>
      </div>

      <div class="card" style="padding:0;overflow:auto;max-height:70vh">
        <table style="font-size:12px">
          <thead style="position:sticky;top:0;background:var(--card);z-index:3">${head}</thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    `;
  }

  // ---- MAESTRO ----
  function pageMaestro(){
    const ms = state.__mast || (state.__mast = { uploadOpen:false, fileName:null, parsing:false, toast:null, q:'' });
    const q = (ms.q||'').toLowerCase();
    const filtered = D.accounts.filter(a=>{
      if (!q) return true;
      const s = D.getSociety(a.society), b = D.getBank(a.bank);
      return [a.number, a.type, a.currency, a.holder, a.parser, s?.name, b?.name].join(' ').toLowerCase().includes(q);
    });
    const rows = filtered.map(a=>{
      const s = D.getSociety(a.society), b = D.getBank(a.bank);
      const p = D.parsers.find(x=>x.name===a.parser);
      return `<tr>
        <td>${s.name}<div class="subtle">${s.jur}</div></td>
        <td>${b.name}</td>
        <td class="mono">${a.number}</td>
        <td>${a.type.replace(/_/g,' ')}</td>
        <td class="mono">${a.currency}</td>
        <td>${a.holder}</td>
        <td class="mono">${a.parser} <span class="subtle">v${p?.version||'?'}</span></td>
        <td><button class="src-btn">Editar</button></td>
      </tr>`;
    }).join('') || `<tr><td colspan="8" class="subtle" style="padding:22px;text-align:center">Sin coincidencias para "${ms.q}"</td></tr>`;
    return `
      ${pageHead('Maestro de cuentas', `${D.accounts.length} cuentas · ${D.societies.length} sociedades · ${D.banks.length} custodios · §9 maestro real`)}
      <div class="toolbar">
        <div class="searchbox"><span class="ico">⌕</span><input id="mast-q" placeholder="Buscar cuenta…" value="${ms.q||''}"></div>
        <div class="spacer"></div>
        ${ms.toast?`<span class="subtle" style="margin-right:12px;color:var(--green)">${ms.toast}</span>`:''}
        <button class="src-btn" id="mast-template">⬇ Plantilla Excel</button>
        <button class="src-btn" id="mast-upload">⬆ Subir Excel</button>
        <button class="btn-apply">+ Nueva cuenta</button>
      </div>

      <div class="mast-upload-strip">
        <div class="dropzone mini" id="mast-dz">
          <div>
            <div class="dropzone-kicker">Maestro · XLSX / CSV</div>
            <div class="dropzone-title">Arrastra <span class="mono">maestro_cuentas.xlsx</span> aquí</div>
            <div class="dropzone-sub">Headers esperados: <span class="mono">sociedad · banco · cuenta · tipo · divisa · holder · parser · account_type · version</span>. Parser <span class="mono">excel/master_accounts</span>.</div>
          </div>
          <div>
            <span class="dropzone-btn ghost" id="mast-pick">Seleccionar archivo</span>
            <input type="file" id="mast-file" accept=".xlsx,.xls,.csv" hidden>
          </div>
        </div>
      </div>

      <div class="card" style="padding:0 24px 12px">
        <table>
          <thead><tr>
            <th>Sociedad</th><th>Banco</th><th>Cuenta</th><th>Tipo</th>
            <th>Divisa</th><th>Holder</th><th>Parser</th><th></th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>

      ${ms.uploadOpen ? renderMaestroUploadModal() : ''}
    `;
  }

  function renderMaestroUploadModal(){
    const ms = state.__mast;
    return `
      <div class="modal-overlay" data-mast-close>
        <div class="modal">
          <div class="modal-head">
            <div>
              <div class="card-kicker">Subir Excel · Maestro de cuentas</div>
              <div class="card-title">${ms.fileName || 'archivo.xlsx'}</div>
            </div>
            <button class="modal-x" data-mast-close aria-label="Cerrar">×</button>
          </div>
          <div class="modal-body">
            <div class="info-banner" style="margin-bottom:14px">
              <span class="lbl">Preview</span>
              Headers detectados <span class="mono">sociedad · banco · cuenta · tipo · divisa · holder · parser · version</span> — parser <span class="mono">excel/master_accounts</span> v1.1
            </div>
            <div class="diff-summary">
              <div class="diff-kpi"><div class="lbl">Cuentas nuevas</div><div class="val ok">+2</div><div class="sub">BICE-042 · GS-9915</div></div>
              <div class="diff-kpi"><div class="lbl">Modificadas</div><div class="val ok">3</div><div class="sub">holder / parser version</div></div>
              <div class="diff-kpi"><div class="lbl">Conflictos</div><div class="val warn">1</div><div class="sub">parser no registrado en §3</div></div>
              <div class="diff-kpi"><div class="lbl">Sin cambios</div><div class="val">${Math.max(D.accounts.length-5,0)}</div><div class="sub">filas idénticas</div></div>
            </div>
            <div class="diff-table">
              <div class="diff-row head"><div>Acción</div><div>Cuenta</div><div>Banco</div><div>Detalle</div></div>
              <div class="diff-row new"><div><span class="pill ok">Nueva</span></div><div class="mono">BICE-042</div><div>BICE</div><div class="subtle">Inversiones Las Raíces · CLP · Brokerage · parser bice/brokerage v3.4.0</div></div>
              <div class="diff-row new"><div><span class="pill ok">Nueva</span></div><div class="mono">GS-9915</div><div>Goldman Sachs</div><div class="subtle">Boatview Limited · USD · ETF · parser goldman_sachs/etf v2.1.0</div></div>
              <div class="diff-row upd"><div><span class="pill neutral">Update</span></div><div class="mono">UBS-206-560552-02</div><div>UBS Suiza</div><div class="subtle">parser version 2.3.2 → 2.3.3</div></div>
              <div class="diff-row upd"><div><span class="pill neutral">Update</span></div><div class="mono">JPM-2600</div><div>JP Morgan</div><div class="subtle">holder: C. Ross → J. T. Ross</div></div>
              <div class="diff-row upd"><div><span class="pill neutral">Update</span></div><div class="mono">BBH-4412</div><div>BBH</div><div class="subtle">etiqueta agregada: "trust"</div></div>
              <div class="diff-row warn"><div><span class="pill part">Conflicto</span></div><div class="mono">WELL-576372</div><div>Wellington</div><div class="subtle">parser <span class="mono">wellington/mandato</span> no existe en §3 — requiere registro previo</div></div>
            </div>
            <div class="subtle" style="font-size:11px;margin-top:14px;line-height:1.6">
              La persistencia real corre en backend (<span class="mono">POST /api/v1/master/accounts/bulk</span>). Este preview es en memoria; "Aplicar" escribirá en <span class="mono">accounts_master</span> y creará auditoría (§8).
            </div>
          </div>
          <div class="modal-foot">
            <button class="src-btn" data-mast-close>Cancelar</button>
            <button class="btn-apply" id="mast-apply">Aplicar 5 cambios</button>
          </div>
        </div>
      </div>
    `;
  }

  function bindMaestro(){
    if (state.route !== '/maestro') return;
    const ms = state.__mast;
    const q = document.getElementById('mast-q');
    if (q) q.addEventListener('input', e=>{ ms.q = e.target.value; /* no full rerender to preserve focus */
      // rerender just rows
      renderRoute();
      setTimeout(()=>{ const el=document.getElementById('mast-q'); if(el){ el.focus(); el.setSelectionRange(el.value.length, el.value.length); } }, 0);
    });
    const pick = document.getElementById('mast-pick');
    const fileInput = document.getElementById('mast-file');
    const upBtn = document.getElementById('mast-upload');
    const tplBtn = document.getElementById('mast-template');
    const dz = document.getElementById('mast-dz');

    const openWith = (name)=>{
      ms.fileName = name || 'maestro_cuentas.xlsx';
      ms.uploadOpen = true;
      ms.parsing = true;
      renderRoute();
      setTimeout(()=>{ ms.parsing = false; renderRoute(); }, 450);
    };

    if (pick) pick.addEventListener('click', ()=> fileInput?.click());
    if (upBtn) upBtn.addEventListener('click', ()=> fileInput?.click());
    if (fileInput) fileInput.addEventListener('change', e=>{
      const f = e.target.files && e.target.files[0];
      if (f) openWith(f.name);
    });
    if (dz){
      ['dragenter','dragover'].forEach(ev=> dz.addEventListener(ev, e=>{ e.preventDefault(); dz.classList.add('hover'); }));
      ['dragleave','drop'].forEach(ev=> dz.addEventListener(ev, e=>{ e.preventDefault(); dz.classList.remove('hover'); }));
      dz.addEventListener('drop', e=>{
        const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
        if (f) openWith(f.name);
      });
      dz.addEventListener('click', e=>{
        if (e.target.closest('#mast-pick')) return;
        fileInput?.click();
      });
    }
    if (tplBtn) tplBtn.addEventListener('click', ()=>{
      const headers = ['sociedad','banco','cuenta','tipo','divisa','holder','parser','account_type','version'];
      const sample = D.accounts.slice(0,3).map(a=>{
        const s = D.getSociety(a.society), b = D.getBank(a.bank);
        const p = D.parsers.find(x=>x.name===a.parser);
        return [s?.name, b?.name, a.number, a.type, a.currency, a.holder, a.parser, a.type, p?.version||''].join(',');
      });
      const csv = headers.join(',') + '\n' + sample.join('\n') + '\n';
      const blob = new Blob([csv], {type:'text/csv;charset=utf-8'});
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url; link.download = 'maestro_cuentas_plantilla.csv';
      document.body.appendChild(link); link.click(); link.remove();
      setTimeout(()=>URL.revokeObjectURL(url), 2000);
    });
    document.querySelectorAll('[data-mast-close]').forEach(el=> el.addEventListener('click', e=>{
      if (e.target !== el && !e.target.hasAttribute('data-mast-close')) return;
      ms.uploadOpen = false; renderRoute();
    }));
    const apply = document.getElementById('mast-apply');
    if (apply) apply.addEventListener('click', ()=>{
      ms.uploadOpen = false;
      ms.toast = '5 cambios aplicados · auditoría registrada';
      renderRoute();
      setTimeout(()=>{ if (state.__mast) { state.__mast.toast = null; renderRoute(); } }, 3500);
    });
  }

  // ---- DICCIONARIO ----
  function pageDiccionario(){
    const tabs = state.__dictTab || 'etf';
    // Overlay de edición
    const editState = state.__dictEdit || (state.__dictEdit = {});
    // Dataset local editable (clonado 1ª vez)
    if (!state.__dictData){
      state.__dictData = {
        buckets: JSON.parse(JSON.stringify(D.buckets)),
        etf: JSON.parse(JSON.stringify(D.etfDictionary)),
        mand: D.mandateCategories.slice(),
      };
    }
    const dd = state.__dictData;

    let content = '';
    if (tabs==='buckets'){
      content = `
        <div class="info-banner" style="margin-bottom:12px">
          <span class="lbl">§6.1</span>
          Los 10 buckets son <b>canónicos</b> — no se agregan ni eliminan. Solo color y orden son editables.
        </div>
        <div class="card" style="padding:0 24px 12px">
        <table class="dict-table">
          <thead><tr><th style="width:42%">Bucket</th><th>Color</th><th style="width:90px">Orden</th><th>Tono CSS</th><th style="width:120px">Aliases</th></tr></thead>
          <tbody>
            ${dd.buckets.map((b,i)=>`<tr>
              <td><span class="sw" style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${b.color};margin-right:8px;vertical-align:middle"></span><b>${b.id}</b></td>
              <td><div class="dict-color"><input type="color" class="dict-color-pick" data-dict-bucket="${i}" value="${b.color}"><span class="mono">${b.color}</span></div></td>
              <td><input type="number" class="dict-inp mono" data-dict-order="${i}" value="${b.order}" style="width:60px"></td>
              <td class="mono subtle">${b.css}</td>
              <td><button class="src-btn" data-dict-aliases="${b.id}">Ver ${countAliasesForBucket(b.id)}</button></td>
            </tr>`).join('')}
          </tbody>
        </table>
        </div>`;
    } else if (tabs==='etf'){
      const filter = (state.__dictEtfQ||'').toLowerCase();
      const rows = dd.etf.map((e,i)=>({...e, _i:i})).filter(e =>
        !filter || e.canonical.toLowerCase().includes(filter) || e.aliases.some(a=>a.toLowerCase().includes(filter))
      );
      content = `
        <div class="dict-toolbar">
          <div class="searchbox" style="min-width:260px"><span class="ico">⌕</span><input id="dict-etf-q" placeholder="Buscar canónico o alias…" value="${state.__dictEtfQ||''}"></div>
          <div class="spacer"></div>
          <div class="subtle" style="font-size:11px">${dd.etf.length} canónicos · ${dd.etf.reduce((s,e)=>s+e.aliases.length,0)} aliases totales</div>
          <button class="src-btn" id="dict-etf-add">+ Agregar canónico</button>
        </div>
        <div class="card" style="padding:0 24px 12px">
        <table class="dict-table">
          <thead><tr>
            <th style="width:160px">Canónico</th>
            <th style="width:160px">Bucket</th>
            <th>Aliases</th>
            <th style="width:44px"></th>
          </tr></thead>
          <tbody>
            ${rows.map(e=>`<tr>
              <td><input type="text" class="dict-inp mono" data-dict-etf-canon="${e._i}" value="${e.canonical}"></td>
              <td>
                <select class="dict-inp" data-dict-etf-bucket="${e._i}">
                  ${dd.buckets.map(b=>`<option value="${b.id}" ${b.id===e.bucket?'selected':''}>${b.id}</option>`).join('')}
                </select>
              </td>
              <td>
                <div class="dict-aliases">
                  ${e.aliases.map((a,j)=>`<span class="alias-chip"><span class="mono">${a}</span><button data-dict-alias-del="${e._i}:${j}" aria-label="Eliminar">×</button></span>`).join('')}
                  <input type="text" class="alias-add" data-dict-alias-add="${e._i}" placeholder="+ alias (enter)">
                </div>
              </td>
              <td><button class="row-del" data-dict-etf-del="${e._i}" aria-label="Eliminar fila">×</button></td>
            </tr>`).join('')}
          </tbody>
        </table>
        </div>
        <div class="subtle" style="font-size:11px;margin-top:8px">Cambios aplicados en memoria — en producción, "Guardar" persistiría el diccionario versionado (§6.2).</div>
      `;
    } else {
      content = `
        <div class="card" style="padding:22px">
          <div class="card-kicker">Categorías canónicas §6.3</div>
          <div class="card-title">Mandatos · <span class="mono">${dd.mand.length}</span> categorías</div>
          <div class="dict-mand-chips">
            ${dd.mand.map((c,i)=>`<span class="alias-chip"><span>${c}</span><button data-dict-mand-del="${i}" aria-label="Eliminar">×</button></span>`).join('')}
            <input type="text" class="alias-add" id="dict-mand-add" placeholder="+ nueva categoría (enter)" style="min-width:220px">
          </div>
          <div class="gap-16 subtle" style="font-size:12px;line-height:1.7">
            <b>Reglas de detección §6.3:</b><br>
            Ignores: <span class="mono">totalportfolio, totalnetmarketvalue, netassets, totalassets, totalmarketvalue</span><br>
            Shortcut: label con <span class="mono">otherinvestment / hedgefund / miscellaneous</span> → <b>other_investments</b><br>
            Override UBS Miami: <span class="mono">contains_any:[emerging] AND contains_all:[fixed,income]</span> → <b>hy_fixed_income</b>
          </div>
        </div>`;
    }

    return `
      ${pageHead('Diccionario', `Buckets §6.1 · ETF §6.2 · Mandatos §6.3 — fuente de verdad de la clasificación`)}
      <div class="dict-header">
        <div class="tabs" style="margin:0">
          ${[['etf','ETF canónicos'],['buckets','Buckets'],['mand','Mandatos']].map(([v,l])=>`<div class="tab ${tabs===v?'active':''}" data-dt="${v}">${l}</div>`).join('')}
        </div>
        <div class="spacer"></div>
        <button class="src-btn" id="dict-upload">⬆ Subir Excel</button>
        <button class="btn-apply" id="dict-save">Guardar cambios</button>
      </div>
      <div class="gap-16">${content}</div>
      ${editState.uploadOpen ? renderDictUploadModal() : ''}
    `;
  }

  function countAliasesForBucket(bucketId){
    const dd = state.__dictData;
    return (dd.etf||[]).filter(e=>e.bucket===bucketId).reduce((s,e)=>s+e.aliases.length,0);
  }

  function renderDictUploadModal(){
    // Mock diff preview de un Excel recién cargado
    return `
      <div class="modal-overlay" data-modal-close>
        <div class="modal">
          <div class="modal-head">
            <div>
              <div class="card-kicker">Subir Excel · Diccionario</div>
              <div class="card-title">diccionario_instrumentos_2026Q1.xlsx</div>
            </div>
            <button class="modal-x" data-modal-close aria-label="Cerrar">×</button>
          </div>
          <div class="modal-body">
            <div class="info-banner" style="margin-bottom:14px">
              <span class="lbl">Preview</span>
              El sistema detectó headers <span class="mono">canonical · bucket · alias_1 · alias_2 · …</span> — parser <span class="mono">excel/instruments_dictionary</span> v1.0
            </div>
            <div class="diff-summary">
              <div class="diff-kpi"><div class="lbl">Nuevos canónicos</div><div class="val ok">+3</div><div class="sub">XLRE · VWCE · USRT</div></div>
              <div class="diff-kpi"><div class="lbl">Aliases agregados</div><div class="val ok">+14</div><div class="sub">a 6 canónicos existentes</div></div>
              <div class="diff-kpi"><div class="lbl">Conflictos</div><div class="val warn">2</div><div class="sub">alias duplicado entre canónicos</div></div>
              <div class="diff-kpi"><div class="lbl">Sin cambios</div><div class="val">41</div><div class="sub">filas ya en el diccionario</div></div>
            </div>
            <div class="diff-table">
              <div class="diff-row head"><div>Acción</div><div>Canónico</div><div>Bucket</div><div>Detalle</div></div>
              <div class="diff-row new"><div><span class="pill ok">Nuevo</span></div><div class="mono">XLRE</div><div>RV DM</div><div class="subtle">SPDR REAL ESTATE SELECT SECTOR · 3 aliases</div></div>
              <div class="diff-row new"><div><span class="pill ok">Nuevo</span></div><div class="mono">VWCE</div><div>RV DM</div><div class="subtle">VANGUARD FTSE ALL-WORLD UCITS ACC · 2 aliases</div></div>
              <div class="diff-row new"><div><span class="pill ok">Nuevo</span></div><div class="mono">USRT</div><div>RV DM</div><div class="subtle">ISHARES CORE US REIT · 1 alias</div></div>
              <div class="diff-row upd"><div><span class="pill neutral">Alias</span></div><div class="mono">VDPA</div><div>RF IG Long</div><div class="subtle">+4 aliases nuevos (VANGUARD USD CORP VARIANTES)</div></div>
              <div class="diff-row warn"><div><span class="pill part">Conflicto</span></div><div class="mono">IWDA vs IEMA</div><div>RV DM / RV EM</div><div class="subtle">Alias "MSCI INDEX FUND" ambiguo — requiere resolución manual</div></div>
            </div>
          </div>
          <div class="modal-foot">
            <button class="src-btn" data-modal-close>Cancelar</button>
            <button class="btn-apply" id="dict-upload-apply">Aplicar 17 cambios</button>
          </div>
        </div>
      </div>
    `;
  }

  function bindDiccionario(){
    const dd = state.__dictData;
    if (!dd) return;

    // Upload modal
    const upBtn = document.getElementById('dict-upload');
    if (upBtn) upBtn.addEventListener('click', () => { state.__dictEdit = {...(state.__dictEdit||{}), uploadOpen:true}; renderRoute(); });
    document.querySelectorAll('[data-modal-close]').forEach(el => el.addEventListener('click', (e) => {
      if (e.target !== el && !e.target.hasAttribute('data-modal-close')) return;
      state.__dictEdit = {...(state.__dictEdit||{}), uploadOpen:false}; renderRoute();
    }));
    const upApply = document.getElementById('dict-upload-apply');
    if (upApply) upApply.addEventListener('click', () => {
      state.__dictEdit = {...(state.__dictEdit||{}), uploadOpen:false};
      state.__dictToast = '17 cambios aplicados al diccionario';
      renderRoute();
    });

    const saveBtn = document.getElementById('dict-save');
    if (saveBtn) saveBtn.addEventListener('click', () => {
      state.__dictToast = 'Diccionario guardado (versión local)';
      renderRoute();
    });

    // Buckets tab
    document.querySelectorAll('[data-dict-bucket]').forEach(el => el.addEventListener('change', e => {
      const i = +el.dataset.dictBucket;
      dd.buckets[i].color = e.target.value;
      renderRoute();
    }));
    document.querySelectorAll('[data-dict-order]').forEach(el => el.addEventListener('change', e => {
      const i = +el.dataset.dictOrder;
      dd.buckets[i].order = +e.target.value;
    }));

    // ETF tab
    const q = document.getElementById('dict-etf-q');
    if (q) q.addEventListener('input', e => { state.__dictEtfQ = e.target.value; renderRoute(); });
    const addEtf = document.getElementById('dict-etf-add');
    if (addEtf) addEtf.addEventListener('click', () => {
      dd.etf.unshift({canonical:'NUEVO', bucket:'RV DM', aliases:[]});
      renderRoute();
    });
    document.querySelectorAll('[data-dict-etf-canon]').forEach(el => el.addEventListener('change', e => {
      dd.etf[+el.dataset.dictEtfCanon].canonical = e.target.value;
    }));
    document.querySelectorAll('[data-dict-etf-bucket]').forEach(el => el.addEventListener('change', e => {
      dd.etf[+el.dataset.dictEtfBucket].bucket = e.target.value;
      renderRoute();
    }));
    document.querySelectorAll('[data-dict-etf-del]').forEach(el => el.addEventListener('click', () => {
      dd.etf.splice(+el.dataset.dictEtfDel, 1);
      renderRoute();
    }));
    document.querySelectorAll('[data-dict-alias-del]').forEach(el => el.addEventListener('click', () => {
      const [i,j] = el.dataset.dictAliasDel.split(':').map(Number);
      dd.etf[i].aliases.splice(j,1);
      renderRoute();
    }));
    document.querySelectorAll('[data-dict-alias-add]').forEach(el => el.addEventListener('keydown', e => {
      if (e.key !== 'Enter' || !e.target.value.trim()) return;
      dd.etf[+el.dataset.dictAliasAdd].aliases.push(e.target.value.trim());
      renderRoute();
    }));

    // Mand tab
    document.querySelectorAll('[data-dict-mand-del]').forEach(el => el.addEventListener('click', () => {
      dd.mand.splice(+el.dataset.dictMandDel,1);
      renderRoute();
    }));
    const mandAdd = document.getElementById('dict-mand-add');
    if (mandAdd) mandAdd.addEventListener('keydown', e => {
      if (e.key !== 'Enter' || !e.target.value.trim()) return;
      dd.mand.push(e.target.value.trim());
      renderRoute();
    });
  }

  // ---- AUDITORÍA ----
  function pageAuditoria(){
    const tab = state.__auditTab || 'log';
    const rows = D.audit.map(e=>`<tr>
      <td class="date">${e.ts}</td>
      <td class="mono">${e.user}</td>
      <td><span class="pill neutral">${e.event}</span></td>
      <td class="mono">${e.obj}</td>
      <td class="subtle">${e.detail}</td>
    </tr>`).join('');

    const logView = `
      <div class="card" style="padding:0 24px 12px">
        <table>
          <thead><tr><th>Timestamp</th><th>Usuario</th><th>Evento</th><th>Objeto</th><th>Detalle</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;

    return `
      ${pageHead('Auditoría', `Log de eventos · parsers · reglas · correcciones · auditor externo. Timestamps UTC (§8).`)}
      <div class="tabs" style="margin-bottom:14px">
        <div class="tab ${tab==='log'?'active':''}" data-at="log">Log interno</div>
        <div class="tab ${tab==='indep'?'active':''}" data-at="indep">Auditorías independientes <span class="tab-badge">NEW</span></div>
      </div>
      ${tab==='log' ? logView : renderAuditIndep()}
    `;
  }

  // Estado inicial del auditor independiente
  function initAuditState(){
    if (state.__audit) return state.__audit;
    state.__audit = {
      configOpen:false,
      runningRun:null,  // {id, progress, total, currentCartola}
      runs:[
        {id:'AI-2026-04-01', ts:'2026-04-02 06:10 UTC', status:'passed',  sampled:12, findings:0, period:'2026-03', agent:'ecoterra-auditor@0.3.1', hash:'a1b2…c9d0', kind:'scheduled'},
        {id:'AI-2026-03-01', ts:'2026-03-02 06:08 UTC', status:'minor',   sampled:10, findings:2, period:'2026-02', agent:'ecoterra-auditor@0.3.1', hash:'77ae…41f2', kind:'scheduled'},
        {id:'AI-2026-02-01', ts:'2026-02-03 06:11 UTC', status:'passed',  sampled:12, findings:0, period:'2026-01', agent:'ecoterra-auditor@0.3.0', hash:'5e33…9b01', kind:'scheduled'},
        {id:'AI-2026-01-01', ts:'2026-01-05 06:15 UTC', status:'major',   sampled:14, findings:1, period:'2025-12', agent:'ecoterra-auditor@0.2.9', hash:'0cd4…a77e', kind:'scheduled'},
      ],
      findings:[
        {run:'AI-2026-03-01', sev:'minor', acct:'UBS-206-560552-01', field:'cash_value',    cartola:'UBS_SW_206-560552_202602.pdf', detail:'Liquidity tok anómalo §4.2 — sugerencia: activar extract_tables() en p.4', status:'open'},
        {run:'AI-2026-03-01', sev:'minor', acct:'JPM-0007',          field:'profit_period', cartola:'JPM_0007_etf_202602.pdf',      detail:'Accrual delta 0.8bp bajo threshold — verificar fórmula §5.1 JPM ETF', status:'acknowledged'},
        {run:'AI-2026-01-01', sev:'major', acct:'BICE-038',          field:'movements_net', cartola:'BICE_038_202512.pdf',          detail:'Aporte detectado via Compras/Aportes(D) en lugar de DETALLE DE MOVIMIENTOS (regla BICE §4.1)', status:'fixed'},
      ],
      samples:[
        {run:'AI-2026-04-01', cartola:'JPM_9001_brokerage_202603.pdf',     acct:'JPM-9001',              fields:6, match:'MATCHED',     diff:'0.00'},
        {run:'AI-2026-04-01', cartola:'UBS_SW_206-560552_202603.pdf',      acct:'UBS-206-560552-02',     fields:6, match:'MATCHED',     diff:'0.00'},
        {run:'AI-2026-04-01', cartola:'BBH_4412_custody_202603.pdf',       acct:'BBH-4412',              fields:6, match:'MATCHED',     diff:'0.00'},
        {run:'AI-2026-04-01', cartola:'GS_9912_etf_202603.pdf',            acct:'GS-9912',               fields:6, match:'MINOR_DIFF',  diff:'0.008%'},
        {run:'AI-2026-04-01', cartola:'WELL_576371_custody_202603.pdf',    acct:'WELL-576371',           fields:6, match:'MATCHED',     diff:'0.00'},
        {run:'AI-2026-04-01', cartola:'ALTOS_C0000-0893_202603.pdf',       acct:'ALTOS-C0000-0893',      fields:6, match:'MATCHED',     diff:'0.00'},
      ],
      // config formulario
      cfg:{ period:'2026-03', sampleSize:12, stratified:true, custodianFilter:[], verifyFields:['ending_value_with_accrual','ending_value_without_accrual','accrual_ending','cash_value','movements_net','profit_period'] },
    };
    return state.__audit;
  }

  function renderAuditIndep(){
    const A = initAuditState();
    const runs = A.runs;
    const findings = A.findings;
    // samples de la corrida más reciente
    const latestRun = runs[0];
    const samples = A.samples.filter(s=>s.run===latestRun?.id);
    const sevPill = s => s==='major' ? 'sev-crit' : (s==='minor' ? 'sev-warn' : 'sev-info');
    const statusPill = s => ({passed:'ok', minor:'part', major:'err'})[s] || 'neutral';
    const matchPill = m => m==='MATCHED' ? 'ok' : (m==='MINOR_DIFF' ? 'part' : 'err');

    return `
      <div class="info-banner" style="margin-bottom:14px">
        <span class="lbl">Placeholder</span>
        <span class="txt">Esta sección expondrá el <b>agente auditor externo</b> — corre <b>aislado</b> de la app, toma una muestra aleatoria de cartolas por período y verifica que los valores exhibidos (ending_value, accrual, movements_net, profit_period) coincidan con la fuente. No escribe en la DB de producción; sólo emite <span class="mono">audit_run</span> + <span class="mono">findings[]</span>.</span>
      </div>

      <div class="row-3">
        ${miniKPI('Corridas totales', runs.length, 'Cadencia mensual programada')}
        ${miniKPI('Cartolas revisadas', runs.reduce((s,r)=>s+r.sampled,0), 'Muestra aleatoria acumulada')}
        ${miniKPI('Hallazgos abiertos', findings.filter(f=>f.status==='open').length, `${findings.filter(f=>f.status==='major').length} mayor · ${findings.filter(f=>f.status==='minor').length} menor`)}
      </div>

      <div class="card gap-16">
        <div class="toolbar" style="padding:0;margin-bottom:10px">
          <div>
            <div class="card-kicker">Historial</div>
            <div class="card-title" style="font-size:18px">Corridas del auditor independiente</div>
          </div>
          <div class="spacer"></div>
          <button class="src-btn" id="audit-schedule" title="Programar próxima corrida · pendiente backend">Programar</button>
          <button class="btn-apply" id="audit-run" ${A.runningRun?'disabled':''}>${A.runningRun?'Ejecutando…':'▶ Correr auditoría'}</button>
        </div>
        ${A.runningRun ? renderAuditRunningStrip(A.runningRun) : ''}
        <table>
          <thead><tr>
            <th>Run ID</th><th>Timestamp</th><th>Período auditado</th><th>Muestra</th>
            <th>Estado</th><th>Hallazgos</th><th>Agente</th><th>Hash resultado</th>
          </tr></thead>
          <tbody>
            ${runs.map(r=>`<tr ${r.id===latestRun?.id && r.kind==='on-demand'?'class="audit-row-new"':''}>
              <td class="mono">${r.id} ${r.kind==='on-demand'?'<span class="pill neutral" style="margin-left:4px">on-demand</span>':''}</td>
              <td class="date">${r.ts}</td>
              <td class="mono">${r.period}</td>
              <td class="mono">${r.sampled} cartolas</td>
              <td><span class="pill ${statusPill(r.status)}">${r.status}</span></td>
              <td class="mono">${r.findings}</td>
              <td class="mono subtle">${r.agent}</td>
              <td class="mono subtle">${r.hash}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>

      <div class="row">
        <div class="card">
          <div class="card-kicker">Última corrida · ${latestRun?.id||'—'}</div>
          <div class="card-title" style="font-size:18px">Muestra aleatoria de cartolas revisadas</div>
          <div class="subtle" style="font-size:11px;margin-bottom:10px">Selección ${A.cfg.stratified?'estratificada por custodio':'uniforme'} · seed determinista por run_id · ${samples.length} cartolas</div>
          <table>
            <thead><tr><th>Cartola</th><th>Cuenta</th><th>Campos</th><th>Reconciliación</th><th>Δ</th></tr></thead>
            <tbody>
              ${samples.map(s=>`<tr>
                <td class="mono">${s.cartola}</td>
                <td class="mono">${s.acct}</td>
                <td class="mono">${s.fields}/6</td>
                <td><span class="pill ${matchPill(s.match)}">${s.match}</span></td>
                <td class="mono">${s.diff}</td>
              </tr>`).join('')}
            </tbody>
          </table>
        </div>
        <div class="card">
          <div class="card-kicker">Hallazgos acumulados</div>
          <div class="card-title" style="font-size:18px">${findings.length} registros · últimos 3 meses</div>
          <div class="gap-16"></div>
          ${findings.map(f=>`
            <div class="audit-finding">
              <div class="audit-finding-head">
                <span class="pill ${sevPill(f.sev)}">${f.sev}</span>
                <span class="mono subtle" style="font-size:11px">${f.run}</span>
                <span class="spacer"></span>
                <span class="pill ${f.status==='fixed'?'ok':(f.status==='acknowledged'?'neutral':'part')}">${f.status}</span>
              </div>
              <div class="audit-finding-body">
                <div><span class="mono">${f.acct}</span> · campo <span class="mono">${f.field}</span></div>
                <div class="subtle" style="font-size:11.5px;line-height:1.55;margin-top:3px">${f.detail}</div>
                <div class="mono subtle" style="font-size:11px;margin-top:4px">${f.cartola}</div>
              </div>
            </div>
          `).join('')}
        </div>
      </div>

      <div class="card gap-16">
        <div class="card-kicker">Contrato del agente</div>
        <div class="card-title" style="font-size:18px">Aislamiento e invariantes</div>
        <div class="gap-16"></div>
        <div class="kv"><span class="k">Entorno</span><span class="v mono">container dedicado · sin acceso a escritura</span></div>
        <div class="kv"><span class="k">Entrada</span><span class="v mono">GET /api/v1/audit/sample?period=&n=</span></div>
        <div class="kv"><span class="k">Salida</span><span class="v mono">POST /api/v1/audit/runs · { run_id, findings[] }</span></div>
        <div class="kv"><span class="k">Regla de muestreo</span><span class="v">Estratificada por custodio · seed = sha256(run_id) · mínimo 1 por custodio activo</span></div>
        <div class="kv"><span class="k">Verifica</span><span class="v mono">ending_value_with_accrual · ending_value_without_accrual · accrual_ending · cash_value · movements_net · profit_period</span></div>
        <div class="kv"><span class="k">Tolerancias</span><span class="v">MATCHED = 0 · MINOR = ≤ 0.01% · MAJOR = &gt; 0.01% (§5.4)</span></div>
        <div class="kv"><span class="k">No hace</span><span class="v subtle">reescribir normalized · aplicar correcciones · mutar audit_log (solo append en <span class="mono">audit_runs</span>)</span></div>
      </div>

      ${A.configOpen ? renderAuditConfigModal() : ''}
    `;
  }

  function renderAuditRunningStrip(run){
    const pct = Math.round((run.progress/run.total)*100);
    return `
      <div class="audit-run-strip">
        <div class="audit-run-strip-head">
          <span class="mono" style="font-weight:600">${run.id}</span>
          <span class="subtle" style="font-size:11px">Ejecutando auditoría independiente · muestra ${run.total} cartolas · período ${run.period}</span>
          <span class="spacer"></span>
          <span class="mono" style="font-size:12px">${run.progress}/${run.total} · ${pct}%</span>
        </div>
        <div class="audit-run-progress"><div class="bar" style="width:${pct}%"></div></div>
        <div class="subtle mono" style="font-size:11px;margin-top:6px">${run.currentCartola || 'Inicializando container aislado…'}</div>
      </div>
    `;
  }

  function renderAuditConfigModal(){
    const A = state.__audit;
    const c = A.cfg;
    const custodians = [...new Set(D.accounts.map(a=>a.bank))].map(code=>{
      const b = D.getBank(code); return {code, name:b?.short||code};
    });
    const activeAccts = D.accounts.length;
    // Máximo posible = una cartola por cuenta del período
    const maxSample = activeAccts;
    const periods = [];
    for (let i=0; i<12; i++){
      const d = new Date(2026, 2-i, 1); // partiendo de Mar 2026 hacia atrás
      const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0');
      periods.push(`${y}-${m}`);
    }
    const fieldLabels = {
      ending_value_with_accrual:'ending_value_with_accrual',
      ending_value_without_accrual:'ending_value_without_accrual',
      accrual_ending:'accrual_ending',
      cash_value:'cash_value',
      movements_net:'movements_net',
      profit_period:'profit_period',
    };
    return `
      <div class="modal-overlay" data-audit-close>
        <div class="modal">
          <div class="modal-head">
            <div>
              <div class="card-kicker">Auditoría independiente</div>
              <div class="card-title">Configurar corrida on-demand</div>
            </div>
            <button class="modal-x" data-audit-close aria-label="Cerrar">×</button>
          </div>
          <div class="modal-body">
            <div class="info-banner" style="margin-bottom:16px">
              <span class="lbl">Aislamiento</span>
              <span class="txt">El agente corre en un <b>container dedicado sin acceso de escritura</b>. Lee una muestra aleatoria de cartolas, verifica valores contra la capa normalizada y emite <span class="mono">audit_run</span> + <span class="mono">findings[]</span>. No escribe en producción.</span>
            </div>

            <div class="audit-cfg-grid">
              <div class="audit-cfg-field">
                <label class="audit-cfg-label">Período a auditar</label>
                <select class="dict-inp" id="audit-cfg-period">
                  ${periods.map(p=>`<option value="${p}" ${p===c.period?'selected':''}>${p}</option>`).join('')}
                </select>
                <div class="audit-cfg-hint">Mes cerrado · usa cartolas ya parseadas</div>
              </div>

              <div class="audit-cfg-field">
                <label class="audit-cfg-label">Tamaño de muestra · <span class="mono" id="audit-cfg-nval">${c.sampleSize}</span> cartolas</label>
                <input type="range" id="audit-cfg-n" min="1" max="${maxSample}" step="1" value="${c.sampleSize}" class="audit-slider">
                <div class="audit-cfg-hint">
                  <span>1</span>
                  <span class="subtle">${activeAccts} cuentas activas · máx ${maxSample}</span>
                  <span>${maxSample}</span>
                </div>
                <div class="audit-cfg-presets">
                  ${[3,6,12,activeAccts].map(n=>`<button type="button" class="audit-preset ${n===c.sampleSize?'active':''}" data-audit-preset="${n}">${n===activeAccts?'Todas':n}</button>`).join('')}
                </div>
              </div>

              <div class="audit-cfg-field">
                <label class="audit-cfg-label">Estrategia de muestreo</label>
                <div class="audit-radio-row">
                  <label class="audit-radio ${c.stratified?'active':''}"><input type="radio" name="audit-strat" value="strat" ${c.stratified?'checked':''}><span><b>Estratificada</b><em>Mínimo 1 por custodio activo</em></span></label>
                  <label class="audit-radio ${!c.stratified?'active':''}"><input type="radio" name="audit-strat" value="uniform" ${!c.stratified?'checked':''}><span><b>Uniforme</b><em>Aleatoria sobre todas las cartolas</em></span></label>
                </div>
              </div>

              <div class="audit-cfg-field">
                <label class="audit-cfg-label">Custodios incluidos <span class="subtle" style="font-weight:400">(vacío = todos)</span></label>
                <div class="audit-chips">
                  ${custodians.map(b=>`<label class="audit-chip ${c.custodianFilter.includes(b.code)?'active':''}"><input type="checkbox" data-audit-cust="${b.code}" ${c.custodianFilter.includes(b.code)?'checked':''}><span>${b.name}</span></label>`).join('')}
                </div>
              </div>

              <div class="audit-cfg-field audit-cfg-field-wide">
                <label class="audit-cfg-label">Campos a verificar <span class="subtle" style="font-weight:400">§1 capa normalizada</span></label>
                <div class="audit-chips">
                  ${Object.keys(fieldLabels).map(f=>`<label class="audit-chip ${c.verifyFields.includes(f)?'active':''}"><input type="checkbox" data-audit-field="${f}" ${c.verifyFields.includes(f)?'checked':''}><span class="mono">${fieldLabels[f]}</span></label>`).join('')}
                </div>
              </div>
            </div>

            <div class="audit-cfg-summary">
              <div><span class="subtle">Se ejecutará</span> <span class="mono"><b id="audit-sum-n">${c.sampleSize}</b></span> <span class="subtle">verificaciones sobre período</span> <span class="mono" id="audit-sum-p">${c.period}</span> <span class="subtle">· estrategia</span> <b id="audit-sum-s">${c.stratified?'estratificada':'uniforme'}</b></div>
              <div class="subtle" style="font-size:11px;margin-top:4px">ETA aprox <span class="mono">${Math.max(1,Math.round(c.sampleSize*0.25))}s</span> · tolerancias §5.4 (MATCHED=0 · MINOR≤0.01% · MAJOR&gt;0.01%)</div>
            </div>
          </div>
          <div class="modal-foot">
            <button class="src-btn" data-audit-close>Cancelar</button>
            <button class="btn-apply" id="audit-cfg-go">▶ Ejecutar ahora</button>
          </div>
        </div>
      </div>
    `;
  }

  function miniKPI(label, val, sub){
    return `<div class="card" style="padding:18px 20px">
      <div class="card-kicker">${label}</div>
      <div class="card-title" style="font-family:'Instrument Serif',serif;font-size:36px;line-height:1.1;margin-top:4px">${val}</div>
      ${sub?`<div class="subtle" style="font-size:11px;margin-top:4px">${sub}</div>`:''}
    </div>`;
  }

  function bindAuditoria(){
    if (state.route !== '/auditoria') return;
    document.querySelectorAll('.tab[data-at]').forEach(t=> t.addEventListener('click', ()=>{
      state.__auditTab = t.dataset.at; renderRoute();
    }));

    const A = state.__audit; if (!A) return;

    // Abrir modal de configuración
    const runBtn = document.getElementById('audit-run');
    if (runBtn) runBtn.addEventListener('click', ()=>{
      if (A.runningRun) return;
      A.configOpen = true;
      renderRoute();
    });

    // Cerrar modal
    document.querySelectorAll('[data-audit-close]').forEach(el=> el.addEventListener('click', e=>{
      if (e.target !== el && !e.target.hasAttribute('data-audit-close')) return;
      A.configOpen = false; renderRoute();
    }));

    // Controles del formulario
    const nInput = document.getElementById('audit-cfg-n');
    const nVal = document.getElementById('audit-cfg-nval');
    const sumN = document.getElementById('audit-sum-n');
    if (nInput) nInput.addEventListener('input', e=>{
      A.cfg.sampleSize = +e.target.value;
      if (nVal) nVal.textContent = A.cfg.sampleSize;
      if (sumN) sumN.textContent = A.cfg.sampleSize;
      // actualiza presets
      document.querySelectorAll('[data-audit-preset]').forEach(p=> p.classList.toggle('active', +p.dataset.auditPreset===A.cfg.sampleSize));
    });
    document.querySelectorAll('[data-audit-preset]').forEach(p=> p.addEventListener('click', ()=>{
      A.cfg.sampleSize = +p.dataset.auditPreset;
      if (nInput) nInput.value = A.cfg.sampleSize;
      if (nVal) nVal.textContent = A.cfg.sampleSize;
      if (sumN) sumN.textContent = A.cfg.sampleSize;
      document.querySelectorAll('[data-audit-preset]').forEach(x=> x.classList.toggle('active', x===p));
    }));

    const periodSel = document.getElementById('audit-cfg-period');
    const sumP = document.getElementById('audit-sum-p');
    if (periodSel) periodSel.addEventListener('change', e=>{
      A.cfg.period = e.target.value;
      if (sumP) sumP.textContent = A.cfg.period;
    });

    const sumS = document.getElementById('audit-sum-s');
    document.querySelectorAll('input[name="audit-strat"]').forEach(r=> r.addEventListener('change', e=>{
      A.cfg.stratified = (e.target.value==='strat');
      if (sumS) sumS.textContent = A.cfg.stratified?'estratificada':'uniforme';
      document.querySelectorAll('.audit-radio').forEach(lbl=>{
        const input = lbl.querySelector('input');
        lbl.classList.toggle('active', input && input.checked);
      });
    }));

    document.querySelectorAll('[data-audit-cust]').forEach(cb=> cb.addEventListener('change', e=>{
      const code = cb.dataset.auditCust;
      if (e.target.checked){ if(!A.cfg.custodianFilter.includes(code)) A.cfg.custodianFilter.push(code); }
      else A.cfg.custodianFilter = A.cfg.custodianFilter.filter(x=>x!==code);
      cb.closest('.audit-chip')?.classList.toggle('active', e.target.checked);
    }));
    document.querySelectorAll('[data-audit-field]').forEach(cb=> cb.addEventListener('change', e=>{
      const f = cb.dataset.auditField;
      if (e.target.checked){ if(!A.cfg.verifyFields.includes(f)) A.cfg.verifyFields.push(f); }
      else A.cfg.verifyFields = A.cfg.verifyFields.filter(x=>x!==f);
      cb.closest('.audit-chip')?.classList.toggle('active', e.target.checked);
    }));

    // Ejecutar corrida
    const goBtn = document.getElementById('audit-cfg-go');
    if (goBtn) goBtn.addEventListener('click', ()=>{
      if (A.cfg.verifyFields.length===0){ alert('Selecciona al menos un campo a verificar.'); return; }
      const total = Math.max(1, Math.min(A.cfg.sampleSize, D.accounts.length));
      const now = new Date();
      const ts = now.toISOString().replace('T',' ').slice(0,16)+' UTC';
      const id = 'AI-'+now.toISOString().slice(0,10)+'-'+Math.floor(Math.random()*900+100);
      A.runningRun = { id, progress:0, total, period:A.cfg.period, currentCartola:null };
      A.configOpen = false;
      renderRoute();
      runAuditSimulation(id);
    });

    // "Programar" — placeholder sigue aquí, pero informativo ahora
    const schBtn = document.getElementById('audit-schedule');
    if (schBtn) schBtn.addEventListener('click', ()=>{
      alert('Programación recurrente — pendiente de backend.\n\nEndpoint: POST /api/v1/audit/schedule\n\nHoy puedes correr on-demand desde el botón "▶ Correr auditoría".');
    });
  }

  // Simulación de corrida del agente auditor (frontend-only)
  function runAuditSimulation(runId){
    const A = state.__audit;
    if (!A || !A.runningRun || A.runningRun.id !== runId) return;

    // Generar muestra determinista a partir de las cuentas
    const allAccts = D.accounts.slice();
    const custFilter = A.cfg.custodianFilter;
    const pool = custFilter.length ? allAccts.filter(a=>custFilter.includes(a.bank)) : allAccts;
    let chosen = [];
    if (A.cfg.stratified){
      // agrupar por banco
      const byBank = {};
      pool.forEach(a=>{ (byBank[a.bank] = byBank[a.bank]||[]).push(a); });
      // 1 por banco mínimo
      Object.values(byBank).forEach(arr=>{
        if (chosen.length < A.runningRun.total) chosen.push(arr[Math.floor(Math.random()*arr.length)]);
      });
      // completar
      while (chosen.length < A.runningRun.total){
        const c = pool[Math.floor(Math.random()*pool.length)];
        if (!chosen.includes(c)) chosen.push(c);
        if (chosen.length >= pool.length) break;
      }
    } else {
      // sample without replacement
      const shuffled = pool.slice().sort(()=>Math.random()-0.5);
      chosen = shuffled.slice(0, A.runningRun.total);
    }

    const period = A.cfg.period.replace('-','');
    const sampleRows = chosen.map(a=>{
      const b = D.getBank(a.bank);
      const shortBank = (b?.short||a.bank).replace(/\s+/g,'_').toUpperCase();
      return {
        run:runId,
        cartola: `${shortBank}_${a.number}_${period}.pdf`,
        acct: a.number,
        fields: A.cfg.verifyFields.length,
        match:'MATCHED', diff:'0.00',
      };
    });

    // Procesar una por una con timeout
    let idx = 0;
    const tick = ()=>{
      if (!A.runningRun || A.runningRun.id !== runId) return; // cancelado
      if (idx >= sampleRows.length){
        finishAuditRun(runId, sampleRows);
        return;
      }
      const row = sampleRows[idx];
      // Introducir algún hallazgo con probabilidad baja (~8%)
      if (Math.random() < 0.08){
        const sev = Math.random() < 0.25 ? 'major' : 'minor';
        row.match = sev==='major' ? 'MAJOR_DIFF' : 'MINOR_DIFF';
        row.diff = sev==='major' ? (0.02 + Math.random()*0.05).toFixed(3)+'%' : (0.001 + Math.random()*0.009).toFixed(3)+'%';
        A.findings.unshift({
          run:runId, sev, acct:row.acct, field:A.cfg.verifyFields[Math.floor(Math.random()*A.cfg.verifyFields.length)],
          cartola:row.cartola,
          detail: sev==='major'
            ? 'Diferencia excede tolerancia MAJOR (§5.4) — revisar parser y conciliación con cartola fuente'
            : 'Delta sub-basis-point dentro de tolerancia MINOR — sugerencia: verificar accrual',
          status:'open',
        });
      }
      A.runningRun.progress = idx+1;
      A.runningRun.currentCartola = `Verificando ${row.cartola} · ${row.acct}`;
      // Append sample incrementalmente
      A.samples.unshift(row);
      renderRoute();
      idx++;
      setTimeout(tick, 200 + Math.random()*180);
    };
    // pequeño delay inicial para mostrar el strip
    setTimeout(tick, 350);
  }

  function finishAuditRun(runId, sampleRows){
    const A = state.__audit;
    if (!A || !A.runningRun || A.runningRun.id !== runId) return;
    const findings = A.findings.filter(f=>f.run===runId);
    const hasMajor = findings.some(f=>f.sev==='major');
    const hasMinor = findings.some(f=>f.sev==='minor');
    const status = hasMajor ? 'major' : (hasMinor ? 'minor' : 'passed');
    const now = new Date();
    const ts = now.toISOString().replace('T',' ').slice(0,16)+' UTC';
    const hash = Array.from({length:4},()=>Math.floor(Math.random()*16).toString(16)).join('')+'…'+Array.from({length:4},()=>Math.floor(Math.random()*16).toString(16)).join('');
    A.runs.unshift({
      id:runId, ts, status,
      sampled: sampleRows.length,
      findings: findings.length,
      period: A.cfg.period,
      agent: 'ecoterra-auditor@0.3.1',
      hash, kind:'on-demand',
    });
    A.runningRun = null;
    renderRoute();
  }

  // ---- CONFIG ----
  function pageConfig(){
    return `
      ${pageHead('Configuración', `Settings globales · CORS · caché · endpoints backend`)}
      <div class="row">
        <div class="card">
          <div class="card-kicker">Backend</div>
          <div class="card-title">Conexión API</div>
          <div class="gap-16"></div>
          <div class="kv"><span class="k">Base URL</span><span class="v">http://localhost:8000/api/v1</span></div>
          <div class="kv"><span class="k">Modo datos</span><span class="v">MOCK</span></div>
          <div class="kv"><span class="k">Capa lectura</span><span class="v">monthly_metrics_normalized</span></div>
          <div class="kv"><span class="k">Fallback</span><span class="v">monthly_closings</span></div>
          <div class="kv"><span class="k">Caché</span><span class="v">invalidate on upload</span></div>
          <div class="gap-16">
            <button class="btn-apply">Probar conexión</button>
          </div>
        </div>
        <div class="card">
          <div class="card-kicker">UI</div>
          <div class="card-title">Preferencias</div>
          <div class="gap-16"></div>
          <div class="kv"><span class="k">Tema</span><span class="v">${state.theme}</span></div>
          <div class="kv"><span class="k">Moneda por defecto</span><span class="v">${state.currency}</span></div>
          <div class="kv"><span class="k">Idioma</span><span class="v">es-CL</span></div>
          <div class="kv"><span class="k">Zona horaria</span><span class="v">America/Santiago</span></div>
        </div>
      </div>

      <div class="card gap-16">
        <div class="card-kicker">Parsers registrados · §3</div>
        <div class="card-title">${D.parsers.length} parsers</div>
        <div class="gap-16"></div>
        <table>
          <thead><tr><th>Parser</th><th>Banco</th><th>Account type</th><th>Versión</th></tr></thead>
          <tbody>
            ${D.parsers.map(p=>`<tr>
              <td class="mono">${p.name}</td>
              <td>${D.getBank(p.bank)?.name}</td>
              <td>${p.account_type}</td>
              <td class="mono">${p.version}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>
    `;
  }

  // ---- Utilities ----
  function pageHead(title, sub){
    return `<div class="page-head"><div class="page-kicker">Ecoterra</div><div class="page-title">${title}</div>${sub?`<div class="page-sub">${sub}</div>`:''}</div>`;
  }

  // ============================================================
  // ROUTER
  // ============================================================
  const ROUTES = {
    '/dashboard':      pageDashboard,
    '/posiciones':     pagePosiciones,
    '/movimientos':    pageMovimientos,
    '/rentabilidades': pageRentabilidades,
    '/alternativos':   pageAlternativos,
    '/normalizada':    pageNormalizada,
    '/correcciones':   pageCorrecciones,
    '/alertas':        pageAlertas,
    '/importar':       pageImportar,
    '/archivos':       pageArchivos,
    '/maestro':        pageMaestro,
    '/diccionario':    pageDiccionario,
    '/auditoria':      pageAuditoria,
    '/config':         pageConfig,
  };

  function renderRoute(){
    state.route = location.hash.slice(1) || '/dashboard';
    renderSidebar();
    const fn = ROUTES[state.route] || pageDashboard;
    try {
      $('#view').innerHTML = fn();
    } catch (err) {
      console.error('[renderRoute] Error rendering', state.route, err);
      $('#view').innerHTML = `
        <div style="padding:28px;font-family:Geist,system-ui,sans-serif">
          <div style="font:22px/1.2 'Instrument Serif',serif;margin-bottom:6px">Error al renderizar ${state.route}</div>
          <div class="subtle" style="margin-bottom:12px">Esto suele pasar cuando un campo esperado del backend llegó vacío o con un shape distinto. Abre la consola (F12) para el stack completo.</div>
          <pre style="background:var(--card);padding:14px;border:1px solid var(--line);border-radius:4px;overflow:auto;font-size:12px;white-space:pre-wrap">${(err && (err.stack || err.message || String(err))).toString().replace(/</g,'&lt;')}</pre>
          <div style="margin-top:12px;font-size:12px" class="subtle">
            Modo datos: <b>${window.__LIVE_DATA ? 'LIVE ' + (window.__LIVE_PERIOD || '') : 'MOCK'}</b>
            ${window.__LIVE_ERROR ? `· Error de bootstrap: ${window.__LIVE_ERROR}` : ''}
          </div>
        </div>
      `;
    }
    // Re-render filter bar if page has slot
    if ($('#filterbar')) {
      state.filtersDirty = JSON.parse(JSON.stringify(state.filters));
      renderFilterBar();
    }
    // Dashboard: segmented currency
    const seg = $('#curSeg');
    if (seg) seg.addEventListener('click', e=>{
      const el = e.target.closest('[data-c]'); if (!el) return;
      state.currency = el.dataset.c;
      localStorage.setItem('ecoterra.cur', state.currency);
      renderRoute();
    });
    // Positions: group switcher
    const pg = $('#posGroup');
    if (pg) pg.addEventListener('click', e=>{
      const el = e.target.closest('[data-g]'); if (!el) return;
      state.__posGroup = el.dataset.g;
      renderRoute();
    });
    // Positions: search
    const ps = $('#posSearch');
    if (ps) ps.addEventListener('input', e=>{
      const q = e.target.value.toLowerCase();
      $$('#posBody tr').forEach(tr=>{
        if (tr.classList.contains('grp')) return;
        tr.style.display = tr.textContent.toLowerCase().includes(q) ? '' : 'none';
      });
    });
    // Diccionario: tab switcher
    $$('.tab[data-dt]').forEach(t=>t.addEventListener('click',()=>{
      state.__dictTab = t.dataset.dt; renderRoute();
    }));
    // Correcciones: inline edit + acciones
    $$('#correcciones-tbody .edit-field').forEach(el=>{
      el.addEventListener('blur', ()=>{
        const tr = el.closest('tr');
        const idx = +tr.dataset.cidx;
        const field = el.dataset.field;
        D.corrections[idx][field] = el.textContent.trim();
      });
      el.addEventListener('keydown', e=>{
        if (e.key==='Enter' && !e.shiftKey){ e.preventDefault(); el.blur(); }
        if (e.key==='Escape'){ el.blur(); }
      });
    });
    $$('#correcciones-tbody button[data-act]').forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const tr = btn.closest('tr');
        const idx = +tr.dataset.cidx;
        const act = btn.dataset.act;
        if (act==='save' || act==='apply'){
          D.corrections[idx].status = 'applied';
        } else if (act==='discard'){
          D.corrections.splice(idx,1);
        }
        renderRoute();
      });
    });
    // Importar: bindings
    bindImportar();
    bindArchivos();
    bindDiccionario();
    bindAlternativos();
    bindMaestro();
    bindAuditoria();

    // Toggle USD/CLP dentro del Dashboard (solo cuando scope=BICE).
    // Dispara re-fetch con nueva currency y re-render.
    $$('.bice-ccy-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const ccy = btn.dataset.ccy;
        if (!ccy || state.refetching || (window.__LIVE_BICE_CCY === ccy)) return;
        state.refetching = true;
        btn.textContent = '…';
        try {
          localStorage.setItem('ecoterra.biceCcy', ccy);
          if (window.swapMockWithScope){
            await window.swapMockWithScope('national', ccy);
          }
        } catch (err){
          console.error('[bice-ccy] refetch error:', err);
        }
        state.refetching = false;
        renderRoute();
      });
    });

    // Drawer "Ver fuente" (trazabilidad §1.5 §10.2). Engancha cualquier botón
    // con data-src-doc="<id>" y hace fetch a /api/v1/sources/{id}.
    $$('[data-src-doc]').forEach(btn => {
      btn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        const docId = btn.dataset.srcDoc;
        if (!docId || !window.API || !window.API.source){
          alert('API no disponible en modo MOCK. Refresca con LIVE para ver la fuente.');
          return;
        }
        btn.disabled = true; btn.textContent = '…';
        try {
          const src = await window.API.source(docId);
          renderSourceDrawer(src);
        } catch (err) {
          alert(`No se pudo cargar la fuente:\n${err.message || err}`);
        } finally {
          btn.disabled = false; btn.textContent = '⊕';
        }
      });
    });

    window.scrollTo(0,0);
  }

  function renderSourceDrawer(src){
    // Drawer modal simple (sin dependencias). Cierra con backdrop o Escape.
    const existing = document.getElementById('src-drawer');
    if (existing) existing.remove();
    const drawer = document.createElement('div');
    drawer.id = 'src-drawer';
    drawer.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.45);display:flex;align-items:center;justify-content:flex-end';
    const pv = src.parser;
    const acct = src.account;
    drawer.innerHTML = `
      <div style="background:var(--bg);color:var(--text);width:min(520px,100%);height:100%;padding:20px 22px;overflow:auto;border-left:1px solid var(--line);font-family:Geist,system-ui,sans-serif">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div style="font-family:'Instrument Serif',serif;font-size:22px">Ver fuente</div>
          <button id="src-close" class="src-btn">✕</button>
        </div>
        <div style="font-size:12.5px;line-height:1.7">
          <div><b>Archivo:</b> <span class="mono">${src.filename || '—'}</span></div>
          <div><b>Tipo:</b> ${src.file_type || '—'}</div>
          <div><b>Banco:</b> ${src.bank_code || '—'} · <b>Período:</b> ${src.period_year || '—'}-${String(src.period_month||'').padStart(2,'0')}</div>
          <div><b>Estado:</b> ${src.status || '—'}</div>
          <div><b>Tamaño:</b> ${src.file_size_bytes ? (src.file_size_bytes/1024).toFixed(1)+' KB' : '—'}</div>
          <div style="margin-top:10px"><b>SHA-256:</b> <span class="mono" style="word-break:break-all;font-size:11px">${src.sha256_hash || '—'}</span></div>
          <div><b>Uploaded:</b> ${src.uploaded_at || '—'}</div>
          <div><b>Processed:</b> ${src.processed_at || '—'}</div>
          ${src.error_message ? `<div style="margin-top:8px;color:var(--red)"><b>Error:</b> ${src.error_message}</div>` : ''}
          ${pv ? `
            <hr style="margin:14px 0;border:0;border-top:1px solid var(--line)">
            <div><b>Parser:</b> <span class="mono">${pv.name}</span> v${pv.version}</div>
            <div><b>Source hash parser:</b> <span class="mono" style="word-break:break-all;font-size:11px">${pv.source_hash || '—'}</span></div>
          ` : '<div style="margin-top:10px" class="subtle">Sin parser asociado registrado.</div>'}
          ${acct ? `
            <hr style="margin:14px 0;border:0;border-top:1px solid var(--line)">
            <div><b>Cuenta:</b> <span class="mono">${acct.account_number}</span></div>
            <div><b>Sociedad:</b> ${acct.society || '—'}</div>
            <div><b>Banco:</b> ${acct.bank || '—'} · <b>Tipo:</b> ${acct.type || '—'} · <b>Moneda:</b> ${acct.currency || '—'}</div>
          ` : ''}
          <hr style="margin:14px 0;border:0;border-top:1px solid var(--line)">
          <div class="subtle" style="font-size:11px">Filepath: ${src.filepath || '—'}</div>
        </div>
      </div>
    `;
    document.body.appendChild(drawer);
    const close = () => drawer.remove();
    drawer.addEventListener('click', (e) => { if (e.target === drawer) close(); });
    drawer.querySelector('#src-close').addEventListener('click', close);
    document.addEventListener('keydown', function onEsc(e){
      if (e.key === 'Escape'){ close(); document.removeEventListener('keydown', onEsc); }
    });
  }

  // ============================================================
  // INIT
  // ============================================================
  window.addEventListener('hashchange', renderRoute);
  document.addEventListener('DOMContentLoaded', async () => {
    applyTheme(state.theme);
    $('#themeLight').addEventListener('click', ()=>applyTheme('light'));
    $('#themeDark') .addEventListener('click', ()=>applyTheme('dark'));

    // Espera a que dataSource.js cargue datos reales del backend (si aplica).
    // Si no hay __DATA_READY o falla, cae a MOCK transparentemente.
    try {
      if (window.__DATA_READY) await window.__DATA_READY;
    } catch (e) {
      console.warn('[init] data ready failed, using MOCK:', e);
    }

    renderRoute();

    // Badge de modo de datos (MOCK vs LIVE). Click → muestra detalle.
    const modeEl = document.createElement('div');
    modeEl.style.cssText = 'position:fixed;right:10px;bottom:10px;padding:6px 10px;font:12px/1.2 Geist,system-ui,sans-serif;border-radius:4px;z-index:9999;opacity:.92;cursor:pointer;max-width:360px';
    const base = (window.API && window.API.config && window.API.config.baseUrl) || '(sin API)';
    if (window.__LIVE_DATA){
      modeEl.textContent = `LIVE · ${window.__LIVE_PERIOD || ''}`;
      modeEl.style.cssText += ';background:#1f6f43;color:#fff';
      modeEl.title = `Datos reales desde ${base}. Click para detalles.`;
    } else {
      const short = window.__LIVE_ERROR
        ? String(window.__LIVE_ERROR).split('\n')[0].slice(0, 80)
        : 'sin error reportado';
      modeEl.textContent = `MOCK · ${short}`;
      modeEl.style.cssText += ';background:#b45309;color:#fff';
      modeEl.title = `Intenté ${base} y fallé. Click para ver el error completo.`;
    }
    modeEl.addEventListener('click', () => {
      const msg = window.__LIVE_DATA
        ? `Modo: LIVE\nBase URL: ${base}\nPeríodo: ${window.__LIVE_PERIOD}\nScope: international (default)`
        : `Modo: MOCK\nBase URL: ${base}\nError:\n${window.__LIVE_ERROR || '(sin mensaje)'}\n\nPara usar datos reales, asegúrate de que el backend corra en ${base} y que CORS permita tu origen.`;
      alert(msg);
    });
    document.body.appendChild(modeEl);
  });
})();
