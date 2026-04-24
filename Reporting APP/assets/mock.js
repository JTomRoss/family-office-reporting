/* ============================================================
 * mock.js — Dataset canónico para el prototipo
 * Respeta RULES_INHERITED.md:
 *   §3  — parsers reales (no inventar)
 *   §6  — buckets y colores canónicos
 *   §9  — maestro de cuentas real
 *   §5  — fórmulas (YTD chain-linking, TWR mensual)
 *   §10 — la UI solo lee de la capa normalizada
 *
 * Todo número proviene de:
 *   - `monthly_metrics_normalized` simulada (campos §1)
 *   - `positions`, `movements`, `alerts`, `files` simulados
 * ============================================================ */
(function(){
  const now = new Date('2026-03-31T00:00:00Z');

  // --- Sociedades (§9) ---
  const societies = [
    {id:'BVL', name:'Boatview Limited',             jur:'IVI',  currency:'USD'},
    {id:'ECI', name:'Ecoterra Internacional SpA',   jur:'CHL',  currency:'USD'},
    {id:'ILR', name:'Inversiones Las Raíces SCC',   jur:'CHL',  currency:'CLP'},
    {id:'TEL', name:'Telmar Investments',           jur:'IVI',  currency:'USD'},
    {id:'ECS', name:'Ecoterra SpA',                 jur:'CHL',  currency:'CLP'},
  ];

  // --- Custodios (§3) ---
  const banks = [
    {code:'bice_inversiones',      name:'BICE Inversiones',     short:'BICE',     country:'CL'},
    {code:'bice_asesorias',        name:'BICE Asesorías · Altos Patrimonios', short:'Altos', country:'CL'},
    {code:'jpmorgan',              name:'J.P. Morgan NY',       short:'JPM NY',   country:'US'},
    {code:'ubs',                   name:'UBS Switzerland',      short:'UBS SW',   country:'CH'},
    {code:'ubs_miami',             name:'UBS Miami',            short:'UBS MIA',  country:'US'},
    {code:'goldman_sachs',         name:'Goldman Sachs',        short:'GS',       country:'US'},
    {code:'bbh',                   name:'Brown Brothers Harriman', short:'BBH',   country:'US'},
    {code:'wellington',            name:'Wellington',           short:'WELL',     country:'US'},
  ];

  // --- Parsers canónicos (§3) ---
  const parsers = [
    {name:'bice/brokerage',                 bank:'bice_inversiones',  account_type:'brokerage',          version:'3.4.0'},
    {name:'bice_asesorias/wealth_management', bank:'bice_asesorias',  account_type:'wealth_management',  version:'1.2.1'},
    {name:'bbh/custody',                    bank:'bbh',               account_type:'custody',            version:'1.4.0'},
    {name:'bbh/report_mandato',             bank:'bbh',               account_type:'report_mandato',     version:'1.1.0'},
    {name:'goldman_sachs/custody',          bank:'goldman_sachs',     account_type:'custody',            version:'2.0.3'},
    {name:'goldman_sachs/etf',              bank:'goldman_sachs',     account_type:'etf',                version:'1.3.0'},
    {name:'goldman_sachs/report_mandato',   bank:'goldman_sachs',     account_type:'report_mandato',     version:'1.2.0'},
    {name:'jpmorgan/brokerage',             bank:'jpmorgan',          account_type:'brokerage',          version:'4.1.2'},
    {name:'jpmorgan/bonds',                 bank:'jpmorgan',          account_type:'bonds',              version:'2.0.0'},
    {name:'jpmorgan/custody',               bank:'jpmorgan',          account_type:'custody',            version:'3.2.1'},
    {name:'jpmorgan/etf',                   bank:'jpmorgan',          account_type:'etf',                version:'3.0.4'},
    {name:'jpmorgan/report_mandato',        bank:'jpmorgan',          account_type:'report_mandato',     version:'2.1.0'},
    {name:'ubs/custody',                    bank:'ubs',               account_type:'custody',            version:'2.3.3'},
    {name:'ubs/report_mandato',             bank:'ubs',               account_type:'report_mandato',     version:'1.5.1'},
    {name:'ubs_miami/custody',              bank:'ubs_miami',         account_type:'custody',            version:'1.4.2'},
    {name:'ubs_miami/report_mandato',       bank:'ubs_miami',         account_type:'report_mandato',     version:'1.2.1'},
    {name:'wellington/custody',             bank:'wellington',        account_type:'custody',            version:'1.1.0'},
  ];

  // --- Cuentas (§9) ---
  const accounts = [
    // Boatview
    {id:'A01', society:'BVL', bank:'jpmorgan',          number:'9001',              type:'brokerage',          currency:'USD', holder:'J.T. Ross',  parser:'jpmorgan/brokerage'},
    {id:'A02', society:'BVL', bank:'jpmorgan',          number:'2600',              type:'report_mandato',     currency:'USD', holder:'J.T. Ross',  parser:'jpmorgan/report_mandato'},
    {id:'A03', society:'BVL', bank:'jpmorgan',          number:'1100',              type:'bonds',              currency:'USD', holder:'J.T. Ross',  parser:'jpmorgan/bonds'},
    {id:'A04', society:'BVL', bank:'jpmorgan',          number:'0007',              type:'etf',                currency:'USD', holder:'J.T. Ross',  parser:'jpmorgan/etf'},
    {id:'A05', society:'BVL', bank:'ubs',               number:'206-560552-01',     type:'custody',            currency:'USD', holder:'J.T. Ross',  parser:'ubs/custody'},
    {id:'A06', society:'BVL', bank:'ubs',               number:'206-560552-02',     type:'report_mandato',     currency:'USD', holder:'J.T. Ross',  parser:'ubs/report_mandato'},
    {id:'A07', society:'BVL', bank:'ubs_miami',         number:'UM-432',            type:'custody',            currency:'USD', holder:'J.T. Ross',  parser:'ubs_miami/custody'},
    {id:'A08', society:'BVL', bank:'goldman_sachs',     number:'GS-9912',           type:'etf',                currency:'USD', holder:'J.T. Ross',  parser:'goldman_sachs/etf'},
    {id:'A09', society:'BVL', bank:'goldman_sachs',     number:'GS-9914',           type:'report_mandato',     currency:'USD', holder:'J.T. Ross',  parser:'goldman_sachs/report_mandato'},
    {id:'A10', society:'BVL', bank:'bbh',               number:'BBH-4412',          type:'custody',            currency:'USD', holder:'J.T. Ross',  parser:'bbh/custody'},
    {id:'A11', society:'BVL', bank:'wellington',        number:'576371',            type:'custody',            currency:'USD', holder:'J.T. Ross',  parser:'wellington/custody'},
    // Ecoterra Intl
    {id:'A12', society:'ECI', bank:'bice_inversiones',  number:'EI-0421',           type:'brokerage',          currency:'USD', holder:'Familia Ross', parser:'bice/brokerage'},
    {id:'A13', society:'ECI', bank:'bice_asesorias',    number:'C0000-0893',        type:'wealth_management',  currency:'CLP', holder:'Familia Ross', parser:'bice_asesorias/wealth_management'},
    {id:'A14', society:'ECI', bank:'ubs_miami',         number:'UM-518',            type:'report_mandato',     currency:'USD', holder:'Familia Ross', parser:'ubs_miami/report_mandato'},
    // Las Raíces
    {id:'A15', society:'ILR', bank:'bice_inversiones',  number:'038',               type:'brokerage',          currency:'CLP', holder:'Familia Ross', parser:'bice/brokerage'},
    {id:'A16', society:'ILR', bank:'bice_asesorias',    number:'237',               type:'wealth_management',  currency:'CLP', holder:'Familia Ross', parser:'bice_asesorias/wealth_management'},
    // Telmar
    {id:'A17', society:'TEL', bank:'bice_inversiones',  number:'112',               type:'brokerage',          currency:'CLP', holder:'A. Ross',     parser:'bice/brokerage'},
    {id:'A18', society:'TEL', bank:'jpmorgan',          number:'3341',              type:'custody',            currency:'USD', holder:'A. Ross',     parser:'jpmorgan/custody'},
    // Ecoterra SpA
    {id:'A19', society:'ECS', bank:'bice_inversiones',  number:'776',               type:'brokerage',          currency:'CLP', holder:'Familia Ross', parser:'bice/brokerage'},
  ];

  // --- Buckets canónicos (§6.1) ---
  const buckets = [
    {id:'Caja',          color:'#D5DEE9', css:'bk-caja',         order:1},
    {id:'RF IG Short',   color:'#2D6FB7', css:'bk-rf-ig-short',  order:2},
    {id:'RF IG Long',    color:'#4D92D9', css:'bk-rf-ig-long',   order:3},
    {id:'HY',            color:'#8AB8EB', css:'bk-hy',           order:4},
    {id:'Non US RF',     color:'#A8CBF0', css:'bk-non-us-rf',    order:5},
    {id:'Alternativos',  color:'#2E7D5A', css:'bk-alt',          order:6},
    {id:'Real Estate',   color:'#6AA56A', css:'bk-re',           order:7},
    {id:'RV EM',         color:'#D85759', css:'bk-rv-em',        order:8},
    {id:'RV DM',         color:'#B53639', css:'bk-rv-dm',        order:9},
  ];

  // --- ETFs canónicos (§6.2) ---
  const etfDictionary = [
    {canonical:'IWDA',         aliases:['ISHARES CORE MSCI WORLD','P ISHARES CORE MSCI WORLD','MSCI WORLD INDEX FUND (ISHARES)'], bucket:'RV DM'},
    {canonical:'IEMA',         aliases:['ISHARES MSCI EM-ACC','MSCI EMERGING MARKETS INDEX FUND (ISHARES)','ISHARES III PLC-ISHARES MSCI EMERGING MARKETS ETF'], bucket:'RV EM'},
    {canonical:'IHYA',         aliases:['ISHARES USD HY CORP USD ACC','MARKIT IBOXX USD LIQUID HY CAPPED INDEX FUND (ISHARES)','ISHARES II PLC-ISHARES $ HIGH YIELD CORP BOND UCITS ETF'], bucket:'HY'},
    {canonical:'VDCA',         aliases:['VAND USDCP1-3 USDA','VANGUARD USD CORPORATE 1-3 YEAR BOND UCITS ETF'], bucket:'RF IG Short'},
    {canonical:'VDPA',         aliases:['VANG USDCPBD USDA','VANG USDCPBD USDA ACC','VANGUARD USD CORPORATE BOND UCITS ETF','VUCP'], bucket:'RF IG Long'},
    {canonical:'SPDR',         aliases:['SPDR BLOOMBERG 1-10 YEAR U.S.','SSGA SPDR ETFS EU I PB L C-SPD ETF ON BLOOMBERG'], bucket:'RF IG Short'},
    {canonical:'Money Market', aliases:['JPM LI-LIQ LVNAV FD - USD - W -','PROCEEDS FROM PENDING SALES'], bucket:'Caja'},
  ];

  // --- Categorías mandato (§6.3) ---
  const mandateCategories = [
    'cash','ig_fixed_income','hy_fixed_income','fixed_income',
    'us_equities','non_us_equities','global_equities','equities',
    'private_equity','real_estate','other_investments'
  ];

  // --- Serie de 13 meses (§5 YTD chain-linking) ---
  // ending_value por sociedad × mes, en USD (base)
  const months = [
    '2025-03','2025-04','2025-05','2025-06','2025-07','2025-08','2025-09',
    '2025-10','2025-11','2025-12','2026-01','2026-02','2026-03'
  ];
  const monthLabels = ['Mar 25','Abr 25','May 25','Jun 25','Jul 25','Ago 25','Sep 25','Oct 25','Nov 25','Dic 25','Ene 26','Feb 26','Mar 26'];

  // Endings por sociedad (USD). Patrimonio total crece a ~58.7M.
  const endingsBySociety = {
    BVL: [29.80, 29.95, 30.12, 30.45, 30.68, 30.74, 31.02, 31.32, 31.58, 31.92, 32.14, 32.41, 32.62],
    ECI: [10.42, 10.48, 10.52, 10.58, 10.67, 10.72, 10.80, 10.88, 10.98, 11.04, 11.10, 11.17, 11.22],
    ILR: [ 6.90,  6.92,  6.95,  6.98,  7.02,  7.08,  7.14,  7.18,  7.22,  7.28,  7.34,  7.38,  7.42],
    TEL: [ 4.72,  4.76,  4.80,  4.85,  4.88,  4.92,  4.96,  5.00,  5.04,  5.08,  5.10,  5.12,  5.14],
    ECS: [ 2.14,  2.16,  2.18,  2.20,  2.22,  2.25,  2.27,  2.28,  2.30,  2.31,  2.32,  2.33,  2.34],
  };

  // Total consolidado USD (millones)
  const totalUSDSeries = months.map((_,i) => {
    let s=0; for (const k in endingsBySociety) s += endingsBySociety[k][i];
    return +s.toFixed(2);
  });

  // Tipos de cambio USD/CLP
  const fxSeries = [890,895,900,905,912,918,925,931,938,945,950,958,972];
  const fx = fxSeries[fxSeries.length-1];

  // Métricas normalizadas (§1) — mock por cuenta × mes
  // Para el dataset visible, exponemos solo el corte del mes actual (idx 12 = Mar 26).
  function pick(arr, i){ return arr[i]; }

  // Asset allocation canónica (§6.1) — % del total consolidado Mar 26
  const allocation = [
    {bucket:'RF IG Long',    pct: 18.2, val: 10.69},
    {bucket:'RF IG Short',   pct: 12.4, val:  7.28},
    {bucket:'RV DM',         pct: 19.8, val: 11.63},
    {bucket:'RV EM',         pct:  8.4, val:  4.93},
    {bucket:'HY',            pct:  6.6, val:  3.87},
    {bucket:'Non US RF',     pct:  4.8, val:  2.81},
    {bucket:'Alternativos',  pct:  9.0, val:  5.29},
    {bucket:'Real Estate',   pct:  3.5, val:  2.06},
    {bucket:'Caja',          pct:  5.5, val:  3.23},
    // residual RF local / RV local en clasificación legacy: dejamos 11.8% para las RF/RV local como 'other'
  ];

  // Breakdowns por sociedad (reusa endingsBySociety)
  const bySociety = societies.map(s => {
    const v = endingsBySociety[s.id][12];
    return {id:s.id, name:s.name, value_usd:v};
  });
  const totalUSD = totalUSDSeries[12]; // 58.74
  bySociety.forEach(r => r.pct = +(r.value_usd/totalUSD*100).toFixed(1));
  bySociety.sort((a,b)=>b.value_usd-a.value_usd);

  // Por custodio
  const byBank = [
    {code:'jpmorgan',          value_usd:19.43},
    {code:'ubs',               value_usd:10.14},
    {code:'goldman_sachs',     value_usd: 9.32},
    {code:'ubs_miami',         value_usd: 6.72},
    {code:'bice_inversiones',  value_usd: 5.91},
    {code:'bbh',               value_usd: 4.92},
    {code:'bice_asesorias',    value_usd: 2.30},
  ].map(x => ({...x, pct:+(x.value_usd/totalUSD*100).toFixed(1)}));

  const byCurrency = [
    {code:'USD', value_usd:48.31, pct:82.2},
    {code:'EUR', value_usd: 3.43, pct: 5.8},
    {code:'CLP', value_usd: 4.82, pct: 8.2},
    {code:'UF',  value_usd: 1.84, pct: 3.1},
    {code:'Otros', value_usd:0.34, pct:0.6},
  ];

  // Rentabilidad mensual del consolidado (%)
  const retMonthly = [0.61,0.51,0.57,1.10,0.75,0.21,0.92,0.97,0.83,1.07,0.69,0.83,0.65];
  // YTD chain-linking 2026: sólo Ene..Mar 26 (idx 10..12)
  function ytdChain(pcts){
    let p=1; for (const r of pcts) p *= (1 + r/100);
    return +((p-1)*100).toFixed(2);
  }
  const ytd2026 = ytdChain(retMonthly.slice(10));  // Ene+Feb+Mar 26
  const ytd2025 = ytdChain(retMonthly.slice(0,10)); // Mar 25..Dic 25

  // === Posiciones por cuenta (muestra amplia) ===
  // Cada fila: {account, instrument, bucket, market_value_usd, weight_pct_cta, currency, maturity?, yield?}
  const positions = [
    // JPM 9001 Brokerage (USD)
    {acct:'A01', instr:'US Treasury 4.25% 2033', bucket:'RF IG Long',   mv:2340000, wt:28.4, ccy:'USD', mat:'2033-08-15', yld:4.21},
    {acct:'A01', instr:'US Treasury 3.875% 2029', bucket:'RF IG Short', mv:1620000, wt:19.6, ccy:'USD', mat:'2029-05-15', yld:4.05},
    {acct:'A01', instr:'Apple Inc. (AAPL)',      bucket:'RV DM',        mv: 810000, wt: 9.8, ccy:'USD'},
    {acct:'A01', instr:'Microsoft (MSFT)',       bucket:'RV DM',        mv: 780000, wt: 9.4, ccy:'USD'},
    {acct:'A01', instr:'JPMorgan Prime MM Fund', bucket:'Caja',         mv: 540000, wt: 6.5, ccy:'USD'},
    {acct:'A01', instr:'Alphabet (GOOGL)',       bucket:'RV DM',        mv: 520000, wt: 6.3, ccy:'USD'},
    {acct:'A01', instr:'NVIDIA (NVDA)',          bucket:'RV DM',        mv: 690000, wt: 8.3, ccy:'USD'},
    {acct:'A01', instr:'Taiwan Semi (TSM)',      bucket:'RV EM',        mv: 420000, wt: 5.1, ccy:'USD'},
    {acct:'A01', instr:'JPM LI-LIQ LVNAV FD - USD - W -', bucket:'Caja',mv: 562000, wt: 6.6, ccy:'USD'},
    // JPM 2600 Mandato
    {acct:'A02', instr:'ISHARES CORE MSCI WORLD', bucket:'RV DM',      mv:1840000, wt:31.2, ccy:'USD'},
    {acct:'A02', instr:'ISHARES MSCI EM-ACC',     bucket:'RV EM',      mv: 620000, wt:10.5, ccy:'USD'},
    {acct:'A02', instr:'VANG USDCPBD USDA',       bucket:'RF IG Long', mv:1280000, wt:21.7, ccy:'USD'},
    {acct:'A02', instr:'VAND USDCP1-3 USDA',      bucket:'RF IG Short',mv: 940000, wt:15.9, ccy:'USD'},
    {acct:'A02', instr:'ISHARES USD HY CORP USD ACC', bucket:'HY',     mv: 610000, wt:10.3, ccy:'USD'},
    {acct:'A02', instr:'Money Market Sweep',      bucket:'Caja',       mv: 318000, wt: 5.4, ccy:'USD'},
    // JPM 1100 Bonos
    {acct:'A03', instr:'Italy BTP 3.85% 2049',    bucket:'Non US RF',  mv: 880000, wt:32.1, ccy:'EUR', mat:'2049-09-01', yld:4.25},
    {acct:'A03', instr:'Spain Bono 3.45% 2043',   bucket:'Non US RF',  mv: 640000, wt:23.4, ccy:'EUR', mat:'2043-07-30', yld:3.88},
    {acct:'A03', instr:'US Treasury 5.00% 2054',  bucket:'RF IG Long', mv: 720000, wt:26.3, ccy:'USD', mat:'2054-05-15', yld:4.72},
    {acct:'A03', instr:'Deposit JPM USD 1.2M',    bucket:'Caja',       mv: 280000, wt:10.2, ccy:'USD'},
    {acct:'A03', instr:'UK Gilt 3.50% 2038',      bucket:'Non US RF',  mv: 220000, wt: 8.0, ccy:'GBP', mat:'2038-01-22', yld:4.12},
    // JPM 0007 ETF
    {acct:'A04', instr:'IWDA',                    bucket:'RV DM',      mv:1120000, wt:42.8, ccy:'USD'},
    {acct:'A04', instr:'IEMA',                    bucket:'RV EM',      mv: 410000, wt:15.7, ccy:'USD'},
    {acct:'A04', instr:'VDCA',                    bucket:'RF IG Short',mv: 380000, wt:14.5, ccy:'USD'},
    {acct:'A04', instr:'VDPA',                    bucket:'RF IG Long', mv: 420000, wt:16.1, ccy:'USD'},
    {acct:'A04', instr:'IHYA',                    bucket:'HY',         mv: 190000, wt: 7.3, ccy:'USD'},
    {acct:'A04', instr:'Money Market',            bucket:'Caja',       mv:  96000, wt: 3.6, ccy:'USD'},
    // UBS SW 01 Custody
    {acct:'A05', instr:'UBS Global Equity Strat', bucket:'RV DM',      mv:1720000, wt:26.8, ccy:'USD'},
    {acct:'A05', instr:'UBS IG Credit Fund',      bucket:'RF IG Long', mv:1380000, wt:21.5, ccy:'USD'},
    {acct:'A05', instr:'UBS Swiss Equity',        bucket:'RV DM',      mv: 620000, wt: 9.7, ccy:'CHF'},
    {acct:'A05', instr:'EM Debt Mandate',         bucket:'Non US RF',  mv: 530000, wt: 8.3, ccy:'USD'},
    {acct:'A05', instr:'UBS Cash Account',        bucket:'Caja',       mv: 210000, wt: 3.3, ccy:'USD'},
    {acct:'A05', instr:'Private Equity Fund III', bucket:'Alternativos',mv: 940000, wt:14.7, ccy:'USD'},
    {acct:'A05', instr:'Hedge Fund of Funds',     bucket:'Alternativos',mv: 520000, wt: 8.1, ccy:'USD'},
    {acct:'A05', instr:'European RE Fund',        bucket:'Real Estate',mv: 490000, wt: 7.6, ccy:'EUR'},
    // UBS SW 02 Report Mandato
    {acct:'A06', instr:'UBS Global Bond',         bucket:'RF IG Long', mv: 920000, wt:24.9, ccy:'USD'},
    {acct:'A06', instr:'UBS IG Short',            bucket:'RF IG Short',mv: 680000, wt:18.4, ccy:'USD'},
    {acct:'A06', instr:'MSCI EAFE Fund',          bucket:'RV DM',      mv: 820000, wt:22.2, ccy:'USD'},
    {acct:'A06', instr:'EM Equities Mandate',     bucket:'RV EM',      mv: 370000, wt:10.0, ccy:'USD'},
    {acct:'A06', instr:'HY Credit Fund',          bucket:'HY',         mv: 280000, wt: 7.6, ccy:'USD'},
    {acct:'A06', instr:'UBS Cash',                bucket:'Caja',       mv: 124000, wt: 3.4, ccy:'USD'},
    {acct:'A06', instr:'Global RE Securities',    bucket:'Real Estate',mv: 510000, wt:13.8, ccy:'USD'},
    // UBS Miami UM-432
    {acct:'A07', instr:'Pimco Income Fund',       bucket:'RF IG Long', mv:1380000, wt:30.9, ccy:'USD'},
    {acct:'A07', instr:'BlackRock Global Allocation', bucket:'RV DM',  mv: 980000, wt:21.9, ccy:'USD'},
    {acct:'A07', instr:'EM Bond Fund',            bucket:'HY',         mv: 410000, wt: 9.2, ccy:'USD'}, // override ubs_miami §6.3
    {acct:'A07', instr:'US Large Cap Growth',     bucket:'RV DM',      mv: 720000, wt:16.1, ccy:'USD'},
    {acct:'A07', instr:'Non-US Developed Equity', bucket:'RV DM',      mv: 420000, wt: 9.4, ccy:'USD'},
    {acct:'A07', instr:'Emerging Markets Equities', bucket:'RV EM',    mv: 280000, wt: 6.3, ccy:'USD'},
    {acct:'A07', instr:'Money Market Sweep',      bucket:'Caja',       mv: 280000, wt: 6.3, ccy:'USD'},
    // GS 9912 ETF
    {acct:'A08', instr:'SPDR BLOOMBERG 1-10 YEAR U.S.', bucket:'RF IG Short', mv: 820000, wt:30.5, ccy:'USD'},
    {acct:'A08', instr:'IWDA',                    bucket:'RV DM',      mv: 720000, wt:26.8, ccy:'USD'},
    {acct:'A08', instr:'IEMA',                    bucket:'RV EM',      mv: 280000, wt:10.4, ccy:'USD'},
    {acct:'A08', instr:'IHYA',                    bucket:'HY',         mv: 320000, wt:11.9, ccy:'USD'},
    {acct:'A08', instr:'GS FI TIPS',              bucket:'RF IG Long', mv: 420000, wt:15.6, ccy:'USD'},
    {acct:'A08', instr:'GS Cash Sweep',           bucket:'Caja',       mv: 130000, wt: 4.8, ccy:'USD'},
    // GS 9914 Mandato
    {acct:'A09', instr:'GS US Equity',            bucket:'RV DM',      mv:1540000, wt:34.2, ccy:'USD'},
    {acct:'A09', instr:'GS Intl Equity',          bucket:'RV DM',      mv: 720000, wt:16.0, ccy:'USD'},
    {acct:'A09', instr:'GS Core FI',              bucket:'RF IG Long', mv: 820000, wt:18.2, ccy:'USD'},
    {acct:'A09', instr:'GS Short Duration',       bucket:'RF IG Short',mv: 440000, wt: 9.8, ccy:'USD'},
    {acct:'A09', instr:'GS Alternatives',         bucket:'Alternativos',mv:620000, wt:13.8, ccy:'USD'},
    {acct:'A09', instr:'GS Real Assets',          bucket:'Real Estate',mv: 210000, wt: 4.7, ccy:'USD'},
    {acct:'A09', instr:'Money Market',            bucket:'Caja',       mv: 142000, wt: 3.2, ccy:'USD'},
    // BBH
    {acct:'A10', instr:'BBH Custody Portfolio A', bucket:'RF IG Long', mv:1420000, wt:28.9, ccy:'USD'},
    {acct:'A10', instr:'BBH Custody Portfolio B', bucket:'RV DM',      mv:1240000, wt:25.2, ccy:'USD'},
    {acct:'A10', instr:'BBH EM Allocation',       bucket:'RV EM',      mv: 380000, wt: 7.7, ccy:'USD'},
    {acct:'A10', instr:'BBH HY Credit',           bucket:'HY',         mv: 320000, wt: 6.5, ccy:'USD'},
    {acct:'A10', instr:'BBH Private Credit',      bucket:'Alternativos',mv:820000, wt:16.7, ccy:'USD'},
    {acct:'A10', instr:'BBH Real Estate',         bucket:'Real Estate',mv: 510000, wt:10.4, ccy:'USD'},
    {acct:'A10', instr:'BBH Cash',                bucket:'Caja',       mv: 228000, wt: 4.6, ccy:'USD'},
    // Wellington
    {acct:'A11', instr:'Wellington Global Opp',   bucket:'RV DM',      mv: 980000, wt:43.2, ccy:'USD'},
    {acct:'A11', instr:'Wellington Fixed Income', bucket:'RF IG Long', mv: 620000, wt:27.3, ccy:'USD'},
    {acct:'A11', instr:'Wellington EM Equity',    bucket:'RV EM',      mv: 280000, wt:12.3, ccy:'USD'},
    {acct:'A11', instr:'Wellington Cash',         bucket:'Caja',       mv: 120000, wt: 5.3, ccy:'USD'},
    {acct:'A11', instr:'Wellington Alt Credit',   bucket:'Alternativos',mv:270000, wt:11.9, ccy:'USD'},
    // BICE Inversiones Ecoterra Intl
    {acct:'A12', instr:'DAP BICE USD 3m',         bucket:'Caja',       mv: 420000, wt:14.3, ccy:'USD'},
    {acct:'A12', instr:'LIQUIDEZ DOLAR',          bucket:'Caja',       mv:  82000, wt: 2.8, ccy:'USD'},
    {acct:'A12', instr:'Bono Banco Chile 2031',   bucket:'RF IG Long', mv: 680000, wt:23.2, ccy:'USD'},
    {acct:'A12', instr:'Fondo BICE US Equity',    bucket:'RV DM',      mv: 540000, wt:18.4, ccy:'USD'},
    {acct:'A12', instr:'Fondo BICE Emerging',     bucket:'RV EM',      mv: 240000, wt: 8.2, ccy:'USD'},
    {acct:'A12', instr:'Fondo BICE RF Intl',      bucket:'RF IG Short',mv: 520000, wt:17.7, ccy:'USD'},
    {acct:'A12', instr:'Fondo BICE Alternativos', bucket:'Alternativos',mv:246000, wt: 8.4, ccy:'USD'},
    {acct:'A12', instr:'Fondo BICE Real Estate',  bucket:'Real Estate',mv: 200000, wt: 6.8, ccy:'USD'},
    // Altos Patrimonios Ecoterra Intl
    {acct:'A13', instr:'Cartera Balanceada Altos',bucket:'RF IG Long', mv:1230000000, wt:52.1, ccy:'CLP'},
    {acct:'A13', instr:'Cartera RV Local Altos',  bucket:'RV DM',      mv: 720000000, wt:30.4, ccy:'CLP'},
    {acct:'A13', instr:'TESORERIA CLP',           bucket:'Caja',       mv: 120000000, wt: 5.1, ccy:'CLP'},
    {acct:'A13', instr:'Cartera RV Intl Altos',   bucket:'RV DM',      mv: 294000000, wt:12.4, ccy:'CLP'},
    // UBS Miami Ecoterra Intl (UM-518 mandato)
    {acct:'A14', instr:'UBS MIA Global Fixed',    bucket:'RF IG Long', mv: 780000, wt:38.0, ccy:'USD'},
    {acct:'A14', instr:'UBS MIA Equity Growth',   bucket:'RV DM',      mv: 520000, wt:25.3, ccy:'USD'},
    {acct:'A14', instr:'UBS MIA EM Fixed',        bucket:'HY',         mv: 310000, wt:15.1, ccy:'USD'}, // override ubs_miami
    {acct:'A14', instr:'UBS MIA Alternatives',    bucket:'Alternativos',mv:220000, wt:10.7, ccy:'USD'},
    {acct:'A14', instr:'UBS MIA Cash',            bucket:'Caja',       mv: 222000, wt:10.8, ccy:'USD'},
    // Las Raíces BICE 038 (CLP)
    {acct:'A15', instr:'TESORERIA',               bucket:'Caja',       mv: 240000000, wt: 8.2, ccy:'CLP'},
    {acct:'A15', instr:'Bono Santander CL 2030',  bucket:'RF IG Long', mv: 920000000, wt:31.4, ccy:'CLP'},
    {acct:'A15', instr:'Acciones SQM-B',          bucket:'RV DM',      mv: 380000000, wt:13.0, ccy:'CLP'},
    {acct:'A15', instr:'Acciones Falabella',      bucket:'RV DM',      mv: 260000000, wt: 8.9, ccy:'CLP'},
    {acct:'A15', instr:'Fondo Mutuo BICE RF',     bucket:'RF IG Short',mv: 520000000, wt:17.7, ccy:'CLP'},
    {acct:'A15', instr:'Fondo Mutuo BICE RV Intl',bucket:'RV DM',      mv: 370000000, wt:12.6, ccy:'CLP'},
    {acct:'A15', instr:'DAP BICE 90d',            bucket:'Caja',       mv: 240000000, wt: 8.2, ccy:'CLP'},
    // Las Raíces Altos 237 (CLP)
    {acct:'A16', instr:'Cartera Balanceada 237',  bucket:'RF IG Long', mv: 820000000, wt:60.0, ccy:'CLP'},
    {acct:'A16', instr:'RV Local Altos',          bucket:'RV DM',      mv: 420000000, wt:30.7, ccy:'CLP'},
    {acct:'A16', instr:'TESORERIA CLP',           bucket:'Caja',       mv: 127000000, wt: 9.3, ccy:'CLP'},
    // Telmar BICE 112
    {acct:'A17', instr:'BCP 10 años',             bucket:'RF IG Long', mv:1420000000, wt:43.1, ccy:'CLP'},
    {acct:'A17', instr:'Acciones CENCOSUD',       bucket:'RV DM',      mv: 620000000, wt:18.8, ccy:'CLP'},
    {acct:'A17', instr:'Fondo RV Chile',          bucket:'RV DM',      mv: 480000000, wt:14.6, ccy:'CLP'},
    {acct:'A17', instr:'Fondo RF Corp',           bucket:'RF IG Short',mv: 520000000, wt:15.8, ccy:'CLP'},
    {acct:'A17', instr:'TESORERIA',               bucket:'Caja',       mv: 253000000, wt: 7.7, ccy:'CLP'},
    // Telmar JPM 3341 Custody
    {acct:'A18', instr:'US Treasuries Portfolio', bucket:'RF IG Long', mv: 840000, wt:37.3, ccy:'USD'},
    {acct:'A18', instr:'US Large Cap',            bucket:'RV DM',      mv: 620000, wt:27.6, ccy:'USD'},
    {acct:'A18', instr:'EM Debt',                 bucket:'HY',         mv: 280000, wt:12.4, ccy:'USD'},
    {acct:'A18', instr:'Money Market JPM',        bucket:'Caja',       mv: 180000, wt: 8.0, ccy:'USD'},
    {acct:'A18', instr:'Private Real Estate',     bucket:'Real Estate',mv: 330000, wt:14.7, ccy:'USD'},
    // Ecoterra SpA BICE 776
    {acct:'A19', instr:'DAP BICE',                bucket:'Caja',       mv: 420000000, wt:18.6, ccy:'CLP'},
    {acct:'A19', instr:'Bono Chile Gob 2032',     bucket:'RF IG Long', mv: 740000000, wt:32.7, ccy:'CLP'},
    {acct:'A19', instr:'Fondo Mutuo BICE RF',     bucket:'RF IG Short',mv: 520000000, wt:23.0, ccy:'CLP'},
    {acct:'A19', instr:'Acciones COPEC',          bucket:'RV DM',      mv: 340000000, wt:15.0, ccy:'CLP'},
    {acct:'A19', instr:'TESORERIA',               bucket:'Caja',       mv: 238000000, wt:10.5, ccy:'CLP'},
  ];

  // === Movimientos últimos 90 días (selección representativa) ===
  const movements = [
    {date:'2026-03-28', acct:'A02', type:'buy',      instr:'US Treasury 5.00% 2054', amount:-480800, ccy:'USD'},
    {date:'2026-03-25', acct:'A05', type:'coupon',   instr:'BTP Italy 3.85% 2049',   amount:  23100, ccy:'EUR'},
    {date:'2026-03-22', acct:'A09', type:'dividend', instr:'Apple Inc.',             amount:   1632, ccy:'USD'},
    {date:'2026-03-20', acct:'A17', type:'sell',     instr:'BCP 10 años',            amount:  62400000, ccy:'CLP'},
    {date:'2026-03-18', acct:'A14', type:'redeem',   instr:'Pimco Income Fund',      amount: 125000, ccy:'USD'},
    {date:'2026-03-15', acct:'A04', type:'buy',      instr:'IWDA',                   amount:-140000, ccy:'USD'},
    {date:'2026-03-12', acct:'A01', type:'dividend', instr:'Microsoft',              amount:   2280, ccy:'USD'},
    {date:'2026-03-10', acct:'A03', type:'coupon',   instr:'Spain Bono 3.45% 2043',  amount:  11050, ccy:'EUR'},
    {date:'2026-03-08', acct:'A12', type:'deposit',  instr:'INVERSION FM TESORERIA', amount:  45000, ccy:'USD', note:'Aporte (regla §4.1)'},
    {date:'2026-03-07', acct:'A15', type:'buy',      instr:'Bono Santander CL 2030', amount: -80000000, ccy:'CLP'},
    {date:'2026-03-05', acct:'A10', type:'withdraw', instr:'RESCATE MM BBH',         amount: -60000, ccy:'USD'},
    {date:'2026-03-02', acct:'A08', type:'buy',      instr:'IEMA',                   amount: -50000, ccy:'USD'},
    {date:'2026-02-27', acct:'A01', type:'buy',      instr:'NVIDIA',                 amount:-120000, ccy:'USD'},
    {date:'2026-02-25', acct:'A06', type:'coupon',   instr:'UBS Global Bond',        amount:   8400, ccy:'USD'},
    {date:'2026-02-22', acct:'A07', type:'dividend', instr:'US Large Cap Growth',    amount:   4120, ccy:'USD'},
    {date:'2026-02-20', acct:'A02', type:'sell',     instr:'VAND USDCP1-3 USDA',     amount:  95000, ccy:'USD'},
    {date:'2026-02-18', acct:'A16', type:'redeem',   instr:'RV Local Altos',         amount:  14000000, ccy:'CLP'},
    {date:'2026-02-15', acct:'A04', type:'fee',      instr:'Custody Fee',            amount:   -420, ccy:'USD'},
    {date:'2026-02-12', acct:'A11', type:'buy',      instr:'Wellington EM Equity',   amount: -30000, ccy:'USD'},
    {date:'2026-02-10', acct:'A09', type:'coupon',   instr:'GS Core FI',             amount:  12400, ccy:'USD'},
    {date:'2026-02-06', acct:'A17', type:'dividend', instr:'Acciones CENCOSUD',      amount:   820000, ccy:'CLP'},
    {date:'2026-02-03', acct:'A03', type:'coupon',   instr:'UK Gilt 3.50% 2038',     amount:   4900, ccy:'GBP'},
    {date:'2026-02-01', acct:'A13', type:'deposit',  instr:'Aporte Familia',         amount:  50000000, ccy:'CLP'},
    {date:'2026-01-28', acct:'A07', type:'buy',      instr:'EM Bond Fund',           amount: -70000, ccy:'USD'},
    {date:'2026-01-25', acct:'A05', type:'fee',      instr:'UBS Management Fee',     amount:   -3800, ccy:'USD'},
    {date:'2026-01-22', acct:'A02', type:'dividend', instr:'ISHARES CORE MSCI WORLD',amount:   5400, ccy:'USD'},
    {date:'2026-01-18', acct:'A10', type:'buy',      instr:'BBH Custody Portfolio A',amount:-100000, ccy:'USD'},
    {date:'2026-01-15', acct:'A19', type:'coupon',   instr:'Bono Chile Gob 2032',    amount:  2100000, ccy:'CLP'},
    {date:'2026-01-10', acct:'A01', type:'sell',     instr:'Microsoft',              amount:  58000, ccy:'USD'},
  ];

  // === Cola de ingesta — PDFs recién subidos esperando revisión (§ Import flow) ===
  // Cada fila: campos detectados por el router (parser, society, account, period)
  // con score de confianza 0..1. Si score < 0.95 → requiere revisión manual.
  // Al setear society+account, la app completa banco/tipo/currency/holder desde maestro.
  const ingestQueue = [
    { id:'U-001', name:'JPM_9001_202603.pdf',         size:'1.2 MB',  hash:'71c0de…9a4e',
      parser:'jpmorgan/brokerage',                version:'4.1.2', parser_score:0.96,
      society:'BVL',  society_score:0.95,
      account:'A01',  account_score:0.97,
      period:'2026-03', period_score:0.99,
      statement_date:'2026-03-31',
      pages:18, status:'ready', warnings:[] },
    { id:'U-002', name:'BICE_Ecoterra_202603.pdf',    size:'412 KB',  hash:'a3f4e9…28b1',
      parser:'bice/brokerage',                    version:'3.4.0', parser_score:0.98,
      society:'ECI',  society_score:0.97,
      account:'A12',  account_score:0.96,
      period:'2026-03', period_score:0.98,
      statement_date:'2026-03-31',
      pages:24, status:'ready', warnings:[] },
    { id:'U-003', name:'UBS_SW_206-560552-01_202603.pdf', size:'3.6 MB', hash:'112299…77fc',
      parser:'ubs/custody',                       version:'2.3.3', parser_score:0.94,
      society:'BVL',  society_score:0.93,
      account:'A05',  account_score:0.81,
      period:'2026-03', period_score:0.99,
      statement_date:'2026-03-31',
      pages:42, status:'review', warnings:['Liquidity token concatenado §4.2'] },
    { id:'U-004', name:'UBS_SW_unknown_202603.pdf',   size:'3.4 MB',  hash:'8821cc…09ae',
      parser:'ubs/custody',                       version:'2.3.3', parser_score:0.78,
      society:'BVL',  society_score:0.62,
      account:null,   account_score:0.44,
      period:'2026-03', period_score:0.98,
      statement_date:'2026-03-31',
      pages:38, status:'review', warnings:['Sufijo portfolio ambiguo (-01 vs -02) §4.2'] },
    { id:'U-005', name:'GS_unidentified_mar26.pdf',   size:'1.4 MB',  hash:'bb77cc…dd33',
      parser:'goldman_sachs/report_mandato',      version:'1.2.0', parser_score:0.52,
      society:'BVL',  society_score:0.71,
      account:null,   account_score:0.38,
      period:'2026-03', period_score:0.94,
      statement_date:'2026-03-31',
      pages:28, status:'review', warnings:['Detección ambigua: goldman_sachs/custody 0.49 vs /report_mandato 0.52', 'ParserConflictError potencial'] },
    { id:'U-006', name:'Altos_C0000-0893_202603.pdf', size:'540 KB',  hash:'4411ff…22aa',
      parser:'bice_asesorias/wealth_management',  version:'1.2.1', parser_score:0.97,
      society:'ECI',  society_score:0.98,
      account:'A13',  account_score:0.96,
      period:'2026-03', period_score:0.99,
      statement_date:'2026-03-31',
      pages:9,  status:'ready', warnings:[] },
    { id:'U-007', name:'BBH_4412_202603.pdf',         size:'1.1 MB',  hash:'cafeba…feed',
      parser:'bbh/custody',                       version:'1.4.0', parser_score:0.97,
      society:'BVL',  society_score:0.98,
      account:'A10',  account_score:0.96,
      period:'2026-03', period_score:0.99,
      statement_date:'2026-03-31',
      pages:14, status:'ready', warnings:[] },
    { id:'U-008', name:'Wellington_576371_202603.pdf', size:'780 KB', hash:'deadbe…ef90',
      parser:'wellington/custody',                version:'1.1.0', parser_score:0.96,
      society:'BVL',  society_score:0.97,
      account:'A11',  account_score:0.98,
      period:'2026-03', period_score:0.98,
      statement_date:'2026-03-31',
      pages:11, status:'ready', warnings:[] },
    { id:'U-009', name:'mandato_unknown_XYZ.pdf',     size:'620 KB',  hash:'ee00dd…4455',
      parser:null,                                version:null,    parser_score:0.23,
      society:null,   society_score:0.12,
      account:null,   account_score:0.09,
      period:'2026-03', period_score:0.61,
      statement_date:null,
      pages:6,  status:'error', warnings:['Ningún parser supera umbral 0.40 §2', 'Requiere asignación manual'] },
    { id:'U-010', name:'JPM_2600_mandato_202603.pdf', size:'2.1 MB',  hash:'88ee12…a09f',
      parser:'jpmorgan/report_mandato',           version:'2.1.0', parser_score:0.94,
      society:'BVL',  society_score:0.96,
      account:'A02',  account_score:0.93,
      period:'2026-03', period_score:0.99,
      statement_date:'2026-03-31',
      pages:31, status:'duplicate', warnings:['Archivo idempotente: SHA-256 ya procesado 2026-04-01 18:30 §1.4'] },
  ];

  // === Fondos alternativos (PE/RE/VC) ===
  // Cada fondo: clase · estrategia · vintage · compromiso · NAV · aportes · distribuido · TVPI · IRR · DPI · moneda · sociedad
  // Series: navSeries (anual), flowsQuarterly (trimestral)
  const strategies = {
    PE: ['Buyout','Secondary','Co-investment','Fondo de fondos','Deuda','Distressed Debt'],
    RE: ['Coinvestment RE','DC REIT','Fund RE','Fund RE Debt'],
    VC: ['Early stage','Growth','Late stage','Sector specific'],
  };
  function mkNavSeries(vintage, peakYear, peakValue){
    // Curva en J típica de alternativos: negativo al principio, crece, plateau
    const years = [];
    for (let y=vintage; y<=2026; y++) years.push(y);
    return years.map(y => {
      const t = (y - vintage) / Math.max(1, peakYear - vintage);
      const growth = t < 1 ? Math.pow(t, 1.8) : 1 - (1-Math.min(1,t))*0.15;
      return { year: y, nav: +(peakValue * growth).toFixed(2) };
    });
  }
  function mkFlows(vintage){
    // Flujos trimestrales realistas: aportes negativos al principio, distribuciones positivas luego
    const flows = [];
    const nYears = 2026 - vintage + 1;
    for (let i=0; i<nYears*4; i++){
      const year = vintage + Math.floor(i/4);
      const quarter = (i%4) + 1;
      const t = i / (nYears*4);
      let amt;
      if (t < 0.3)      amt = -Math.round((Math.random()*400+100)*10)/10; // aportes
      else if (t < 0.5) amt = (Math.random()>0.5 ? -1 : 1) * Math.round((Math.random()*200+50)*10)/10;
      else              amt = Math.round((Math.random()*500+100)*10)/10; // distribuciones
      flows.push({ year, quarter, amount: amt*1000 });
    }
    return flows;
  }

  // 18 PE + 8 RE + 6 VC = 32 fondos
  const altRaw = [
    // --- PRIVATE EQUITY ---
    ['PE','Buyout','BBH Capital Partners V','BVL',2016,'USD',3.0,1.12,1.1,-3.4,4.1,1.56,0.124,1.22],
    ['PE','Buyout','West Street Capital Partners VII','TEL',2017,'USD',6.0,1.04,2.7,-6.2,7.5,1.64,0.148,1.20],
    ['PE','Co-investment','Stepstone Capital Partners IV','ECI',2018,'USD',3.0,0.86,3.5,-2.6,1.6,1.96,0.125,0.61],
    ['PE','Secondary','Lexington Capital Partners IX','ECI',2018,'USD',3.0,0.847,2.6,-2.5,1.4,1.58,0.133,0.54],
    ['PE','Buyout','Carlyle Europe Partners V','ECI',2018,'EUR',3.4,1.05,2.6,-3.6,0.766,0.92,-0.034,0.21],
    ['PE','Buyout','TPG Partners VIII, L.P.','ECI',2019,'USD',3.0,0.925,3.5,-2.8,0.649,1.49,0.109,0.23],
    ['PE','Secondary','Strategic Partners VIII','ECI',2019,'USD',3.0,0.40,1.6,-1.2,0.935,2.09,0.168,0.77],
    ['PE','Buyout','HPH Investments Master Fund, LP','ECI',2019,'USD',1.5,1.059,1.7,-1.6,0.394,1.34,0.067,0.25],
    ['PE','Buyout','Partners Group Direct Equity 2019','ECI',2019,'USD',3.0,0.879,3.4,-2.6,0.129,1.33,0.069,0.05],
    ['PE','Buyout','The Seventh Cinven Fund','ECI',2019,'EUR',2.8,0.968,3.4,-2.7,0.458,1.41,0.094,0.17],
    ['PE','Secondary','Stepstone Secondary Opportunities','ECI',2019,'USD',3.0,0.497,1.1,-1.5,1.3,1.62,0.126,0.90],
    ['PE','Secondary','Coller Capital Partners VIII','ECI',2019,'USD',3.0,0.617,2.6,-1.9,0.884,1.87,0.173,0.48],
    ['PE','Buyout','Blackstone Capital Partners VIII','BVL',2020,'USD',3.0,0.939,3.0,-2.8,0.790,1.36,0.099,0.28],
    ['PE','Fondo de fondos','HarbourVest Partners XII','ECI',2020,'USD',2.5,0.720,2.3,-1.8,0.180,1.21,0.065,0.09],
    ['PE','Deuda','Ares Capital Europe V','TEL',2020,'EUR',2.0,0.850,1.4,-1.7,0.520,1.14,0.088,0.31],
    ['PE','Distressed Debt','Oaktree Opportunities XI','BVL',2021,'USD',2.5,0.620,1.9,-1.55,0.150,1.18,0.075,0.10],
    ['PE','Co-investment','KKR Asian Fund IV','ECI',2021,'USD',2.0,0.550,1.6,-1.1,0.080,1.08,0.031,0.07],
    ['PE','Buyout','CVC Capital Partners IX','BVL',2022,'EUR',3.5,0.280,1.2,-0.98,0.025,0.98,0.012,0.03],
    // --- REAL ESTATE ---
    ['RE','Fund RE','Blackstone Real Estate X','BVL',2017,'USD',5.0,0.950,3.8,-4.75,2.1,1.42,0.092,0.55],
    ['RE','Coinvestment RE','Brookfield Strategic RE III','TEL',2018,'USD',3.0,0.880,2.2,-2.64,1.3,1.29,0.078,0.49],
    ['RE','DC REIT','Prologis Logistics JV','ECI',2019,'USD',2.5,0.920,1.8,-2.3,0.950,1.23,0.065,0.41],
    ['RE','Fund RE','Starwood Global Opportunity XI','BVL',2019,'USD',4.0,0.825,3.1,-3.3,1.6,1.42,0.098,0.48],
    ['RE','Fund RE Debt','KKR Real Estate Credit','ECI',2020,'USD',2.0,0.700,1.5,-1.4,0.720,1.29,0.071,0.51],
    ['RE','Coinvestment RE','Hines European Core Plus','TEL',2020,'EUR',3.0,0.650,2.1,-1.95,0.320,1.16,0.055,0.16],
    ['RE','Fund RE','Carlyle Realty Partners IX','BVL',2021,'USD',2.5,0.580,1.7,-1.45,0.180,1.30,0.089,0.12],
    ['RE','DC REIT','Nuveen Global Cities','ECI',2022,'USD',2.0,0.410,1.2,-0.82,0.050,0.95,-0.025,0.06],
    // --- VENTURE CAPITAL ---
    ['VC','Growth','Sequoia Capital Growth IX','BVL',2018,'USD',2.0,0.950,1.8,-1.90,1.1,1.53,0.118,0.58],
    ['VC','Early stage','Accel XV','TEL',2019,'USD',1.5,0.880,1.6,-1.32,0.850,1.67,0.134,0.64],
    ['VC','Late stage','Tiger Global PIP XIV','ECI',2020,'USD',3.0,0.720,2.0,-2.16,0.420,1.21,0.048,0.19],
    ['VC','Sector specific','Lightspeed Venture XIV','ECI',2020,'USD',1.5,0.680,1.0,-1.02,0.310,1.31,0.092,0.30],
    ['VC','Early stage','Kaszek Ventures VI','BVL',2021,'USD',1.0,0.520,0.7,-0.52,0.040,1.08,0.029,0.07],
    ['VC','Growth','Andreessen Horowitz Growth IV','BVL',2022,'USD',2.5,0.350,1.1,-0.87,0.025,0.98,-0.008,0.03],
  ];

  const altFunds = altRaw.map(([clase,strategy,name,society,vintage,currency,commit,pctCalled,nav,aporte,distrib,tvpi,irr,dpi], idx) => {
    const peakYear = Math.min(2026, vintage + 5 + Math.floor(Math.random()*3));
    return {
      id: `ALT-${String(idx+1).padStart(3,'0')}`,
      class: clase,               // 'PE' | 'RE' | 'VC'
      strategy,
      name,
      society,
      vintage,
      currency,
      commitment:    commit,      // M
      pct_called:    pctCalled,
      nav:           nav,         // M USD equivalente
      contributions: aporte,      // M (negativo = aportes pagados)
      distributed:   distrib,     // M
      tvpi, irr, dpi,
      navSeries: mkNavSeries(vintage, peakYear, nav),
      flows: mkFlows(vintage),
    };
  });

  // KPIs globales por clase
  function aggregateClass(cls){
    const f = cls ? altFunds.filter(x => x.class===cls) : altFunds;
    const commit = f.reduce((s,x)=>s+x.commitment,0);
    const nav    = f.reduce((s,x)=>s+x.nav,0);
    const apo    = f.reduce((s,x)=>s+x.contributions,0);
    const dist   = f.reduce((s,x)=>s+x.distributed,0);
    const tvpi   = (nav + dist) / Math.abs(apo || 1);
    const dpi    = dist / Math.abs(apo || 1);
    // IRR ponderado por NAV
    const irr    = f.reduce((s,x)=>s + x.irr * x.nav, 0) / Math.max(nav, 0.01);
    return { count:f.length, commit, nav, contributions:apo, distributed:dist, tvpi, irr, dpi };
  }
  const altAggregate = {
    global: aggregateClass(null),
    PE: aggregateClass('PE'),
    RE: aggregateClass('RE'),
    VC: aggregateClass('VC'),
  };

  // === Archivos recientes (ingesta) ===
  const files = [
    {name:'BICE_Ecoterra_202603.pdf',  size:'412 KB', hash:'a3f4e9…28b1', parser:'bice/brokerage',            version:'3.4.0', status:'SUCCESS', score:0.92, acct:'A12', date:'2026-04-02 09:14'},
    {name:'JPM_9001_202603.pdf',       size:'1.2 MB', hash:'71c0de…9a4e', parser:'jpmorgan/brokerage',        version:'4.1.2', status:'SUCCESS', score:0.96, acct:'A01', date:'2026-04-01 18:32'},
    {name:'JPM_2600_mandato_202603.pdf', size:'2.1 MB', hash:'88ee12…a09f', parser:'jpmorgan/report_mandato', version:'2.1.0', status:'SUCCESS', score:0.94, acct:'A02', date:'2026-04-01 18:30'},
    {name:'UBS_SW_206-560552_202603.pdf', size:'3.6 MB', hash:'112299…77fc', parser:'ubs/custody',            version:'2.3.3', status:'PARTIAL', score:0.88, acct:'A05', date:'2026-04-02 11:20', warning:'Liquidity token concatenado (§4.2)'},
    {name:'UBS_MIA_432_202603.pdf',    size:'1.8 MB', hash:'aa44bb…cc11', parser:'ubs_miami/custody',        version:'1.4.2', status:'SUCCESS', score:0.91, acct:'A07', date:'2026-04-02 12:02'},
    {name:'GS_9912_etf_202603.pdf',    size:'920 KB', hash:'ff00aa…1122', parser:'goldman_sachs/etf',        version:'1.3.0', status:'SUCCESS', score:0.93, acct:'A08', date:'2026-04-02 12:48'},
    {name:'GS_9914_mandato_202603.pdf', size:'1.4 MB', hash:'bb77cc…dd33', parser:'goldman_sachs/report_mandato', version:'1.2.0', status:'ERROR',  score:0.41, acct:'A09', date:'2026-04-02 13:05', warning:'Detección ambigua — múltiples parsers GS'},
    {name:'BBH_4412_202603.pdf',       size:'1.1 MB', hash:'cafeba…feed', parser:'bbh/custody',              version:'1.4.0', status:'SUCCESS', score:0.90, acct:'A10', date:'2026-04-02 14:18'},
    {name:'Wellington_576371_202603.pdf', size:'780 KB', hash:'deadbe…ef90', parser:'wellington/custody',    version:'1.1.0', status:'SUCCESS', score:0.89, acct:'A11', date:'2026-04-02 14:42'},
    {name:'Altos_C0000-0893_202603.pdf', size:'540 KB', hash:'4411ff…22aa', parser:'bice_asesorias/wealth_management', version:'1.2.1', status:'SUCCESS', score:0.87, acct:'A13', date:'2026-04-03 10:10'},
    {name:'JPM_0007_etf_202603.pdf',   size:'680 KB', hash:'997788…1144', parser:'jpmorgan/etf',             version:'3.0.4', status:'SUCCESS', score:0.95, acct:'A04', date:'2026-04-01 18:45'},
    {name:'JPM_1100_bonds_202603.pdf', size:'840 KB', hash:'aabbcc…ddee', parser:'jpmorgan/bonds',           version:'2.0.0', status:'PARTIAL', score:0.82, acct:'A03', date:'2026-04-01 18:55', warning:'Falta statement_date — fallback por nombre'},
  ];

  // === Alertas de calidad (§5.4 / §10.5) ===
  const alerts = [
    {id:'Q-001', sev:'critical', kind:'MAJOR_DIFF', title:'Rentabilidad anormal Pimco Income Fund +18.4% mensual', acct:'A07', month:'2026-03', detail:'Supera umbral +15% (ver §5.4). Revisar precio fuente Bloomberg.'},
    {id:'Q-002', sev:'critical', kind:'IDENTITY',   title:'Identidad rota: saldo_ini + movs ≠ saldo_fin',           acct:'A02', month:'2026-03', detail:'Diff USD 412.18 · Revisar accrual de cupones (§5.1 UBS-style).'},
    {id:'Q-003', sev:'warning',  kind:'MISSING',    title:'Cartola pendiente · GS 9914',                              acct:'A09', month:'2026-03', detail:'Sin sincronización desde 03-15 · conexión custodio.'},
    {id:'Q-004', sev:'warning',  kind:'STALE_PRICE',title:'14 instrumentos con precio obsoleto > 5 días',             acct:null,  month:'2026-03', detail:'Mayoría alternativos, fuente Bloomberg.'},
    {id:'Q-005', sev:'warning',  kind:'BUCKET',     title:'3 instrumentos cayeron a default bucket RV DM',            acct:'A05', month:'2026-03', detail:'Revisar diccionario §6.1 · "UBS Swiss Equity" / "MSCI EAFE" / "Global RE".'},
    {id:'Q-006', sev:'info',     kind:'RECON',      title:'MINOR_DIFF · Reconciliación diario vs mensual',            acct:'A01', month:'2026-03', detail:'Diff 0.008% — dentro de tolerancia, marcado como MINOR_DIFF.'},
    {id:'Q-007', sev:'critical', kind:'PARSER',     title:'ParserConflictError en ingesta GS',                        acct:null,  month:'2026-03', detail:'Dos parsers declararon (goldman_sachs, report_mandato). Revisar §2.'},
    {id:'Q-008', sev:'info',     kind:'NORM',       title:'Cobertura normalized: 98.4%',                              acct:null,  month:'2026-03', detail:'2 celdas sin fila en monthly_metrics_normalized → fallback a monthly_closings.'},
  ];

  // === Correcciones propuestas (§10.3) ===
  const corrections = [
    {id:'C-001', status:'pending',  scope:'parser', title:'BICE: "INVERSION FM TESORERIA" debe contar como aporte', detail:'Agregar regla en §4.1 — alias de aporte.', proposed:'2026-04-02', parser:'bice/brokerage'},
    {id:'C-002', status:'pending',  scope:'dict',   title:'ETF "VANG USDCPBD USDA ACC" → canonical VDPA',            detail:'Alias ya existe en §6.2, falta propagar a ingesta GS.', proposed:'2026-04-02', parser:'goldman_sachs/etf'},
    {id:'C-003', status:'applied',  scope:'parser', title:'UBS SW: fallback Liquidity via extract_tables',           detail:'Meses 2025-05/08/11 (§4.2 bug aceptado). Aplicado el 2026-03-28.', proposed:'2026-03-25', parser:'ubs/custody'},
    {id:'C-004', status:'pending',  scope:'bucket', title:'"EM Bond Fund" en UBS Miami → hy_fixed_income',           detail:'Override §6.3 ubs_miami. Aplicar a 2 posiciones.', proposed:'2026-04-01', parser:'ubs_miami/custody'},
    {id:'C-005', status:'reviewed', scope:'calc',   title:'JPM ETF: ajustar Accrual_prev cuando cambia de mandato',  detail:'Caso edge §5.1 — 1 ocurrencia en 2025-Q3.', proposed:'2026-03-20', parser:'jpmorgan/etf'},
  ];

  // === Auditoría ===
  const audit = [
    {ts:'2026-04-03 10:10', user:'jt.ross',   event:'parser.load',   obj:'Altos_C0000-0893_202603.pdf', detail:'status=SUCCESS parser=bice_asesorias/wealth_management v1.2.1'},
    {ts:'2026-04-02 14:42', user:'system',    event:'normalize',     obj:'A11·2026-03',                  detail:'Upsert monthly_metrics_normalized'},
    {ts:'2026-04-02 13:05', user:'system',    event:'parser.error',  obj:'GS_9914_mandato_202603.pdf',   detail:'ParserConflictError — ver Alertas Q-007'},
    {ts:'2026-04-02 11:20', user:'system',    event:'parser.partial',obj:'UBS_SW_206-560552_202603.pdf', detail:'Warning Liquidity §4.2'},
    {ts:'2026-04-01 18:55', user:'jt.ross',   event:'upload',        obj:'JPM_1100_bonds_202603.pdf',    detail:'SHA-256 aabbcc…ddee'},
    {ts:'2026-03-28 09:12', user:'system',    event:'rule.apply',    obj:'C-003',                        detail:'UBS SW Liquidity fallback → reproceso 3 meses'},
    {ts:'2026-03-25 16:41', user:'jt.ross',   event:'rule.propose',  obj:'C-001',                        detail:'BICE alias aporte'},
  ];

  // === API pública (mimica endpoints REST) ===
  window.MOCK = {
    // Maestros
    societies, banks, accounts, parsers, buckets, etfDictionary, mandateCategories,
    // Tiempo
    months, monthLabels, fxSeries, fx, asOf: now,
    // Dashboard
    kpis: {
      patrimonio_usd: totalUSDSeries[12],
      patrimonio_clp: totalUSDSeries[12] * fx,
      variacion_usd:  +(totalUSDSeries[12] - totalUSDSeries[11]).toFixed(2),
      variacion_pct:  +((totalUSDSeries[12]/totalUSDSeries[11]-1)*100).toFixed(2),
      twr_mom:        retMonthly[12],
      twr_ytd:        ytd2026,
      twr_ytd_prev:   ytd2025,
      liquidez_usd:   3.23,
      liquidez_pct:   5.5,
      benchmark_diff: 0.19,
    },
    totalUSDSeries, retMonthly,
    allocation, bySociety, byBank, byCurrency,
    positions, movements, files, alerts, corrections, audit, ingestQueue,
    altFunds, altAggregate, strategies,

    // Normalización — campos canónicos §1 · solo último mes para la vista
    normalized: accounts.map(a => {
      const accPos = positions.filter(p => p.acct===a.id);
      const total = accPos.reduce((s,p)=>s+p.mv,0);
      const cash  = accPos.filter(p=>p.bucket==='Caja').reduce((s,p)=>s+p.mv,0);
      const movsNet = movements.filter(m=>m.acct===a.id && m.date.startsWith('2026-03'))
                               .reduce((s,m)=>s+m.amount,0);
      return {
        account_id:a.id, account_number:a.number, society:a.society, bank:a.bank,
        month:'2026-03',
        ending_value_with_accrual: total,
        ending_value_without_accrual: Math.round(total*0.997),
        accrual_ending: Math.round(total*0.003),
        cash_value: cash,
        movements_net: movsNet,
        profit_period: Math.round(total*0.0065 - movsNet),
        currency: a.currency,
        parser: a.parser,
        source: a.currency==='CLP' ? 'normalized' : 'normalized',
      };
    }),

    // Helpers
    getBank: code => banks.find(b=>b.code===code),
    getSociety: id => societies.find(s=>s.id===id),
    getAccount: id => accounts.find(a=>a.id===id),
    getBucket: id => buckets.find(b=>b.id===id),
    formatUSD(v, compact=true){
      if (v==null) return '—';
      const abs=Math.abs(v);
      if (compact && abs>=1e6) return (v/1e6).toFixed(2)+'M';
      if (compact && abs>=1e3) return (v/1e3).toFixed(1)+'K';
      return v.toLocaleString('en-US',{maximumFractionDigits:0});
    },
    formatCLP(v, compact=true){
      if (v==null) return '—';
      const abs=Math.abs(v);
      if (compact && abs>=1e9) return (v/1e9).toFixed(2)+' MMM';
      if (compact && abs>=1e6) return (v/1e6).toFixed(1)+' MM';
      return v.toLocaleString('es-CL',{maximumFractionDigits:0});
    },
    formatPct(v){ if (v==null) return '—'; return (v>=0?'+':'')+v.toFixed(2)+'%'; }
  };
})();
