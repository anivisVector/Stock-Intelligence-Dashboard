let companies = [];
let activeSymbol = null;
let activeDays = 30;

let showClose = true;
let showMA = true;

let priceChart = null;
let compareChart = null;

let __hoverLineRegistered = false;
let __priceCountAnimId = 0;

function clamp(x, min, max) {
  const n = Number(x);
  if (!Number.isFinite(n)) return min;
  return Math.min(max, Math.max(min, n));
}

function setError(msg) {
  const el = document.getElementById('errorBox');
  const text = msg || '';
  el.textContent = text;
  el.classList.toggle('show', Boolean(text));
}

function setStatus(msg) {
  document.getElementById('statusText').textContent = msg;
}

function setGlobalLoading(on, hint) {
  const overlay = document.getElementById('appLoading');
  const elHint = document.getElementById('loadingHint');
  if (typeof hint === 'string') elHint.textContent = hint;
  overlay.classList.toggle('show', Boolean(on));
  overlay.setAttribute('aria-hidden', String(!on));
}

function setCardLoading(id, on) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('show', Boolean(on));
}

function setEmpty(id, on, text) {
  const el = document.getElementById(id);
  if (!el) return;
  if (typeof text === 'string') el.textContent = text;
  el.style.display = on ? 'block' : 'none';
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return '—';
  return Number(x).toFixed(digits);
}

function fmtMoney(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return '—';
  const n = Number(x);
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtInt(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return '—';
  const n = Number(x);
  return Math.round(n).toLocaleString();
}

function setTrend(el, direction) {
  if (!el) return;
  el.classList.remove('up', 'down', 'flat');
  if (direction === 'up') {
    el.classList.add('up');
    el.textContent = '▲';
  } else if (direction === 'down') {
    el.classList.add('down');
    el.textContent = '▼';
  } else {
    el.classList.add('flat');
    el.textContent = '•';
  }
}

function setSkeleton(on) {
  const ids = [
    'statPrice',
    'statChangePct',
    'statChangeAbs',
    'statVolume',
    'metricHigh',
    'metricLow',
    'metricAvg',
    'metricVol',
    'gainerSymbol',
    'gainerMeta',
    'loserSymbol',
    'loserMeta',
  ];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.toggle('skeleton', Boolean(on));
  });
}

function setChip(id, text, level) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.classList.remove('good', 'warn', 'bad', 'neutral');
  if (level) el.classList.add(level);
}

function resetInsightUI() {
  setChip('chipTrend', 'Trend', 'neutral');
  setChip('chipRisk', 'Risk', 'neutral');
  setChip('chipVol', 'Vol', 'neutral');
  const meter = document.getElementById('volMeter');
  const meterValue = document.getElementById('volMeterValue');
  if (meter) meter.style.setProperty('--p', '0');
  if (meterValue) meterValue.textContent = '—';
}

function updateVolMeter(volatility) {
  const meter = document.getElementById('volMeter');
  const meterValue = document.getElementById('volMeterValue');
  const v = Number(volatility);
  if (!meter || !meterValue || !Number.isFinite(v)) {
    resetInsightUI();
    return;
  }

  // volatility is typically a small decimal (e.g., 0.01–0.04). Map to 0–100.
  const pct = clamp(v * 1000, 0, 100);
  meter.style.setProperty('--p', String(pct));
  meterValue.textContent = `${(v * 100).toFixed(2)}%`;
}

function updateInsightChips() {
  const elPrice = document.getElementById('statPrice');
  const pct = Number(elPrice?.dataset?.pct || NaN);
  const vol = Number(document.getElementById('metricVol')?.textContent || NaN);

  // Trend chip (based on last candle change)
  if (Number.isFinite(pct)) {
    if (pct >= 0.75) setChip('chipTrend', 'Trend · Up', 'good');
    else if (pct <= -0.75) setChip('chipTrend', 'Trend · Down', 'bad');
    else setChip('chipTrend', 'Trend · Flat', 'neutral');
  } else {
    setChip('chipTrend', 'Trend', 'neutral');
  }

  // Vol chip (based on 30d volatility)
  if (Number.isFinite(vol)) {
    if (vol >= 0.03) setChip('chipVol', 'Vol · High', 'bad');
    else if (vol >= 0.015) setChip('chipVol', 'Vol · Med', 'warn');
    else setChip('chipVol', 'Vol · Low', 'good');
  } else {
    setChip('chipVol', 'Vol', 'neutral');
  }

  // Risk chip (simple blend)
  if (Number.isFinite(vol) && Number.isFinite(pct)) {
    const riskScore = Math.abs(pct) + vol * 100; // pct points + vol percent
    if (riskScore >= 6) setChip('chipRisk', 'Risk · High', 'bad');
    else if (riskScore >= 3) setChip('chipRisk', 'Risk · Med', 'warn');
    else setChip('chipRisk', 'Risk · Low', 'good');
  } else if (Number.isFinite(vol)) {
    if (vol >= 0.03) setChip('chipRisk', 'Risk · High', 'bad');
    else if (vol >= 0.015) setChip('chipRisk', 'Risk · Med', 'warn');
    else setChip('chipRisk', 'Risk · Low', 'good');
  } else {
    setChip('chipRisk', 'Risk', 'neutral');
  }
}

function asArray(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.data)) return payload.data;
  if (payload && Array.isArray(payload.rows)) return payload.rows;
  return null;
}

async function apiGet(path) {
  // API calls unchanged: same endpoints + query patterns
  const res = await fetch(path, { cache: 'no-store' });
  const text = await res.text();

  let body;
  let parsed = false;
  try {
    body = text ? JSON.parse(text) : null;
    parsed = true;
  } catch {
    body = null;
  }

  if (!res.ok) {
    const msg =
      (body && typeof body === 'object' && body.error) ||
      `Request failed (${res.status}): ${text?.slice(0, 180) || 'No response body'}`;
    throw new Error(msg);
  }

  // JSON can legitimately be `null` (e.g., /top-losers when no data).
  // Only treat as non-JSON if parsing failed.
  if (!parsed) {
    throw new Error(`Non-JSON response from ${path}: ${text?.slice(0, 180) || 'No response body'}`);
  }

  return body;
}

function getVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function getChartTheme() {
  const text = getVar('--text') || '#e5e7eb';
  const muted = getVar('--muted') || '#94a3b8';
  const border = getVar('--border') || 'rgba(148,163,184,.18)';
  return { text, muted, border };
}

function buildLineDataset(label, data, color) {
  return {
    label,
    data,
    borderColor: color,
    backgroundColor: color,
    pointRadius: 0,
    tension: 0.32,
    borderWidth: 2,
    fill: false,
  };
}

function registerHoverLinePlugin() {
  if (__hoverLineRegistered) return;
  if (typeof Chart === 'undefined') return;

  const plugin = {
    id: 'hoverLine',
    afterDraw(chart) {
      const tooltip = chart.tooltip;
      if (!tooltip) return;
      const active = tooltip.getActiveElements ? tooltip.getActiveElements() : (tooltip._active || []);
      if (!active || active.length === 0) return;

      const { ctx, chartArea } = chart;
      const x = active[0].element && active[0].element.x;
      if (!Number.isFinite(x)) return;

      const theme = getChartTheme();
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.lineWidth = 1;
      ctx.strokeStyle = theme.border || 'rgba(148,163,184,.18)';
      ctx.globalAlpha = 0.95;
      ctx.stroke();
      ctx.restore();
    },
  };

  Chart.register(plugin);
  __hoverLineRegistered = true;
}

function animateMoney(el, from, to, durationMs = 650) {
  if (!el) return;
  const start = performance.now();
  const animToken = ++__priceCountAnimId;

  const fmt = (n) =>
    Number(n).toLocaleString(undefined, {
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    });

  const step = (now) => {
    if (animToken !== __priceCountAnimId) return;
    const t = clamp((now - start) / durationMs, 0, 1);
    // easeOutCubic
    const e = 1 - Math.pow(1 - t, 3);
    const v = from + (to - from) * e;
    el.textContent = fmt(v);
    if (t < 1) requestAnimationFrame(step);
  };

  requestAnimationFrame(step);
}

function ensurePriceChart() {
  const ctx = document.getElementById('priceChart');
  if (priceChart) return priceChart;
  const theme = getChartTheme();

  registerHoverLinePlugin();

  Chart.defaults.font.family = 'Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
  Chart.defaults.color = theme.muted;

  priceChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      animation: { duration: 900, easing: 'easeOutQuart' },
      transitions: {
        active: { animation: { duration: 260 } },
        resize: { animation: { duration: 320 } },
      },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, labels: { usePointStyle: true, boxWidth: 8, color: theme.muted } },
        tooltip: { backgroundColor: 'rgba(17,24,39,.92)', titleColor: '#fff', bodyColor: '#fff' },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, color: theme.muted }, grid: { color: theme.border } },
        y: { beginAtZero: false, ticks: { color: theme.muted }, grid: { color: theme.border } },
      },
    },
  });

  return priceChart;
}

function ensureCompareChart() {
  const ctx = document.getElementById('compareChart');
  if (compareChart) return compareChart;
  const theme = getChartTheme();

  registerHoverLinePlugin();

  compareChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      animation: { duration: 900, easing: 'easeOutQuart' },
      transitions: {
        active: { animation: { duration: 260 } },
        resize: { animation: { duration: 320 } },
      },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, labels: { usePointStyle: true, boxWidth: 8, color: theme.muted } },
        tooltip: { backgroundColor: 'rgba(17,24,39,.92)', titleColor: '#fff', bodyColor: '#fff' },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, color: theme.muted }, grid: { color: theme.border } },
        y: { beginAtZero: false, ticks: { color: theme.muted }, grid: { color: theme.border } },
      },
    },
  });

  return compareChart;
}

function renderCompanyList() {
  const list = document.getElementById('companyList');
  const q = (document.getElementById('companySearch')?.value || '').trim().toLowerCase();
  list.innerHTML = '';

  const filtered = q
    ? companies.filter((c) => `${c.name} ${c.symbol}`.toLowerCase().includes(q))
    : companies;

  if (filtered.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = 'No companies found.';
    list.appendChild(empty);
    return;
  }

  filtered.forEach((c) => {
    const item = document.createElement('button');
    item.type = 'button';
    item.className = 'company-item' + (c.symbol === activeSymbol ? ' active' : '');

    const initials = (c.symbol || c.name || '??')
      .replace(/[^A-Za-z0-9]/g, '')
      .slice(0, 2)
      .toUpperCase();

    item.innerHTML = `
      <div class="company-left">
        <div class="avatar" aria-hidden="true">${initials}</div>
        <div class="company-mid">
          <div class="company-name">${c.name}</div>
          <div class="company-sub">${c.symbol}</div>
        </div>
      </div>
      <div class="company-right">
        <span>${c.symbol}</span>
      </div>
    `;
    item.onclick = () => selectCompany(c.symbol);
    list.appendChild(item);
  });
}

function setActiveFilter(days) {
  activeDays = days;
  document.getElementById('btn30').classList.toggle('active', days === 30);
  document.getElementById('btn90').classList.toggle('active', days === 90);
  document.getElementById('btn1y').classList.toggle('active', days === 365);
  if (activeSymbol) {
    // Refresh main chart + metrics
    loadCompany(activeSymbol)
      .then(() => {
        // If the user already ran a compare, keep it in sync with the new range.
        if (compareChart && compareChart.data && compareChart.data.datasets && compareChart.data.datasets.length > 0) {
          return runCompare().catch(() => {});
        }
      })
      .catch((err) => {
        setError(err?.message || String(err));
      });
  }
}

async function loadSummary(symbol) {
  const summary = await apiGet(`/summary/${encodeURIComponent(symbol)}`);
  document.getElementById('metricHigh').textContent = fmtNum(summary['52_week_high']);
  document.getElementById('metricLow').textContent = fmtNum(summary['52_week_low']);
  document.getElementById('metricAvg').textContent = fmtNum(summary.average_close);
  document.getElementById('metricVol').textContent = fmtNum(summary.volatility, 6);

  // Trend arrows are contextual: based on current price vs the metric value.
  const lastPrice = Number(document.getElementById('statPrice')?.dataset?.raw || NaN);
  const high = Number(summary['52_week_high']);
  const low = Number(summary['52_week_low']);
  const avg = Number(summary.average_close);

  setTrend(document.getElementById('trendHigh'), Number.isFinite(lastPrice) && Number.isFinite(high) && lastPrice >= high ? 'up' : 'flat');
  setTrend(document.getElementById('trendLow'), Number.isFinite(lastPrice) && Number.isFinite(low) && lastPrice <= low ? 'down' : 'flat');
  setTrend(document.getElementById('trendAvg'), Number.isFinite(lastPrice) && Number.isFinite(avg) ? (lastPrice >= avg ? 'up' : 'down') : 'flat');
  setTrend(document.getElementById('trendVol'), 'flat');

  updateVolMeter(summary.volatility);
  updateInsightChips();
}

async function loadMovers() {
  const [g, l] = await Promise.all([apiGet('/top-gainers'), apiGet('/top-losers')]);

  document.getElementById('gainerSymbol').textContent = g?.symbol || '—';
  document.getElementById('gainerMeta').textContent = g
    ? `Date: ${g.date} · Return: ${fmtNum(g.daily_return, 6)} · Close: ${fmtNum(g.close)}`
    : '—';

  document.getElementById('loserSymbol').textContent = l?.symbol || '—';
  document.getElementById('loserMeta').textContent = l
    ? `Date: ${l.date} · Return: ${fmtNum(l.daily_return, 6)} · Close: ${fmtNum(l.close)}`
    : '—';
}

function renderPriceDatasets(rows) {
  const labels = rows.map((r) => r.date);
  const close = rows.map((r) => r.close);
  const ma7 = rows.map((r) => r.ma7);

  const datasets = [];
  if (showClose) datasets.push(buildLineDataset('Close', close, '#4f46e5'));
  if (showMA) datasets.push(buildLineDataset('MA(7)', ma7, '#22c55e'));

  const chart = ensurePriceChart();
  chart.data.labels = labels;
  chart.data.datasets = datasets;
  chart.update();
}

async function loadCompany(symbol) {
  setError('');
  setStatus('Loading data…');
  document.getElementById('activeSymbol').textContent = symbol;
  const sym2 = document.getElementById('activeSymbol2');
  if (sym2) sym2.textContent = symbol;

  setSkeleton(true);
  setCardLoading('priceLoading', true);
  setEmpty('priceEmpty', false);

  let rows;
  try {
    const payload = await apiGet(`/data/${encodeURIComponent(symbol)}?days=${activeDays}`);
    rows = asArray(payload);
    if (!rows) {
      const msg =
        (payload && typeof payload === 'object' && payload.error) ||
        'Unexpected response from /data endpoint.';
      throw new Error(msg);
    }
  } catch (err) {
    const chart = ensurePriceChart();
    chart.data.labels = [];
    chart.data.datasets = [];
    chart.update();

    document.getElementById('metricHigh').textContent = '—';
    document.getElementById('metricLow').textContent = '—';
    document.getElementById('metricAvg').textContent = '—';
    document.getElementById('metricVol').textContent = '—';

    document.getElementById('statPrice').textContent = '—';
    document.getElementById('statPrice').dataset.raw = '';
    document.getElementById('statPrice').dataset.pct = '';
    document.getElementById('statChangePct').textContent = '—';
    document.getElementById('statChangeAbs').textContent = '—';
    document.getElementById('statVolume').textContent = '—';
    setTrend(document.getElementById('statTrend'), 'flat');

    resetInsightUI();

    setCardLoading('priceLoading', false);
    setEmpty('priceEmpty', true, 'No data to display.');
    setStatus('No data available (offline/rate-limited).');
    setSkeleton(false);
    setError(err?.message || String(err));
    return;
  }

  setCardLoading('priceLoading', false);

  if (!rows || rows.length === 0) {
    setEmpty('priceEmpty', true, 'No data returned for this range.');
    setStatus('No data returned.');
    setSkeleton(false);
    resetInsightUI();
    return;
  }

  // Header stats: last price, change, volume
  const last = rows[rows.length - 1];
  const prev = rows.length > 1 ? rows[rows.length - 2] : null;
  const lastClose = Number(last?.close);
  const prevClose = Number(prev?.close);
  const volume = last?.volume;

  const elPrice = document.getElementById('statPrice');

  const prevRaw = Number(elPrice?.dataset?.raw || NaN);
  if (Number.isFinite(lastClose)) {
    if (Number.isFinite(prevRaw) && Math.abs(lastClose - prevRaw) > 0) {
      animateMoney(elPrice, prevRaw, lastClose);
    } else {
      elPrice.textContent = fmtMoney(lastClose);
    }
    elPrice.dataset.raw = String(lastClose);
  } else {
    elPrice.textContent = '—';
    elPrice.dataset.raw = '';
  }

  if (Number.isFinite(lastClose) && Number.isFinite(prevClose) && prevClose !== 0) {
    const delta = lastClose - prevClose;
    const pct = (delta / prevClose) * 100;
    elPrice.dataset.pct = Number.isFinite(pct) ? String(pct) : '';
    document.getElementById('statChangePct').textContent = `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
    document.getElementById('statChangeAbs').textContent = `(${delta >= 0 ? '+' : ''}${delta.toFixed(2)})`;
    setTrend(document.getElementById('statTrend'), pct > 0 ? 'up' : pct < 0 ? 'down' : 'flat');
  } else {
    elPrice.dataset.pct = '';
    document.getElementById('statChangePct').textContent = '—';
    document.getElementById('statChangeAbs').textContent = '—';
    setTrend(document.getElementById('statTrend'), 'flat');
  }

  document.getElementById('statVolume').textContent = fmtInt(volume);

  renderPriceDatasets(rows);
  await loadSummary(symbol);
  const firstDate = rows[0]?.date;
  const lastDate = rows[rows.length - 1]?.date;
  const rangeLabel = activeDays === 365 ? '1y' : activeDays + 'd';
  const rangeSuffix = firstDate && lastDate ? ` (${rows.length} pts, ${firstDate} → ${lastDate})` : ` (${rows.length} pts)`;
  setStatus(`Showing ${rangeLabel}${rangeSuffix} for ${symbol}`);

  setSkeleton(false);
}

async function selectCompany(symbol) {
  activeSymbol = symbol;
  renderCompanyList();
  await loadCompany(symbol);
}

function populateCompareSelects() {
  const s1 = document.getElementById('cmp1');
  const s2 = document.getElementById('cmp2');
  s1.innerHTML = '';
  s2.innerHTML = '';

  companies.forEach((c) => {
    const o1 = document.createElement('option');
    o1.value = c.symbol;
    o1.textContent = `${c.name} (${c.symbol})`;
    s1.appendChild(o1);

    const o2 = document.createElement('option');
    o2.value = c.symbol;
    o2.textContent = `${c.name} (${c.symbol})`;
    s2.appendChild(o2);
  });

  if (companies.length >= 2) {
    s1.value = companies[0].symbol;
    s2.value = companies[1].symbol;
  }
}

async function runCompare() {
  setError('');
  const symbol1 = document.getElementById('cmp1').value;
  const symbol2 = document.getElementById('cmp2').value;

  if (!symbol1 || !symbol2) return;
  if (symbol1 === symbol2) {
    setError('Please choose two different symbols to compare.');
    setEmpty('compareEmpty', true, 'Select two different symbols to compare.');
    return;
  }

  setEmpty('compareEmpty', false);
  setCardLoading('compareLoading', true);
  setStatus('Loading comparison…');

  let rows;
  try {
    const payload = await apiGet(
      `/compare?symbol1=${encodeURIComponent(symbol1)}&symbol2=${encodeURIComponent(symbol2)}&days=${activeDays}`
    );
    rows = asArray(payload);
    if (!rows) {
      const msg =
        (payload && typeof payload === 'object' && payload.error) ||
        'Unexpected response from /compare endpoint.';
      throw new Error(msg);
    }
  } catch (err) {
    const chart = ensureCompareChart();
    chart.data.labels = [];
    chart.data.datasets = [];
    chart.update();
    setCardLoading('compareLoading', false);
    setEmpty('compareEmpty', true, 'No comparison data available.');
    setError(err?.message || String(err));
    return;
  }

  setCardLoading('compareLoading', false);

  if (!rows || rows.length === 0) {
    const chart = ensureCompareChart();
    chart.data.labels = [];
    chart.data.datasets = [];
    chart.update();
    setEmpty('compareEmpty', true, 'No overlapping dates found for these symbols.');
    setStatus('No comparison data returned.');
    return;
  }

  const labels = rows.map((r) => r.date);
  const series1 = rows.map((r) => r[symbol1]);
  const series2 = rows.map((r) => r[symbol2]);

  const chart = ensureCompareChart();
  chart.data.labels = labels;
  chart.data.datasets = [
    buildLineDataset(`${symbol1} Close`, series1, '#7c3aed'),
    buildLineDataset(`${symbol2} Close`, series2, '#f97316'),
  ];
  chart.update();

  setStatus(`Comparing ${symbol1} vs ${symbol2} (${activeDays === 365 ? '1y' : activeDays + 'd'})`);
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('theme', theme);

  // Recreate charts so grid/tick colors update cleanly.
  if (priceChart) {
    priceChart.destroy();
    priceChart = null;
  }
  if (compareChart) {
    compareChart.destroy();
    compareChart = null;
  }

  ensurePriceChart();
  ensureCompareChart();
}

function initTheme() {
  const saved = localStorage.getItem('theme');
  const prefersLight = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches;
  const theme = saved || (prefersLight ? 'light' : 'dark');
  applyTheme(theme);
}

async function init() {
  try {
    setGlobalLoading(true, 'Loading companies…');
    setStatus('Loading companies…');

    companies = await apiGet('/companies');
    renderCompanyList();
    populateCompareSelects();

    document.getElementById('btn30').onclick = () => setActiveFilter(30);
    document.getElementById('btn90').onclick = () => setActiveFilter(90);
    document.getElementById('btn1y').onclick = () => setActiveFilter(365);

    document.getElementById('btnCompare').onclick = () => runCompare();
    document.getElementById('navCompare').onclick = () => {
      document.getElementById('compareSection').scrollIntoView({ behavior: 'smooth', block: 'start' });
      runCompare().catch(() => {});
    };

    document.getElementById('toggleClose').onclick = () => {
      showClose = !showClose;
      document.getElementById('toggleClose').classList.toggle('active', showClose);
      if (activeSymbol) loadCompany(activeSymbol).catch(() => {});
    };

    document.getElementById('toggleMA').onclick = () => {
      showMA = !showMA;
      document.getElementById('toggleMA').classList.toggle('active', showMA);
      if (activeSymbol) loadCompany(activeSymbol).catch(() => {});
    };

    document.getElementById('companySearch').addEventListener('input', () => renderCompanyList());

    document.addEventListener('keydown', (e) => {
      if (e.key === '/' && document.activeElement?.tagName !== 'INPUT') {
        e.preventDefault();
        document.getElementById('companySearch').focus();
      }
    });

    document.getElementById('themeToggle').onclick = () => {
      const current = document.documentElement.getAttribute('data-theme') || 'dark';
      applyTheme(current === 'dark' ? 'light' : 'dark');
      // re-render charts with existing data
      if (activeSymbol) loadCompany(activeSymbol).catch(() => {});
    };

    await loadMovers();

    if (companies.length > 0) {
      // Best-effort initial selection: missing CSV shouldn't crash the whole UI.
      await selectCompany(companies[0].symbol).catch((err) => {
        setError(err?.message || String(err));
      });
    } else {
      setStatus('No companies found.');
    }

    setGlobalLoading(false);
  } catch (err) {
    setError(err.message || String(err));
    setStatus('Failed to initialize.');
    setGlobalLoading(false);
  }
}

initTheme();
init();
