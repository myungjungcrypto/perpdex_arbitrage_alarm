// PerpDEX Arbitrage Monitor - Dashboard Frontend
(() => {
  'use strict';

  // ── State ──
  const state = {
    prices: new Map(),   // "pair:exchange" -> PriceData
    spreads: new Map(),  // "pair:exA:exB" -> SpreadData
    statuses: new Map(), // exchange -> ExchangeStatus
    exchanges: new Set(),
    pairs: new Set(),
    connected: false,
  };

  let ws = null;
  let reconnectTimer = null;
  let renderTimer = null;
  const RENDER_THROTTLE_MS = 150;

  // ── DOM refs ──
  const $ = (sel) => document.querySelector(sel);
  const connStatus = $('#connection-status');
  const exchangeStatusesEl = $('#exchange-statuses');
  const spreadBody = $('#spread-body');
  const priceHead = $('#price-head');
  const priceBody = $('#price-body');
  const alertLog = $('#alert-log');
  const filterPair = $('#filter-pair');
  const filterExchange = $('#filter-exchange');
  const filterMinSpread = $('#filter-min-spread');
  const sortBy = $('#sort-by');
  const statsEl = $('#stats');

  // ── WebSocket ──
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}`;
    ws = new WebSocket(url);

    ws.onopen = () => {
      state.connected = true;
      connStatus.textContent = 'Connected';
      connStatus.className = 'status connected';
    };

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        handleMessage(msg);
      } catch { /* ignore parse errors */ }
    };

    ws.onclose = () => {
      state.connected = false;
      connStatus.textContent = 'Disconnected';
      connStatus.className = 'status disconnected';
      scheduleReconnect();
    };

    ws.onerror = () => {
      ws.close();
    };
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    connStatus.textContent = 'Reconnecting...';
    connStatus.className = 'status reconnecting';
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, 2000);
  }

  // ── Message handling ──
  function handleMessage(msg) {
    if (msg.type === 'snapshot') {
      // Full reset
      state.prices.clear();
      state.spreads.clear();
      state.statuses.clear();
      state.exchanges.clear();
      state.pairs.clear();
    }

    if (msg.prices) {
      for (const p of msg.prices) {
        state.prices.set(`${p.pair}:${p.exchange}`, p);
        state.exchanges.add(p.exchange);
        state.pairs.add(p.pair);
      }
    }

    if (msg.spreads) {
      for (const s of msg.spreads) {
        const [exA, exB] = [s.longExchange, s.shortExchange].sort();
        state.spreads.set(`${s.pair}:${exA}:${exB}`, s);
      }
    }

    if (msg.statuses) {
      for (const s of msg.statuses) {
        state.statuses.set(s.exchange, s);
        state.exchanges.add(s.exchange);
      }
    }

    scheduleRender();
  }

  // ── Render throttle ──
  function scheduleRender() {
    if (renderTimer) return;
    renderTimer = setTimeout(() => {
      renderTimer = null;
      render();
    }, RENDER_THROTTLE_MS);
  }

  // ── Render ──
  function render() {
    renderExchangeStatuses();
    renderSpreadTable();
    renderPriceTable();
    renderStats();
  }

  function renderExchangeStatuses() {
    const exchanges = Array.from(state.statuses.values()).sort((a, b) => a.exchange.localeCompare(b.exchange));
    exchangeStatusesEl.innerHTML = exchanges.map((s) => {
      const age = s.lastMessageAt ? formatAge(Date.now() - s.lastMessageAt) : '-';
      return `<div class="exchange-chip">
        <span class="dot ${s.connected ? 'on' : 'off'}"></span>
        <span>${s.exchange}</span>
        <span class="meta">${s.subscribedPairs}p | ${age}</span>
      </div>`;
    }).join('');
  }

  function renderSpreadTable() {
    const pairFilter = filterPair.value.toUpperCase();
    const exFilter = filterExchange.value.toLowerCase();
    const minSpread = parseFloat(filterMinSpread.value) || 0;
    const sort = sortBy.value;

    let spreads = Array.from(state.spreads.values());

    // Filter
    if (pairFilter) {
      spreads = spreads.filter((s) => s.pair.includes(pairFilter));
    }
    if (exFilter) {
      spreads = spreads.filter((s) =>
        s.longExchange.toLowerCase().includes(exFilter) ||
        s.shortExchange.toLowerCase().includes(exFilter)
      );
    }
    if (minSpread > 0) {
      spreads = spreads.filter((s) => Math.abs(s.spreadPct) >= minSpread);
    }

    // Sort
    spreads.sort((a, b) => {
      switch (sort) {
        case 'spreadPct': return Math.abs(b.spreadPct) - Math.abs(a.spreadPct);
        case 'spreadAbs': return Math.abs(b.spreadAbs) - Math.abs(a.spreadAbs);
        case 'pair': return a.pair.localeCompare(b.pair);
        case 'timestamp': return b.timestamp - a.timestamp;
        default: return 0;
      }
    });

    // Limit display to top 200 for performance
    const display = spreads.slice(0, 200);
    const now = Date.now();

    spreadBody.innerHTML = display.map((s) => {
      const isSlow = !!s.isSlowSpread;
      const pctClass = s.spreadPct >= 0.5 ? 'spread-high' :
                        s.spreadPct > 0 ? 'spread-positive' : 'spread-negative';
      const rowClass = isSlow ? 'spread-slow' : '';
      const slowTag = isSlow ? '<span class="slow-tag">vs AVG</span>' : '';
      const age = formatAge(now - s.timestamp);
      return `<tr class="${rowClass}">
        <td><strong>${s.pair}</strong>${slowTag}</td>
        <td>${s.longExchange}</td>
        <td class="num price-ask">$${fmt(s.longAsk)}</td>
        <td>${s.shortExchange}</td>
        <td class="num price-bid">$${fmt(s.shortBid)}</td>
        <td class="num ${pctClass}">$${fmt(s.spreadAbs)}</td>
        <td class="num ${pctClass}">${s.spreadPct.toFixed(4)}%</td>
        <td class="num">${age}</td>
      </tr>`;
    }).join('');
  }

  function renderPriceTable() {
    const pairFilter = filterPair.value.toUpperCase();
    const exchanges = Array.from(state.exchanges).sort();

    let pairs = Array.from(state.pairs).sort();
    if (pairFilter) {
      pairs = pairs.filter((p) => p.includes(pairFilter));
    }

    // Limit to 100 pairs for price table
    pairs = pairs.slice(0, 100);

    // Header
    priceHead.innerHTML = `<tr>
      <th>Pair</th>
      ${exchanges.map((e) => `<th>${e}<br><span style="font-weight:normal;font-size:10px">bid / ask</span></th>`).join('')}
    </tr>`;

    const now = Date.now();

    // Body
    priceBody.innerHTML = pairs.map((pair) => {
      const cells = exchanges.map((ex) => {
        const key = `${pair}:${ex}`;
        const p = state.prices.get(key);
        if (!p) return '<td class="num price-cell">-</td>';

        const stale = (now - p.timestamp) > 10000;
        const cls = stale ? 'price-stale' : '';
        return `<td class="num price-cell ${cls}">
          <span class="price-bid">${fmt(p.bid)}</span> / <span class="price-ask">${fmt(p.ask)}</span>
        </td>`;
      }).join('');

      return `<tr><td><strong>${pair}</strong></td>${cells}</tr>`;
    }).join('');
  }

  function renderStats() {
    statsEl.textContent = `${state.prices.size} prices | ${state.spreads.size} spreads | ${state.exchanges.size} exchanges | ${state.pairs.size} pairs`;
  }

  // ── Utils ──
  function fmt(num) {
    if (num === undefined || num === null) return '-';
    if (num >= 1000) return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (num >= 1) return num.toFixed(4);
    return num.toFixed(6);
  }

  function formatAge(ms) {
    if (ms < 0) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    if (ms < 3600000) return `${(ms / 60000).toFixed(0)}m`;
    return `${(ms / 3600000).toFixed(0)}h`;
  }

  // ── Event listeners ──
  filterPair.addEventListener('input', scheduleRender);
  filterExchange.addEventListener('input', scheduleRender);
  filterMinSpread.addEventListener('input', scheduleRender);
  sortBy.addEventListener('change', scheduleRender);

  // ── Init ──
  connect();

  // Re-render every second for age updates
  setInterval(() => {
    if (state.prices.size > 0) render();
  }, 1000);
})();
