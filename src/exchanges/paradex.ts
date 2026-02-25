import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Paradex uses JSON-RPC 2.0 over WebSocket
// BBO channel is event-driven (no artificial throttling, fires on price/size change)
// markets_summary for mark/oracle prices
// Pair format: "BTC-USD-PERP", "ETH-USD-PERP", etc.

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 15000;

export class ParadexAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private rpcId = 0;
  private shouldReconnect = true;
  private canonicalByExSymbol = new Map<string, string>();

  constructor(config: ExchangeConfig) {
    super('paradex');
    // No trailing slash — /v1/ returns 404, /v1 is correct
    this.wsUrl = config.ws_url?.replace(/\/$/, '') ?? 'wss://ws.api.prod.paradex.trade/v1';
    this.restUrl = config.rest_url ?? 'https://api.prod.paradex.trade';
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC-USD-PERP"
    const base = canonical.replace(/-PERP$/, '');
    return `${base}-USD-PERP`;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.canonicalByExSymbol.clear();
    for (const canonical of pairs) {
      this.canonicalByExSymbol.set(this.mapPairToExchange(canonical), canonical);
    }
    this.shouldReconnect = true;
    await this.connectWs();
    await this.fetchRestSnapshot();
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    this.clearTimers();
    if (this.ws) { this.ws.close(1000); this.ws = null; }
    this.setConnected(false);
  }

  private connectWs(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.log.info({ url: this.wsUrl }, 'Connecting');
      const ws = new WebSocket(this.wsUrl);
      let resolved = false;

      ws.on('open', () => {
        this.log.info('Connected');
        this.ws = ws;
        this.reconnectAttempts = 0;
        this.setConnected(true);
        this.startPing();
        this.subscribe();
        if (!resolved) { resolved = true; resolve(); }
      });

      ws.on('message', (data) => {
        try { this.handleMessage(JSON.parse(data.toString())); }
        catch (err) { this.log.error({ err }, 'Parse error'); }
      });

      ws.on('close', (code) => {
        this.log.warn({ code }, 'Disconnected');
        this.setConnected(false);
        this.clearTimers();
        if (this.shouldReconnect) this.scheduleReconnect();
        if (!resolved) { resolved = true; reject(new Error(`WS closed: ${code}`)); }
      });

      ws.on('error', (err) => {
        this.log.error({ err: err.message }, 'WS error');
        this.emit('error', err);
        if (!resolved) { resolved = true; reject(err); }
      });
    });
  }

  private subscribe() {
    if (!this.ws) return;

    // Subscribe to BBO for each pair (event-driven, no refresh_rate needed)
    for (const canonical of this.pairs) {
      const exSym = this.mapPairToExchange(canonical);
      this.ws.send(JSON.stringify({
        jsonrpc: '2.0',
        method: 'subscribe',
        params: { channel: `bbo.${exSym}` },
        id: ++this.rpcId,
      }));
    }

    // Also subscribe to markets_summary for mark/oracle prices
    this.ws.send(JSON.stringify({
      jsonrpc: '2.0',
      method: 'subscribe',
      params: { channel: 'markets_summary' },
      id: ++this.rpcId,
    }));

    this.log.info({ count: this.pairs.length }, 'Subscribed to BBO + markets_summary');
  }

  private handleMessage(msg: any) {
    const now = Date.now();

    // Log subscription responses and errors
    if (msg.error) {
      this.log.warn({ error: msg.error, id: msg.id }, 'JSON-RPC error response');
    }
    if (msg.result !== undefined && msg.id) {
      this.log.debug({ result: msg.result, id: msg.id }, 'Subscription confirmed');
    }

    // JSON-RPC notification
    if (msg.params?.channel) {
      const channel = msg.params.channel as string;

      if (channel.startsWith('bbo.')) {
        this.handleBbo(msg.params.data, now);
      } else if (channel === 'markets_summary') {
        this.handleMarketsSummary(msg.params.data, now);
      }
    }
  }

  private handleBbo(data: any, now: number) {
    if (!data?.market) return;
    const canonical = this.canonicalByExSymbol.get(data.market);
    if (!canonical) return;

    const bid = parseFloat(data.bid);
    const ask = parseFloat(data.ask);
    if (isNaN(bid) || isNaN(ask)) return;

    this.emitPrice({
      exchange: this.name,
      pair: canonical,
      bid,
      ask,
      mid: (bid + ask) / 2,
      timestamp: now,
      source: 'ws',
    });
  }

  private handleMarketsSummary(data: any[], now: number) {
    if (!Array.isArray(data)) return;
    for (const item of data) {
      const canonical = this.canonicalByExSymbol.get(item.symbol ?? item.market);
      if (!canonical) continue;

      const mark = parseFloat(item.mark_price ?? '0');
      if (mark <= 0) continue;

      const bid = parseFloat(item.bid ?? String(mark));
      const ask = parseFloat(item.ask ?? String(mark));

      this.emitPrice({
        exchange: this.name,
        pair: canonical,
        bid: isNaN(bid) ? mark : bid,
        ask: isNaN(ask) ? mark : ask,
        mid: mark,
        timestamp: now,
        source: 'ws',
      });
    }
  }

  private async fetchRestSnapshot() {
    try {
      const res = await fetch(`${this.restUrl}/v1/markets/summary?market=ALL`);
      const data = await res.json() as { results: any[] };
      const now = Date.now();

      for (const item of (data.results ?? [])) {
        const canonical = this.canonicalByExSymbol.get(item.symbol);
        if (!canonical) continue;

        const mark = parseFloat(item.mark_price ?? '0');
        if (mark <= 0) continue;

        const bid = parseFloat(item.bid ?? String(mark));
        const ask = parseFloat(item.ask ?? String(mark));

        this.emitPrice({
          exchange: this.name,
          pair: canonical,
          bid: isNaN(bid) ? mark : bid,
          ask: isNaN(ask) ? mark : ask,
          mid: mark,
          timestamp: now,
          source: 'rest',
        });
      }
      this.log.info('REST snapshot loaded');
    } catch (err) {
      this.log.error({ err }, 'REST snapshot failed');
    }
  }

  private startPing() {
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) this.ws.ping();
    }, PING_INTERVAL_MS);
  }

  private scheduleReconnect() {
    const delay = Math.min(RECONNECT_BASE_MS * 2 ** this.reconnectAttempts, RECONNECT_MAX_MS);
    this.reconnectAttempts++;
    this.log.info({ delay, attempt: this.reconnectAttempts }, 'Reconnecting');
    this.reconnectTimer = setTimeout(async () => {
      try { await this.connectWs(); } catch { /* close handler retries */ }
    }, delay);
  }

  private clearTimers() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
  }
}

registerAdapter('paradex', (config) => new ParadexAdapter(config));
