import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Nado: wss://gateway.prod.nado.xyz/v1/subscribe
// Requires header: Sec-WebSocket-Extensions: permessage-deflate
// Channels: best_bid_offer (event-driven), book_depth (~50ms batched)
// Pair format: "BTC-PERP", "ETH-PERP", "SOL-PERP", "BNB-PERP"

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 25000; // Nado expects ping every 30s

export class NadoAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private shouldReconnect = true;
  private canonicalByExSymbol = new Map<string, string>();

  constructor(config: ExchangeConfig) {
    super('nado');
    this.wsUrl = config.ws_url ?? 'wss://gateway.prod.nado.xyz/v1/subscribe';
    this.restUrl = config.rest_url ?? 'https://gateway.prod.nado.xyz/v1';
  }

  mapPairToExchange(canonical: string): string {
    // Nado uses same format: "BTC-PERP"
    return canonical;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.canonicalByExSymbol.clear();
    for (const c of pairs) this.canonicalByExSymbol.set(this.mapPairToExchange(c), c);
    this.shouldReconnect = true;
    await this.connectWs();
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
      const ws = new WebSocket(this.wsUrl, {
        perMessageDeflate: true,
        headers: { 'Sec-WebSocket-Extensions': 'permessage-deflate' },
      });
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

    for (const canonical of this.pairs) {
      const market = this.mapPairToExchange(canonical);
      // Subscribe to best_bid_offer (fires on change, no throttling)
      this.ws.send(JSON.stringify({
        method: 'subscribe',
        params: { channel: 'best_bid_offer', market },
      }));
    }
    this.log.info({ count: this.pairs.length }, 'Subscribed to best_bid_offer');
  }

  private handleMessage(msg: any) {
    const now = Date.now();

    if (msg.channel === 'best_bid_offer' || msg.type === 'best_bid_offer') {
      this.handleBbo(msg.data ?? msg, now);
    }
  }

  private handleBbo(data: any, now: number) {
    const market = data.market ?? data.symbol;
    if (!market) return;
    const canonical = this.canonicalByExSymbol.get(market);
    if (!canonical) return;

    const bid = parseFloat(data.best_bid_price ?? data.bid ?? '0');
    const ask = parseFloat(data.best_ask_price ?? data.ask ?? '0');
    if (!bid || !ask) return;

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

registerAdapter('nado', (config) => new NadoAdapter(config));
