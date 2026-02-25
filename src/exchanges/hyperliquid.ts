import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Hyperliquid uses simple coin names: "BTC", "ETH", "SOL", "HYPE", "BNB"
// allMids subscription gives real-time mid prices for ALL assets in one stream
// For bid/ask we use l2Book or metaAndAssetCtxs REST fallback

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 15000;

export class HyperliquidAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private canonicalByExSymbol = new Map<string, string>();
  private shouldReconnect = true;

  constructor(config: ExchangeConfig) {
    super('hyperliquid');
    this.wsUrl = config.ws_url ?? 'wss://api.hyperliquid.xyz/ws';
    this.restUrl = config.rest_url ?? 'https://api.hyperliquid.xyz';
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC"
    return canonical.replace(/-PERP$/, '');
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);

    // Build reverse lookup for fast mapping
    this.canonicalByExSymbol.clear();
    for (const canonical of pairs) {
      const exSym = this.mapPairToExchange(canonical);
      this.canonicalByExSymbol.set(exSym, canonical);
    }

    this.shouldReconnect = true;
    await this.connectWs();

    // Also fetch initial full price snapshot via REST
    await this.fetchRestSnapshot();
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    this.clearTimers();
    if (this.ws) {
      this.ws.close(1000, 'disconnect');
      this.ws = null;
    }
    this.setConnected(false);
  }

  private connectWs(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.log.info({ url: this.wsUrl }, 'Connecting to WebSocket');
      const ws = new WebSocket(this.wsUrl);
      let resolved = false;

      ws.on('open', () => {
        this.log.info('WebSocket connected');
        this.ws = ws;
        this.reconnectAttempts = 0;
        this.setConnected(true);
        this.startPing();
        this.subscribe();
        if (!resolved) { resolved = true; resolve(); }
      });

      ws.on('message', (data) => {
        try {
          this.handleMessage(JSON.parse(data.toString()));
        } catch (err) {
          this.log.error({ err }, 'Failed to parse WS message');
        }
      });

      ws.on('close', (code, reason) => {
        this.log.warn({ code, reason: reason.toString() }, 'WebSocket closed');
        this.setConnected(false);
        this.clearTimers();
        if (this.shouldReconnect) this.scheduleReconnect();
        if (!resolved) { resolved = true; reject(new Error(`WS closed: ${code}`)); }
      });

      ws.on('error', (err) => {
        this.log.error({ err: err.message }, 'WebSocket error');
        this.emit('error', err);
        if (!resolved) { resolved = true; reject(err); }
      });
    });
  }

  private subscribe() {
    if (!this.ws) return;

    // Subscribe to allMids - gives mid prices for ALL assets in one stream
    this.ws.send(JSON.stringify({
      method: 'subscribe',
      subscription: { type: 'allMids' },
    }));
    this.log.info('Subscribed to allMids');

    // Subscribe to l2Book for each tracked pair to get bid/ask
    for (const canonical of this.pairs) {
      const coin = this.mapPairToExchange(canonical);
      this.ws.send(JSON.stringify({
        method: 'subscribe',
        subscription: { type: 'l2Book', coin },
      }));
    }
    this.log.info({ count: this.pairs.length }, 'Subscribed to l2Book for pairs');
  }

  private handleMessage(msg: any) {
    if (!msg.channel) return;

    const now = Date.now();

    if (msg.channel === 'allMids') {
      this.handleAllMids(msg.data, now);
    } else if (msg.channel === 'l2Book') {
      this.handleL2Book(msg.data, now);
    }
  }

  private handleAllMids(data: { mids: Record<string, string> }, now: number) {
    const { mids } = data;
    for (const [coin, midStr] of Object.entries(mids)) {
      const canonical = this.canonicalByExSymbol.get(coin);
      if (!canonical) continue; // Not a tracked pair

      const mid = parseFloat(midStr);
      if (isNaN(mid) || mid <= 0) continue;

      // allMids only gives mid price; bid/ask will be updated by l2Book
      // But we still emit with mid as a baseline
      const priceData: PriceData = {
        exchange: this.name,
        pair: canonical,
        bid: mid,
        ask: mid,
        mid,
        timestamp: now,
        source: 'ws',
      };
      this.emitPrice(priceData);
    }
  }

  private handleL2Book(data: any, now: number) {
    const coin = data.coin as string;
    const canonical = this.canonicalByExSymbol.get(coin);
    if (!canonical) return;

    const levels = data.levels;
    if (!levels || levels.length < 2) return;

    // levels[0] = bids (sorted desc), levels[1] = asks (sorted asc)
    const bids = levels[0];
    const asks = levels[1];

    if (!bids?.length || !asks?.length) return;

    const bid = parseFloat(bids[0].px);
    const ask = parseFloat(asks[0].px);
    if (isNaN(bid) || isNaN(ask)) return;

    const mid = (bid + ask) / 2;

    const priceData: PriceData = {
      exchange: this.name,
      pair: canonical,
      bid,
      ask,
      mid,
      timestamp: now,
      source: 'ws',
    };
    this.emitPrice(priceData);
  }

  private async fetchRestSnapshot() {
    try {
      const res = await fetch(`${this.restUrl}/info`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'metaAndAssetCtxs' }),
      });
      const [meta, assetCtxs] = await res.json() as [any, any[]];
      const now = Date.now();

      const universe = meta.universe as { name: string }[];

      for (let i = 0; i < universe.length; i++) {
        const coin = universe[i].name;
        const canonical = this.canonicalByExSymbol.get(coin);
        if (!canonical) continue;

        const ctx = assetCtxs[i];
        if (!ctx) continue;

        const mid = parseFloat(ctx.midPx ?? ctx.markPx ?? '0');
        const bid = mid; // REST snapshot doesn't have BBO; WS l2Book will refine
        const ask = mid;

        if (mid <= 0) continue;

        const priceData: PriceData = {
          exchange: this.name,
          pair: canonical,
          bid,
          ask,
          mid,
          timestamp: now,
          source: 'rest',
        };
        this.emitPrice(priceData);
      }

      this.log.info({ count: universe.length }, 'REST snapshot loaded');
    } catch (err) {
      this.log.error({ err }, 'Failed to fetch REST snapshot');
    }
  }

  private startPing() {
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.ping();
      }
    }, PING_INTERVAL_MS);
  }

  private scheduleReconnect() {
    const delay = Math.min(
      RECONNECT_BASE_MS * Math.pow(2, this.reconnectAttempts),
      RECONNECT_MAX_MS,
    );
    this.reconnectAttempts++;
    this.log.info({ delay, attempt: this.reconnectAttempts }, 'Scheduling reconnect');

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this.connectWs();
      } catch {
        // connectWs rejection will trigger another reconnect via close handler
      }
    }, delay);
  }

  private clearTimers() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
  }
}

registerAdapter('hyperliquid', (config) => new HyperliquidAdapter(config));
