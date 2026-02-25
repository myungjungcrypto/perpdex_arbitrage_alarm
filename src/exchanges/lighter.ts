import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Lighter: wss://mainnet.zklighter.elliot.ai/stream
// Order Book Channel: sends asks/bids every 50ms, initial snapshot then deltas
// Market indices: need to fetch from REST first
// REST: GET /api/v1/orderBooks -> list of markets with index

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 15000;

// Lighter uses numeric market indices. We map after fetching market list.
interface LighterMarket {
  orderBookIndex: number;
  symbol: string;
}

export class LighterAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private shouldReconnect = true;
  private canonicalByExSymbol = new Map<string, string>();
  private marketIndexMap = new Map<number, string>(); // index -> canonical
  private markets: LighterMarket[] = [];

  constructor(config: ExchangeConfig) {
    super('lighter');
    this.wsUrl = config.ws_url ?? 'wss://mainnet.zklighter.elliot.ai/stream';
    this.restUrl = config.rest_url ?? 'https://mainnet.zklighter.elliot.ai';
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC-USD" (Lighter convention)
    const base = canonical.replace(/-PERP$/, '');
    return `${base}-USD`;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.canonicalByExSymbol.clear();
    for (const c of pairs) this.canonicalByExSymbol.set(this.mapPairToExchange(c), c);

    // Fetch market list to get orderbook indices
    await this.fetchMarkets();
    this.shouldReconnect = true;
    await this.connectWs();
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    this.clearTimers();
    if (this.ws) { this.ws.close(1000); this.ws = null; }
    this.setConnected(false);
  }

  private async fetchMarkets() {
    try {
      const res = await fetch(`${this.restUrl}/api/v1/orderBooks`);
      const data = await res.json() as any;
      const books = data.order_books ?? data.orderBooks ?? data ?? [];

      this.marketIndexMap.clear();
      for (const book of (Array.isArray(books) ? books : [])) {
        const symbol = book.symbol ?? book.name ?? '';
        const index = book.order_book_index ?? book.orderBookIndex ?? book.index;
        if (index === undefined) continue;

        // Try multiple symbol formats to match
        const canonical = this.canonicalByExSymbol.get(symbol);
        if (canonical) {
          this.marketIndexMap.set(Number(index), canonical);
          this.markets.push({ orderBookIndex: Number(index), symbol });
        }
      }
      this.log.info({ mapped: this.marketIndexMap.size }, 'Markets fetched');
    } catch (err) {
      this.log.error({ err }, 'Failed to fetch markets');
    }
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

    // Subscribe to order book for each tracked market index
    for (const [index] of this.marketIndexMap) {
      this.ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'orderbook',
        order_book_index: index,
      }));
    }
    this.log.info({ count: this.marketIndexMap.size }, 'Subscribed to orderbook channels');
  }

  private handleMessage(msg: any) {
    const now = Date.now();

    // Handle order book updates
    const index = msg.order_book_index ?? msg.orderBookIndex;
    if (index !== undefined) {
      const canonical = this.marketIndexMap.get(Number(index));
      if (!canonical) return;

      const bids = msg.bids ?? msg.asks_bids?.[1] ?? [];
      const asks = msg.asks ?? msg.asks_bids?.[0] ?? [];

      if (bids.length > 0 && asks.length > 0) {
        const bid = parseFloat(bids[0].price ?? bids[0].px ?? bids[0][0] ?? '0');
        const ask = parseFloat(asks[0].price ?? asks[0].px ?? asks[0][0] ?? '0');
        if (bid > 0 && ask > 0) {
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
      }
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

registerAdapter('lighter', (config) => new LighterAdapter(config));
