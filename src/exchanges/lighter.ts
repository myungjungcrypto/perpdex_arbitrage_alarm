import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Lighter: wss://mainnet.zklighter.elliot.ai/stream
// Uses numeric order_book_index (market index). Must map from API first.
// Two endpoints to get symbol mapping:
//   1. GET /api/v1/funding-rates -> { funding_rates: [{ market_id, symbol, ... }] }
//   2. GET /api/v1/orderBookDetails -> market metadata with decimal precision
// WS orderbook channel: subscribe with order_book_index, get bids/asks arrays

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 15000;

export class LighterAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private shouldReconnect = true;

  // market_index -> canonical pair
  private marketIndexMap = new Map<number, string>();

  constructor(config: ExchangeConfig) {
    super('lighter');
    this.wsUrl = config.ws_url ?? 'wss://mainnet.zklighter.elliot.ai/stream';
    this.restUrl = config.rest_url ?? 'https://mainnet.zklighter.elliot.ai';
  }

  mapPairToExchange(canonical: string): string {
    const base = canonical.replace(/-PERP$/, '');
    return `${base}-USD`;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return undefined;
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    await this.fetchMarkets();

    if (this.marketIndexMap.size === 0) {
      this.log.warn('No markets mapped. Check symbol format.');
    }

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
    this.marketIndexMap.clear();

    // Primary: try orderBooks endpoint (has market_id + symbol)
    await this.tryOrderBooksV2();

    // Fallback: try funding-rates
    if (this.marketIndexMap.size === 0) {
      await this.tryFundingRates();
    }

    // Last resort: try orderBookDetails for indices 0-10
    if (this.marketIndexMap.size === 0) {
      await this.tryOrderBookDetails();
    }

    this.log.info({
      mapped: this.marketIndexMap.size,
      mappings: Object.fromEntries(this.marketIndexMap),
    }, 'Market mapping complete');
  }

  private async tryFundingRates() {
    try {
      const res = await fetch(`${this.restUrl}/api/v1/funding-rates`);
      const data = await res.json() as any;
      const rates = data?.funding_rates ?? data ?? [];

      if (!Array.isArray(rates)) return;

      for (const item of rates) {
        const marketId = item.market_id ?? item.marketId ?? item.order_book_index;
        const symbol = item.symbol ?? '';
        if (marketId === undefined || !symbol) continue;

        const canonical = this.matchCanonical(symbol);
        if (canonical) {
          this.marketIndexMap.set(Number(marketId), canonical);
        }
      }
      this.log.debug({ source: 'funding-rates', found: rates.length }, 'Tried funding-rates');
    } catch (err) {
      this.log.debug({ err }, 'funding-rates endpoint failed');
    }
  }

  // Primary endpoint for symbol -> market_id mapping
  private async tryOrderBooksV2() {
    try {
      const res = await fetch(`${this.restUrl}/api/v1/orderBooks`);
      const data = await res.json() as any;

      // Response: { code: 0, order_books: [{ market_id, symbol, ... }] }
      const books = data?.order_books ?? data?.orderBooks ?? (Array.isArray(data) ? data : []);

      for (const book of books) {
        const index = book.market_id ?? book.order_book_index ?? book.orderBookIndex ?? book.index;
        const symbol = book.symbol ?? book.name ?? '';
        if (index === undefined) continue;

        this.log.debug({ index, symbol, keys: Object.keys(book) }, 'orderBooks entry');

        const canonical = this.matchCanonical(symbol);
        if (canonical) {
          this.marketIndexMap.set(Number(index), canonical);
        }
      }
      this.log.debug({
        source: 'orderBooks',
        found: books.length,
        sampleKeys: books[0] ? Object.keys(books[0]) : [],
        sampleSymbol: books[0]?.symbol ?? books[0]?.name ?? 'none',
      }, 'Tried orderBooks');
    } catch (err) {
      this.log.debug({ err }, 'orderBooks endpoint failed');
    }
  }

  // Legacy fallback for older API versions
  private async tryOrderBooks() {
    // Already handled by tryOrderBooksV2
  }

  private async tryOrderBookDetails() {
    try {
      // Try fetching details for common indices 0-10
      for (let i = 0; i <= 10; i++) {
        const res = await fetch(`${this.restUrl}/api/v1/orderBookDetails?order_book_index=${i}`);
        if (!res.ok) continue;
        const data = await res.json() as any;
        const symbol = data?.symbol ?? data?.name ?? '';
        if (symbol) {
          const canonical = this.matchCanonical(symbol);
          if (canonical) {
            this.marketIndexMap.set(i, canonical);
          }
        }
      }
      this.log.debug({ source: 'orderBookDetails' }, 'Tried orderBookDetails');
    } catch (err) {
      this.log.debug({ err }, 'orderBookDetails failed');
    }
  }

  private matchCanonical(symbol: string): string | undefined {
    if (!symbol) return undefined;

    // Direct match: "BTC-PERP"
    if (this.pairs.includes(symbol)) return symbol;

    // Short ticker: "BTC" or "SOL" -> "BTC-PERP"
    const upper = symbol.toUpperCase().trim();
    const shortCanonical = `${upper}-PERP`;
    if (this.pairs.includes(shortCanonical)) return shortCanonical;

    // Strip -USD/-USDC suffix: "BTC-USD" -> "BTC-PERP"
    const base1 = upper.replace(/-USD[C]?$/, '').replace(/_USD[C]?$/, '');
    const canonical1 = `${base1}-PERP`;
    if (this.pairs.includes(canonical1)) return canonical1;

    // Strip concatenated suffix: "BTCUSD" -> "BTC-PERP"
    const base2 = upper.replace(/USD[C]?$/, '');
    const canonical2 = `${base2}-PERP`;
    if (this.pairs.includes(canonical2)) return canonical2;

    // "BTC USDC Perp" format -> "BTC-PERP"
    const base3 = upper.split(/\s+/)[0];
    if (base3) {
      const canonical3 = `${base3}-PERP`;
      if (this.pairs.includes(canonical3)) return canonical3;
    }

    return undefined;
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

    for (const [index, canonical] of this.marketIndexMap) {
      this.ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'order_book',
        order_book_index: index,
      }));
      this.log.debug({ index, canonical }, 'Subscribing to order_book');
    }
    this.log.info({ count: this.marketIndexMap.size }, 'Subscribed to order_book channels');
  }

  private handleMessage(msg: any) {
    const now = Date.now();

    // Lighter WS format: { channel: "order_book:0", type: "update/order_book",
    //   order_book: { asks: [{price, size}], bids: [{price, size}] }, timestamp }
    const channel = msg.channel as string | undefined;

    if (channel?.startsWith('order_book:')) {
      const indexStr = channel.split(':')[1];
      const index = Number(indexStr);
      const canonical = this.marketIndexMap.get(index);
      if (!canonical) return;

      // Data is inside msg.order_book wrapper
      const ob = msg.order_book ?? msg;
      const bids = ob.bids ?? ob.b ?? [];
      const asks = ob.asks ?? ob.a ?? [];

      if (bids.length > 0 && asks.length > 0) {
        const bid = this.parsePrice(bids[0]);
        const ask = this.parsePrice(asks[0]);
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
      return;
    }

    // Fallback: try top-level order_book_index format
    const index = msg.order_book_index ?? msg.orderBookIndex;
    if (index !== undefined) {
      const canonical = this.marketIndexMap.get(Number(index));
      if (!canonical) return;

      const bids = msg.bids ?? msg.b ?? [];
      const asks = msg.asks ?? msg.a ?? [];

      if (bids.length > 0 && asks.length > 0) {
        const bid = this.parsePrice(bids[0]);
        const ask = this.parsePrice(asks[0]);
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

  private parsePrice(entry: any): number {
    if (typeof entry === 'number') return entry;
    if (typeof entry === 'string') return parseFloat(entry);
    if (Array.isArray(entry)) return parseFloat(entry[0]);
    return parseFloat(entry.price ?? entry.p ?? entry.px ?? '0');
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
