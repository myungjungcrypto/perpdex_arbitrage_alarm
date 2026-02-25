import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Nado: wss://gateway.prod.nado.xyz/v1/subscribe
// Uses numeric product_id (NOT string market names)
// Prices are in x18 format (multiply by 1e18): e.g., $20,000 = 20000 * 1e18
// Subscribe format: { method: "subscribe", stream: { type: "best_bid_offer", product_id: N }, id: N }
// BBO event: { type: "best_bid_offer", product_id, bid_price, bid_qty, ask_price, ask_qty, timestamp }

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30000;
const PING_INTERVAL_MS = 25000;
const X18 = 1e18;

export class NadoAdapter extends BaseExchangeAdapter {
  private ws: WebSocket | null = null;
  private wsUrl: string;
  private restUrl: string;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private shouldReconnect = true;
  private subId = 0;

  // product_id -> canonical pair name
  private productIdToCanonical = new Map<number, string>();
  // canonical -> product_id
  private canonicalToProductId = new Map<string, number>();

  constructor(config: ExchangeConfig) {
    super('nado');
    this.wsUrl = config.ws_url ?? 'wss://gateway.prod.nado.xyz/v1/subscribe';
    this.restUrl = config.rest_url ?? 'https://gateway.prod.nado.xyz/v1';
  }

  mapPairToExchange(canonical: string): string {
    return canonical;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return exSymbol; // Not used, we use product_id
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.shouldReconnect = true;

    // First fetch product list to get product_id -> symbol mapping
    await this.fetchProducts();

    if (this.productIdToCanonical.size === 0) {
      this.log.warn('No product IDs mapped, will still try to connect');
    }

    await this.connectWs();
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    this.clearTimers();
    if (this.ws) { this.ws.close(1000); this.ws = null; }
    this.setConnected(false);
  }

  private async fetchProducts() {
    try {
      const res = await fetch(`${this.restUrl}/query?type=all_products`);
      const data = await res.json() as any;
      this.log.debug({ keys: data ? Object.keys(data) : [] }, 'Products response');

      // Parse perp products
      const perpProducts = data?.perp_products ?? data?.perpProducts ?? [];
      const spotProducts = data?.spot_products ?? data?.spotProducts ?? [];
      const allProducts = [...perpProducts, ...spotProducts];

      for (const product of allProducts) {
        const productId = product.product_id ?? product.productId;
        if (productId === undefined) continue;

        // Extract symbol from config or directly
        const symbol = product.symbol ?? product.config?.symbol ?? product.name ?? '';
        const pid = Number(productId);

        // Try to match to our canonical pairs
        // Nado symbols might be: "BTC", "ETH", "SOL", "BNB", "HYPE"
        // Or: "BTC-PERP", "wBTC"
        const canonical = this.findCanonical(symbol);
        if (canonical) {
          this.productIdToCanonical.set(pid, canonical);
          this.canonicalToProductId.set(canonical, pid);
        }
      }

      this.log.info({
        mapped: this.productIdToCanonical.size,
        mappings: Object.fromEntries(this.productIdToCanonical),
        totalProducts: allProducts.length,
      }, 'Products fetched');
    } catch (err) {
      this.log.error({ err }, 'Failed to fetch products');
    }
  }

  private findCanonical(symbol: string): string | undefined {
    // Direct match: "BTC-PERP" -> "BTC-PERP"
    if (this.pairs.includes(symbol)) return symbol;

    // Base match: "BTC" or "wBTC" -> "BTC-PERP"
    const base = symbol.replace(/^w/, '').toUpperCase(); // strip "w" prefix
    const canonical = `${base}-PERP`;
    if (this.pairs.includes(canonical)) return canonical;

    return undefined;
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

    // Subscribe to each product's best_bid_offer
    for (const [productId, canonical] of this.productIdToCanonical) {
      this.ws.send(JSON.stringify({
        method: 'subscribe',
        stream: {
          type: 'best_bid_offer',
          product_id: productId,
        },
        id: ++this.subId,
      }));
      this.log.debug({ productId, canonical }, 'Subscribing to BBO');
    }

    this.log.info({ count: this.productIdToCanonical.size }, 'Subscribed to best_bid_offer');
  }

  private handleMessage(msg: any) {
    const now = Date.now();

    // Subscription response
    if (msg.id && msg.result !== undefined) {
      this.log.debug({ id: msg.id, result: msg.result }, 'Subscription response');
      return;
    }

    if (msg.error) {
      this.log.warn({ error: msg.error }, 'Subscription error');
      return;
    }

    // BBO event
    if (msg.type === 'best_bid_offer') {
      this.handleBbo(msg, now);
    }
  }

  private handleBbo(data: any, now: number) {
    const productId = data.product_id ?? data.productId;
    if (productId === undefined) return;

    const canonical = this.productIdToCanonical.get(Number(productId));
    if (!canonical) return;

    // Prices are in x18 format
    const bid = this.parseX18(data.bid_price ?? data.bidPrice ?? '0');
    const ask = this.parseX18(data.ask_price ?? data.askPrice ?? '0');

    if (bid <= 0 || ask <= 0) return;

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

  private parseX18(value: string | number): number {
    const raw = typeof value === 'string' ? parseFloat(value) : value;
    if (isNaN(raw) || raw === 0) return 0;
    return raw / X18;
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
