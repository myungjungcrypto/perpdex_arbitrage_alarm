import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Extended: Starknet-based, order book stream at 100ms push (10ms with ?depth=1)
// WS: wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{market}
// REST: https://api.starknet.extended.exchange/api/v1
// Pair format: "BTC-USD", "ETH-USD" (NOT "BTC-USDC")
// IMPORTANT: User-Agent header is REQUIRED for all requests (403 without it)
// Orderbook fields: b (bids), a (asks), each entry has p (price), q (quantity)

const RECONNECT_BASE_MS = 2000;
const RECONNECT_MAX_MS = 60000;
const MAX_RECONNECT_ATTEMPTS = 5;
const USER_AGENT = 'perpdex-arbitrage-alarm/1.0';

export class ExtendedAdapter extends BaseExchangeAdapter {
  private connections = new Map<string, WebSocket>(); // canonical -> ws
  private wsBaseUrl: string;
  private restUrl: string;
  private shouldReconnect = true;
  private canonicalByExSymbol = new Map<string, string>();
  private pairReconnectAttempts = new Map<string, number>();

  constructor(config: ExchangeConfig) {
    super('extended');
    this.wsBaseUrl = config.ws_url ?? 'wss://api.starknet.extended.exchange/stream.extended.exchange/v1';
    this.restUrl = config.rest_url ?? 'https://api.starknet.extended.exchange/api/v1';
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC-USD"
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

    this.shouldReconnect = true;

    const connectPromises = pairs.map((canonical) => this.connectPairWs(canonical));
    const results = await Promise.allSettled(connectPromises);

    const connected = results.filter(r => r.status === 'fulfilled').length;
    this.log.info({ connected, total: pairs.length }, 'Connections established');

    if (connected > 0) this.setConnected(true);
    await this.fetchRestSnapshot();
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    for (const [, ws] of this.connections) ws.close(1000);
    this.connections.clear();
    this.pairReconnectAttempts.clear();
    this.setConnected(false);
  }

  private connectPairWs(canonical: string): Promise<void> {
    const exSym = this.mapPairToExchange(canonical);
    // Use ?depth=1 for BBO only (10ms push, lighter payload)
    const url = `${this.wsBaseUrl}/orderbooks/${exSym}?depth=1`;

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, {
        headers: { 'User-Agent': USER_AGENT },
      });

      ws.on('open', () => {
        this.connections.set(canonical, ws);
        this.pairReconnectAttempts.set(canonical, 0); // reset on success
        this.log.info({ pair: canonical, exSym }, 'Pair stream connected');
        resolve();
      });

      ws.on('message', (data) => {
        try {
          const msg = JSON.parse(data.toString());
          this.handleOrderBook(canonical, msg);
        } catch (err) {
          this.log.error({ err, pair: canonical }, 'Parse error');
        }
      });

      ws.on('pong', () => { /* keepalive ok */ });
      ws.on('ping', () => { ws.pong(); });

      ws.on('close', (code) => {
        this.connections.delete(canonical);
        const attempts = (this.pairReconnectAttempts.get(canonical) ?? 0) + 1;
        this.pairReconnectAttempts.set(canonical, attempts);

        if (this.shouldReconnect && attempts <= MAX_RECONNECT_ATTEMPTS) {
          const delay = Math.min(RECONNECT_BASE_MS * 2 ** (attempts - 1), RECONNECT_MAX_MS);
          this.log.warn({ pair: canonical, code, attempt: attempts, nextRetryMs: delay }, 'Stream closed, retrying');
          setTimeout(() => this.connectPairWs(canonical).catch(() => {}), delay);
        } else if (attempts > MAX_RECONNECT_ATTEMPTS) {
          this.log.error({ pair: canonical, attempts }, 'Max reconnect attempts reached, giving up');
        }
        if (this.connections.size === 0) this.setConnected(false);
      });

      ws.on('error', (err) => {
        this.log.error({ err: err.message, pair: canonical }, 'Stream error');
        reject(err);
      });
    });
  }

  private handleOrderBook(canonical: string, msg: any) {
    const now = Date.now();

    // Extended WS format: { ts, type: "SNAPSHOT", data: { m: "BTC-USD", b: [{p, q, c}], a: [{p, q, c}] }, seq }
    // Data can be at msg.data level (wrapped) or top level
    const ob = msg.data ?? msg;

    const bids = ob.b ?? ob.bids ?? [];
    const asks = ob.a ?? ob.asks ?? [];

    if (bids.length > 0 && asks.length > 0) {
      const bid = parseFloat(bids[0].p ?? bids[0].price ?? bids[0][0] ?? '0');
      const ask = parseFloat(asks[0].p ?? asks[0].price ?? asks[0][0] ?? '0');
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

  private async fetchRestSnapshot() {
    try {
      // Extended REST: GET /api/v1/info/markets
      // Response: { status, data: [{ name: "BTC-USD", marketStats: { markPrice, bidPrice, askPrice, ... } }] }
      const res = await fetch(`${this.restUrl}/info/markets`, {
        headers: { 'User-Agent': USER_AGENT },
      });
      const json = await res.json() as any;
      const now = Date.now();
      const markets = json?.data ?? (Array.isArray(json) ? json : (json?.markets ?? []));

      for (const market of markets) {
        const sym = market.name ?? market.m ?? market.market ?? market.symbol ?? '';
        const canonical = this.canonicalByExSymbol.get(sym);
        if (!canonical) continue;

        // Prices are in marketStats sub-object
        const stats = market.marketStats ?? market;
        const mark = parseFloat(stats.markPrice ?? stats.mark_price ?? stats.lastPrice ?? stats.last_price ?? '0');
        if (mark <= 0) continue;

        const bid = parseFloat(stats.bidPrice ?? stats.bid_price ?? String(mark));
        const ask = parseFloat(stats.askPrice ?? stats.ask_price ?? String(mark));

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
      this.log.info({ count: markets.length }, 'REST snapshot loaded');
    } catch (err) {
      this.log.error({ err }, 'REST snapshot failed');
    }
  }
}

registerAdapter('extended', (config) => new ExtendedAdapter(config));
