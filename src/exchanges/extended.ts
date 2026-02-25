import WebSocket from 'ws';
import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Extended: Starknet-based, order book stream at 100ms push
// WS: wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{market}
// REST: https://api.starknet.extended.exchange/api/v1
// Pair format: "BTC-USDC" or "BTC-USDC-PERP"
// Server pings every 15s, expects pong within 10s

const RECONNECT_BASE_MS = 2000;
const RECONNECT_MAX_MS = 60000;
const MAX_RECONNECT_ATTEMPTS = 5;

export class ExtendedAdapter extends BaseExchangeAdapter {
  private connections = new Map<string, WebSocket>(); // canonical -> ws
  private wsBaseUrl: string;
  private restUrl: string;
  private shouldReconnect = true;
  private canonicalByExSymbol = new Map<string, string>();
  private pairReconnectAttempts = new Map<string, number>(); // per-pair retry count

  constructor(config: ExchangeConfig) {
    super('extended');
    this.wsBaseUrl = config.ws_url ?? 'wss://api.starknet.extended.exchange/stream.extended.exchange/v1';
    this.restUrl = config.rest_url ?? 'https://api.starknet.extended.exchange/api/v1';
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC-USDC"
    const base = canonical.replace(/-PERP$/, '');
    return `${base}-USDC`;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.canonicalByExSymbol.clear();
    for (const c of pairs) this.canonicalByExSymbol.set(this.mapPairToExchange(c), c);

    this.shouldReconnect = true;

    // Connect one WS per pair (Extended uses per-market streams)
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
    const url = `${this.wsBaseUrl}/orderbooks/${exSym}`;

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);

      ws.on('open', () => {
        this.connections.set(canonical, ws);
        this.log.debug({ pair: canonical }, 'Pair stream connected');
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

      ws.on('ping', () => {
        ws.pong();
      });

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
    const bids = msg.bids ?? msg.buy ?? [];
    const asks = msg.asks ?? msg.sell ?? [];

    if (bids.length > 0 && asks.length > 0) {
      const bid = parseFloat(bids[0].price ?? bids[0][0] ?? '0');
      const ask = parseFloat(asks[0].price ?? asks[0][0] ?? '0');
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
      const res = await fetch(`${this.restUrl}/markets`);
      const data = await res.json() as any[];
      const now = Date.now();

      for (const market of (Array.isArray(data) ? data : [])) {
        const sym = market.market ?? market.symbol ?? '';
        const canonical = this.canonicalByExSymbol.get(sym);
        if (!canonical) continue;

        const mark = parseFloat(market.mark_price ?? market.last_price ?? '0');
        if (mark <= 0) continue;

        this.emitPrice({
          exchange: this.name,
          pair: canonical,
          bid: mark,
          ask: mark,
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
}

registerAdapter('extended', (config) => new ExtendedAdapter(config));
