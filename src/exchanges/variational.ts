import { BaseExchangeAdapter } from './base.js';
import { registerAdapter } from './registry.js';
import type { ExchangeConfig, PriceData } from '../types.js';

// Variational: REST-only (no WebSocket)
// GET /metadata/stats -> mark_price, volume_24h, funding_rate, bid/ask
// Rate limit: 10 requests per 10 seconds per IP
// Prices may be cached for up to 600 seconds

const DEFAULT_POLL_INTERVAL = 1000; // 1 second

export class VariationalAdapter extends BaseExchangeAdapter {
  private restUrl: string;
  private pollInterval: number;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private canonicalByExSymbol = new Map<string, string>();

  constructor(config: ExchangeConfig) {
    super('variational');
    this.restUrl = config.rest_url ?? 'https://omni-client-api.prod.ap-northeast-1.variational.io';
    this.pollInterval = config.poll_interval_ms ?? DEFAULT_POLL_INTERVAL;
  }

  mapPairToExchange(canonical: string): string {
    // "BTC-PERP" -> "BTC-PERP" or ticker format from API
    return canonical;
  }

  mapPairFromExchange(exSymbol: string): string | undefined {
    return this.canonicalByExSymbol.get(exSymbol);
  }

  async connect(pairs: string[]): Promise<void> {
    this.buildPairMappings(pairs);
    this.canonicalByExSymbol.clear();
    for (const c of pairs) this.canonicalByExSymbol.set(this.mapPairToExchange(c), c);

    // Initial fetch
    await this.poll();

    this.setConnected(true);

    // Start polling
    this.pollTimer = setInterval(() => this.poll(), this.pollInterval);
    this.log.info({ interval: this.pollInterval }, 'REST polling started');
  }

  async disconnect(): Promise<void> {
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    this.setConnected(false);
  }

  private async poll() {
    try {
      const res = await fetch(`${this.restUrl}/metadata/stats`);
      const data = await res.json() as any;
      const now = Date.now();

      // Response format: { listings: [{ ticker, mark_price, quotes: { size_1k: { bid, ask } } }], ... }
      const items = data?.listings ?? data?.results ?? (Array.isArray(data) ? data : []);

      if (items.length > 0) {
        this.log.debug({
          sampleKeys: Object.keys(items[0]),
          sampleTicker: items[0].ticker ?? items[0].symbol,
          count: items.length,
        }, 'Poll response sample');
      } else {
        this.log.debug({
          responseType: typeof data,
          keys: data ? Object.keys(data) : [],
        }, 'Poll response - no listings found');
      }

      for (const item of items) {
        const ticker = item.ticker ?? item.symbol ?? '';
        const canonical = this.findCanonical(ticker);
        if (!canonical) continue;

        const mark = parseFloat(item.mark_price ?? '0');
        if (mark <= 0) continue;

        // Bid/ask from quotes.size_1k (smallest trade size tier)
        const quotes = item.quotes?.size_1k ?? item.quotes?.['size_1k'] ?? {};
        const bid = parseFloat(quotes.bid ?? item.bid_price ?? item.bid ?? String(mark));
        const ask = parseFloat(quotes.ask ?? item.ask_price ?? item.ask ?? String(mark));

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
      this.lastMessageAt = now;
    } catch (err) {
      this.log.error({ err }, 'Poll failed');
    }
  }

  private findCanonical(ticker: string): string | undefined {
    if (!ticker) return undefined;

    // Try direct match: "BTC-PERP" -> "BTC-PERP"
    if (this.pairs.includes(ticker)) return ticker;

    // Variational uses short tickers: "BTC" -> "BTC-PERP"
    const upper = ticker.toUpperCase().trim();
    const canonical = `${upper}-PERP`;
    if (this.pairs.includes(canonical)) return canonical;

    // Try splitting compound names: "BTC-USD-PERP" -> "BTC-PERP"
    const base = upper.split('-')[0];
    if (base) {
      const fromBase = `${base}-PERP`;
      if (this.pairs.includes(fromBase)) return fromBase;
    }

    return undefined;
  }
}

registerAdapter('variational', (config) => new VariationalAdapter(config));
