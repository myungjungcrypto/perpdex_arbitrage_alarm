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
      const data = await res.json() as any[];
      const now = Date.now();

      for (const item of (Array.isArray(data) ? data : [])) {
        const ticker = item.ticker ?? item.symbol ?? '';
        const canonical = this.findCanonical(ticker);
        if (!canonical) continue;

        const mark = parseFloat(item.mark_price ?? '0');
        if (mark <= 0) continue;

        // Variational provides bid/ask at various sizes
        const bid = parseFloat(item.bid_price ?? item.bid ?? String(mark));
        const ask = parseFloat(item.ask_price ?? item.ask ?? String(mark));

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
    // Try direct match first
    let canonical = this.canonicalByExSymbol.get(ticker);
    if (canonical) return canonical;

    // Try normalized: "BTC-USD-PERP" -> "BTC-PERP", etc.
    const base = ticker.split('-')[0]?.toUpperCase();
    if (base) {
      canonical = this.canonicalByExSymbol.get(`${base}-PERP`);
      if (canonical) return canonical;
    }

    return undefined;
  }
}

registerAdapter('variational', (config) => new VariationalAdapter(config));
