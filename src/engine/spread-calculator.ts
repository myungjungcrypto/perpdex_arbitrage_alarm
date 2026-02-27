import { EventEmitter } from 'node:events';
import type { PriceData, SpreadData } from '../types.js';
import type { PriceStore } from './price-store.js';

/**
 * Incremental spread calculator.
 *
 * Fast exchanges: pairwise comparison (skip slow exchanges).
 * Slow exchanges: compared against market average of fast exchanges.
 */

export class SpreadCalculator extends EventEmitter {
  private priceStore: PriceStore;
  private maxStaleMs: number;
  private slowExchanges: Set<string>;
  private slowMaxStaleMs: number;
  private minFastSources: number;

  // Cache latest spreads: "pair:exA:exB" -> SpreadData
  private spreadCache = new Map<string, SpreadData>();

  constructor(
    priceStore: PriceStore,
    opts: {
      maxStaleMs?: number;
      slowExchanges?: Set<string>;
      slowMaxStaleMs?: number;
      minFastSources?: number;
    } = {},
  ) {
    super();
    this.priceStore = priceStore;
    this.maxStaleMs = opts.maxStaleMs ?? 10000;
    this.slowExchanges = opts.slowExchanges ?? new Set();
    this.slowMaxStaleMs = opts.slowMaxStaleMs ?? 120000;
    this.minFastSources = opts.minFastSources ?? 2;

    // Listen to price updates and recalculate incrementally
    this.priceStore.on('update', (data) => this.onPriceUpdate(data));
  }

  private onPriceUpdate(updated: PriceData) {
    const pairPrices = this.priceStore.getPairPrices(updated.pair);
    if (pairPrices.size < 2) return;

    const now = Date.now();
    const isSlow = this.slowExchanges.has(updated.exchange);

    if (isSlow) {
      // Slow exchange updated → compare against market average only
      this.computeSlowSpread(updated.pair, updated.exchange, now);
      return;
    }

    // Fast exchange updated → pairwise against other fast exchanges
    const entries = Array.from(pairPrices.entries());
    for (const [otherExchange, otherPrice] of entries) {
      if (otherExchange === updated.exchange) continue;
      if (this.slowExchanges.has(otherExchange)) continue; // skip slow
      if (now - otherPrice.timestamp > this.maxStaleMs) continue;

      this.emitPairwiseSpread(updated, otherPrice, now);
    }

    // Also recompute slow exchange spreads (average just changed)
    for (const slowEx of this.slowExchanges) {
      if (pairPrices.has(slowEx)) {
        this.computeSlowSpread(updated.pair, slowEx, now);
      }
    }
  }

  /** Standard pairwise spread between two fast exchanges */
  private emitPairwiseSpread(updated: PriceData, other: PriceData, now: number) {
    let longExchange: string, shortExchange: string;
    let longAsk: number, shortBid: number;

    if (updated.ask < other.bid) {
      longExchange = updated.exchange;
      shortExchange = other.exchange;
      longAsk = updated.ask;
      shortBid = other.bid;
    } else if (other.ask < updated.bid) {
      longExchange = other.exchange;
      shortExchange = updated.exchange;
      longAsk = other.ask;
      shortBid = updated.bid;
    } else {
      if (updated.ask <= other.ask) {
        longExchange = updated.exchange;
        shortExchange = other.exchange;
        longAsk = updated.ask;
        shortBid = other.bid;
      } else {
        longExchange = other.exchange;
        shortExchange = updated.exchange;
        longAsk = other.ask;
        shortBid = updated.bid;
      }
    }

    const spreadAbs = shortBid - longAsk;
    const spreadPct = longAsk > 0 ? (spreadAbs / longAsk) * 100 : 0;

    const [exA, exB] = [longExchange, shortExchange].sort();
    const cacheKey = `${updated.pair}:${exA}:${exB}`;

    const spread: SpreadData = {
      pair: updated.pair,
      longExchange,
      shortExchange,
      longAsk,
      shortBid,
      spreadAbs,
      spreadPct,
      timestamp: now,
    };

    this.spreadCache.set(cacheKey, spread);
    this.emit('spread', spread);
  }

  /** Compare a slow exchange's price against the average of fast exchanges */
  private computeSlowSpread(pair: string, slowExchange: string, now: number) {
    const pairPrices = this.priceStore.getPairPrices(pair);
    const slowPrice = pairPrices.get(slowExchange);
    if (!slowPrice) return;

    // Allow older data for slow exchanges
    if (now - slowPrice.timestamp > this.slowMaxStaleMs) return;

    // Compute average of fresh fast exchange prices
    let sumBid = 0, sumAsk = 0, count = 0;
    for (const [ex, price] of pairPrices) {
      if (this.slowExchanges.has(ex)) continue;
      if (now - price.timestamp > this.maxStaleMs) continue;
      sumBid += price.bid;
      sumAsk += price.ask;
      count++;
    }

    if (count < this.minFastSources) return;

    const avgBid = sumBid / count;
    const avgAsk = sumAsk / count;

    // Determine direction
    let longExchange: string, shortExchange: string;
    let longAsk: number, shortBid: number;

    if (slowPrice.ask < avgBid) {
      // Slow exchange is cheaper → buy slow, sell market
      longExchange = slowExchange;
      shortExchange = 'market-avg';
      longAsk = slowPrice.ask;
      shortBid = avgBid;
    } else if (avgAsk < slowPrice.bid) {
      // Market is cheaper → buy market, sell slow
      longExchange = 'market-avg';
      shortExchange = slowExchange;
      longAsk = avgAsk;
      shortBid = slowPrice.bid;
    } else {
      // No positive spread — emit with best possible
      if (slowPrice.ask <= avgAsk) {
        longExchange = slowExchange;
        shortExchange = 'market-avg';
        longAsk = slowPrice.ask;
        shortBid = avgBid;
      } else {
        longExchange = 'market-avg';
        shortExchange = slowExchange;
        longAsk = avgAsk;
        shortBid = slowPrice.bid;
      }
    }

    const spreadAbs = shortBid - longAsk;
    const spreadPct = longAsk > 0 ? (spreadAbs / longAsk) * 100 : 0;

    const cacheKey = `${pair}:${slowExchange}:market-avg`;

    const spread: SpreadData = {
      pair,
      longExchange,
      shortExchange,
      longAsk,
      shortBid,
      spreadAbs,
      spreadPct,
      timestamp: now,
      isSlowSpread: true,
    };

    this.spreadCache.set(cacheKey, spread);
    this.emit('spread', spread);
  }

  /** Get all current spreads */
  getAllSpreads(): SpreadData[] {
    return Array.from(this.spreadCache.values());
  }

  /** Get spreads for a specific pair */
  getSpreadsByPair(pair: string): SpreadData[] {
    const results: SpreadData[] = [];
    for (const [key, spread] of this.spreadCache) {
      if (key.startsWith(`${pair}:`)) results.push(spread);
    }
    return results;
  }

  /** Get top N spreads by absolute percentage */
  getTopSpreads(n: number): SpreadData[] {
    return Array.from(this.spreadCache.values())
      .sort((a, b) => Math.abs(b.spreadPct) - Math.abs(a.spreadPct))
      .slice(0, n);
  }
}
