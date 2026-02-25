import { EventEmitter } from 'node:events';
import type { PriceData, SpreadData } from '../types.js';
import type { PriceStore } from './price-store.js';

/**
 * Incremental spread calculator.
 *
 * When a price updates for pair P on exchange E:
 * - Fetches all exchange prices for pair P
 * - Computes spreads between E and every other exchange with a price for P
 *
 * For 500 pairs × 7 exchanges: at most 21 comparisons per price update.
 */

export class SpreadCalculator extends EventEmitter {
  private priceStore: PriceStore;
  private maxStaleMs: number;

  // Cache latest spreads: "pair:exA:exB" -> SpreadData
  private spreadCache = new Map<string, SpreadData>();

  constructor(priceStore: PriceStore, maxStaleMs = 10000) {
    super();
    this.priceStore = priceStore;
    this.maxStaleMs = maxStaleMs;

    // Listen to price updates and recalculate incrementally
    this.priceStore.on('update', (data) => this.onPriceUpdate(data));
  }

  private onPriceUpdate(updated: PriceData) {
    const pairPrices = this.priceStore.getPairPrices(updated.pair);
    if (pairPrices.size < 2) return; // Need at least 2 exchanges

    const now = Date.now();
    const entries = Array.from(pairPrices.entries());

    // Compare updated exchange against all other exchanges
    for (const [otherExchange, otherPrice] of entries) {
      if (otherExchange === updated.exchange) continue;

      // Skip stale prices
      if (now - otherPrice.timestamp > this.maxStaleMs) continue;

      // Determine direction: who has lower ask (buy) and who has higher bid (sell)
      let longExchange: string, shortExchange: string;
      let longAsk: number, shortBid: number;

      if (updated.ask < otherPrice.bid) {
        // Buy on updated exchange, sell on other
        longExchange = updated.exchange;
        shortExchange = otherExchange;
        longAsk = updated.ask;
        shortBid = otherPrice.bid;
      } else if (otherPrice.ask < updated.bid) {
        // Buy on other exchange, sell on updated
        longExchange = otherExchange;
        shortExchange = updated.exchange;
        longAsk = otherPrice.ask;
        shortBid = updated.bid;
      } else {
        // No positive spread — still emit with the best possible spread
        if (updated.ask <= otherPrice.ask) {
          longExchange = updated.exchange;
          shortExchange = otherExchange;
          longAsk = updated.ask;
          shortBid = otherPrice.bid;
        } else {
          longExchange = otherExchange;
          shortExchange = updated.exchange;
          longAsk = otherPrice.ask;
          shortBid = updated.bid;
        }
      }

      const spreadAbs = shortBid - longAsk;
      const spreadPct = longAsk > 0 ? (spreadAbs / longAsk) * 100 : 0;

      // Use sorted exchange names as cache key for consistency
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
