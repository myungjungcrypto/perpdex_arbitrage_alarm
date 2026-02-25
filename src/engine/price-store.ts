import { EventEmitter } from 'node:events';
import type { PriceData } from '../types.js';

/**
 * In-memory price store optimized for 500+ pairs × 7 exchanges.
 *
 * Structure: Map<pair, Map<exchange, PriceData>>
 * - O(1) lookup by pair+exchange
 * - O(n_exchanges) to get all prices for a pair (for spread calc)
 * - Memory: ~200 bytes per entry × 3,500 entries ≈ 700KB
 */

export class PriceStore extends EventEmitter {
  // pair -> exchange -> latest price
  private store = new Map<string, Map<string, PriceData>>();

  /** Update or insert a price entry. Returns true if the price actually changed. */
  update(data: PriceData): boolean {
    let exchangeMap = this.store.get(data.pair);
    if (!exchangeMap) {
      exchangeMap = new Map();
      this.store.set(data.pair, exchangeMap);
    }

    const prev = exchangeMap.get(data.exchange);

    // Skip if stale (older than current data)
    if (prev && prev.timestamp > data.timestamp) return false;

    // Skip if price hasn't changed (avoid unnecessary spread recalc)
    if (prev && prev.bid === data.bid && prev.ask === data.ask && prev.mid === data.mid) {
      // Update timestamp only
      prev.timestamp = data.timestamp;
      return false;
    }

    exchangeMap.set(data.exchange, data);
    this.emit('update', data);
    return true;
  }

  /** Get latest price for a specific pair+exchange */
  get(pair: string, exchange: string): PriceData | undefined {
    return this.store.get(pair)?.get(exchange);
  }

  /** Get all exchange prices for a pair */
  getPairPrices(pair: string): Map<string, PriceData> {
    return this.store.get(pair) ?? new Map();
  }

  /** Get all prices (for dashboard snapshot) */
  getAllPrices(): PriceData[] {
    const result: PriceData[] = [];
    for (const exchangeMap of this.store.values()) {
      for (const price of exchangeMap.values()) {
        result.push(price);
      }
    }
    return result;
  }

  /** Get all tracked pairs */
  getPairs(): string[] {
    return Array.from(this.store.keys());
  }

  /** Get count of stored entries */
  get size(): number {
    let count = 0;
    for (const exchangeMap of this.store.values()) {
      count += exchangeMap.size;
    }
    return count;
  }

  /** Check if a price is stale (no update for given ms) */
  isStale(pair: string, exchange: string, maxAgeMs: number): boolean {
    const price = this.get(pair, exchange);
    if (!price) return true;
    return Date.now() - price.timestamp > maxAgeMs;
  }

  clear() {
    this.store.clear();
  }
}
