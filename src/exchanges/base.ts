import { EventEmitter } from 'node:events';
import type { PriceData, ExchangeStatus, PairMapping } from '../types.js';
import { createChildLogger } from '../logger.js';
import type { Logger } from 'pino';

export abstract class BaseExchangeAdapter extends EventEmitter {
  readonly name: string;
  protected log: Logger;
  protected pairs: string[] = [];
  protected pairMap: Map<string, PairMapping> = new Map(); // exchangeSymbol -> mapping
  protected connected = false;
  protected lastMessageAt = 0;
  private _latencyMs = 0;

  constructor(name: string) {
    super();
    this.name = name;
    this.log = createChildLogger(name);
  }

  /** Convert canonical pair name to exchange-specific symbol */
  abstract mapPairToExchange(canonical: string): string;

  /** Convert exchange-specific symbol back to canonical pair name */
  abstract mapPairFromExchange(exchangeSymbol: string): string | undefined;

  /** Connect to exchange and subscribe to price feeds */
  abstract connect(pairs: string[]): Promise<void>;

  /** Gracefully disconnect */
  abstract disconnect(): Promise<void>;

  /** Get current connection status */
  getStatus(): ExchangeStatus {
    return {
      exchange: this.name,
      connected: this.connected,
      lastMessageAt: this.lastMessageAt,
      subscribedPairs: this.pairs.length,
      latencyMs: this._latencyMs,
    };
  }

  protected emitPrice(data: PriceData) {
    this.lastMessageAt = Date.now();
    this.emit('price', data);
  }

  protected emitStatus() {
    this.emit('status', this.getStatus());
  }

  protected setConnected(value: boolean) {
    this.connected = value;
    this.emitStatus();
  }

  protected setLatency(ms: number) {
    this._latencyMs = ms;
  }

  protected buildPairMappings(canonicalPairs: string[]) {
    this.pairs = canonicalPairs;
    this.pairMap.clear();
    for (const canonical of canonicalPairs) {
      const exSym = this.mapPairToExchange(canonical);
      this.pairMap.set(exSym, { canonical, exchangeSymbol: exSym });
    }
  }
}
