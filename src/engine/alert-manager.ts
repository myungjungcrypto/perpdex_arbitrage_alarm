import { EventEmitter } from 'node:events';
import type { SpreadData, SpreadAlert } from '../types.js';
import { getThreshold } from '../config.js';
import { createChildLogger } from '../logger.js';

const log = createChildLogger('alert-manager');

interface TrackingEntry {
  firstSeenAt: number;
  lastSpread: SpreadData;
  alerted: boolean;
  lastAlertAt: number;
}

export class AlertManager extends EventEmitter {
  // "pair:exA:exB" -> tracking
  private tracking = new Map<string, TrackingEntry>();
  private alertCount = 0;

  onSpread(spread: SpreadData) {
    const [exA, exB] = [spread.longExchange, spread.shortExchange].sort();
    const key = `${spread.pair}:${exA}:${exB}`;
    const threshold = getThreshold(spread.pair, spread.isSlowSpread);
    const now = Date.now();

    if (spread.spreadPct >= threshold.min_spread_pct) {
      let entry = this.tracking.get(key);
      if (!entry) {
        entry = { firstSeenAt: now, lastSpread: spread, alerted: false, lastAlertAt: 0 };
        this.tracking.set(key, entry);
      } else {
        entry.lastSpread = spread;
      }

      // Check if sustained long enough
      const duration = now - entry.firstSeenAt;
      if (duration >= threshold.sustained_ms && !entry.alerted) {
        // Check cooldown
        if (now - entry.lastAlertAt >= threshold.cooldown_ms) {
          this.fireAlert(entry, duration, key);
        }
      }
    } else {
      // Spread dropped below threshold — reset tracking
      this.tracking.delete(key);
    }
  }

  private fireAlert(entry: TrackingEntry, durationMs: number, key: string) {
    const alertId = `alert-${++this.alertCount}-${Date.now()}`;
    const alert: SpreadAlert = {
      ...entry.lastSpread,
      durationMs,
      alertId,
    };

    entry.alerted = true;
    entry.lastAlertAt = Date.now();

    log.warn({
      pair: alert.pair,
      long: alert.longExchange,
      short: alert.shortExchange,
      spreadPct: alert.spreadPct.toFixed(4),
      durationMs,
    }, 'SPREAD ALERT');

    this.emit('alert', alert);

    // Reset alerted flag so it can fire again after cooldown
    // (if spread persists, a new entry cycle will start)
    entry.alerted = false;
    entry.firstSeenAt = Date.now();
  }

  /** Get all currently tracked (above-threshold) entries */
  getActiveTracking(): { key: string; entry: TrackingEntry }[] {
    const result: { key: string; entry: TrackingEntry }[] = [];
    for (const [key, entry] of this.tracking) {
      result.push({ key, entry });
    }
    return result;
  }
}
