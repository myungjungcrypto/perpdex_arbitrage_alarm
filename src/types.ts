export interface PriceData {
  exchange: string;
  pair: string;
  bid: number;
  ask: number;
  mid: number;
  timestamp: number; // unix ms
  source: 'ws' | 'rest';
}

export interface SpreadData {
  pair: string;
  longExchange: string;  // buy here (lower ask)
  shortExchange: string; // sell here (higher bid)
  longAsk: number;
  shortBid: number;
  spreadAbs: number;     // shortBid - longAsk
  spreadPct: number;     // spreadAbs / longAsk * 100
  timestamp: number;
  isSlowSpread?: boolean; // true = slow exchange vs market average
}

export interface SpreadAlert extends SpreadData {
  durationMs: number;
  alertId: string;
}

export interface ExchangeStatus {
  exchange: string;
  connected: boolean;
  lastMessageAt: number;
  subscribedPairs: number;
  latencyMs: number;
}

export interface ThresholdConfig {
  min_spread_pct: number;
  sustained_ms: number;
  cooldown_ms: number;
}

export interface ExchangeConfig {
  enabled: boolean;
  ws_url?: string;
  rest_url?: string;
  poll_interval_ms?: number;
}

export interface SlowExchangeConfig {
  exchanges: string[];
  max_stale_ms: number;
  min_fast_sources: number;
  threshold: ThresholdConfig;
}

export interface AppConfig {
  exchanges: Record<string, ExchangeConfig>;
  pairs: string[];
  slow_exchanges?: SlowExchangeConfig;
  thresholds: {
    default: ThresholdConfig;
    [pair: string]: Partial<ThresholdConfig>;
  };
  dashboard: {
    port: number;
    update_interval_ms: number;
    auth?: {
      username: string;
      password: string;
    };
  };
  alerts: {
    telegram: {
      enabled: boolean;
      bot_token?: string;
      chat_id?: string;
    };
  };
}

// Pair name normalization: each exchange uses different naming conventions
// Internal canonical form: "BTC-PERP", "ETH-PERP", etc.
export interface PairMapping {
  canonical: string;        // "BTC-PERP"
  exchangeSymbol: string;   // "BTC" (hyperliquid), "BTC-USD-PERP" (paradex), etc.
}

// Dashboard types
export interface DashboardSnapshot {
  prices: PriceData[];
  spreads: SpreadData[];
  statuses: ExchangeStatus[];
  timestamp: number;
}

export interface DashboardUpdate {
  type: 'snapshot' | 'delta';
  prices?: PriceData[];
  spreads?: SpreadData[];
  statuses?: ExchangeStatus[];
  timestamp: number;
}
