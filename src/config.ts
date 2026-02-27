import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { parse } from 'yaml';
import { config as loadEnv } from 'dotenv';
import type { AppConfig } from './types.js';

loadEnv();

let _config: AppConfig | null = null;

export function loadConfig(configPath?: string): AppConfig {
  const filePath = configPath ?? resolve(process.cwd(), 'config.yaml');
  const raw = readFileSync(filePath, 'utf-8');
  const parsed = parse(raw) as AppConfig;

  // Override with env vars
  if (process.env.TELEGRAM_BOT_TOKEN) {
    parsed.alerts.telegram.bot_token = process.env.TELEGRAM_BOT_TOKEN;
  }
  if (process.env.TELEGRAM_CHAT_ID) {
    parsed.alerts.telegram.chat_id = process.env.TELEGRAM_CHAT_ID;
  }
  if (process.env.DASHBOARD_PORT) {
    parsed.dashboard.port = parseInt(process.env.DASHBOARD_PORT, 10);
  }
  if (process.env.DASHBOARD_USER && process.env.DASHBOARD_PASS) {
    parsed.dashboard.auth = {
      username: process.env.DASHBOARD_USER,
      password: process.env.DASHBOARD_PASS,
    };
  }

  // Ensure defaults
  parsed.thresholds.default ??= { min_spread_pct: 0.3, sustained_ms: 2000, cooldown_ms: 30000 };
  parsed.dashboard ??= { port: 3000, update_interval_ms: 200 };

  _config = parsed;
  return parsed;
}

export function getConfig(): AppConfig {
  if (!_config) throw new Error('Config not loaded. Call loadConfig() first.');
  return _config;
}

export function getThreshold(pair: string, isSlow = false) {
  const cfg = getConfig();
  if (isSlow && cfg.slow_exchanges?.threshold) {
    return { ...cfg.thresholds.default, ...cfg.slow_exchanges.threshold };
  }
  const pairOverride = cfg.thresholds[pair];
  const defaults = cfg.thresholds.default;
  return { ...defaults, ...pairOverride } as Required<typeof defaults>;
}

export function getSlowExchangeConfig() {
  const cfg = getConfig();
  return cfg.slow_exchanges ?? { exchanges: [], max_stale_ms: 120000, min_fast_sources: 2, threshold: cfg.thresholds.default as Required<typeof cfg.thresholds.default> };
}

export function getSlowExchangeSet(): Set<string> {
  return new Set(getSlowExchangeConfig().exchanges);
}
