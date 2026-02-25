import type { BaseExchangeAdapter } from './base.js';
import type { ExchangeConfig } from '../types.js';
import { createChildLogger } from '../logger.js';

const log = createChildLogger('registry');

type AdapterFactory = (config: ExchangeConfig) => BaseExchangeAdapter;

const factories = new Map<string, AdapterFactory>();

export function registerAdapter(name: string, factory: AdapterFactory) {
  factories.set(name, factory);
  log.debug({ name }, 'Registered exchange adapter');
}

export function createAdapter(name: string, config: ExchangeConfig): BaseExchangeAdapter | null {
  const factory = factories.get(name);
  if (!factory) {
    log.warn({ name }, 'No adapter registered for exchange');
    return null;
  }
  return factory(config);
}

export function createAllAdapters(
  exchangeConfigs: Record<string, ExchangeConfig>,
): Map<string, BaseExchangeAdapter> {
  const adapters = new Map<string, BaseExchangeAdapter>();
  for (const [name, config] of Object.entries(exchangeConfigs)) {
    if (!config.enabled) {
      log.info({ name }, 'Exchange disabled, skipping');
      continue;
    }
    const adapter = createAdapter(name, config);
    if (adapter) {
      adapters.set(name, adapter);
    }
  }
  return adapters;
}
