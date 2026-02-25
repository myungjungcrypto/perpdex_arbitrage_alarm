import { loadConfig } from './config.js';
import { logger, createChildLogger } from './logger.js';
import { createAllAdapters } from './exchanges/index.js';
import { PriceStore } from './engine/price-store.js';
import { SpreadCalculator } from './engine/spread-calculator.js';
import { AlertManager } from './engine/alert-manager.js';
import { TelegramNotifier } from './alerts/telegram.js';
import { DashboardServer } from './dashboard/server.js';

const log = createChildLogger('main');

async function main() {
  log.info('Starting PerpDEX Arbitrage Monitor...');

  // 1. Load config
  const config = loadConfig();
  log.info({ pairs: config.pairs.length, exchanges: Object.keys(config.exchanges).length }, 'Config loaded');

  // 2. Create engine components
  const priceStore = new PriceStore();
  const spreadCalc = new SpreadCalculator(priceStore);
  const alertManager = new AlertManager();
  const telegram = new TelegramNotifier(
    config.alerts.telegram.bot_token,
    config.alerts.telegram.chat_id,
    config.alerts.telegram.enabled,
  );

  // 3. Wire spread events to alert manager
  spreadCalc.on('spread', (spread) => alertManager.onSpread(spread));

  // 4. Wire alerts to telegram
  alertManager.on('alert', (alert) => {
    telegram.sendAlert(alert).catch((err) => log.error({ err }, 'Telegram alert failed'));
  });

  // 5. Create exchange adapters
  const adapters = createAllAdapters(config.exchanges);
  log.info({ active: Array.from(adapters.keys()) }, 'Adapters created');

  // 6. Wire adapter price events to price store
  for (const [name, adapter] of adapters) {
    adapter.on('price', (price) => {
      priceStore.update(price);
    });

    adapter.on('error', (err) => {
      log.error({ exchange: name, err: err.message }, 'Adapter error');
    });

    adapter.on('status', (status) => {
      log.debug({ exchange: name, connected: status.connected, pairs: status.subscribedPairs }, 'Status update');
    });
  }

  // 7. Start dashboard
  const dashboard = new DashboardServer(
    priceStore,
    spreadCalc,
    adapters,
    config.dashboard.update_interval_ms,
  );
  await dashboard.start(config.dashboard.port);

  // 8. Connect all adapters (in parallel, with error handling per adapter)
  const connectResults = await Promise.allSettled(
    Array.from(adapters.entries()).map(async ([name, adapter]) => {
      try {
        await adapter.connect(config.pairs);
        log.info({ exchange: name }, 'Connected');
      } catch (err: any) {
        log.error({ exchange: name, err: err.message }, 'Failed to connect (will retry via reconnect)');
      }
    }),
  );

  const connected = connectResults.filter((r) => r.status === 'fulfilled').length;
  log.info({ connected, total: adapters.size }, 'Initial connections complete');

  // 9. Periodic health logging
  setInterval(() => {
    const statuses = Array.from(adapters.values()).map((a) => {
      const s = a.getStatus();
      return `${s.exchange}:${s.connected ? 'OK' : 'DOWN'}`;
    });
    log.info({
      exchanges: statuses.join(', '),
      prices: priceStore.size,
      spreads: spreadCalc.getAllSpreads().length,
    }, 'Health check');
  }, 30000);

  // 10. Graceful shutdown
  const shutdown = async () => {
    log.info('Shutting down...');
    dashboard.stop();
    for (const adapter of adapters.values()) {
      await adapter.disconnect().catch(() => {});
    }
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  log.info({
    dashboard: `http://localhost:${config.dashboard.port}`,
    pairs: config.pairs,
  }, 'PerpDEX Arbitrage Monitor is running');
}

main().catch((err) => {
  logger.fatal({ err }, 'Fatal error');
  process.exit(1);
});
