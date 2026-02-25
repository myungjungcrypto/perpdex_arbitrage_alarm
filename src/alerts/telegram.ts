import type { SpreadAlert } from '../types.js';
import { createChildLogger } from '../logger.js';

const log = createChildLogger('telegram');

export class TelegramNotifier {
  private botToken: string;
  private chatId: string;
  private enabled: boolean;

  constructor(botToken?: string, chatId?: string, enabled = false) {
    this.botToken = botToken ?? '';
    this.chatId = chatId ?? '';
    this.enabled = enabled && !!this.botToken && !!this.chatId;

    if (enabled && !this.enabled) {
      log.warn('Telegram enabled in config but missing bot_token or chat_id');
    }
  }

  async sendAlert(alert: SpreadAlert): Promise<void> {
    if (!this.enabled) return;

    const message = this.formatAlert(alert);

    try {
      const url = `https://api.telegram.org/bot${this.botToken}/sendMessage`;
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: this.chatId,
          text: message,
          parse_mode: 'Markdown',
        }),
      });

      if (!res.ok) {
        const body = await res.text();
        log.error({ status: res.status, body }, 'Failed to send Telegram message');
      }
    } catch (err) {
      log.error({ err }, 'Telegram send error');
    }
  }

  private formatAlert(alert: SpreadAlert): string {
    const dir = alert.spreadAbs > 0
      ? `Buy ${alert.longExchange} → Sell ${alert.shortExchange}`
      : `Negative spread`;
    const time = new Date(alert.timestamp).toISOString().replace('T', ' ').slice(0, 19);

    return [
      `🚨 *SPREAD ALERT: ${alert.pair}*`,
      '',
      `${alert.longExchange}: $${alert.longAsk.toFixed(2)} (ask)`,
      `${alert.shortExchange}: $${alert.shortBid.toFixed(2)} (bid)`,
      '',
      `Spread: $${alert.spreadAbs.toFixed(2)} (${alert.spreadPct.toFixed(4)}%)`,
      `Direction: ${dir}`,
      `Duration: ${(alert.durationMs / 1000).toFixed(1)}s`,
      `Time: ${time} UTC`,
    ].join('\n');
  }
}
