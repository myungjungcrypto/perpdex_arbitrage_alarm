import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { resolve, extname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { WebSocketServer, WebSocket } from 'ws';
import type { PriceStore } from '../engine/price-store.js';
import type { SpreadCalculator } from '../engine/spread-calculator.js';
import type { BaseExchangeAdapter } from '../exchanges/base.js';
import type { DashboardUpdate, ExchangeStatus, PriceData, SpreadData } from '../types.js';
import { createChildLogger } from '../logger.js';

const log = createChildLogger('dashboard');
const __dirname = dirname(fileURLToPath(import.meta.url));

const MIME_TYPES: Record<string, string> = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
};

export class DashboardServer {
  private wss: WebSocketServer | null = null;
  private clients = new Set<WebSocket>();
  private updateTimer: ReturnType<typeof setInterval> | null = null;
  private priceStore: PriceStore;
  private spreadCalc: SpreadCalculator;
  private adapters: Map<string, BaseExchangeAdapter>;
  private updateIntervalMs: number;
  private auth?: { username: string; password: string };

  // Batch buffer: collect changes between dashboard pushes
  private pendingPrices = new Map<string, PriceData>(); // "pair:exchange" -> latest
  private pendingSpreads = new Map<string, SpreadData>(); // "pair:exA:exB" -> latest

  constructor(
    priceStore: PriceStore,
    spreadCalc: SpreadCalculator,
    adapters: Map<string, BaseExchangeAdapter>,
    updateIntervalMs = 200,
    auth?: { username: string; password: string },
  ) {
    this.priceStore = priceStore;
    this.spreadCalc = spreadCalc;
    this.adapters = adapters;
    this.updateIntervalMs = updateIntervalMs;
    this.auth = auth;

    if (this.auth) {
      log.info('Dashboard Basic Auth enabled');
    }

    // Buffer price and spread updates
    this.priceStore.on('update', (p) => {
      this.pendingPrices.set(`${p.pair}:${p.exchange}`, p);
    });
    this.spreadCalc.on('spread', (s) => {
      const [exA, exB] = [s.longExchange, s.shortExchange].sort();
      this.pendingSpreads.set(`${s.pair}:${exA}:${exB}`, s);
    });
  }

  start(port: number): Promise<void> {
    return new Promise((resolve_) => {
      const server = createServer((req, res) => {
        this.handleHttp(req, res);
      });

      this.wss = new WebSocketServer({
        server,
        verifyClient: (info, done) => {
          if (!this.auth) return done(true);
          const ok = this.checkAuth(info.req);
          if (!ok) {
            done(false, 401, 'Unauthorized');
          } else {
            done(true);
          }
        },
      });

      this.wss.on('connection', (ws) => {
        this.clients.add(ws);
        log.info({ clients: this.clients.size }, 'Dashboard client connected');

        // Send full snapshot on connect
        this.sendSnapshot(ws);

        ws.on('close', () => {
          this.clients.delete(ws);
          log.debug({ clients: this.clients.size }, 'Dashboard client disconnected');
        });

        ws.on('error', () => {
          this.clients.delete(ws);
        });
      });

      // Periodic batched updates
      this.updateTimer = setInterval(() => this.pushDelta(), this.updateIntervalMs);

      server.listen(port, () => {
        log.info({ port }, 'Dashboard running');
        resolve_();
      });
    });
  }

  private checkAuth(req: import('node:http').IncomingMessage): boolean {
    if (!this.auth) return true;
    const header = req.headers.authorization ?? '';
    if (!header.startsWith('Basic ')) return false;
    const decoded = Buffer.from(header.slice(6), 'base64').toString();
    const [user, pass] = decoded.split(':');
    return user === this.auth.username && pass === this.auth.password;
  }

  private sendUnauthorized(res: import('node:http').ServerResponse) {
    res.writeHead(401, {
      'WWW-Authenticate': 'Basic realm="PerpDEX Dashboard"',
      'Content-Type': 'text/plain',
    });
    res.end('Unauthorized');
  }

  private handleHttp(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) {
    if (!this.checkAuth(req)) {
      this.sendUnauthorized(res);
      return;
    }

    // API endpoints
    if (req.url === '/api/snapshot') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(this.buildSnapshot()));
      return;
    }

    // Static file serving
    let filePath = req.url === '/' ? '/index.html' : req.url!;

    // Security: prevent directory traversal
    filePath = filePath.replace(/\.\./g, '');

    const fullPath = resolve(__dirname, 'public', filePath.slice(1));
    const ext = extname(fullPath);
    const mime = MIME_TYPES[ext] ?? 'application/octet-stream';

    try {
      const content = readFileSync(fullPath);
      res.writeHead(200, { 'Content-Type': mime });
      res.end(content);
    } catch {
      res.writeHead(404);
      res.end('Not found');
    }
  }

  private buildSnapshot() {
    return {
      prices: this.priceStore.getAllPrices(),
      spreads: this.spreadCalc.getAllSpreads(),
      statuses: this.getStatuses(),
      timestamp: Date.now(),
    };
  }

  private sendSnapshot(ws: WebSocket) {
    const msg: DashboardUpdate = {
      type: 'snapshot',
      ...this.buildSnapshot(),
    };
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }

  private pushDelta() {
    if (this.clients.size === 0) {
      // No clients — just clear buffers
      this.pendingPrices.clear();
      this.pendingSpreads.clear();
      return;
    }

    if (this.pendingPrices.size === 0 && this.pendingSpreads.size === 0) return;

    const msg: DashboardUpdate = {
      type: 'delta',
      timestamp: Date.now(),
    };

    if (this.pendingPrices.size > 0) {
      msg.prices = Array.from(this.pendingPrices.values());
      this.pendingPrices.clear();
    }

    if (this.pendingSpreads.size > 0) {
      msg.spreads = Array.from(this.pendingSpreads.values());
      this.pendingSpreads.clear();
    }

    // Always include statuses
    msg.statuses = this.getStatuses();

    const payload = JSON.stringify(msg);
    for (const client of this.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    }
  }

  private getStatuses(): ExchangeStatus[] {
    const statuses: ExchangeStatus[] = [];
    for (const adapter of this.adapters.values()) {
      statuses.push(adapter.getStatus());
    }
    return statuses;
  }

  stop() {
    if (this.updateTimer) { clearInterval(this.updateTimer); this.updateTimer = null; }
    for (const client of this.clients) client.close();
    this.clients.clear();
    this.wss?.close();
  }
}
