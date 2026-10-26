import { PushOutcome, StockClients, StockLine } from '../src/channels/channel-clients';
import { Channel, sleep } from '../src/common/types';
import { Db } from '../src/db/db.service';
import { CircuitBreaker } from '../src/erp/circuit-breaker';
import { ErpClient, ErpSalesOrder } from '../src/erp/erp-client';
import { ErpWorker } from '../src/erp/erp-worker.service';
import { IngestService } from '../src/ingest/ingest.service';
import { OrderProcessor } from '../src/inventory/order-processor.service';
import { shouldPush, StockPusher } from '../src/push/stock-pusher.service';
import { drain, evt, freshDb, ingestAll } from './helpers';

const db = new Db();
const ingest = new IngestService(db);
const proc = new OrderProcessor(db);
afterAll(() => db.pool.end());

class FakeErp implements ErpClient {
  docs = new Map<string, string>();
  creates = 0;
  down = false;
  commitThenTimeout = false;
  async findByExternalDoc(ext: string) {
    if (this.down) throw new Error('503 maintenance');
    return this.docs.get(ext) ?? null;
  }
  async create(o: ErpSalesOrder) {
    if (this.down) throw new Error('503 maintenance');
    this.creates++;
    this.docs.set(o.externalDocumentNumber, `SO-${this.creates}`);
    if (this.commitThenTimeout) {
      this.commitThenTimeout = false;
      const e = new Error('timeout');
      e.name = 'TimeoutError';
      throw e;
    }
    return `SO-${this.creates}`;
  }
  async cancel() {}
}

const due = () => db.query(`UPDATE erp_outbox SET next_attempt_at = now() WHERE status = 'pending'`);

describe('ERP delivery (exactly once)', () => {
  beforeEach(async () => {
    await freshDb(db, { 'MRS-LIP-104': 100 });
    await ingestAll(ingest, [evt('NYKAA', 'E1', 'ORDER_CREATED', [['MRS-LIP-104', 1]]), evt('WEBSITE', 'E2', 'ORDER_CREATED', [['MRS-LIP-104', 1]])]);
    await drain(proc);
  });

  it('ERP committed but our call timed out -> retry finds the doc, no duplicate', async () => {
    const erp = new FakeErp();
    erp.commitThenTimeout = true;
    const w = new ErpWorker(db, erp);
    await w.processNext(); // creates SO-1, then "times out"
    await due();
    while (await w.processNext());
    expect(erp.creates).toBe(2); // E1 once, E2 once
    expect(erp.docs.size).toBe(2);
    const { rows } = await db.query(`SELECT status FROM erp_outbox ORDER BY id`);
    expect(rows.every((r) => r.status === 'done')).toBe(true);
  });

  it('Sunday downtime: breaker opens, nothing is lost or dead, everything is delivered after recovery', async () => {
    for (let i = 0; i < 20; i++) await ingestAll(ingest, [evt('BLINKIT', `S${i}`, 'ORDER_CREATED', [['MRS-LIP-104', 1]])]);
    await drain(proc);
    const erp = new FakeErp();
    erp.down = true;
    const w = new ErpWorker(db, erp);

    for (let i = 0; i < 50; i++) {
      await w.processNext();
      await due();
    }
    expect(w.breaker.state).toBe('open');
    const during = await db.query(`SELECT count(*) FILTER (WHERE status = 'dead')::int dead, max(attempts)::int max_attempts FROM erp_outbox`);
    expect(during.rows[0].dead).toBe(0);
    expect(during.rows[0].max_attempts).toBeLessThanOrEqual(3); // the breaker stops the hammering

    erp.down = false;
    await sleep(350); // breaker half-opens
    await due();
    while (await w.processNext());
    expect(erp.docs.size).toBe(22);
    const { rows } = await db.query(`SELECT count(*)::int n FROM erp_outbox WHERE status = 'done'`);
    expect(rows[0].n).toBe(22);
  });
});

describe('stock push', () => {
  class FakeClients {
    sent: Record<string, StockLine[][]> = {};
    failNext = false;
    get(channel: Channel) {
      return {
        channel,
        push: async (lines: StockLine[]): Promise<PushOutcome[]> => {
          (this.sent[channel] ??= []).push(lines);
          const fail = this.failNext;
          this.failNext = false;
          return lines.map((l) => ({ sku: l.sku, ok: !fail, error: fail ? 'timeout' : undefined, retryable: true }));
        },
      };
    }
  }

  beforeEach(() => freshDb(db, { 'MRS-LIP-104': 20, 'MRS-KJL-011': 200 }));

  it('coalesces many orders on one SKU into a single absolute-quantity push', async () => {
    for (let i = 0; i < 7; i++) await ingestAll(ingest, [evt('NYKAA', `P${i}`, 'ORDER_CREATED', [['MRS-LIP-104', 1]])]);
    await drain(proc, 4);
    const fake = new FakeClients();
    const pusher = new StockPusher(db, fake as unknown as StockClients);
    await pusher.pushOnce('FLIPKART');
    // available - safety buffer - Flipkart holdback (polled channel)
    expect(fake.sent.FLIPKART).toEqual([[{ sku: 'MRS-LIP-104', qty: 20 - 7 - 1 - 3 }]]);
    expect(await pusher.pushOnce('FLIPKART')).toBe(0); // nothing left dirty
  });

  it('failed push stays dirty with backoff and is retried', async () => {
    await ingestAll(ingest, [evt('BLINKIT', 'B1', 'ORDER_CREATED', [['MRS-LIP-104', 2]])]);
    await drain(proc);
    const fake = new FakeClients();
    fake.failNext = true;
    const pusher = new StockPusher(db, fake as unknown as StockClients);
    await pusher.pushOnce('BLINKIT');
    const r1 = await db.query(`SELECT dirty, attempts, last_error FROM channel_stock WHERE sku = 'MRS-LIP-104' AND channel = 'BLINKIT'`);
    expect(r1.rows[0]).toEqual({ dirty: true, attempts: 1, last_error: 'timeout' });
    await db.query(`UPDATE channel_stock SET next_attempt_at = now()`);
    await pusher.pushOnce('BLINKIT');
    const r2 = await db.query(`SELECT dirty, attempts, last_pushed_qty FROM channel_stock WHERE sku = 'MRS-LIP-104' AND channel = 'BLINKIT'`);
    expect(r2.rows[0]).toEqual({ dirty: false, attempts: 0, last_pushed_qty: 17 });
  });

  it('lowest-stock SKUs are pushed first', async () => {
    await db.query(`UPDATE channel_stock SET dirty = true, dirty_since = now(), last_pushed_qty = NULL`);
    const fake = new FakeClients();
    await new StockPusher(db, fake as unknown as StockClients).pushOnce('NYKAA', 1);
    expect(fake.sent.NYKAA[0][0].sku).toBe('MRS-LIP-104');
  });
});

describe('pure logic', () => {
  it('shouldPush: low stock always, unchanged never, Nykaa suppresses tiny changes on healthy SKUs', () => {
    expect(shouldPush('NYKAA', null, 100)).toBe(true);
    expect(shouldPush('NYKAA', 5, 5)).toBe(false);
    expect(shouldPush('NYKAA', 4, 3)).toBe(true);
    expect(shouldPush('NYKAA', 200, 195)).toBe(false);
    expect(shouldPush('FLIPKART', 200, 195)).toBe(true);
    expect(shouldPush('NYKAA', 200, 150)).toBe(true);
  });

  it('circuit breaker opens, half-opens with a single probe, closes on success', () => {
    let now = 0;
    const b = new CircuitBreaker(2, 1000, () => now);
    b.failure();
    b.failure();
    expect(b.allow()).toBe(false);
    now = 1000;
    expect(b.allow()).toBe(true);
    expect(b.allow()).toBe(false); // only one probe
    b.success();
    expect(b.state).toBe('closed');
  });
});
