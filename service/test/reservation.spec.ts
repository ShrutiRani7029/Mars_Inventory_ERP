import { Db } from '../src/db/db.service';
import { IngestService } from '../src/ingest/ingest.service';
import { OrderProcessor } from '../src/inventory/order-processor.service';
import { MetricsService } from '../src/ops/metrics.service';
import { drain, evt, freshDb, ingestAll, inv, orderStatus } from './helpers';

const db = new Db();
const ingest = new IngestService(db);
const proc = new OrderProcessor(db);
const metrics = new MetricsService(db);

afterAll(() => db.pool.end());

describe('inventory consistency', () => {
  beforeEach(() => freshDb(db, { 'MRS-LIP-104': 1, 'MRS-KJL-011': 5, 'MRS-FND-200': 50 }));

  it('last unit: 12 orders from all channels at once -> exactly one wins, the rest are OVERSOLD', async () => {
    const chans = ['WEBSITE', 'NYKAA', 'FLIPKART', 'BLINKIT'] as const;
    await ingestAll(ingest, Array.from({ length: 12 }, (_, i) => evt(chans[i % 4], `O-${i}`, 'ORDER_CREATED', [['MRS-LIP-104', 1]])));

    await drain(proc, 12); // 12 workers racing on the same row

    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 1, reserved: 1 });
    const { rows } = await db.query(`SELECT status, count(*)::int n FROM orders GROUP BY status ORDER BY status`);
    expect(rows).toEqual([{ status: 'OVERSOLD', n: 11 }, { status: 'RESERVED', n: 1 }]);
    const ledger = await db.query(`SELECT count(*)::int n FROM inventory_ledger WHERE reason = 'RESERVE'`);
    expect(ledger.rows[0].n).toBe(1);
    expect((await metrics.consistency()).ok).toBe(true);
  });

  it('multi-item order is all-or-nothing (no partial reservation)', async () => {
    await ingestAll(ingest, [evt('NYKAA', 'A', 'ORDER_CREATED', [['MRS-KJL-011', 2], ['MRS-LIP-104', 2]])]);
    await drain(proc);
    expect(await orderStatus(db, 'NYKAA', 'A')).toBe('OVERSOLD');
    expect(await inv(db, 'MRS-KJL-011')).toEqual({ on_hand: 5, reserved: 0 });
  });

  it('heavy concurrent mixed load never over-reserves and the ledger always replays to the truth', async () => {
    const events = Array.from({ length: 200 }, (_, i) => {
      const items: [string, number][] = [['MRS-FND-200', 1 + (i % 2)]];
      if (i % 7 === 0) items.push(['MRS-KJL-011', 1]);
      return evt((['WEBSITE', 'NYKAA', 'FLIPKART', 'BLINKIT'] as const)[i % 4], `M-${i}`, 'ORDER_CREATED', items);
    });
    await ingestAll(ingest, events);
    await drain(proc, 16);
    const fnd = await inv(db, 'MRS-FND-200');
    expect(fnd.reserved).toBeLessThanOrEqual(50);
    expect(fnd.reserved).toBeGreaterThanOrEqual(49); // stock gets used up, not stranded
    expect((await inv(db, 'MRS-KJL-011')).reserved).toBeLessThanOrEqual(5);
    expect((await metrics.consistency()).ok).toBe(true);
  });
});

describe('duplicates and ordering', () => {
  beforeEach(() => freshDb(db, { 'MRS-LIP-104': 10 }));

  const nykaa = { event_type: 'ORDER_CREATED', event_ts: '2026-10-23T11:04:52+05:30', order: { order_no: 'NYK-1', items: [{ sku_code: 'MRS-LIP-104', qty: 2, price: 349 }] } };

  it('same Nykaa webhook twice -> stored once, reserved once', async () => {
    expect((await ingest.ingestRaw('NYKAA', nykaa)).status).toBe('accepted');
    expect((await ingest.ingestRaw('NYKAA', nykaa)).status).toBe('duplicate');
    await drain(proc);
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 10, reserved: 2 });
  });

  it('a re-sent CREATE with a different timestamp (passes layer 1) still cannot reserve twice', async () => {
    await ingest.ingestRaw('NYKAA', nykaa);
    await ingest.ingestRaw('NYKAA', { ...nykaa, event_ts: '2026-10-23T11:05:30+05:30' });
    await drain(proc, 2);
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 10, reserved: 2 });
    const { rows } = await db.query(`SELECT outcome FROM inbound_events ORDER BY id`);
    expect(rows.map((r) => r.outcome).sort()).toEqual(['ignored:duplicate-create(RESERVED)', 'reserved']);
  });

  it('CANCEL before CREATE -> tombstone, late CREATE ignored, no stock reserved, nothing sent to ERP', async () => {
    const t = new Date();
    await ingestAll(ingest, [evt('NYKAA', 'X', 'ORDER_CANCELLED', [['MRS-LIP-104', 1]], new Date(t.getTime() + 1000))]);
    await drain(proc);
    await ingestAll(ingest, [evt('NYKAA', 'X', 'ORDER_CREATED', [['MRS-LIP-104', 1]], t)]);
    await drain(proc);
    expect(await orderStatus(db, 'NYKAA', 'X')).toBe('CANCELLED');
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 10, reserved: 0 });
    expect((await db.query(`SELECT count(*)::int n FROM erp_outbox`)).rows[0].n).toBe(0);
  });

  it('cancel releases stock exactly once and skips the unsent ERP job', async () => {
    await ingestAll(ingest, [evt('BLINKIT', 'C', 'ORDER_CREATED', [['MRS-LIP-104', 3]])]);
    await drain(proc);
    await ingestAll(ingest, [evt('BLINKIT', 'C', 'ORDER_CANCELLED', []), evt('BLINKIT', 'C', 'ORDER_CANCELLED', [])]);
    await drain(proc, 2);
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 10, reserved: 0 });
    const { rows } = await db.query(`SELECT kind, status FROM erp_outbox`);
    expect(rows).toEqual([{ kind: 'CREATE', status: 'skipped' }]);
  });

  it('cancel after the order reached the ERP queues an ERP cancellation', async () => {
    await ingestAll(ingest, [evt('WEBSITE', 'W', 'ORDER_CREATED', [['MRS-LIP-104', 1]])]);
    await drain(proc);
    await db.query(`UPDATE erp_outbox SET status = 'done', attempts = 1, erp_doc_no = 'SO-1'`);
    await ingestAll(ingest, [evt('WEBSITE', 'W', 'ORDER_CANCELLED', [])]);
    await drain(proc);
    const { rows } = await db.query(`SELECT kind, status FROM erp_outbox ORDER BY id`);
    expect(rows).toEqual([{ kind: 'CREATE', status: 'done' }, { kind: 'CANCEL', status: 'pending' }]);
  });

  it('ship then return puts stock back on sale with a full audit trail', async () => {
    await ingestAll(ingest, [evt('FLIPKART', 'F', 'ORDER_CREATED', [['MRS-LIP-104', 2]])]);
    await drain(proc);
    await ingestAll(ingest, [evt('FLIPKART', 'F', 'ORDER_SHIPPED', [])]);
    await drain(proc);
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 8, reserved: 0 });
    await ingestAll(ingest, [evt('FLIPKART', 'F', 'ORDER_RETURNED', [])]);
    await drain(proc);
    expect(await inv(db, 'MRS-LIP-104')).toEqual({ on_hand: 10, reserved: 0 });
    const { rows } = await db.query(`SELECT reason FROM inventory_ledger WHERE sku = 'MRS-LIP-104' ORDER BY id`);
    expect(rows.map((r) => r.reason)).toEqual(['INITIAL', 'RESERVE', 'SHIP', 'RETURN']);
    expect((await metrics.consistency()).ok).toBe(true);
  });

  it('unknown SKU is a poison message -> dead immediately, visible to ops, no stock moved', async () => {
    await ingestAll(ingest, [evt('NYKAA', 'U', 'ORDER_CREATED', [['NOPE-1', 1]])]);
    await drain(proc);
    const { rows } = await db.query(`SELECT status, attempts, last_error FROM inbound_events`);
    expect(rows[0].status).toBe('dead');
    expect(rows[0].attempts).toBe(1);
    expect(rows[0].last_error).toMatch(/unknown SKU/);
    expect(await orderStatus(db, 'NYKAA', 'U')).toBeUndefined();
  });

  it('unparseable payload is stored as dead instead of being dropped', async () => {
    expect((await ingest.ingestRaw('BLINKIT', { type: 'ORDER_PLACED' })).status).toBe('rejected');
    const { rows } = await db.query(`SELECT status, last_error FROM inbound_events`);
    expect(rows[0]).toMatchObject({ status: 'dead' });
  });
});
