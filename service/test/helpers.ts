import { Channel, CHANNELS, OrderEvent, OrderEventType } from '../src/common/types';
import { Db } from '../src/db/db.service';
import { IngestService } from '../src/ingest/ingest.service';
import { OrderProcessor } from '../src/inventory/order-processor.service';
import { migrate } from '../src/scripts/migrate';

export async function freshDb(db: Db, stock: Record<string, number>) {
  await migrate();
  await db.query(`TRUNCATE inventory_ledger, order_items, erp_outbox, orders, inbound_events, channel_stock, inventory, skus, poll_cursors RESTART IDENTITY CASCADE`);
  for (const [sku, q] of Object.entries(stock)) {
    await db.query(`INSERT INTO skus (sku, name) VALUES ($1, $1)`, [sku]);
    await db.query(`INSERT INTO inventory (sku, on_hand) VALUES ($1, $2)`, [sku, q]);
    await db.query(
      `INSERT INTO inventory_ledger (sku, delta_on_hand, on_hand_after, reserved_after, reason, idempotency_key, actor) VALUES ($1,$2,$2,0,'INITIAL','INITIAL:'||$1,'test')`,
      [sku, q],
    );
    await db.query(`INSERT INTO channel_stock (sku, channel, last_pushed_qty, dirty, dirty_since) SELECT $1, unnest($2::text[]), $3, false, NULL`, [sku, CHANNELS, Math.max(0, q - 1)]);
  }
}

let seq = 0;
export function evt(channel: Channel, orderId: string, event: OrderEventType, items: [string, number][], at = new Date()): OrderEvent {
  return {
    channel,
    external_order_id: orderId,
    event,
    event_time: at.toISOString(),
    items: items.map(([sku, qty]) => ({ sku, qty, unit_price: 100 })),
    dedupe_key: `test:${++seq}`,
  };
}

/** Drain the event queue with `workers` concurrent processors. */
export async function drain(p: OrderProcessor, workers = 1) {
  await Promise.all(
    Array.from({ length: workers }, async () => {
      while (await p.processNext());
    }),
  );
}

export async function ingestAll(ingest: IngestService, events: OrderEvent[]) {
  for (const e of events) await ingest.ingestEvent(e, e);
}

export async function inv(db: Db, sku: string) {
  const { rows } = await db.query(`SELECT on_hand, reserved FROM inventory WHERE sku = $1`, [sku]);
  return rows[0] as { on_hand: number; reserved: number };
}

export async function orderStatus(db: Db, channel: Channel, id: string) {
  const { rows } = await db.query(`SELECT status FROM orders WHERE channel = $1 AND external_order_id = $2`, [channel, id]);
  return rows[0]?.status;
}
