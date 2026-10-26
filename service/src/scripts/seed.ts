import { Client } from 'pg';
import { CHANNELS } from '../common/types';
import { config } from '../config';

/**
 * Dev seed: wipes all data and creates N SKUs (default 1,200, like the brief).
 * The first 20 are "hot" sale SKUs with very little stock, to provoke the last-unit race.
 * Channels start in sync (assumption: initial catalogue stock is a one-time bulk upload),
 * so channel_stock starts clean with last_pushed_qty = what each channel should show.
 */
const CATS = ['LIP', 'KJL', 'FND', 'NAI', 'EYE', 'BLU'];

export async function seed(n = Number(process.env.SEED_SKUS ?? 1200), url = config.databaseUrl) {
  const c = new Client({ connectionString: url });
  await c.connect();
  try {
    await c.query('BEGIN');
    await c.query(`TRUNCATE inventory_ledger, order_items, erp_outbox, orders, inbound_events, channel_stock, inventory, skus, poll_cursors RESTART IDENTITY CASCADE`);
    const skus: string[] = [];
    const qty: number[] = [];
    for (let i = 0; i < n; i++) {
      skus.push(`MRS-${CATS[i % CATS.length]}-${String(100 + Math.floor(i / CATS.length)).padStart(3, '0')}`);
      qty.push(i < 20 ? 3 + (i % 13) : 20 + Math.floor(Math.random() * 280));
    }
    await c.query(`INSERT INTO skus (sku, name, safety_buffer) SELECT s, 'Mars ' || s, $2 FROM unnest($1::text[]) s`, [skus, config.minSafetyBuffer]);
    await c.query(`INSERT INTO inventory (sku, on_hand) SELECT * FROM unnest($1::text[], $2::int[])`, [skus, qty]);
    await c.query(
      `INSERT INTO inventory_ledger (sku, delta_on_hand, on_hand_after, reserved_after, reason, idempotency_key, actor, note)
       SELECT s, q, q, 0, 'INITIAL', 'INITIAL:' || s, 'seed', 'opening stock' FROM unnest($1::text[], $2::int[]) AS t(s, q)`,
      [skus, qty],
    );
    await c.query(
      `INSERT INTO channel_stock (sku, channel, last_pushed_qty, last_pushed_at, dirty, dirty_since)
       -- Flipkart starts dirty so its first push applies the polled-channel holdback.
       SELECT i.sku, ch, GREATEST(0, i.on_hand - s.safety_buffer), now(), ch = 'FLIPKART', CASE WHEN ch = 'FLIPKART' THEN now() END
         FROM inventory i JOIN skus s USING (sku), unnest($1::text[]) ch`,
      [CHANNELS],
    );
    await c.query('COMMIT');
    console.log(`seeded ${n} SKUs (20 hot, low-stock: ${skus.slice(0, 3).join(', ')} ...)`);
    return { skus, qty };
  } finally {
    await c.end();
  }
}

if (require.main === module) {
  seed().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
