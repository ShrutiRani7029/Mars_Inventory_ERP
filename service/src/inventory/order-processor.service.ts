import { Injectable, Logger } from '@nestjs/common';
import { config } from '../config';
import { isNonRetryable, NonRetryableError, OrderEvent, OrderItem, OrderStatus } from '../common/types';
import { Db, Tx } from '../db/db.service';
import { markDirty, moveStock } from './stock';

interface LockedOrder {
  id: number;
  status: OrderStatus;
  inserted: boolean;
}

/**
 * Turns normalized order events into stock movements.
 *
 * Each event is claimed from `inbound_events` with FOR UPDATE SKIP LOCKED and applied in
 * the SAME transaction that marks it done, so an event's effects happen exactly once
 * even if the worker crashes half way (the transaction just rolls back and is retried).
 */
@Injectable()
export class OrderProcessor {
  private readonly log = new Logger(OrderProcessor.name);

  constructor(private readonly db: Db) {}

  /** Process one pending event. Returns false when there is nothing to do. */
  async processNext(): Promise<boolean> {
    let claimed: number | null = null;
    try {
      return await this.db.tx(async (c) => {
        const { rows } = await c.query(
          `SELECT id, normalized FROM inbound_events
            WHERE status = 'pending' AND next_attempt_at <= now()
            ORDER BY next_attempt_at, id
            FOR UPDATE SKIP LOCKED LIMIT 1`,
        );
        if (!rows.length) return false;
        claimed = Number(rows[0].id);
        const outcome = await this.apply(c, rows[0].normalized as OrderEvent);
        await c.query(
          `UPDATE inbound_events SET status = 'done', outcome = $2, processed_at = now(), attempts = attempts + 1, last_error = NULL
            WHERE id = $1`,
          [claimed, outcome],
        );
        return true;
      });
    } catch (err) {
      if (claimed === null) throw err;
      await this.recordFailure(claimed, err);
      return true;
    }
  }

  private async recordFailure(id: number, err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    const dead = isNonRetryable(err);
    const { rows } = await this.db.query(
      `UPDATE inbound_events
          SET attempts = attempts + 1,
              last_error = $2,
              status = CASE WHEN $3 OR attempts + 1 >= $4 THEN 'dead' ELSE 'pending' END,
              -- exponential backoff with jitter: 2s, 4s, 8s ... capped at 15 min
              next_attempt_at = now() + make_interval(secs => LEAST(900, 2 * power(2, attempts)) * (0.5 + random() / 2))
        WHERE id = $1 RETURNING status, attempts`,
      [id, msg, dead, config.maxEventAttempts],
    );
    const r = rows[0];
    if (r?.status === 'dead') this.log.error(`event ${id} dead after ${r.attempts} attempt(s): ${msg}`);
    else this.log.warn(`event ${id} failed (attempt ${r?.attempts}), will retry: ${msg}`);
  }

  /** Apply one event inside the caller's transaction. Returns a short outcome for the audit trail. */
  async apply(c: Tx, e: OrderEvent): Promise<string> {
    if (!e) throw new NonRetryableError('event has no normalized payload');
    const order = await this.lockOrInsertOrder(c, e);
    await c.query(`UPDATE orders SET last_event_time = GREATEST(last_event_time, $2), updated_at = now() WHERE id = $1`, [order.id, e.event_time]);
    switch (e.event) {
      case 'ORDER_CREATED':
        return this.onCreated(c, e, order);
      case 'ORDER_CANCELLED':
        return this.onCancelled(c, e, order);
      case 'ORDER_SHIPPED':
        return this.onShipped(c, order);
      case 'ORDER_RETURNED':
        return this.onReturned(c, e, order);
      default:
        throw new NonRetryableError(`unknown event ${(e as OrderEvent).event}`);
    }
  }

  /**
   * Get the order row locked for this transaction, creating it if needed. Every event for
   * the same order serialises here, which is what makes out-of-order handling simple.
   */
  private async lockOrInsertOrder(c: Tx, e: OrderEvent): Promise<LockedOrder> {
    const ins = await c.query(
      `INSERT INTO orders (channel, external_order_id, status, last_event_time, payment_mode, shipping_pincode, total_amount)
       VALUES ($1, $2, 'NEW', $3, $4, $5, $6)
       ON CONFLICT (channel, external_order_id) DO NOTHING
       RETURNING id, status`,
      [e.channel, e.external_order_id, e.event_time, e.payment_mode ?? null, e.shipping_pincode ?? null, e.total_amount ?? null],
    );
    if (ins.rowCount) return { id: Number(ins.rows[0].id), status: ins.rows[0].status, inserted: true };
    const sel = await c.query(`SELECT id, status FROM orders WHERE channel = $1 AND external_order_id = $2 FOR UPDATE`, [e.channel, e.external_order_id]);
    return { id: Number(sel.rows[0].id), status: sel.rows[0].status, inserted: false };
  }

  private async setStatus(c: Tx, id: number, status: OrderStatus) {
    await c.query(`UPDATE orders SET status = $2, updated_at = now() WHERE id = $1`, [id, status]);
  }

  private async insertItems(c: Tx, orderId: number, items: OrderItem[]) {
    for (const it of items) {
      await c.query(
        `INSERT INTO order_items (order_id, sku, qty, unit_price) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
        [orderId, it.sku, it.qty, it.unit_price],
      );
    }
  }

  private async loadItems(c: Tx, orderId: number): Promise<OrderItem[]> {
    const { rows } = await c.query(`SELECT sku, qty, unit_price FROM order_items WHERE order_id = $1 ORDER BY sku`, [orderId]);
    return rows.map((r) => ({ sku: r.sku, qty: r.qty, unit_price: Number(r.unit_price) }));
  }

  private async onCreated(c: Tx, e: OrderEvent, order: LockedOrder): Promise<string> {
    if (!order.inserted) {
      // Either a duplicate delivery, or CREATE arriving after we already saw CANCEL (tombstone).
      return order.status === 'CANCELLED' ? 'ignored:create-after-cancel' : `ignored:duplicate-create(${order.status})`;
    }

    const skus = e.items.map((i) => i.sku);
    const known = await c.query(`SELECT sku FROM skus WHERE sku = ANY($1)`, [skus]);
    if (known.rowCount !== new Set(skus).size) {
      const found = new Set(known.rows.map((r) => r.sku));
      throw new NonRetryableError(`unknown SKU(s): ${skus.filter((s) => !found.has(s)).join(',')}`);
    }

    await this.insertItems(c, order.id, e.items);

    // Reserve every line or none. SKUs are always locked in sorted order -> no deadlocks
    // between multi-item orders that share SKUs.
    const sorted = [...e.items].sort((a, b) => a.sku.localeCompare(b.sku));
    await c.query('SAVEPOINT reserve');
    for (const it of sorted) {
      const ok = await moveStock(c, {
        sku: it.sku,
        deltaOnHand: 0,
        deltaReserved: it.qty,
        reason: 'RESERVE',
        orderId: order.id,
        idempotencyKey: `RESERVE:${order.id}:${it.sku}`,
        actor: e.channel,
        note: e.external_order_id,
      });
      if (!ok) {
        await c.query('ROLLBACK TO SAVEPOINT reserve');
        await this.setStatus(c, order.id, 'OVERSOLD');
        // The channel showed stock we don't have: force a fresh push for these SKUs.
        await markDirty(c, skus);
        this.log.warn(`OVERSOLD ${e.channel} ${e.external_order_id}: no stock for ${it.sku}`);
        return `oversold:${it.sku}`;
      }
    }
    await c.query('RELEASE SAVEPOINT reserve');
    await this.setStatus(c, order.id, 'RESERVED');
    await c.query(`INSERT INTO erp_outbox (order_id, kind) VALUES ($1, 'CREATE') ON CONFLICT DO NOTHING`, [order.id]);
    await markDirty(c, skus);
    return 'reserved';
  }

  private async release(c: Tx, order: LockedOrder, actor: string, reason: 'RELEASE' | 'RETURN') {
    const items = await this.loadItems(c, order.id);
    for (const it of items) {
      const ok = await moveStock(c, {
        sku: it.sku,
        deltaOnHand: reason === 'RETURN' ? it.qty : 0,
        deltaReserved: reason === 'RELEASE' ? -it.qty : 0,
        reason,
        orderId: order.id,
        idempotencyKey: `${reason}:${order.id}:${it.sku}`,
        actor,
      });
      if (!ok) throw new Error(`invariant broken: cannot ${reason} ${it.sku} for order ${order.id}`);
    }
    await markDirty(c, items.map((i) => i.sku));
  }

  private async onCancelled(c: Tx, e: OrderEvent, order: LockedOrder): Promise<string> {
    if (order.inserted) {
      // CANCEL arrived before CREATE: keep a tombstone so the late CREATE is ignored.
      await this.insertItems(c, order.id, e.items);
      await this.setStatus(c, order.id, 'CANCELLED');
      return 'tombstone:cancel-before-create';
    }
    switch (order.status) {
      case 'RESERVED': {
        await this.release(c, order, e.channel, 'RELEASE');
        await this.setStatus(c, order.id, 'CANCELLED');
        // Not yet sent to the ERP (and never attempted) -> just skip it. Otherwise tell the ERP.
        const skipped = await c.query(
          `UPDATE erp_outbox SET status = 'skipped' WHERE order_id = $1 AND kind = 'CREATE' AND status = 'pending' AND attempts = 0 RETURNING id`,
          [order.id],
        );
        if (!skipped.rowCount) {
          await c.query(`INSERT INTO erp_outbox (order_id, kind) VALUES ($1, 'CANCEL') ON CONFLICT DO NOTHING`, [order.id]);
        }
        return 'released';
      }
      case 'OVERSOLD':
        await this.setStatus(c, order.id, 'CANCELLED');
        return 'cancelled-oversold';
      case 'SHIPPED':
        return 'ignored:cancel-after-ship';
      default:
        return `ignored:cancel-when-${order.status}`;
    }
  }

  /** ERP dispatched the goods: they leave the warehouse, so on_hand and reserved both drop. */
  private async onShipped(c: Tx, order: LockedOrder): Promise<string> {
    if (order.status !== 'RESERVED') return `ignored:ship-when-${order.status}`;
    const items = await this.loadItems(c, order.id);
    for (const it of items) {
      const ok = await moveStock(c, {
        sku: it.sku,
        deltaOnHand: -it.qty,
        deltaReserved: -it.qty,
        reason: 'SHIP',
        orderId: order.id,
        idempotencyKey: `SHIP:${order.id}:${it.sku}`,
        actor: 'ERP',
      });
      if (!ok) throw new Error(`invariant broken: cannot ship ${it.sku} for order ${order.id}`);
    }
    await this.setStatus(c, order.id, 'SHIPPED');
    return 'shipped';
  }

  /** Return received at the warehouse: stock goes back on sale. Before dispatch it is just a cancel. */
  private async onReturned(c: Tx, e: OrderEvent, order: LockedOrder): Promise<string> {
    if (order.status === 'SHIPPED') {
      await this.release(c, order, e.channel, 'RETURN');
      await this.setStatus(c, order.id, 'RETURNED');
      return 'returned';
    }
    if (order.status === 'RESERVED') return this.onCancelled(c, { ...e, event: 'ORDER_CANCELLED' }, order);
    if (order.inserted) {
      await this.setStatus(c, order.id, 'CANCELLED');
      return 'tombstone:return-before-create';
    }
    return `ignored:return-when-${order.status}`;
  }
}
